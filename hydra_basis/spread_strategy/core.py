from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from hydra_basis.risk_management.persistence import atomic_write_json

BPS = Decimal("10000")
ZERO = Decimal("0")
VENUES = {"aster", "hyperliquid", "variational"}


def number(value) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("non-finite financial value")
    return result


@dataclass(frozen=True)
class Config:
    symbol: str
    short_venue: str
    long_venue: str
    maker_venue: str
    total_quantity: Decimal
    clip_quantity: Decimal
    entry_bps: Decimal
    take_profit_bps: Decimal
    # Rates are fractions, e.g. 0.0005 means 0.05%. No venue fee assumptions.
    fees: dict[str, dict[str, Decimal]]
    min_profit_bps: Decimal = Decimal("5")
    slippage_buffer_bps: Decimal = Decimal("5")
    funding_budget_bps: Decimal = Decimal("5")
    leverage: int = 1
    poll_seconds: float = 2.0
    max_quote_age_seconds: float = 5.0
    max_request_seconds: float = 3.0
    maker_timeout_seconds: float = 5.0
    max_hold_seconds: float = 3600.0
    stop_loss_usd: Decimal = Decimal("25")
    execution_method: str = "maker_taker"

    def __post_init__(self):
        if not self.symbol or self.symbol != self.symbol.strip().upper():
            raise ValueError("symbol must be a canonical uppercase symbol")
        if not {self.short_venue, self.long_venue} <= VENUES:
            raise ValueError("supported venues: aster, hyperliquid, variational")
        if self.short_venue == self.long_venue or self.maker_venue not in self.venues:
            raise ValueError("choose different venues and a maker from the pair")
        if self.execution_method not in {"maker_taker", "taker_taker"}:
            raise ValueError("execution_method must be maker_taker or taker_taker")
        for name in ("total_quantity", "clip_quantity", "stop_loss_usd"):
            if number(getattr(self, name)) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.clip_quantity > self.total_quantity:
            raise ValueError("clip_quantity exceeds total_quantity")
        for name in ("min_profit_bps", "slippage_buffer_bps", "funding_budget_bps"):
            if number(getattr(self, name)) < 0:
                raise ValueError(f"{name} must not be negative")
        if number(self.entry_bps) <= number(self.take_profit_bps):
            raise ValueError("take_profit_bps must be below entry_bps")
        if not isinstance(self.leverage, int) or isinstance(self.leverage, bool) or self.leverage < 1:
            raise ValueError("leverage must be a positive integer")
        for name in ("poll_seconds", "max_quote_age_seconds", "max_request_seconds",
                     "maker_timeout_seconds", "max_hold_seconds"):
            if number(getattr(self, name)) <= 0:
                raise ValueError(f"{name} must be positive")
        for venue in self.venues:
            for role in ("maker", "taker"):
                rate = number(self.fees[venue][role])
                if not ZERO <= rate < 1:
                    raise ValueError("fee rates must be fractions between 0 and 1")

    @property
    def venues(self):
        return (self.short_venue, self.long_venue)

    @property
    def taker_venue(self):
        return self.long_venue if self.maker_venue == self.short_venue else self.short_venue

    def fee_rate(self, venue: str) -> Decimal:
        # Existing GTC adapters do not guarantee post-only: budget the worse role.
        return max(number(self.fees[venue]["maker"]), number(self.fees[venue]["taker"]))

    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(asdict(self), default=str, sort_keys=True).encode()).hexdigest()

    @classmethod
    def load(cls, path: Path):
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        for key in ("total_quantity", "clip_quantity", "entry_bps", "take_profit_bps",
                    "min_profit_bps", "slippage_buffer_bps", "funding_budget_bps", "stop_loss_usd"):
            if key in payload:
                payload[key] = number(payload[key])
        payload["fees"] = {v: {role: number(rate) for role, rate in rates.items()}
                           for v, rates in payload["fees"].items()}
        return cls(**payload)


@dataclass(frozen=True)
class Quote:
    bid: Decimal
    ask: Decimal
    received_ms: int
    source_ms: int | None = None
    request_seconds: float = 0.0

    def validate(self, *, now_ms: int, config: Config):
        if not ZERO < number(self.bid) <= number(self.ask):
            raise ValueError("invalid bid/ask")
        limit_ms = config.max_quote_age_seconds * 1000
        if not 0 <= now_ms - self.received_ms <= limit_ms:
            raise ValueError("stale or future receipt timestamp")
        if self.source_ms is not None and not -2000 <= now_ms - self.source_ms <= limit_ms:
            raise ValueError("stale or future exchange timestamp")
        if not 0 <= self.request_seconds <= config.max_request_seconds:
            raise ValueError("quote request too slow")


@dataclass
class State:
    fingerprint: str
    mode: str
    strategy_id: str
    phase: str = "WAITING"
    quantity: str = "0"
    entered_quantity: str = "0"
    short_average: str = "0"
    long_average: str = "0"
    entry_fees_remaining: str = "0"
    realized_gross_usd: str = "0"
    estimated_trading_fees_usd: str = "0"
    first_entry_ms: int | None = None
    exit_reason: str | None = None
    pending: dict | None = None
    error: str | None = None
    fills: list[dict] = field(default_factory=list)


class StateStore:
    def __init__(self, path: Path):
        self.path = path

    def load(self, config: Config, *, live: bool) -> State:
        mode = "live" if live else "paper"
        if not self.path.exists():
            import uuid
            return State(config.fingerprint(), mode, f"spread-{uuid.uuid4().hex}")
        # Corrupt state must never be silently replaced with an empty strategy.
        state = State(**json.loads(self.path.read_text(encoding="utf-8")))
        if state.fingerprint != config.fingerprint() or state.mode != mode:
            raise ValueError("state config/mode mismatch; use a separate state file")
        if state.pending is not None or state.phase == "PAUSED":
            raise RuntimeError("strategy requires manual order/position reconciliation; automatic replay refused")
        if state.phase not in {"WAITING", "HOLDING", "EXITING", "DONE"}:
            raise ValueError("invalid strategy phase")
        for name in ("quantity", "entered_quantity", "short_average", "long_average",
                     "entry_fees_remaining", "estimated_trading_fees_usd"):
            if number(getattr(state, name)) < 0:
                raise ValueError("invalid negative state field")
        if number(state.quantity) > number(state.entered_quantity) or number(state.entered_quantity) > config.total_quantity:
            raise ValueError("state quantity exceeds strategy capacity")
        if number(state.quantity) > 0 and (not state.first_entry_ms or
                number(state.short_average) <= 0 or number(state.long_average) <= 0):
            raise ValueError("held quantity missing entry accounting")
        if state.phase in {"HOLDING", "EXITING"} and number(state.quantity) <= 0:
            raise ValueError("active state requires positive held quantity")
        if state.phase in {"WAITING", "DONE"} and number(state.quantity) != 0:
            raise ValueError("flat state cannot own a position")
        if state.phase == "EXITING" and state.exit_reason not in {"take_profit", "max_hold", "stop_loss"}:
            raise ValueError("exit state requires a valid latched reason")
        return state

    def save(self, state: State):
        atomic_write_json(self.path, asdict(state))


class Broker(Protocol):
    async def reconcile(self, state: State) -> None: ...
    async def quotes(self, quantity: Decimal) -> dict[str, Quote]: ...
    async def execute(self, intent: str, quantity: Decimal, state: State,
                      force: bool = False) -> dict: ...
    async def sync_registry(self, state: State) -> None: ...


def spread_bps(short_price: Decimal, long_price: Decimal) -> Decimal:
    return (number(short_price) - number(long_price)) / number(long_price) * BPS


def executable_prices(config: Config, quotes: dict[str, Quote], intent: str,
                      *, maker: bool = False) -> tuple[Decimal, Decimal]:
    short, long = quotes[config.short_venue], quotes[config.long_venue]
    if intent == "entry":
        return (short.ask if maker and config.maker_venue == config.short_venue else short.bid,
                long.bid if maker and config.maker_venue == config.long_venue else long.ask)
    return (short.bid if maker and config.maker_venue == config.short_venue else short.ask,
            long.ask if maker and config.maker_venue == config.long_venue else long.bid)


def entry_net_bps(config: Config, short: Decimal, long: Decimal) -> Decimal:
    # Conservative target estimate: reserve four fees, including price scaling.
    exit_short = long * (1 + max(ZERO, config.take_profit_bps) / BPS)
    fees = ((short + exit_short) * config.fee_rate(config.short_venue)
            + long * 2 * config.fee_rate(config.long_venue)) / long * BPS
    return (spread_bps(short, long) - config.take_profit_bps - fees
            - config.slippage_buffer_bps - config.funding_budget_bps)


def projected_exit_net(config: Config, state: State, short: Decimal,
                       long: Decimal, quantity: Decimal) -> Decimal:
    held = number(state.quantity)
    gross = quantity * (number(state.short_average) - short + long - number(state.long_average))
    opening_fees = number(state.entry_fees_remaining) * quantity / held
    closing_fees = quantity * (short * config.fee_rate(config.short_venue)
                              + long * config.fee_rate(config.long_venue))
    reserve = quantity * number(state.long_average) * (
        config.funding_budget_bps + config.slippage_buffer_bps) / BPS
    return gross - opening_fees - closing_fees - reserve


def entry_allowed(config: Config, short: Decimal, long: Decimal) -> bool:
    return (spread_bps(short, long) >= config.entry_bps
            and entry_net_bps(config, short, long) >= config.min_profit_bps)


def exit_allowed(config: Config, state: State, short: Decimal, long: Decimal,
                 quantity: Decimal) -> bool:
    return (spread_bps(short, long) <= config.take_profit_bps
            and projected_exit_net(config, state, short, long, quantity)
            >= quantity * number(state.long_average) * config.min_profit_bps / BPS)


class Strategy:
    def __init__(self, config: Config, broker: Broker, store: StateStore, *, live: bool,
                 now_ms=None):
        self.config, self.broker, self.store = config, broker, store
        self.state = store.load(config, live=live)
        self.now_ms = now_ms or (lambda: int(time.time() * 1000))
        self._lock = asyncio.Lock()

    async def step(self) -> dict:
        async with self._lock:
            s, c = self.state, self.config
            if s.phase in {"DONE", "PAUSED"}:
                return {"phase": s.phase, "error": s.error}
            try:
                await self.broker.reconcile(s)
                await self.broker.sync_registry(s)
                held = number(s.quantity)
                quantity = min(c.clip_quantity, held if s.phase == "EXITING" else
                               max(held, c.total_quantity - number(s.entered_quantity)))
                books = await self.broker.quotes(quantity)
                now = self.now_ms()
                for venue in c.venues:
                    books[venue].validate(now_ms=now, config=c)
                entry = executable_prices(c, books, "entry")
                exit_prices = executable_prices(c, books, "exit")
                event = {"phase": s.phase, "entry_bps": str(spread_bps(*entry)),
                         "exit_bps": str(spread_bps(*exit_prices)), "quantity": s.quantity}
                if held > 0:
                    timeout = now - s.first_entry_ms >= c.max_hold_seconds * 1000
                    loss = projected_exit_net(c, s, *exit_prices, held) <= -c.stop_loss_usd
                    if timeout or loss or exit_allowed(c, s, *exit_prices, held):
                        s.phase = "EXITING"
                        s.exit_reason = s.exit_reason or ("max_hold" if timeout else "stop_loss" if loss else "take_profit")
                        self.store.save(s)
                # Exit latches permanently; no further entry after the first exit.
                if s.phase == "EXITING":
                    force = s.exit_reason != "take_profit"
                    if force or exit_allowed(c, s, *exit_prices, held):
                        await self._execute("exit", min(c.clip_quantity, held), force=force)
                elif number(s.entered_quantity) < c.total_quantity and entry_allowed(c, *entry):
                    await self._execute("entry", min(c.clip_quantity, c.total_quantity - number(s.entered_quantity)))
                return {**event, "phase": s.phase, "quantity": s.quantity, "exit_reason": s.exit_reason}
            except BaseException as exc:
                s.phase, s.error = "PAUSED", str(exc)
                self.store.save(s)
                raise

    async def _execute(self, intent: str, quantity: Decimal, *, force: bool = False):
        if quantity <= 0:
            raise RuntimeError("invalid zero clip")
        s, c = self.state, self.config
        # Durable intent before any remote action. A crash never replays it.
        s.pending = {"intent": intent, "quantity": str(quantity), "force": force,
                     "started_ms": self.now_ms()}
        self.store.save(s)
        result = await self.broker.execute(intent, quantity, s, force)
        s.pending["result"] = result
        self.store.save(s)
        if result.get("skipped") and result.get("ok"):
            s.pending = None
            self.store.save(s)
            return
        if not result.get("ok") or not result.get("hedge_verified"):
            raise RuntimeError("both fills must be verified before updating the strategy")
        filled = number(result["quantity"])
        short, long = number(result["short_price"]), number(result["long_price"])
        if not ZERO < filled <= quantity or min(short, long) <= 0:
            raise RuntimeError("invalid confirmed fill")
        fees = filled * (short * c.fee_rate(c.short_venue) + long * c.fee_rate(c.long_venue))
        held = number(s.quantity)
        if intent == "entry":
            total = held + filled
            s.short_average = str((number(s.short_average) * held + short * filled) / total)
            s.long_average = str((number(s.long_average) * held + long * filled) / total)
            s.quantity = str(total)
            s.entered_quantity = str(number(s.entered_quantity) + filled)
            s.entry_fees_remaining = str(number(s.entry_fees_remaining) + fees)
            s.first_entry_ms = s.first_entry_ms or self.now_ms()
            s.phase = "HOLDING"
        else:
            s.realized_gross_usd = str(number(s.realized_gross_usd) + filled * (
                number(s.short_average) - short + long - number(s.long_average)))
            s.entry_fees_remaining = str(number(s.entry_fees_remaining) * (held - filled) / held)
            s.quantity = str(held - filled)
            s.phase = "DONE" if held == filled else "EXITING"
        s.estimated_trading_fees_usd = str(number(s.estimated_trading_fees_usd) + fees)
        s.fills.append({"intent": intent, "quantity": str(filled), "short_price": str(short),
                        "long_price": str(long), "estimated_fee_usd": str(fees), "ts_ms": self.now_ms()})
        s.pending = None
        self.store.save(s)
        await self.broker.sync_registry(s)
