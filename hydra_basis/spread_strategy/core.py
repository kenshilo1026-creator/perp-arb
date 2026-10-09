"""Config, pricing math and the durable order ledger for the spread strategy.

The trading lifecycle follows Gate CrossEx's ``auto`` strategy: enter while the
entry spread is wide, take profit while the exit spread is narrow, repeat until
stopped. Exposure is derived from this strategy's own recorded fills.
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import asdict, dataclass, field, fields
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path

from hydra_basis.risk_management.persistence import atomic_write_json

BPS = Decimal("10000")
ZERO = Decimal("0")
ONE = Decimal("1")
EPSILON = Decimal("1e-12")
VENUES = {"aster", "arcus", "hyperliquid", "entropy", "lighter", "mexc", "ondo", "variational"}
# Variational orders go through the browser extension and block until filled,
# so they cannot rest as a cancellable post-only quote.
MAKER_VENUES = {"aster", "arcus", "hyperliquid", "entropy", "lighter", "mexc", "ondo"}
TERMINAL_STATES = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}
LEGACY_KEYS = {"poll_seconds", "max_quote_age_seconds", "max_request_seconds",
               "maker_timeout_seconds", "max_hold_seconds"}


def number(value) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("non-finite financial value")
    return result


def now_ms() -> int:
    return int(time.time() * 1000)


def round_step(value: Decimal, step: Decimal | None, direction: str) -> Decimal:
    if not step or step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_CEILING if direction == "up" else ROUND_FLOOR)
    return units * step


@dataclass(frozen=True)
class Config:
    symbol: str
    short_venue: str
    long_venue: str
    # Maximum matched position (base units) and the size of each order.
    total_quantity: Decimal
    clip_quantity: Decimal
    entry_bps: Decimal
    take_profit_bps: Decimal
    # Rates are fractions, e.g. 0.0005 means 0.05%. No venue fee assumptions.
    fees: dict[str, dict[str, Decimal]]
    execution_method: str = "maker_taker"
    maker_venue: str | None = None
    min_profit_bps: Decimal = Decimal("5")
    slippage_buffer_bps: Decimal = Decimal("5")
    funding_budget_bps: Decimal = Decimal("5")
    short_leverage: int = 1
    long_leverage: int = 1
    tick_seconds: float = 0.5
    order_timeout_seconds: float = 20.0
    requote_interval_seconds: float = 2.0
    market_freshness_seconds: float = 15.0
    max_transport_lag_seconds: float = 3.0
    future_tolerance_seconds: float = 2.0
    repair_cooldown_seconds: float = 3.0
    variational_poll_seconds: float = 2.0
    # Minimum seconds between order-status polls per venue (Lighter's REST limits are tight).
    order_poll_seconds: dict[str, float] = field(default_factory=lambda: {"lighter": 3.0})
    # Clips shrink to what the books can fill profitably, but never below this notional
    # (venue minimums apply too); 0 means venue minimums only.
    min_clip_notional_usd: Decimal = Decimal("0")
    # Minimum gap between taker clips so thin books can refill.
    clip_interval_seconds: float = 1.0
    # Optional emergency exit; None disables it (the original has no such exit).
    stop_loss_usd: Decimal | None = None

    def __post_init__(self):
        if not self.symbol or self.symbol != self.symbol.strip().upper():
            raise ValueError("symbol must be a canonical uppercase symbol")
        if not {self.short_venue, self.long_venue} <= VENUES:
            raise ValueError(f"supported venues: {', '.join(sorted(VENUES))}")
        if self.short_venue == self.long_venue:
            raise ValueError("choose two different venues")
        if self.execution_method not in {"maker_taker", "taker_taker"}:
            raise ValueError("execution_method must be maker_taker or taker_taker")
        if self.execution_method == "maker_taker":
            if self.maker_venue not in self.venues:
                raise ValueError("maker_venue must be one of the pair")
            if self.maker_venue not in MAKER_VENUES:
                raise ValueError("variational cannot rest post-only quotes; use it as the taker leg")
        for name in ("total_quantity", "clip_quantity"):
            if number(getattr(self, name)) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.clip_quantity > self.total_quantity:
            raise ValueError("clip_quantity exceeds total_quantity")
        for name in ("min_profit_bps", "slippage_buffer_bps", "funding_budget_bps", "min_clip_notional_usd"):
            if number(getattr(self, name)) < 0:
                raise ValueError(f"{name} must not be negative")
        if number(self.entry_bps) <= 0:
            raise ValueError("entry_bps must be greater than zero")
        if number(self.entry_bps) <= number(self.take_profit_bps):
            raise ValueError("take_profit_bps must be below entry_bps")
        if self.stop_loss_usd is not None and number(self.stop_loss_usd) <= 0:
            raise ValueError("stop_loss_usd must be positive or null")
        for name in ("short_leverage", "long_leverage"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for item in fields(self):
            if item.name.endswith("_seconds") and item.name not in {"order_poll_seconds", "clip_interval_seconds"} \
                    and number(getattr(self, item.name)) <= 0:
                raise ValueError(f"{item.name} must be positive")
        if number(self.clip_interval_seconds) < 0:
            raise ValueError("clip_interval_seconds must not be negative")
        if any(number(value) <= 0 for value in self.order_poll_seconds.values()):
            raise ValueError("order_poll_seconds must be positive")
        for venue in self.venues:
            for role in ("maker", "taker"):
                rate = number(self.fees[venue][role])
                if not ZERO <= rate < 1:
                    raise ValueError("fee rates must be fractions between 0 and 1")

    @property
    def venues(self):
        return (self.short_venue, self.long_venue)

    @property
    def maker_taker(self) -> bool:
        return self.execution_method == "maker_taker"

    @property
    def maker_leg(self) -> str:
        return "short" if self.maker_venue == self.short_venue else "long"

    def venue_of(self, leg: str) -> str:
        return self.short_venue if leg == "short" else self.long_venue

    def poll_seconds(self, venue: str) -> float:
        return float(self.order_poll_seconds.get(venue, 0.5))

    def leverage_of(self, venue: str) -> int:
        return self.short_leverage if venue == self.short_venue else self.long_leverage

    def fee_rate(self, venue: str) -> Decimal:
        # Post-only quotes earn the maker rate; every other order takes liquidity.
        role = "maker" if self.maker_taker and venue == self.maker_venue else "taker"
        return number(self.fees[venue][role])

    def identity(self) -> str:
        # Thresholds may change between runs; the venue pair and symbol may not.
        key = f"{self.symbol}:{self.short_venue}:{self.long_venue}"
        return hashlib.sha256(key.encode()).hexdigest()[:16]

    def to_dict(self) -> dict:
        return json.loads(json.dumps(asdict(self), default=str))

    @classmethod
    def load(cls, path: Path):
        return cls.from_dict(json.loads(path.read_text(encoding="utf-8-sig")))

    @classmethod
    def from_dict(cls, payload: dict):
        payload = dict(payload)
        legacy = payload.pop("leverage", None)
        if legacy is not None:
            payload.setdefault("short_leverage", legacy)
            payload.setdefault("long_leverage", legacy)
        unsupported = sorted(LEGACY_KEYS & payload.keys())
        if unsupported:
            raise ValueError(f"config keys no longer supported: {', '.join(unsupported)}")
        for key in ("total_quantity", "clip_quantity", "entry_bps", "take_profit_bps",
                    "min_profit_bps", "slippage_buffer_bps", "funding_budget_bps", "min_clip_notional_usd"):
            if key in payload:
                payload[key] = number(payload[key])
        if payload.get("stop_loss_usd") is not None:
            payload["stop_loss_usd"] = number(payload["stop_loss_usd"])
        payload["fees"] = {v: {role: number(rate) for role, rate in rates.items()}
                           for v, rates in payload["fees"].items()}
        if "order_poll_seconds" in payload:
            payload["order_poll_seconds"] = {v: float(s) for v, s in payload["order_poll_seconds"].items()}
        return cls(**payload)


# ---------------------------------------------------------------------------
# Pricing. "short" is the leg sold on entry, "long" the leg bought on entry.
# Entry spread: (short bid - long ask) / long ask; exit: (short ask - long bid) / long bid.
# ---------------------------------------------------------------------------

def spread_bps(short_price: Decimal, long_price: Decimal) -> Decimal:
    return (number(short_price) - number(long_price)) / number(long_price) * BPS


def entry_ratio_required(c: Config) -> Decimal:
    """Minimum short/long price ratio passing both the raw and the net entry gate.

    Net edge reserves a round trip of fees, the target exit spread, funding and
    slippage: (r-1)*B - (r + 1 + tp/B)*fS*B - 2*fL*B - tp - slip - funding >= min.
    """
    fs, fl = c.fee_rate(c.short_venue), c.fee_rate(c.long_venue)
    tp = c.take_profit_bps
    raw = ONE + c.entry_bps / BPS
    net = (c.min_profit_bps + BPS + (ONE + tp / BPS) * fs * BPS + 2 * fl * BPS + tp
           + c.slippage_buffer_bps + c.funding_budget_bps) / ((ONE - fs) * BPS)
    return max(raw, net)


def entry_allowed(c: Config, short: Decimal, long: Decimal) -> bool:
    return short / long >= entry_ratio_required(c)


@dataclass
class Leg:
    quantity: str = "0"   # signed: negative short, positive long
    average: str = "0"    # average cost of the open quantity
    realized: str = "0"   # gross realized price PnL (USD)

    def apply(self, signed_quantity: Decimal, price: Decimal):
        held, avg = number(self.quantity), number(self.average)
        after = held + signed_quantity
        if held == 0 or (held > 0) == (signed_quantity > 0):
            avg = (avg * abs(held) + price * abs(signed_quantity)) / abs(after)
        else:
            closed = min(abs(held), abs(signed_quantity))
            direction = ONE if held > 0 else -ONE
            self.realized = str(number(self.realized) + closed * (price - avg) * direction)
            if after == 0:
                avg = ZERO
            elif (after > 0) != (held > 0):
                avg = price  # flipped through zero: the remainder opened at this fill
        self.quantity, self.average = str(after), str(avg)


def matched_quantity(short: Leg, long: Leg) -> Decimal:
    s, l = number(short.quantity), number(long.quantity)
    if s >= 0 or l <= 0:
        return ZERO
    return min(-s, l)


def imbalance(short: Leg, long: Leg) -> Decimal:
    return number(short.quantity) + number(long.quantity)


def exit_net_per_unit(c: Config, short: Leg, long: Leg, s: Decimal, l: Decimal) -> Decimal:
    """Net per unit if both legs close at (s, l), after opening/closing fees and reserves."""
    return _exit_base(c, short, long) - s * (ONE + c.fee_rate(c.short_venue)) + l * (ONE - c.fee_rate(c.long_venue))


def _exit_base(c: Config, short: Leg, long: Leg) -> Decimal:
    fs, fl = c.fee_rate(c.short_venue), c.fee_rate(c.long_venue)
    sa, la = number(short.average), number(long.average)
    reserve = la * (c.funding_budget_bps + c.slippage_buffer_bps + c.min_profit_bps) / BPS
    return sa * (ONE - fs) - la * (ONE + fl) - reserve


def exit_allowed(c: Config, short: Leg, long: Leg, s: Decimal, l: Decimal) -> bool:
    return spread_bps(s, l) <= c.take_profit_bps and exit_net_per_unit(c, short, long, s, l) >= 0


def maker_boundary(c: Config, intent: str, taker_price: Decimal, short: Leg, long: Leg) -> Decimal:
    """Worst maker price that still satisfies every gate against the taker's price."""
    fs, fl = c.fee_rate(c.short_venue), c.fee_rate(c.long_venue)
    if intent == "entry":
        ratio = entry_ratio_required(c)
        return taker_price * ratio if c.maker_leg == "short" else taker_price / ratio
    k = ONE + c.take_profit_bps / BPS
    base = _exit_base(c, short, long)
    if c.maker_leg == "short":   # buy back the short at s; long sells at taker bid
        return min(taker_price * k, (base + taker_price * (ONE - fl)) / (ONE + fs))
    return max(taker_price / k, (taker_price * (ONE + fs) - base) / (ONE - fl))  # sell the long at l


# ---------------------------------------------------------------------------
# Durable ledger
# ---------------------------------------------------------------------------

@dataclass
class Order:
    id: str
    venue: str
    leg: str            # short | long
    side: str           # BUY | SELL
    purpose: str        # quote_entry | quote_exit | taker_entry | taker_exit | repair | stop
    quantity: str
    reduce_only: bool
    price: str | None = None          # limit price for quotes
    reference_price: str | None = None  # quote used at decision time (taker orders)
    state: str = "PENDING_SUBMIT"     # PENDING_SUBMIT | OPEN | UNKNOWN | terminal
    executed_quantity: str = "0"
    average_price: str | None = None
    average_estimated: bool = False
    remote: dict | None = None        # adapter order result used for status/cancel
    clip: str | None = None
    created_ms: int = field(default_factory=now_ms)
    updated_ms: int = field(default_factory=now_ms)
    error: str | None = None

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def signed_direction(self) -> Decimal:
        return ONE if self.side == "BUY" else -ONE


@dataclass
class State:
    identity: str
    mode: str
    strategy_id: str
    status: str = "RUNNING"   # RUNNING | STOPPING | STOPPED | PAUSED
    short: Leg = field(default_factory=Leg)
    long: Leg = field(default_factory=Leg)
    fees_usd: str = "0"
    orders: list[Order] = field(default_factory=list)
    reason: str | None = None

    def leg(self, name: str) -> Leg:
        return self.short if name == "short" else self.long

    def open_orders(self) -> list[Order]:
        return [order for order in self.orders if not order.terminal]

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict) -> "State":
        payload = dict(payload)
        payload["short"] = Leg(**payload.get("short", {}))
        payload["long"] = Leg(**payload.get("long", {}))
        payload["orders"] = [Order(**item) for item in payload.get("orders", [])]
        return cls(**payload)


class StateStore:
    KEEP_TERMINAL = 200

    def __init__(self, path: Path):
        self.path = path

    def load(self, config: Config, *, live: bool) -> State:
        mode = "live" if live else "paper"
        if not self.path.exists():
            return State(config.identity(), mode, f"spread-{uuid.uuid4().hex}")
        # Corrupt state must never be silently replaced with an empty strategy.
        state = State.from_dict(json.loads(self.path.read_text(encoding="utf-8")))
        if state.identity != config.identity() or state.mode != mode:
            raise ValueError("state belongs to another symbol/venue pair or mode; use a separate state file")
        if state.status not in {"RUNNING", "STOPPING", "STOPPED", "PAUSED"}:
            raise ValueError("invalid strategy status")
        return state

    def save(self, state: State):
        # Exposure lives in the leg totals, so old terminal orders can be pruned.
        terminal = [order for order in state.orders if order.terminal]
        if len(terminal) > self.KEEP_TERMINAL:
            drop = {order.id for order in terminal[: len(terminal) - self.KEEP_TERMINAL]}
            state.orders = [order for order in state.orders if order.id not in drop]
        atomic_write_json(self.path, state.to_dict())
