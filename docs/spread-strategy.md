# Cross-venue perpetual spread strategy

An independent Python implementation of the `auto` spread strategy in
[Gate CrossEx](https://github.com/your-quantguy/gate-crossex), using this
project's Aster, Hyperliquid, Lighter, MEXC and Variational adapters. To scan
all venues and run several symbols automatically, see the
[dispatcher](spread-dispatcher.md). It does not use a Gate
account, Gate SDK or CrossEx shared margin, and imports no source from that
repository.

## Run

```powershell
python scripts/run_spread_strategy.py --config configs/spread_strategy.example.json --max-seconds 60
```

The default is **paper mode**: live public quotes, simulated venues, no
credentials, no orders, no position-registry writes. Paper market orders fill in
full at the top of book; a paper post-only quote fills at its limit once the
opposite best price reaches it. Neither models depth, queue position or latency,
so paper results are not evidence of realizable profit.

```powershell
python scripts/run_spread_strategy.py --config configs/my_spread_strategy.json --live
```

Only `--live` sends orders and loads the project's `.env`. Edit a copy of the
example first: its fees are placeholders, and quantities must satisfy each
venue's minimums (checked at startup).

The strategy runs until Ctrl+C, a pause, or an optional stop-loss. Ctrl+C
cancels resting quotes, hedges any residual imbalance, and leaves the hedged
position open; the next run resumes it.

## Lifecycle (as in the original `auto` strategy)

Every `tick_seconds` (0.5 s):

1. Settle order updates: poll resting quotes and record new fills.
2. Repair any hedge imbalance (see below).
3. Trade:
   * `taker_taker`: if the entry spread passes, send both legs as market orders
     (`min(clip, remaining capacity)`); otherwise, if holding and the exit
     spread passes, send both reduce-only exit legs (`min(clip, matched)`).
   * `maker_taker`: keep an entry quote resting while capacity remains, and an
     exit quote resting while a position is held. Both can rest at once.

Entry and take-profit stay armed together: after a take profit the strategy
re-enters whenever the entry spread reopens. There is no one-shot cycle and no
completion state.

### Spreads

* Entry: `(short bid - long ask) / long ask * 10000` must be `>= entry_bps`.
* Exit: `(short ask - long bid) / long bid * 10000` must be `<= take_profit_bps`.

### Net-profit gate (kept from the earlier version; not in the original)

Entry also requires the estimated net edge to reach `min_profit_bps` after a
round trip of fees, the target exit spread, `funding_budget_bps` and
`slippage_buffer_bps`. Exit also requires the net per unit to reach
`min_profit_bps`, using the actual average entry costs of both legs. The fee
budget uses the maker rate for the maker venue in `maker_taker` mode (quotes
are post-only) and the taker rate everywhere else.

### Maker quotes

The quote rests at the worst maker price that still passes every gate against
the taker venue's current price (the "boundary"), rounded to the venue tick.
When the maker venue's best price already beats the boundary, it joins that
best price instead. A sell is never priced below the best ask and a buy never
above the best bid, so the post-only order cannot cross.

The quote is re-placed when the desired price moves by at least one tick and
`requote_interval_seconds` have passed since it was placed. Quotes are
post-only (Aster `GTX`, Hyperliquid `Alo`, Lighter `POST_ONLY`, MEXC order type 2). A post-only order that would
have crossed is treated as harmless and re-quoted on a later tick.

Variational cannot be the maker venue: its browser orders block until filled
and cannot rest as a cancellable quote. Use it as the taker leg.

### Hedge repair

Exposure comes from this strategy's own recorded fills (the state file), as in
the original. When the two legs differ, a market order repairs the gap:

* after an entry fill, the smaller (lagging) leg is topped up;
* after a take-profit fill, the larger leg is reduced (reduce-only). The
  original always tops up the smaller leg, which after a maker exit fill would
  reopen the leg that just closed. This implementation does not.

If a top-up is below the venue minimums, the excess leg is trimmed instead.
Failed repairs retry every `repair_cooldown_seconds`, and after three failures
the strategy pauses.

### Failures (as in the original)

* A rejected submission backs off for `2 s * 2^min(4, failures)`, capped at
  60 s; a rate-limit error waits at least 30 s.
* If both entry legs are rejected for margin or balance, the strategy pauses.
* After five consecutive failed submissions, the strategy pauses.
* An order whose outcome cannot be resolved within `order_timeout_seconds`
  pauses the strategy immediately. Unknown orders are never resubmitted.

A pause cancels resting quotes and stops trading; open positions stay open.

## Market data

* Aster: `bookTicker` WebSocket, source timestamp from the event.
* Hyperliquid: `l2Book` WebSocket, source timestamp from the book.
* Lighter: `ticker` WebSocket, source timestamp from the message.
* MEXC: `sub.depth.full` WebSocket (the ticker channel runs 1–3 s behind).
  Quantities are converted to whole contracts; registry quantities for MEXC
  legs are written in contracts, the unit the risk supervisor reads.
* Variational: REST `metadata/stats` every `variational_poll_seconds`, priced
  at the clip's USD size tier. It has no source timestamp, so freshness uses
  local receipt time.

A venue's quote is unusable when its connection is down, the quote is older
than `market_freshness_seconds` (15 s), it is more than
`future_tolerance_seconds` (2 s) in the future, or source-to-receipt lag
exceeds `max_transport_lag_seconds` (3 s). Nothing triggers without fresh
quotes from both venues. Disconnected streams reconnect with backoff.

## Startup checks

* Every remembered non-terminal order must be proven terminal. Quotes are
  cancelled, and other orders are queried. An order left `PENDING_SUBMIT`
  (crash during dispatch) or unresolvable blocks startup.
* Order sizes: the first clip and any smaller final remainder must meet each
  venue's minimum size, lot step and minimum notional (Aster `exchangeInfo`,
  Hyperliquid `szDecimals` and a 10 USD minimum, Lighter `orderBookDetails`,
  MEXC contract size, minimum volume and volume step). Variational publishes no
  such rules and is not validated.
* Margin (live): each venue's available balance must cover the remaining
  capacity's notional divided by that leg's leverage, plus 10%.
* No other strategy may own either venue-symbol in the position registry.

## Configuration

| Key | Meaning |
|---|---|
| `total_quantity` | Maximum matched position, base units |
| `clip_quantity` | Size of each order/quote, base units |
| `entry_bps`, `take_profit_bps` | Spread triggers; take profit must be below entry |
| `execution_method` | `maker_taker` (needs `maker_venue`) or `taker_taker` |
| `fees` | Fractions per venue and role (`0.0005` = 0.05%) |
| `min_profit_bps`, `funding_budget_bps`, `slippage_buffer_bps` | Net-profit gate reserves |
| `short_leverage`, `long_leverage` | Per-leg leverage (isolated margin) |
| `tick_seconds` | Evaluation interval (0.5) |
| `order_timeout_seconds` | Wait for a market order outcome (20) |
| `requote_interval_seconds` | Minimum quote age before repricing (2) |
| `market_freshness_seconds`, `future_tolerance_seconds`, `max_transport_lag_seconds` | Quote freshness (15 / 2 / 3) |
| `repair_cooldown_seconds` | Wait between failed repair attempts (3) |
| `variational_poll_seconds` | Variational quote polling (2) |
| `order_poll_seconds` | Per-venue minimum seconds between order-status polls (`{"lighter": 3}`; others 0.5) |
| `stop_loss_usd` | Optional, default `null` (off), see below |

Thresholds and sizes may change between runs. The symbol and venue pair are
fixed per state file.

### Optional stop-loss

The original has no risk exit, and neither does this one by default. When
`stop_loss_usd` is set and the gross unrealized PnL of the matched position at
current exit prices falls to `-stop_loss_usd`, the strategy cancels quotes,
closes everything with reduce-only market clips regardless of spread, and
stops permanently (`STOPPED`).

The larger cross-venue risk is not the spread but one leg being liquidated on
its own isolated-margin venue while the other leg profits. Keep leverage low,
and keep the project's risk supervisor running: this strategy publishes both
legs to the position registry for its margin top-ups.

## State and recovery

State defaults to `data/spread_strategies/<symbol+venues hash>.<mode>.json`.
It holds the per-leg exposure and average cost, gross realized PnL, estimated
fees and recent orders. An order is saved as `PENDING_SUBMIT` before dispatch.
A per-symbol OS lock prevents two instances on one symbol.

When paused (`status: PAUSED`, with `reason`):

1. Inspect both venues' positions and open orders.
2. For each unresolved order the bot names, settle it with the verified fill:
   `--settle-order <id>=<qty>[@<avg price>]` (`<qty>` may be 0).
3. Restart with `--resume`.

The bot does not see manual trades on the venues. Do not trade the same
venue-symbol by hand while it holds a position. If you must, flatten both
venues and start a new state file.

## Verification

```powershell
python -m pytest tests/test_spread_strategy.py -q
```

Tests cover spread and boundary math, Hyperliquid price rules, size checks,
order-status parsing, feed freshness, the taker cycle with re-entry, quote
placement, requoting, partial fills, direction-aware hedge repair, backoff and
pause rules, unknown-outcome handling, restart recovery, manual settlement,
margin preflight, Variational settlement and registry publication. Live order
submission against the real venues has not been exercised by the automated
tests.
