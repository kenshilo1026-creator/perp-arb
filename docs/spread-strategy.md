# Cross-venue perpetual spread strategy

An independent Python implementation of the paired spread-convergence workflow
reviewed in [Gate CrossEx](https://github.com/your-quantguy/gate-crossex). It uses
this project's Aster, Hyperliquid and Variational adapters directly; it does not
use a Gate account, Gate SDK, or CrossEx shared margin. No source from that
repository is imported into this implementation.

## Run

From the project root, using the project's existing Python environment:

```powershell
python scripts/run_spread_strategy.py --config configs/spread_strategy.example.json --max-ticks 10
```

The default is **paper mode**: public quotes, immediate simulated fills, no
authenticated adapter construction, no orders, no writes to the position
registry. These fills do not model maker queue priority, depth, or slippage and
are not evidence of realized profit. State is retained even after `--max-ticks`.

Edit a copy of the example configuration before enabling live execution. Its
fees are placeholders, not claims about any venue/account's actual fees. Small
example quantities also need checking against each venue's minimum and lot size.

```powershell
python scripts/run_spread_strategy.py --config configs/my_spread_strategy.json --live
```

Only `--live` enables actual orders and loads the project's existing `.env`.
The command does not install dependencies or change credential configuration.
This implementation has been tested with deterministic exchange simulators;
real venue order submission has not been exercised by its automated tests.

For a Variational pair, set either `short_venue` or `long_venue` to
`variational`, and choose one of those venues as `maker_venue`. The command
starts the existing browser broker on 127.0.0.1:8768 and portfolio/fill feed on
127.0.0.1:8766, then waits for the project's extension and portfolio stream.
Use the existing browser extension setup. Another broker already occupying
these ports must be stopped first. Fees for both chosen venues are required.

## Configuration

* `total_quantity` / `clip_quantity`: base-token units, not USD; both legs target
  the same actual quantity. Symbols must represent the same underlying contract
  units on both venues. An alias alone cannot validate economic equivalence.
* `entry_bps`: `(short bid - long ask) / long ask * 10000`. For example 40 = 0.4%.
* `take_profit_bps`: `(original short ask - original long bid) / long bid * 10000`.
  This must be below the entry threshold. Entry and exit use different bid/ask sides.
* `execution_method`: `maker_taker` or `taker_taker`. In maker mode, a fresh pair
  quote gates every maker dispatch; the limit joins the passive top of the book.
  A known terminal zero fill returns to monitoring. Partial maker fills hedge
  only the confirmed quantity. Repricing is intentionally disabled within a
  clip: after the configured timeout, the existing executor cancels/reconciles
  it and the next strategy tick reevaluates both venues. GTC is not guaranteed
  post-only. No economic veto is applied after a maker fill: its counterpart
  must be hedged even if the spread has moved.
* `fees`: fractions (`0.0005` = 0.05%), explicitly supplied per venue and role.
  Because the adapters use GTC, this strategy reserves the worse maker/taker
  rate for both sides of both opening and closing. It does not fetch account
  fee tiers or fee rebates automatically.
* `min_profit_bps`: minimum estimated net edge; not merely the gross spread.
  Entry reserves a round trip, the target residual spread, funding budget and
  slippage buffer. Exit uses the actual recorded entry average prices and
  allocated opening fees, estimated close fees, and the same reserves.
* `funding_budget_bps`: a conservative assumed net funding cost for the cycle.
  It is not a feed of actual funding payments or a promise that funding is bounded.
* `slippage_buffer_bps`: additional estimated cost allowance. Current public
  quotes and this buffer are not a depth-based execution guarantee.
* `max_hold_seconds` / `stop_loss_usd`: latch a risk exit; subsequent clips use
  confirmed market orders with `reduce_only` regardless of the profit gate.
  An unavailable quote, uncertain fill or failed reconciliation pauses instead
  of blindly sending orders. These controls cannot guarantee a maximum loss.
* `max_quote_age_seconds` / `max_request_seconds`: reject stale timestamps and
  slow quote requests. Hyperliquid source timestamps are checked. Aster's
  legacy `lastUpdateId` is not treated as a timestamp; Aster and Variational
  currently use local receipt/request timing, which cannot prove the freshness
  of upstream data. Variational quotes are refetched at the clip's estimated
  USD size tier. Aster/Hyperliquid top-of-book reads do not model full depth.

Entry is allowed only if both the raw entry threshold and estimated net edge
pass. Therefore an example threshold of 40 bps may still reject a 40 bps quote
when fees and reserves would leave insufficient profit.

## State, ownership and recovery

The command runs one lifecycle:

```text
WAITING -> HOLDING -> EXITING -> DONE
                    errors -> PAUSED
```

The first exit permanently latches: no more entry clips after that transition,
even if the spread widens again. Start a deliberate new cycle with a new state
file only after the old cycle has completed and its positions are flat.

State defaults to `data/spread_strategies/<config hash>.<mode>.json`. `--state`
can specify another file. Config changes and paper/live changes cannot reuse
the same state. The saved record includes weighted actual entry averages,
confirmed quantity, entry/exit fills, estimated trading fees, gross realized
price PnL, the exit reason and any pending order intent. Estimated fees and
funding reserves are not actual cashflow accounting.

Before remote dispatch the intent is atomically persisted. If the process dies
with a pending intent, the next run refuses automatic replay. A normal known
HOLDING/EXITING state can resume only after remote positions and open orders
reconcile. Corrupt state/registry also fails closed. A per-symbol OS lock
prevents concurrent instances of this command using different state filenames.

Live mode requires exclusive ownership of the selected venue-symbol positions:
flat at initial startup, or exactly the saved short/long quantity on resume.
It refuses unrelated positions, other registered strategies, and open orders.
Variational's open-order check uses the extension's existing read-only browser
table lookup, which depends on its active page/extension state. Do not manually
trade the same venue-symbol pair while the bot runs. Existing legacy order
commands do not participate in this new command's process lock.

Successful fills update the existing position registry under the new strategy
ID, preserving margin-topup metadata. The existing risk supervisor may close
positions independently; the next tick then pauses on a position mismatch.
There is no cross-process transaction with that supervisor, so avoid running
conflicting close workflows on the same position simultaneously.

Ctrl+C between clips retains resumable state and does not close open positions.
An interrupt during dispatch retains the pending intent and pauses. For PAUSED
or pending states, inspect actual positions and all remote orders, settle/cancel
unknown orders, and reconcile the state and registry manually before restarting.
Do not delete state as a shortcut while positions or orders remain open.

Missing actual fill averages also pauses, even when position deltas confirm
fills. Submitted limit prices are never substituted for actual averages.
Hyperliquid maker fills are queried by order ID from `userFills`; missing or
truncated fill history prevents automatic accounting. Market retry fills are
weighted across their confirmed quantities.

## Verification

```powershell
python -m pytest tests/test_spread_strategy.py tests/test_order_service.py tests/test_execution_engine.py tests/test_spread_monitor.py -q
```

Tests cover bid/ask direction, fees/funding reserves, partial fills, permanent
exit latching, restart protection, durable intent, concurrent ticks, maker price
guards, all three venue routes, the real existing clip executor with simulated
adapters, reduce-only exits, unknown outcomes, and read-only reconciliation APIs.

Read-only API references used for the additional adapter methods:

* [Aster open orders](https://asterdex.github.io/aster-api-website/futures-v3/account&trades/#current-all-open-orders-user_data)
* [Hyperliquid info endpoint: openOrders and userFills](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
