# Hedge execution safety

Live open and close execution now require three explicit operator limits. They are not
defaulted because the acceptable quote age, slippage, and maker exposure are
strategy/risk decisions:

```text
--max-hedge-slippage-bps BPS
--max-quote-age-ms MILLISECONDS
--max-maker-reprices COUNT
```

Example syntax only (the values below are placeholders, not recommendations):

```text
python scripts/place_order.py --mode open --ticker BTC \
  --max-hedge-slippage-bps <BPS> --max-quote-age-ms <MS> \
  --max-maker-reprices <COUNT>

python scripts/run_spot_perp_arbitrage.py --live --mode open --symbol BTC \
  --short-venue aster --total-size <QTY> --clip-size <QTY> --leverage <X> \
  --max-hedge-slippage-bps <BPS> --max-quote-age-ms <MS> \
  --max-maker-reprices <COUNT>
```

`max-hedge-slippage-bps` is directional and measured from the full requested
quantity's executable VWAP: a BUY cap is above the VWAP and a SELL cap is below
it. It is not a required-positive-spread rule; a funding-carry strategy may
deliberately pay an entry cost. The CLI and result telemetry report the planned
bound and realized gap in bps separately.

The actual IOC also enforces a **0.2% adverse pair gap, before fees**, using the
maker fill VWAP (or its conservative post-only limit when no average is available).
Only the final terminal maker response supplies that VWAP: an earlier partial
average cannot represent additional fills during cancellation. If the final
average is unavailable, realized maker price/gap is N/A while the submitted
post-only limit still supplies the conservative hedge bound.
Maker SELL / hedge BUY: `min(original_cap, maker_price * 1.002)`.
Maker BUY / hedge SELL: `max(original_cap, maker_price * 0.998)`.
This intersection can tighten the original cap but never widens it, including
after repricing and on partial retries. A preview gap override does not override
this execution bound. There is no market fallback if this bound prevents a fill.

The maker is submitted post-only. The hedge is a marketable IOC limit at the
explicit worst price. An adapter that cannot guarantee both properties fails
before maker placement. A bounded hedge can reject or partially fill; it is not
atomic and is not risk-free. Terminal partial fills retry only the proven
remainder at the same cap. ACK-only or unknown outcomes are not retried because
that could duplicate a live order.

Quote freshness uses a local monotonic receive age and, when the venue supplies
a real epoch event timestamp, also checks the source age. Sequence numbers are
not treated as clocks. Every bounded maker watches full-clip hedge depth,
source freshness and the intersected cap while resting. The worst depth level,
not just VWAP, must fit the IOC cap. Aster and MEXC hedge legs use top-20 snapshot
streams; Aster subscribes to `depth20@100ms`. Other venues currently use REST
checks with a 100 ms pause between requests, so response latency still limits
their cancellation speed. Disconnects and stale snapshots trigger cancellation
and final-fill reconciliation before replacement.

After a confirmed maker fill, submit the bounded IOC immediately: no REST quote
refresh and no stale-quote veto on the critical path. The exchange enforces the
price cap even when cached liquidity disappears. Only a proven terminal remainder
may be retried, with no fixed 500 ms sleep. Unknown ACKs still require reconciliation
and never authorize a second potentially duplicating order.

MEXC uses its native `IMMEDIATE_OR_CANCEL` order type and recognizes
`PARTIALLY_CANCELED`. Precision comes from `quotePrecision` / `baseAssetPrecision`
when tick/lot filters are absent, with minimum quantity and notional checks.
Status queries with missing cumulative quantity or mismatched order identity
cannot authorize a replacement or another hedge.

If the maker fills but the hedge remains partial or unknown, execution stops
before the next batch and writes a non-secret record to
`data/hedge_residuals.jsonl`. The record contains symbol, venues, sides,
quantities, cap, uncertainty status, and error text; it does not contain account
identifiers or credentials.

A terminal hedge fill remains confirmed even if the aggregate position endpoint
lags. That condition is still reported as uncertain pair-position confirmation,
but the durable record keeps the proven filled quantity and never reports the
whole hedge as missing or authorizes a duplicate hedge.

Multiple hedge fills are quantity-weighted across all terminal attempts, including
status-query results. If any positive fill lacks a confirmed average, report N/A
rather than the last order's price. Timing distinguishes adapter dispatch, start of
submission when available, and cumulative submit/status requests; none is a claim
about the two exchanges' actual fill-time difference.

The Web form passes all three limits through open, close and spot-perp execution
(visible form values: 20 bps, 1000 ms, 3 reprices). Missing live API fields are
rejected before execution. Funding auto-close accepts the same three flags on
`scripts/run_risk_manager.py`; with live funding protection enabled they must be
supplied at startup. Existing liquidation emergency-close behavior is unchanged.

Current limitations:

- Variational browser execution cannot guarantee post-only maker placement or
  accept a bounded IOC hedge price, so hardened live execution rejects that leg
  before maker placement.
- Arcus currently exposes only BBO and Ondo requests depth 1 in the shared
  preview adapters. Hardened execution requires full-clip depth and therefore
  rejects those venues before maker placement until an L2/size-aware quote is
  wired into the execution snapshot.
- REST depth is limited to the levels returned by each venue. If those levels do
  not cover the full clip, execution fails rather than extrapolating top of book.
- Cross-exchange fills are never atomic. Competitors and disappearing liquidity
  cannot be eliminated; the controls bound price and stop unresolved exposure.
