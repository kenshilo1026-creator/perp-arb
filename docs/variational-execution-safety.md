# Variational paired execution

`scripts/place_order.py` enables verified execution for open and close pairs
containing Variational. The web maker/taker flow uses the same open-clip function.

- Before maker submission, the executor checks both live positions and prepares
  the taker's margin/leverage where supported. Existing imbalance blocks a new batch.
- Variational trade messages carry their position baseline. A trade message is
  fill evidence, not proof that the remaining order is terminal.
- Failed maker responses retain the original error, request/order identifiers,
  price and position baseline. Missing baselines are supplemented from the live
  pre-dispatch position. `[maker-failure]` logs the cause; `[maker-reconcile] after
  cleanup` shows whether cancellation was followed by a fill, unchanged position,
  or inconclusive data. A cancellation acknowledgement alone does not prove zero fill.
- After cancelling a partial maker, the executor polls its position delta for
  up to ten observations, including fills arriving during cancellation. It hedges
  the resulting quantity rather than the first trade's quantity.
- Hyperliquid IOC responses expose `totalSz` as the actual terminal fill quantity.
  The response format and IOC remainder semantics are documented in the
  [Hyperliquid exchange API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint).
- Terminal partial hedge fills are retried using only the remainder, with at
  least three total attempts. Acknowledgements and transport failures are checked
  against the pre-submit position. An unknown partial/unchanged position does
  not authorize another order because the previous order may still fill.
- Both live positions must reflect the executed quantity before success. Position
  registry writes for verified execution cannot synthesize a missing live leg.
- Recovery orders only go to the original hedge venue and side, for the remaining
  quantity. A failed hedge never triggers an opposite order or a position close
  on the filled maker leg. If terminal failures exhaust the retry budget, the
  batch stops and reports the confirmed fill and outstanding hedge quantity.

Cross-exchange execution is not atomic. Exchange downtime, cancellation failure,
or an unknown pending hedge can still leave exposure. Such cases
stop with an error instead of claiming success or blindly resubmitting. Check the
exchange orders and positions before resuming. No existing imbalance is repaired
by starting another batch. These checks assume no concurrent manual/automated
trades in the same positions during execution.

Offline regression coverage: `python -m unittest tests.test_hedge_safety
tests.test_variational_fill_safety tests.test_aster_cancel_race -q`.
