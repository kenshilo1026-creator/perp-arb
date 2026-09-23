# Aster / Lighter partial close execution

`scripts/place_order.py` now enables fill verification and close-only orders for
every perpetual close pair. Each batch captures both live positions before
submission, validates the close direction and quantity, and requires both final
positions to reflect the same executed quantity before returning success.

The Aster / Lighter regression had several independent causes:

- Lighter REST `filled_base_amount` is already a token amount. Dividing it by
  the submission multiplier understated fills (for example, 210 became 21).
- Disappearance from `accountActiveOrders` was incorrectly treated as a full
  fill. The adapter now queries `accountOrders` by client order index, checks the
  market, and treats a missing record as unknown.
- The Lighter marketable hedge used GTC. It now uses an IOC limit with the
  existing slippage cap, and a terminal partial fill authorizes only the remainder.
- Repricing ignored fills during cancellation, and a replacement could inherit
  the previous order's cancel result. Each order now has its own cancel state;
  positive terminal fills are hedged before any further batch.
- Close orders previously enabled reduce-only only for Variational. Aster,
  Lighter and Hyperliquid now receive reduce-only; MEXC uses its close-side codes.

Aster and Lighter cancellation must return a terminal cumulative fill, including
fills that raced the cancel. A cancel acknowledgement or an unknown-order error
alone cannot authorize a replacement. Final maker quantities come from terminal
order records, so a lagging position snapshot cannot understate the hedge target.
Both positions are checked after hedging. Aster and Lighter hedge acknowledgements
are queried for terminal execution; confirmed partial fills are retried only on
the original counterpart, in its original closing direction.

An interrupt during an Aster / Lighter maker wait cancels the maker, completes
the counterpart of any confirmed fill, then exits without starting another batch.
This requires successful cancellation and order confirmation. Forced process
termination, unavailable exchanges, unknown orders or exhausted hedge retries can
still leave exposure; they cannot be made atomic across exchanges. No reverse
maker order is used as compensation. Existing mismatches block new batches and
are not automatically repaired by this change.

API references: [Lighter OrderApi](https://github.com/elliottech/lighter-python/blob/main/docs/OrderApi.md),
[Lighter signer IOC options](https://github.com/elliottech/lighter-python/blob/main/lighter/signer_client.py),
[Lighter order amount schema](https://github.com/elliottech/lighter-agent-kit/blob/main/references/schemas-read.md).

Offline regression tests: `python -m pytest tests/test_aster_lighter_close_safety.py
tests/test_hedge_safety.py tests/test_aster_cancel_race.py tests/test_variational_fill_safety.py -q`.

## Hyperliquid / MEXC after the merge

Hyperliquid and MEXC now follow the same terminal-order checks before maker
replacement, including cleanup after an exhausted wait or interrupt. Hyperliquid
normalizes its nested order status and computes cumulative fills from original
size minus remaining size. MEXC normalizes order states 3/4/5 and uses `dealVol`,
including zero. Missing quantities and nonterminal cancellations cannot authorize
a replacement or hedge remainder. MEXC hedge acknowledgements are now queried
until their execution is known, allowing confirmed terminal remainders to be filled.

An actual maker overfill is hedged on the original counterpart and then raises
an error instead of reporting a successful batch. No maker reversal is submitted.

References: [Hyperliquid order status](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#query-order-status-by-oid-or-cloid),
[MEXC order status](https://mexcdevelop.github.io/apidocs/contract_v1_en/#get-order-by-order-id).
Regression tests: `tests/test_hyperliquid_mexc_fill_safety.py`.
