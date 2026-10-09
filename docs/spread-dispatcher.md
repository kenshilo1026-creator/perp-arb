# Spread dispatcher

Scans every enabled venue for cross-venue perpetual spreads and runs one
[spread strategy](spread-strategy.md) group per opportunity. A group is one
symbol traded between one pair of venues. Supported venues: Aster,
Arcus, Hyperliquid, Entropy, Lighter, MEXC and Variational (taker leg only).

## Run

```powershell
python scripts/run_spread_dispatcher.py                 # paper (default)
python scripts/run_spread_dispatcher.py --live          # real orders
```

To only see opportunities and estimated profit, without opening anything
(not even paper positions):

```powershell
python scripts/run_spread_dispatcher.py --dry-run                # every 15 s until Ctrl+C
python scripts/run_spread_dispatcher.py --dry-run --max-seconds 60 --top 15
```

The dry run uses market data only: no credentials, adapters, locks or groups.
Each report lists:

* opportunities that pass the entry gate;
* the best near misses, with the reason they are blocked;
* existing groups of the selected mode (paper, or `--live`), read from their
  state files: current exit spread, PnL if closed now, and PnL at take profit.

Profit "at take profit" assumes the exit happens when the exit spread reaches
`take_profit_bps` (the strategy's rule) with the long leg's price unchanged.
It covers all four fills' fees at the configured rates, and excludes funding,
slippage, depth and lot rounding.

### Depth

The quote store keeps top of book with size for every symbol. For symbols
with a group, a pending launch, or a dry-run candidate, it also keeps depth:
Aster depth10, Lighter order_book (deltas applied), Arcus l2Orderbook and MEXC
depth.full; Hyperliquid and Entropy books carry 20 levels already. A launch
waits until the entry still passes at the full clip size against that depth.
The dry run's 深度後bps column shows the entry spread at clip size
(不足 = visible depth cannot fill a clip, 無數據 = depth subscription just
started).

### 24-hour spread history

For every opportunity at or above `history.check_bps` (30), the dispatcher
rebuilds that pair's spread over the last `history.lookback_hours` (24) from
per-minute prices. It reports:

* the median and 90th-percentile spread;
* the share of minutes at or above the threshold;
* how many times the spread reached the threshold, and how many of those
  times it later came back to the take-profit level (with the median minutes
  that took);
* how long the current episode has lasted, and minutes since the spread last
  converged.

Each pair gets one label:

| Label | Meaning |
|---|---|
| 持續型 persistent | At or above the threshold at least `persistent_pct` (70%) of the time and never back to take profit: a structural gap, so a position may never take profit |
| 反覆收斂型 reverting | Reached the threshold before and came back to take profit |
| 瞬間型 new spike | Almost never this wide, and only just appeared |
| 間歇型 intermittent | Anything else |
| 資料不足 insufficient | Under `min_coverage_pct` (50%) of minutes have both prices |

With `block_persistent` (default on), a launch on a persistent spread is
rejected and the symbol is put on cooldown.

Prices are 1-minute candle closes from Aster, Hyperliquid, Entropy, MEXC and
Arcus. Lighter's candle API is not public (HTTP 403) and Variational has
none, so the dispatcher, dry run included, records their per-minute mid
prices to `data/spread_dispatcher/minute_mids.json.gz` (kept for 24 hours).
Lighter pairs therefore show "insufficient" until the dispatcher has run for
a while. The spread is close/mid based, so it reads about half a bid/ask
spread wider on each side than the executable entry spread.

Paper mode uses live public data and simulated venues, with no credentials,
orders or registry writes. Live mode loads `.env` and builds an authenticated
adapter for every enabled venue at startup. It exits immediately if a venue's
credentials are missing, so remove that venue from `venues` first.

Ctrl+C cancels resting quotes, hedges any residual imbalance, and leaves group
positions open. The next run restores every group from
`data/spread_dispatcher/dispatcher.<mode>.json` and continues managing it.

## Configuration (`configs/spread_dispatcher.json`)

| Key | Default | Meaning |
|---|---|---|
| `venues` | aster, hyperliquid, lighter, mexc | Venues to scan and trade |
| `max_groups` | 5 | Concurrent groups (one symbol each) |
| `group_notional_usd` | 100 | Maximum position per group, per leg |
| `clip_notional_usd` | 50 | Size of each order; the group holds a whole number of clips |
| `min_clip_notional_usd` | 20 | Smallest clip when depth forces clips to shrink |
| `execution_method` | `taker_taker` | Or `maker_taker`, with the maker leg chosen by `maker_preference` |
| `fees` | per venue | Fractions. Defaults are placeholders: set your account's actual rates |
| `leverage` | 1 per venue | Isolated leverage per venue |
| `entry_bps`, `take_profit_bps`, `min_profit_bps`, `slippage_buffer_bps`, `funding_budget_bps` | 40 / 10 / 5 / 5 / 5 | Strategy thresholds, see the strategy doc |
| `confirm_seconds` | 3 | An opportunity must persist this long before a group launches |
| `max_book_spread_bps` | 20 | Skip a venue whose own bid/ask spread is wider than this |
| `max_price_deviation_pct` | 1 | Skip pairs whose mid prices differ more (likely different contracts) |
| `max_abs_funding_rate_pct` | 0.1 | Skip pairs where either funding rate exceeds this |
| `symbol_allowlist`, `symbol_blocklist` | empty | Restrict or exclude symbols |
| `reject_cooldown_seconds` | 600 | Retry delay after a launch is rejected |
| `idle_retire_seconds` | 1800 | Retire a flat group with no fills for this long, freeing its slot |
| `strategy` | | Engine settings passed to every group (tick, timeouts, freshness, stop loss) |

## What it does

1. **Market data.** One shared connection set for all groups:
   * Aster `!bookTicker`
   * Hyperliquid `l2Book` for each active coin, subscribed gradually (a burst
     of subscriptions makes Hyperliquid drop the connection)
   * Lighter `ticker`
   * MEXC `sub.ticker` for every symbol, plus `sub.depth.full` for symbols
     with a group. MEXC ticker snapshots run 1–3 s behind the book, so they
     are used only to discover opportunities; orders are driven by depth.
   * Variational: REST polling.
2. **Scanning.** Every `scan_seconds`, for each symbol on two or more venues
   and each direction, the scanner computes the entry spread
   `(short bid - long ask) / long ask` with the same fee and net-profit gate
   the strategy uses. Results are filtered by freshness, book spread, price
   deviation and funding, then ranked by spread.
3. **Launching.** When an opportunity persists for `confirm_seconds` and a
   slot is free, the dispatcher:
   * waits for trade-grade quotes from both venues;
   * loads the venues' lot, tick and minimum rules;
   * sizes the group;
   * runs the strategy's startup checks (order sizes and, in live mode,
     margin).

   A failure puts the symbol on cooldown.
4. **Groups** run the normal strategy loop: enter, take profit, re-enter. A
   group that is flat with no fills for `idle_retire_seconds` is retired.
   A paused group keeps its slot until resumed.

Only one group per symbol, and a symbol traded by the dispatcher is locked
against the standalone `run_spread_strategy.py` (and vice versa).

### Arcus

[Arcus](https://docs.arcus.xyz/api-reference/introduction) perps: crypto, US
equities, ETFs and indices, quoted as `<BASE>-USD` (`NVDA-USD`).

* Credentials (`.env`): `ARCUS_API_SIGNING_KEY` (the Ed25519 signing key
  shown once on app.arcus.xyz/api-keys), `ARCUS_ADDRESS` (the master wallet
  the key is registered to), `ARCUS_ACCOUNT_INDEX` (subaccount, default 0).
  `ARCUS_BASE_URL=https://api.testnet.arcus.xyz` switches to testnet.
* Orders: Ed25519-signed REST. Prices and sizes are signed as integer ticks
  and quantums. Maker legs are post-only (`ALO`); taker legs are IOC limits
  50 bps through the book. Placement is asynchronous (202 `ACK`), so fills
  are read back from `GET /v1/order/{id}`.
* Margin: the first opening order per market sets isolated margin at the
  configured leverage. If a cross position already exists there, the mode
  is left unchanged. Margin top-ups are not supported by the adapter.
* Fees: Base tier maker 0%, taker 0.0225% (`GET /v1/feeTiers`).
* Funding: hourly, with history from `GET /v1/fundingRates` (microsecond
  timestamps). Registered for the funding monitor and backfill as venue
  `arcus`.
* Market data: BBO WebSocket for every online market on one socket
  (Arcus caps a socket at 100 subscriptions).
* Rate limits: reads are weighted per IP (1,500/min). Order writes do not
  use that budget.
* Overlap: 57 of Arcus's 60 online markets also trade on another venue.
* Equities outside regular trading hours can reject fills beyond the
  off-hours trading bound (`FILL_WILL_EXCEED_TRADING_BOUND`).

### Entropy

[Entropy](https://docs.entropy.io/) is not a separate exchange. It is a HIP-3
builder-deployed perp dex on Hyperliquid named `io` (coins `io:OAI`,
`io:SNDK`…), so it trades through the Hyperliquid API with the same key and
account (`HYPERLIQUID_PRIVATE_KEY`), and the strategy calls the venue
`entropy`.

* Markets: equity, index and pre-IPO perps. Every market is isolated-only
  (`strictIsolated`: margin can be added but not removed). The order asset id
  is 100000 + 10000 × dex index + market index.
* Orders: Hyperliquid's limit types, so post-only (`Alo`), IOC and GTC all
  work: both maker and taker legs are supported.
* Funding: hourly, with history from Hyperliquid's `fundingHistory`
  (`io:XXX`). It is registered for the funding monitor and backfill as venue
  `entropy` (symbols `IO:XXX`, like trade.xyz's `XYZ:XXX`).
* Fees: HIP-3 markets cost 2× normal Hyperliquid rates (taker 0.090%, maker
  0.030% at the base tier), reduced 90% in growth mode. The config assumes
  growth mode (taker 0.009%, maker 0.003%). A market without growth mode is
  rejected at launch, since its fees would be 10× the configured rate.
* Margin: HIP-3 dexes keep their own margin state. The preflight reads the io
  dex's `withdrawable`; if it shows no balance, make USDC available to the io
  dex in Hyperliquid first.
* Overlap: only DRAM, EWY, NBIS and SNDK are also on Aster, Lighter or MEXC.
  OAI, ANTH, GPRO, IONQ and TCNT trade only on Entropy among the supported
  venues.
* Outside US market hours the oracle changes slowly and the funding
  multiplier drops. Spreads against other venues' equity perps can then
  persist.

### Symbols

Only symbols whose name means the same contract on both venues are traded.
Aliases such as `1000PEPE` / `kPEPE` / `PEPE` differ in units by 1000× and
are skipped. The price-deviation filter is a second guard against mismatched
contracts.

### MEXC units

MEXC orders and positions count contracts (ETH: 0.01 ETH per contract). The
strategy converts to and from base units for MEXC and only trades whole
contracts. For the position registry, MEXC legs are written in contracts,
because the risk supervisor compares registry quantities with the adapter's
contract-denominated positions. The existing MEXC adapter itself still takes
contracts; other scripts that pass coin quantities to it are unchanged.

## Paused or blocked groups

The status line lists every group. `PAUSED` means the strategy stopped
itself (see `reason`). `BLOCKED` means a restored group failed its startup
checks, for example because an order outcome is unknown.

1. Inspect both venues' positions and open orders.
2. Settle each unresolved order with the verified fill:
   `--settle <group>:<order id>=<qty>[@<avg price>]` (this also resumes it).
3. Or resume after review: `--resume <group>`.

## Notifications

Group start, pause and retirement are sent to Telegram when
`TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set; otherwise they are
printed.

## Things to know before going live

* Paper fills assume full size at the top of book and say nothing about
  realizable profit.
* Some symbols, notably tokenized stocks on Aster and Lighter (TSLA, AAPL…),
  can show persistent spreads from different pricing or trading hours rather
  than temporary dislocations. Consider `symbol_blocklist` for them.
* Each leg is isolated margin on its own venue: one leg can be liquidated
  while the other profits. Keep leverage low and run the risk supervisor.
  Lighter margin top-ups are not supported by its adapter.
* Lighter's REST limits are tight; order-status polling there defaults to
  every 3 s (`strategy.order_poll_seconds`).
