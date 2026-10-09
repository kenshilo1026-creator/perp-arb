# Spread dispatcher

Scans every enabled venue for cross-venue perpetual spreads and runs one
[spread strategy](spread-strategy.md) group per opportunity. A group is one
symbol traded between one pair of venues. Supported venues: Aster,
Hyperliquid, Entropy, Lighter, MEXC and Variational (taker leg only).

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
