# Wallex premium study: 20 high-volume TMN markets, 14 days

Data for offline analysis of the TIZ premium mean-reversion strategy. Built by `uv run python -m tiz.study <COINS>` from
`data/<COIN>.json` (fetched with `uv run python -m tiz.backtest fetch 14 <COINS>`).

- Period: 2026-10-08 ~14:30 to 2026-10-22 ~12:50 Tehran time (14 days, 1-minute resolution).
- Markets (by 24h TMN volume on 2026-10-22, all allowed on the challenge account, all listed on Binance):
  PEPE, BTC, PUMP, ETH, BNB, DOGE, XRP, XAUT, ARB, SOL, TRX, S, LTC, SUI, APE, ADA, UNI, FIL, SAND, SHIB (each `<COIN>TMN`).
- Sources: Wallex UDF candles `/v1/udf/history` (resolution 1), Binance spot klines via `data-api.binance.vision`, Wallex USDTTMN candles.

## Files

### `candles/<MARKET>.csv`: one row per minute
| column | meaning |
|---|---|
| `time_utc`, `time_tehran` | minute start |
| `traded` | 1 if the Wallex candle had volume > 0 that minute |
| `open`, `high`, `low`, `close` | Wallex candle in TMN (empty if Wallex returned no candle) |
| `volume_base`, `volume_tmn` | Wallex volume in coins and in TMN (`volume_base × close`) |
| `binance_usdt_close` | Binance `<COIN>USDT` 1m close (carried forward over gaps) |
| `usdttmn_close` | Wallex USDTTMN 1m close (carried forward over gaps) |
| `fair_tmn` | `binance_usdt_close × usdttmn_close` |
| `premium_close_bps`, `premium_low_bps`, `premium_high_bps` | `(price / fair − 1) × 10,000` for close, low and high |

Note: Wallex returns zero-volume minutes that repeat the last price; filter on `traded = 1` for real trades.

### `premium_stats.csv`: one row per market
Share of minutes with trades, average TMN volume per day, premium median / standard deviation / 5th and 95th
percentile / min / max (traded minutes only), and `dip_minutes_below_base_<N>bps_per_day`: minutes per day whose low
was more than N bps below the trailing 30-minute mean premium (the bot's baseline), i.e. how often a resting buy at
that depth would have been touched.

### `backtest_grid.csv`: one row per (market, fee, entry edge, window)
Each market backtested alone with the live bot code (`src/tiz/bot.py`, AI judge off) plus `ALL` = the 20 markets
together in one account (max 3 positions, so markets compete for slots).
- `fee_maker_pct`: 0.25 (rate `/v1/account/fee` reports for this account; taker 0.3) or 0 (Wallex says spot is fee-free).
- `entry_edge_pct`: 0.4, 0.5, 0.6, 0.8, 1.0, 1.2: how far below baseline-fair the resting buy sits.
- `window`: `train` = first 7 days, `test` = last 7 days, `full` = 14 days. Pick parameters on train, judge them on test.
- `trades`, `wins`, `pnl_tmn`, `return_pct` (on 5,000,000 TMN), `max_drawdown_pct`, `stop_losses`, `max_holds`.

### `trades.csv`: every closed trade of every `full` run
`run_market` (which run), `fee_maker_pct`, `entry_edge_pct`, `market`, open/close time (Tehran), `hold_min`, `entry`,
`exit` (TMN), `pnl_tmn`, `pnl_pct`, `exit_reason` (`take_profit`, `stop_loss`, `max_hold`, `end_of_challenge`).

## Backtest rules (same as the live bot)
- Position 20% of equity, at most 3 at once (40% stays TMN), stop loss 1.5%, max hold 60 min, daily stop 3%.
- Buy: resting limit at `fair × (1 + baseline) × (1 − edge)`, baseline = time-weighted EMA (30 min) of the mid premium.
- Sell: limit at baseline-fair, never below entry + fees + 0.1%.
- Fill model: a resting buy fills when a later minute's low is 0.05% below it and that minute traded ≥ 1,000,000 TMN;
  a sell likewise with the high. Orders that cross the book fill at close ± 0.05% (taker). Queue position and
  slippage are not modeled, so results are optimistic.

## First read of the results (test window = last 7 days)
- At the account's reported fee (0.25%), the combined `ALL` run loses at every edge. Single markets that stay
  positive in both train and test: TRXTMN (edge 0.6%: train +1.4%, test +1.0%, 14d +2.5%, drawdown 0.6%), BNBTMN,
  BTCTMN and ADATMN at edge 1.2% (small gains, few trades).
- At zero fee, every market is profitable on test; the strongest are ARB, S, PEPE, TRX, PUMP (edge 0.4%).
- `ALL` underperforms the best single markets because slots go to whichever market dips first; market selection
  or ranking is the main lever.
