"""Market study export: one folder (study/) with per-market minute data, premium statistics and a backtest grid.

uv run python -m tiz.study BTC,ETH,...      (needs data/<COIN>.json from `tiz.backtest fetch`)
See study/README.md for every file and column."""
import asyncio
import csv
import json
import statistics
import sys
from datetime import datetime, timezone

from tiz import backtest as T
from tiz import bot as B

OUT = B.ROOT / "study"
EDGES = (0.004, 0.005, 0.006, 0.008, 0.010, 0.012)
FEES = ((0.0025, 0.003), (0.0, 0.0))


def iso(t, tz=timezone.utc):
    return datetime.fromtimestamp(t, tz).strftime("%Y-%m-%d %H:%M")


def write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def candles(coin, usdt):
    d = json.load(open(T.DATA / f"{coin}.json"))
    wx = {int(k): v for k, v in d["wx"].items()}
    bn = {int(k): v for k, v in d["bn"].items()}
    t0, t1 = max(min(wx), min(bn), min(usdt)), min(max(wx), max(bn), max(usdt))
    rows, u, b = [], None, None
    for t in range(t0, t1 + 1, 60):
        u, b = usdt.get(t, u), bn.get(t, b)
        if u is None or b is None:
            continue
        fair = b * u
        c = wx.get(t)
        rows.append(dict(time_utc=iso(t), time_tehran=iso(t, B.TEHRAN), traded=int(c is not None and c[4] > 0),
                         open=c[0] if c else "", high=c[1] if c else "", low=c[2] if c else "", close=c[3] if c else "",
                         volume_base=c[4] if c else 0, volume_tmn=round(c[4] * c[3]) if c else 0,
                         binance_usdt_close=b, usdttmn_close=u, fair_tmn=round(fair, 8),
                         premium_close_bps=round((c[3] / fair - 1) * 1e4, 2) if c else "",
                         premium_low_bps=round((c[2] / fair - 1) * 1e4, 2) if c else "",
                         premium_high_bps=round((c[1] / fair - 1) * 1e4, 2) if c else ""))
    return rows


def premium_stats(coin, rows):
    tr = [r for r in rows if r["traded"]]
    p = [r["premium_close_bps"] for r in tr]
    q = statistics.quantiles(p, n=100) if len(p) > 100 else [0] * 99
    base = []
    for i, r in enumerate(rows):  # 30-min trailing mean of the close premium, the bot's baseline
        if r["traded"]:
            base.append((i, r["premium_close_bps"]))
    dips = {e: 0 for e in (20, 40, 60, 80, 100)}
    win = []
    for i, prem in base:
        win = [x for x in win if x[0] > i - 30] + [(i, prem)]
        if len(win) < 10:
            continue
        mean = statistics.mean(x[1] for x in win[:-1]) if len(win) > 1 else prem
        low = rows[i]["premium_low_bps"]
        for e in dips:
            if low < mean - e:
                dips[e] += 1
    days = len(rows) / 1440
    out = dict(market=coin + "TMN", days=round(days, 2), minutes_traded_pct=round(len(tr) / len(rows) * 100, 1),
               avg_volume_tmn_per_day=round(sum(r["volume_tmn"] for r in rows) / days),
               premium_median_bps=round(statistics.median(p), 1), premium_sd_bps=round(statistics.pstdev(p), 1),
               premium_p5_bps=round(q[4], 1), premium_p95_bps=round(q[94], 1),
               premium_min_bps=round(min(p), 1), premium_max_bps=round(max(p), 1),
               **{f"dip_minutes_below_base_{e}bps_per_day": round(n / days, 1) for e, n in dips.items()})
    return out


async def grid(coins):
    rows, trades = [], []
    for coin in coins + ["ALL"]:
        B.COINS = coins if coin == "ALL" else [coin]
        u, series = T.load()
        t0 = max(min(u), *(min(w) for w, _ in series.values()))
        t1 = min(max(u), *(max(w) for w, _ in series.values()))
        mid = (t0 + t1) // 2
        for fm, ft in FEES:
            for edge in EDGES:
                B.FEE_MAKER, B.FEE_TAKER, B.ENTRY_EDGE = fm, ft, edge
                for name, a, b in (("train", t0, mid), ("test", mid, t1), ("full", t0, t1)):
                    r = await T.run(a, b, tag="study", quiet=True)
                    rows.append(dict(market=coin if coin == "ALL" else coin + "TMN", fee_maker_pct=fm * 100, entry_edge_pct=edge * 100,
                                     window=name, start_tehran=r["period"][0][:16], end_tehran=r["period"][1][:16],
                                     trades=r["trades"], wins=r["wins"], pnl_tmn=r["pnl_tmn"], return_pct=r["return_pct"],
                                     max_drawdown_pct=r["max_drawdown_pct"], stop_losses=r["exits"].get("stop_loss", 0),
                                     max_holds=r["exits"].get("max_hold", 0)))
                    if name == "full":
                        trades += [dict(run_market=rows[-1]["market"], fee_maker_pct=fm * 100, entry_edge_pct=edge * 100,
                                        market=x["sym"], opened_tehran=iso(x["open"], B.TEHRAN), closed_tehran=iso(x["close"], B.TEHRAN),
                                        hold_min=round((x["close"] - x["open"]) / 60), entry=x["entry"], exit=x["exit"],
                                        pnl_tmn=x["pnl_tmn"], pnl_pct=x["pnl_pct"], exit_reason=x["reason"]) for x in r["trade_list"]]
        print("backtested", coin, flush=True)
    return rows, trades


def main(coins):
    usdt = {int(k): v[3] for k, v in json.load(open(T.DATA / "USDT.json"))["wx"].items()}
    stats = []
    for c in coins:
        rows = candles(c, usdt)
        write(OUT / "candles" / f"{c}TMN.csv", rows)
        stats.append(premium_stats(c, rows))
        print("candles", c, len(rows), flush=True)
    write(OUT / "premium_stats.csv", stats)
    rows, trades = asyncio.run(grid(coins))
    write(OUT / "backtest_grid.csv", rows)
    write(OUT / "trades.csv", trades)


if __name__ == "__main__":
    main(sys.argv[1].split(","))
