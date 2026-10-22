"""Replay the exact live Bot over 1-minute history (Wallex UDF candles + Binance klines), AI judge off.

uv run python -m tiz.backtest fetch 3      # download last 3 days into data/
uv run python -m tiz.backtest              # run, writes results/backtest.{db,json,decisions.jsonl}
Fill model: a resting buy fills when a later minute's low trades below it, a sell when the high trades above it;
orders that cross the book fill at the close ± half spread (BT_SPREAD) as taker."""
import asyncio
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime

import httpx

from tiz import bot as B

DATA, OUT = B.ROOT / "data", B.ROOT / "results"
SPREAD = float(os.environ.get("BT_SPREAD", 0.001))


def fetch(days):
    now = int(time.time()) // 60 * 60
    frm = now - days * 86400

    def wallex(sym):
        c = {}
        for f in range(frm, now, 43200):  # UDF answers no_data for long ranges
            r = httpx.get("https://api.wallex.ir/v1/udf/history", timeout=30,
                          params={"symbol": sym, "resolution": "1", "from": f, "to": min(f + 43200, now)}).json()
            if r.get("s") == "ok":
                for i, t in enumerate(r["t"]):
                    c[t] = [float(r[k][i]) for k in ("o", "h", "l", "c", "v")]
        return c

    def binance(sym):
        out, s = {}, frm * 1000
        while s < now * 1000:
            k = httpx.get("https://data-api.binance.vision/api/v3/klines", timeout=30,
                          params={"symbol": sym, "interval": "1m", "startTime": s, "limit": 1000}).json()
            if not isinstance(k, list) or not k:
                break
            out.update({x[0] // 1000: float(x[4]) for x in k})
            s = k[-1][0] + 60000
        return out

    DATA.mkdir(exist_ok=True)
    for c in B.COINS:
        json.dump({"wx": wallex(c + "TMN"), "bn": binance(c + "USDT")}, open(DATA / f"{c}.json", "w"))
        print("fetched", c)
    json.dump({"wx": wallex("USDTTMN")}, open(DATA / "USDT.json", "w"))
    json.dump(httpx.get("https://api.wallex.ir/v1/markets", timeout=30).json(), open(DATA / "markets.json", "w"))


def load():
    usdt = {int(k): v[3] for k, v in json.load(open(DATA / "USDT.json"))["wx"].items()}
    series = {}
    for c in B.COINS:
        d = json.load(open(DATA / f"{c}.json"))
        series[c] = ({int(k): v for k, v in d["wx"].items()}, {int(k): v for k, v in d["bn"].items()})
    return usdt, series


async def run():
    usdt, series = load()
    info = json.load(open(DATA / "markets.json"))["result"]["symbols"]
    OUT.mkdir(exist_ok=True)
    for f in ("backtest.db", "backtest_decisions.jsonl"):
        (OUT / f).unlink(missing_ok=True)
    j = B.Journal(str(OUT / "backtest.db"), str(OUT / "backtest_decisions.jsonl"))
    markets = [B.Market(c + "TMN", int(info[c + "TMN"]["tickSize"]), int(info[c + "TMN"]["stepSize"])) for c in B.COINS]
    broker = B.Paper(5_000_000.0)
    bot = B.Bot(broker, j, markets)
    B.END_TS = float("inf")  # history is before the challenge end
    ts_all = sorted(set(usdt).intersection(*[set(w) & set(b) for w, b in series.values()]))
    u, h = None, SPREAD / 2
    for t in ts_all:
        u = usdt.get(t, u)
        now = t + 59.0
        for c, (w, bn) in series.items():
            o, hi, lo, cl, v = w[t]
            m = bot.m[c + "TMN"]
            await bot.check_fills(m, now, (lo * (1 - 2 * h), lo))  # someone sold down to the low
            await bot.check_fills(m, now, (hi, hi * (1 + 2 * h)))  # someone bought up to the high
            bot.on_depth(m.sym, "bid", [(cl * (1 - h), 0)], now)
            bot.on_depth(m.sym, "ask", [(cl * (1 + h), 0)], now)
            bot.on_ref(c, bn[t], now)
        bot.on_depth("USDTTMN", "bid", [(u - 1, 0)], now)
        bot.on_depth("USDTTMN", "ask", [(u + 1, 0)], now)
        await bot.tick(now)
    # mark-to-market and force-close anything left, like the end-of-challenge flatten
    B.END_TS = now
    bot.last_bal = 0
    await bot.tick(now + 1)
    rows = j.db.execute("select sym, ts_close, pnl_tmn, pnl_pct, reason from trades").fetchall()
    eq = bot.equity()
    by_sym, by_day, reasons = defaultdict(lambda: [0, 0.0]), Counter(), Counter(r[4] for r in rows)
    for s, tc, pnl, _, _ in rows:
        by_sym[s][0] += 1
        by_sym[s][1] += pnl
        by_day[datetime.fromtimestamp(tc, B.TEHRAN).date().isoformat()] += 1
    res = dict(period=[B.iso(ts_all[0]), B.iso(ts_all[-1])], minutes=len(ts_all), coins=B.COINS,
               params=dict(entry_edge=B.ENTRY_EDGE, fee_maker=B.FEE_MAKER, fee_taker=B.FEE_TAKER, pos_frac=B.POS_FRAC,
                           max_pos=B.MAX_POS, stop_loss=B.STOP_LOSS, max_hold=B.MAX_HOLD, spread=SPREAD),
               trades=len(rows), wins=sum(r[2] > 0 for r in rows), pnl_tmn=round(sum(r[2] for r in rows)),
               final_equity=round(eq), return_pct=round((eq / 5_000_000 - 1) * 100, 3),
               max_drawdown_pct=round(bot.max_dd * 100, 3), exits=dict(reasons), trades_per_day=dict(by_day),
               by_market={s: dict(trades=n, pnl_tmn=round(p)) for s, (n, p) in sorted(by_sym.items())},
               left_in_coins={k: v for k, v in broker.bal.items() if k != "TMN" and v > 1e-12})
    json.dump(res, open(OUT / "backtest.json", "w"), indent=1)
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    if sys.argv[1:2] == ["fetch"]:
        fetch(int(sys.argv[2]) if len(sys.argv) > 2 else 3)
    else:
        asyncio.run(run())
