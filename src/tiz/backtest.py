"""Replay the exact live Bot over 1-minute history (Wallex UDF candles + Binance klines), AI judge off.

uv run python -m tiz.backtest fetch 3      # download last 3 days into data/
uv run python -m tiz.backtest              # run, writes results/backtest.{db,json,decisions.jsonl}
Fill model: a resting buy fills when a later minute's low trades BT_MARGIN below it and that minute traded at least
BT_MIN_VOL TMN; a sell likewise with the high;
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
MARGIN = float(os.environ.get("BT_MARGIN", 0.0005))
MIN_VOL = float(os.environ.get("BT_MIN_VOL", 1_000_000))  # TMN traded in the minute


def get(url, params):
    for i in range(5):  # Wallex/Binance mirrors drop connections now and then
        try:
            return httpx.get(url, params=params, timeout=30).json()
        except httpx.HTTPError:
            time.sleep(2 ** i)
    raise SystemExit(f"giving up on {url} {params}")


def fetch(days, coins):
    now = int(time.time()) // 60 * 60
    frm = now - days * 86400

    def wallex(sym):
        c = {}
        for f in range(frm, now, 43200):  # UDF answers no_data for long ranges
            r = get("https://api.wallex.ir/v1/udf/history",
                    {"symbol": sym, "resolution": "1", "from": f, "to": min(f + 43200, now)})
            if r.get("s") == "ok":
                for i, t in enumerate(r["t"]):
                    c[t] = [float(r[k][i]) for k in ("o", "h", "l", "c", "v")]
        return c

    def binance(sym):
        out, s = {}, frm * 1000
        while s < now * 1000:
            k = get("https://data-api.binance.vision/api/v3/klines",
                    {"symbol": sym, "interval": "1m", "startTime": s, "limit": 1000})
            if not isinstance(k, list) or not k:
                break
            out.update({x[0] // 1000: float(x[4]) for x in k})
            s = k[-1][0] + 60000
        return out

    DATA.mkdir(exist_ok=True)
    for c in coins:
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


async def run(t_from=None, t_to=None, tag="backtest", quiet=False):
    """Every minute in [t_from, t_to): missing candles (no trades that minute) carry the last close forward."""
    usdt, series = load()
    info = json.load(open(DATA / "markets.json"))["result"]["symbols"]
    OUT.mkdir(exist_ok=True)
    for f in () if quiet else (f"{tag}.db", f"{tag}_decisions.jsonl"):
        (OUT / f).unlink(missing_ok=True)
    j = B.Journal(":memory:", None) if quiet else B.Journal(str(OUT / f"{tag}.db"), str(OUT / f"{tag}_decisions.jsonl"))
    markets = [B.Market(c + "TMN", int(info[c + "TMN"]["tickSize"]), int(info[c + "TMN"]["stepSize"])) for c in B.COINS]
    broker = B.Paper(5_000_000.0)
    bot = B.Bot(broker, j, markets)
    B.END_TS = float("inf")  # history is before the challenge end
    lo_t = max(min(usdt), *(max(min(w), min(b)) for w, b in series.values()), t_from or 0)
    hi_t = min(max(usdt), *(min(max(w), max(b)) for w, b in series.values()), t_to or float("inf"))
    last = {c: None for c in series}
    u, h, equity = None, SPREAD / 2, []
    for t in range(lo_t // 60 * 60, int(hi_t), 60):
        u = usdt.get(t, u)
        now = t + 59.0
        for c, (w, bn) in series.items():
            m = bot.m[c + "TMN"]
            if t in w:
                o, hi, lo, cl, v = w[t]
                if v * cl >= MIN_VOL:  # a thin print can't have filled our ~1M TMN order
                    lo_, hi_ = lo * (1 + MARGIN), hi * (1 - MARGIN)  # price must trade through us, not just touch
                    await bot.check_fills(m, now, (lo_ * (1 - 2 * h), lo_))  # someone sold down to the low
                    await bot.check_fills(m, now, (hi_, hi_ * (1 + 2 * h)))  # someone bought up to the high
                last[c] = cl
            if bn.get(t):
                bot.on_ref(c, bn[t], now)
            if last[c]:
                bot.on_depth(m.sym, "bid", [(last[c] * (1 - h), 0)], now)
                bot.on_depth(m.sym, "ask", [(last[c] * (1 + h), 0)], now)
        if u is None or not all(last.values()):
            continue
        bot.on_depth("USDTTMN", "bid", [(u - 1, 0)], now)
        bot.on_depth("USDTTMN", "ask", [(u + 1, 0)], now)
        await bot.tick(now)
        if t % 1800 == 0:
            equity.append([t, round(bot.equity())])
    # force-close anything left, like the end-of-challenge flatten
    B.END_TS = now
    bot.last_bal = 0
    await bot.tick(now + 1)
    equity.append([int(now), round(bot.equity())])
    rows = j.db.execute("select sym, ts_open, ts_close, entry, exit, pnl_tmn, pnl_pct, reason from trades").fetchall()
    eq = bot.equity()
    by_sym, by_day, reasons = defaultdict(lambda: [0, 0.0, 0]), Counter(), Counter(r[7] for r in rows)
    for s, _, tc, _, _, pnl, _, _ in rows:
        by_sym[s][0] += 1
        by_sym[s][1] += pnl
        by_sym[s][2] += pnl > 0
        by_day[datetime.fromtimestamp(tc, B.TEHRAN).date().isoformat()] += 1
    res = dict(tag=tag, period=[B.iso(lo_t), B.iso(hi_t)], days=round((hi_t - lo_t) / 86400, 2), coins=B.COINS,
               params=dict(entry_edge=B.ENTRY_EDGE, fee_maker=B.FEE_MAKER, fee_taker=B.FEE_TAKER, pos_frac=B.POS_FRAC,
                           max_pos=B.MAX_POS, stop_loss=B.STOP_LOSS, max_hold=B.MAX_HOLD, spread=SPREAD,
                           fill_margin=MARGIN, min_minute_volume_tmn=MIN_VOL),
               trades=len(rows), wins=sum(r[5] > 0 for r in rows), pnl_tmn=round(sum(r[5] for r in rows)),
               final_equity=round(eq), return_pct=round((eq / 5_000_000 - 1) * 100, 3),
               max_drawdown_pct=round(bot.max_dd * 100, 3), exits=dict(reasons), trades_per_day=dict(sorted(by_day.items())),
               by_market={s: dict(trades=n, wins=wn, pnl_tmn=round(p)) for s, (n, p, wn) in sorted(by_sym.items())},
               equity=equity,
               trade_list=[dict(sym=r[0], open=int(r[1]), close=int(r[2]), entry=r[3], exit=r[4], pnl_tmn=round(r[5]),
                                pnl_pct=round(r[6] * 100, 3), reason=r[7]) for r in rows])
    if not quiet:
        json.dump(res, open(OUT / f"{tag}.json", "w"), indent=1)
    if not quiet:
        print(json.dumps({k: v for k, v in res.items() if k not in ("equity", "trade_list")}, indent=1))
    return res


async def sweep():
    """Pick the entry edge on the first half (train), report it on the unseen second half (test)."""
    usdt, series = load()
    t0 = max(min(usdt), *(min(w) for w, _ in series.values()))
    t1 = min(max(usdt), *(max(w) for w, _ in series.values()))
    mid = (t0 + t1) // 2
    out = []
    for fee in (0.0025, 0.0):
        for edge in (0.004, 0.005, 0.006, 0.008, 0.010, 0.012):
            B.FEE_MAKER = B.FEE_TAKER = fee
            if fee:
                B.FEE_TAKER = 0.003
            B.ENTRY_EDGE = edge
            row = dict(fee=fee, edge=edge)
            for name, a, b in (("train", t0, mid), ("test", mid, t1), ("full", t0, t1)):
                r = await run(a, b, tag=f"sweep_{name}", quiet=True)
                row[name] = {k: r[k] for k in ("trades", "wins", "return_pct", "max_drawdown_pct", "period")}
            out.append(row)
            print(fee, edge, {k: (row[k]["trades"], row[k]["return_pct"], row[k]["max_drawdown_pct"])
                              for k in ("train", "test", "full")}, flush=True)
    json.dump(out, open(OUT / "sweep.json", "w"), indent=1)


if __name__ == "__main__":
    if sys.argv[1:2] == ["fetch"]:
        fetch(int(sys.argv[2]) if len(sys.argv) > 2 else 3, sys.argv[3].split(",") if len(sys.argv) > 3 else B.COINS)
    elif sys.argv[1:2] == ["sweep"]:
        asyncio.run(sweep())
    else:
        asyncio.run(run())
