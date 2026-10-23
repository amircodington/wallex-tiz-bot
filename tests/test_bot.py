import asyncio

import pytest

from tiz import bot as B

REF, USDT_MID = 80_000.0, 270_000.0
FAIR = REF * USDT_MID  # 21.6e9 TMN


class Mem(B.Journal):
    def __init__(self):
        super().__init__(":memory:", None)
        self.recs = []

    def decision(self, rec):
        self.recs.append(rec)

    def trades(self):
        return self.db.execute("select pnl_tmn, reason from trades").fetchall()


def make(monkeypatch, **kw):
    monkeypatch.setattr(B, "WARMUP", 10.0)
    monkeypatch.setattr(B, "POLL", 0.0)
    monkeypatch.setattr(B, "QUALIFY", 0)
    monkeypatch.setattr(B, "END_TS", 10_000_000.0)
    for k, v in kw.items():
        monkeypatch.setattr(B, k, v)
    return B.Bot(B.Paper(5_000_000.0), Mem(), [B.Market("BTCTMN", 0, 8)])


def feed(b, now, mid, spread=0.0004):
    b.on_depth("BTCTMN", "bid", [(mid * (1 - spread / 2), 0)], now)
    b.on_depth("BTCTMN", "ask", [(mid * (1 + spread / 2), 0)], now)
    b.on_depth("USDTTMN", "bid", [(USDT_MID - 1, 0)], now)
    b.on_depth("USDTTMN", "ask", [(USDT_MID + 1, 0)], now)
    b.on_ref("BTC", REF, now)


def run(b, t, mids):
    async def go():
        for mid in mids:
            feed(b, t[0], mid)
            await b.tick(t[0])
            t[0] += 1
    asyncio.run(go())


def order(b):
    return b.m["BTCTMN"].order


def test_round_trip_profit(monkeypatch):
    b, t = make(monkeypatch), [1000.0]
    run(b, t, [FAIR * 1.002] * 20)  # warm up at +0.2% premium
    assert order(b)["side"] == "BUY"
    bid_px = order(b)["price"]
    assert bid_px <= FAIR * 1.002 * (1 - B.ENTRY_EDGE) + 1
    run(b, t, [bid_px * 0.999] * 2)  # dip trades through our bid
    assert b.m["BTCTMN"].pos_qty > 0 and order(b)["side"] == "SELL"
    run(b, t, [FAIR * 1.002 * 1.006] * 3)  # premium reverts above baseline
    (pnl, reason), = b.j.trades()
    assert pnl > 0 and reason == "take_profit"
    assert b.broker.bal["TMN"] > 5_000_000
    assert any(r.get("decision") == "trade_closed" for r in b.j.recs)


def test_position_size_capped(monkeypatch):
    b, t = make(monkeypatch), [1000.0]
    run(b, t, [FAIR] * 20)
    o = b.broker.orders[order(b)["cid"]]
    assert o["price"] * o["qty"] <= 0.2 * 5_000_000 + 1


def test_stop_loss_and_daily_halt(monkeypatch):
    b, t = make(monkeypatch, DAILY_STOP=0.001), [1000.0]
    run(b, t, [FAIR] * 20)
    bid_px = order(b)["price"]
    run(b, t, [bid_px * 0.999] * 2)
    run(b, t, [bid_px * 0.97] * 3)  # crash through the stop
    pnl, reason = b.j.trades()[-1]
    assert reason == "stop_loss" and pnl < 0
    assert b.halted and order(b) is None  # no new entries after the daily stop


def test_ai_reject_blocks_entry(monkeypatch):
    b, t = make(monkeypatch), [1000.0]
    b.judge = B.Judge(enabled=True)  # no verdict yet → fail closed
    run(b, t, [FAIR] * 20)
    assert order(b) is None
    assert any(r.get("decision") == "skipped" for r in b.j.recs)
    b.judge.verdicts, b.judge.ts = {"BTCTMN": dict(verdict="approve", confidence=0.8, reason="ok")}, t[0]
    run(b, t, [FAIR] * 2)
    assert order(b)["side"] == "BUY"


def test_stale_feed_pauses(monkeypatch):
    b, t = make(monkeypatch), [1000.0]
    run(b, t, [FAIR] * 20)
    assert order(b)
    b.on_ref("BTC", REF, t[0] - 31)
    asyncio.run(b.tick(t[0]))
    assert order(b) is None and b.m["BTCTMN"].was_stale


def test_end_of_challenge_flattens(monkeypatch):
    b, t = make(monkeypatch), [1000.0]
    run(b, t, [FAIR] * 20)
    bid_px = order(b)["price"]
    run(b, t, [bid_px * 0.999] * 2)
    monkeypatch.setattr(B, "END_TS", t[0] + 100)  # inside the flatten window
    run(b, t, [bid_px * 0.999] * 3)
    assert b.m["BTCTMN"].pos_qty == 0 and b.j.trades()[-1][1] == "end_of_challenge"
    assert order(b) is None  # no new entries while closing


def test_live_parse():
    r = {"symbol": "BTCTMN", "side": "BUY", "price": "100", "origQty": "0.001", "executedQty": "0.001",
         "executedSum": "0.1", "active": False, "status": "FILLED", "fills": [{"fee": "0.000001", "feeAsset": "BTC"}]}
    st = B.Live._parse(r)
    assert st["done"] and st["avg"] == pytest.approx(100) and st["fee_base"] == pytest.approx(1e-6) and st["nfills"] == 1


def test_qualify_needs_history_and_reverting_dips(monkeypatch):
    b = make(monkeypatch, QUALIFY=1)
    m = b.m["BTCTMN"]
    assert not b.qualify(m, 0.0) and "history" in m.qual_why
    # 2 days of calm premium around a 0.2% baseline
    m.hist.extend((60.0 * i, 0.002, REF, 0.0004, 0.002) for i in range(2 * 1440))
    m.qual_ts = 0.0
    assert b.qualify(m, 2 * 86400.0)
    # same history but three deep dips that never come back
    m.hist.clear()
    m.hist.extend((60.0 * i, -0.01 if i % 600 == 0 else 0.002, REF, 0.0004, 0.002) for i in range(2 * 1440))
    m.qual_ts = 0.0
    assert b.qualify(m, 2 * 86400.0)  # spikes are rare, sd still small, and each dip reverts next minute
    m.hist.clear()
    m.hist.extend((60.0 * i, -0.01 if (i // 200) % 2 else 0.002, REF, 0.0004, 0.002) for i in range(2 * 1440))
    m.qual_ts = 0.0
    assert not b.qualify(m, 2 * 86400.0) and "reverted" in m.qual_why


def test_qualify_rejects_thin_market(monkeypatch):
    b = make(monkeypatch, QUAL_VOL=20e9)  # liquidity gate holds even with the structural check off
    m = b.m["BTCTMN"]
    m.hist.extend((60.0 * i, 0.002, REF, 0.0004, 0.002) for i in range(2 * 1440))
    m.vol_day = 5e9
    assert not b.qualify(m, 2 * 86400.0) and "thin" in m.qual_why


def test_warm_history_fills_qualifier(monkeypatch):
    from tiz import backtest as T
    n = 2 * 1440
    t1 = 10 * 86400
    monkeypatch.setattr(T, "fetch_wallex", lambda sym, frm, to: {
        t: [USDT_MID] * 4 + [1.0] if sym == "USDTTMN" else [FAIR * 1.002] * 4 + [25e9 / 1440 / (FAIR * 1.002)]
        for t in range(to - n * 60, to, 60)})
    monkeypatch.setattr(T, "fetch_binance", lambda sym, frm, to: {t: REF for t in range(to - n * 60, to, 60)})
    monkeypatch.setattr(B.time, "time", lambda: float(t1))
    b = make(monkeypatch, QUALIFY=1)
    B.warm_history(b, days=2.0)
    m = b.m["BTCTMN"]
    assert len(m.hist) == n and abs(m.base - 0.002) < 1e-9 and m.vol_day == pytest.approx(25e9, rel=1e-6)
    assert b.qualify(m, float(t1))


def test_exit_decay_slides_to_breakeven(monkeypatch):
    b = make(monkeypatch, EXIT_DECAY=1, MAX_HOLD=4000.0, DECAY_AFTER=2000.0, FEE_MAKER=0.0, MIN_PROFIT=0.0, EXIT_EDGE=0.0)
    m = b.m["BTCTMN"]
    m.pos_qty, m.pos_cost, m.pos_ts, m.entry_base = 1.0, 100.0, 0.0, 0.0
    assert b.take_price(m, 110.0, 0.0, 2000.0) == 110  # full target until DECAY_AFTER
    assert b.take_price(m, 110.0, 0.0, 3000.0) == 105  # halfway down
    assert b.take_price(m, 110.0, 0.0, 4000.0) == 100  # break-even at MAX_HOLD


def _trailing(monkeypatch, mode):
    b, t = make(monkeypatch, TRAIL=mode), [1000.0]
    run(b, t, [FAIR * 1.002] * 20)
    run(b, t, [order(b)["price"] * 0.999] * 2)  # buy fills, no resting take-profit order
    m = b.m["BTCTMN"]
    tp = b.take_price(m, m.fair, 0.0, t[0])
    assert m.pos_qty > 0 and order(b) is None
    run(b, t, [tp * 1.001] * 2)  # take-profit level reached: trailing, still holding
    assert m.pos_qty > 0 and m.trail_hi
    return b, t, m, tp


def test_trail_locks_profit(monkeypatch):
    b, t, m, tp = _trailing(monkeypatch, "atr")
    run(b, t, [tp * 1.010] * 3 + [tp * 1.007] * 2)  # runs further up, then pulls back
    (pnl, reason), = b.j.trades()
    exit_px = b.j.db.execute("select exit from trades").fetchone()[0]
    assert reason == "trail" and pnl > 0 and exit_px > tp


def test_trail_floor(monkeypatch):
    b, t, m, tp = _trailing(monkeypatch, "sar")
    asyncio.run(b.check_fills(m, t[0], (tp * 0.99, tp * 0.99)))  # immediate collapse inside a minute
    (pnl, reason), = b.j.trades()
    exit_px = b.j.db.execute("select exit from trades").fetchone()[0]
    assert reason == "trail" and exit_px == pytest.approx(tp * (1 - B.TRAIL_GIVEBACK))


def test_ai_http_reads_openai_reply(monkeypatch):
    from tiz import ai_http
    monkeypatch.setenv("AI_BASE_URL", "https://gw.example/v1/")
    monkeypatch.setenv("AI_API_KEY", "k")
    seen = {}

    class R:
        def raise_for_status(self):
            pass

        def json(self):
            return {"choices": [{"message": {"content": '{"markets": {}}'}}]}

    monkeypatch.setattr(ai_http.httpx, "post", lambda url, **kw: seen.update(url=url, **kw) or R())
    assert ai_http.ask("hi") == '{"markets": {}}'
    assert seen["url"] == "https://gw.example/v1/chat/completions" and seen["headers"]["Authorization"] == "Bearer k"


def test_live_equity_counts_locked_balance(monkeypatch):
    class Rest:
        async def balances(self):
            return {"TMN": (500_000.0, 4_500_000.0)}  # three resting buys lock 4.5M

    b = make(monkeypatch)
    b.broker = B.Live(Rest())
    b.m["BTCTMN"].book["bid"] = (FAIR, 0.0)
    b.bal = asyncio.run(b.broker.balances())
    assert b.equity() == 5_000_000


def test_chat_settings_stay_inside_limits():
    from tiz import chat, dash
    s, dropped = chat.clean_set({"live": {"POS_FRAC": 0.34, "MAX_POS": 3.0, "ENTRY_EDGE": 0.003, "AI": "off", "MODE": "x"},
                                 "other": {"POS_FRAC": 0.1}}, dash.LIMITS, dash.AI_CMDS)
    assert s == {"live": {"POS_FRAC": 0.34, "MAX_POS": 3, "AI": "off"}}
    assert dropped == ["live.ENTRY_EDGE=0.003", "live.MODE=x"]
    assert chat.parse('ok ```{"reply": "سلام", "set": {}}```')["reply"] == "سلام"
    assert chat.parse("plain text")["reply"] == "plain text"


def test_place_422_alerts_only_when_it_lasts(monkeypatch):
    b = make(monkeypatch)
    m, sent, fails = b.m["BTCTMN"], [], [4]
    b.alert = sent.append
    real = b.broker.place

    async def flaky(*a):
        if fails[0]:
            fails[0] -= 1
            raise B.WallexError("POST /v1/account/orders -> 422 422")
        return await real(*a)
    b.broker.place = flaky
    go = lambda: asyncio.run(b.place(m, 1000.0, "BUY", FAIR * 0.99, 0.0001, "entry", (FAIR, FAIR), None, None, {}))
    for _ in range(2):
        go()
    assert sent == [] and m.order is None  # the usual post-cancel 422: retried quietly
    go()
    assert len(sent) == 1 and "3 in a row" in sent[0] and sent[0].startswith("🚨 <b>Error</b>")
    go(), go()
    assert m.order and m.place_fails == 0 and len(sent) == 1


def test_telegram_alerts_formatted_and_not_repeated(monkeypatch):
    b, sent = make(monkeypatch), []
    b.alert = sent.append
    for _ in range(3):
        b.event("error", "ConnectTimeout('<x>')")  # an outage: same error every 11 s
    b.event("warm", "BTCTMN: 72h of history")
    b.event("trade", "BTCTMN SELL 0.1 @ 1 (take_profit) PnL +4,484 TMN (+0.20%)")
    assert len(sent) == 2 and "&lt;x&gt;" in sent[0] and "Signal Ops" in sent[0]
    assert sent[1].startswith("✅ <b>Trade closed in profit</b>")


def test_network_blip_is_quiet_and_outage_alerts_once(monkeypatch):
    b, sent = make(monkeypatch), []
    b.alert = sent.append
    for t in range(0, 30, 11):
        b.trouble("net", "error", "ConnectTimeout('')", 1000.0 + t, B.NET_GRACE)
    b.clear("net")
    assert sent == []  # a 30 s blip: log only
    for t in range(0, 200, 11):
        b.trouble("net", "error", "ConnectTimeout('')", 2000.0 + t, B.NET_GRACE)
    b.clear("net")
    assert len(sent) == 2 and "for 66s" in sent[0] and sent[1].startswith("✅ <b>Back to normal</b>")


def test_trend_mode_buys_a_rise_and_sells_above_fees(monkeypatch):
    b, t = make(monkeypatch, STRATEGY="trend", STOP_LOSS=0.01), [1000.0]
    async def climb():  # Wallex and the global price rise together: +0.96% in 40 min
        for i in range(2400):
            feed(b, t[0], FAIR * (1 + 0.000004 * i))
            b.on_ref("BTC", REF * (1 + 0.000004 * i), t[0])
            await b.tick(t[0])
            t[0] += 1
    asyncio.run(climb())
    assert order(b)["side"] == "BUY" and abs(order(b)["price"] / b.m["BTCTMN"].book["bid"][0] - 1) <= 2 * B.REQUOTE
    mid = FAIR * (1 + 0.000004 * 2400)
    run(b, t, [mid * 0.998] * 2)  # trades through our bid
    m = b.m["BTCTMN"]
    entry = m.pos_cost / m.pos_qty
    assert m.pos_qty > 0 and order(b)["side"] == "SELL"
    assert order(b)["price"] >= entry * (1 + B.FEE_MAKER + B.TREND_TP)
