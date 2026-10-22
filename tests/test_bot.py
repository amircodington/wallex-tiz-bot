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
