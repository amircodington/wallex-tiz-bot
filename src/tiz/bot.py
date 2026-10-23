"""TIZ: Wallex premium mean-reversion bot with an AI judge and a hard risk gate.

Signal (rule): fair = Binance <COIN>USDT mid × Wallex USDTTMN mid (REF_SOURCE=wallex: Wallex's own <COIN>USDT mid). Each Wallex <COIN>TMN market trades at a
slowly drifting premium to fair; we track it with an EMA (baseline). A dip of ENTRY_EDGE below baseline-fair
is a liquidity gap that historically closes within the hour, so we rest a limit buy there and sell back at
baseline-fair (never below breakeven + fees + MIN_PROFIT).
AI judge: every AI_EVERY seconds an LLM reads per-market context and approves/rejects quoting each market,
with a written reason and confidence. Only approved markets get entry orders; ranking by confidence fills slots.
Risk gate (code, AI cannot override): POS_FRAC per position, MAX_POS concurrent (rest stays TMN), STOP_LOSS,
MAX_HOLD, DAILY_STOP, stale-feed pause, no entries ENTRY_CUTOFF before END_TS, flatten FLATTEN_BEFORE END_TS.
Run: uv run python -m tiz.bot   (MODE=paper → shadow test on live data; MODE=live → real orders)."""
import asyncio
import json
import logging
import math
import os
import shlex
import sqlite3
import statistics
import tempfile
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import websockets

from tiz.wallex import Rest, WallexError, stream_depth

log = logging.getLogger("tiz")
ROOT = Path(__file__).resolve().parents[2]
TEHRAN = timezone(timedelta(hours=3, minutes=30))


def load_env(path=ROOT / ".env"):
    if os.path.exists(path):
        for line in open(path):
            k, sep, v = line.strip().partition("=")
            if sep and k and not k.startswith("#"):
                os.environ.setdefault(k.strip(), v.split(" #")[0].strip().strip('"'))


load_env()


def env(k, d):
    return type(d)(os.environ.get(k, d))


MODE = env("MODE", "paper")
COINS = env("COINS", "TRX,BTC,BNB,ETH,XRP,DOGE,XAUT").split(",")  # markets that revert reliably (study/, 14 days)
# watched from the start but traded only after a real fill proves fees are 0: their dips pay only without fees
ZERO_FEE_COINS = [c for c in env("ZERO_FEE_COINS", "PEPE,PUMP,SOL,ARB,S").split(",") if c and c not in COINS]
FEE_MAKER = env("FEE_MAKER", 0.0025)  # /v1/account/fee for TMN markets on 2026-10-22; live re-reads it
FEE_TAKER = env("FEE_TAKER", 0.003)
ENTRY_EDGE = env("ENTRY_EDGE", 0.008)  # buy this far below baseline-fair
ZERO_FEE_EDGE = env("ZERO_FEE_EDGE", 0.005)  # used once a real fill proves fees are 0 (backtest optimum at fee 0)
EXIT_EDGE = env("EXIT_EDGE", 0.0)  # sell this far above baseline-fair
MIN_PROFIT = env("MIN_PROFIT", 0.001)  # net of both fees
STOP_LOSS = env("STOP_LOSS", 0.03)  # 1.5% cut dips that later reverted
MAX_HOLD = env("MAX_HOLD", 4 * 3600.0)
EXIT_DECAY = env("EXIT_DECAY", 0)  # after DECAY_AFTER the sell target slides linearly to break-even + MIN_PROFIT at MAX_HOLD
DECAY_AFTER = env("DECAY_AFTER", MAX_HOLD / 2)
TRAIL = env("TRAIL", "off")  # off | atr | sar: past the take-profit level, follow the price up and sell on a pullback
TRAIL_K = env("TRAIL_K", 2.0)  # atr: stop = high - K x (sd of 1-min mid returns, last 30 min) x high
TRAIL_GIVEBACK = env("TRAIL_GIVEBACK", 0.002)  # never give back more than this below the take-profit level
ORDER_TTL = env("ORDER_TTL", 120.0)
PLACE_ALERT = env("PLACE_ALERT", 3)  # alert after this many failed places in a row on one market
PULL = env("PULL", 0.003)  # a buy this far above its ideal price is pulled at once, skipping the status-poll wait
REQUOTE = env("REQUOTE", 0.0005)  # replace a resting order once its ideal price moved this much (stale quotes get picked off)
QUALIFY = env("QUALIFY", 0)  # structural check per market on its last QUAL_DAYS of premiums; off: it cost money on safe-7 (README)
QUAL_DAYS = env("QUAL_DAYS", 3.0)
QUAL_MIN_HOURS = env("QUAL_MIN_HOURS", 24.0)  # less history than this: market not qualified yet
QUAL_SD = env("QUAL_SD", 0.005)  # premium standard deviation must stay below 50 bps
QUAL_DIP = env("QUAL_DIP", 0.006)  # a dip = close premium this far below baseline
QUAL_REV = env("QUAL_REV", 0.9)  # share of dips back at baseline within 60 min
QUAL_VOL = env("QUAL_VOL", 20e9)  # mean TMN traded per day over QUAL_DAYS (0 = off); thin books don't absorb our size
SHOCK_SEC = env("SHOCK_SEC", 300.0)
SHOCK_REF = env("SHOCK_REF", 0.005)  # no entries after Binance falls this much within SHOCK_SEC
SHOCK_USDT = env("SHOCK_USDT", 0.003)  # ... or USDTTMN moves this much
POS_FRAC = env("POS_FRAC", 0.2)  # share of equity per position
MAX_POS = env("MAX_POS", 3)  # positions + open entry orders; 3 × 20% keeps 40% in TMN
DAILY_STOP = env("DAILY_STOP", 0.03)
STALE = env("STALE", 30.0)
POLL = env("POLL", 3.0)  # seconds between order-status checks per order
BASELINE_SEC = env("BASELINE_SEC", 1800.0)
WARMUP = env("WARMUP", 600.0)
END_TS = datetime.fromisoformat(env("END_TS", "2026-10-23T23:50:00+03:30")).timestamp()
ENTRY_CUTOFF = env("ENTRY_CUTOFF", 3 * 3600.0)
FLATTEN_BEFORE = env("FLATTEN_BEFORE", 3600.0)
AI_EVERY = env("AI_EVERY", 300.0)
REF_SOURCE = env("REF_SOURCE", "auto")  # auto | binance | wallex: <COIN>USDT reference; auto = binance unless it refuses us (451 on Iran IPs)
AI_CMD = env("AI_CMD", "claude -p --model haiku")  # any CLI that reads a prompt on stdin and prints JSON
AI_MAX_AGE = env("AI_MAX_AGE", 900.0)  # older verdicts count as reject
MIN_NOTIONAL = 50_000  # TMN, /v1/markets
PAPER_TMN = env("PAPER_TMN", 5_000_000.0)
DB = env("DB", str(ROOT / "journal.db"))
DECISIONS = env("DECISIONS", str(ROOT / "decisions.jsonl"))


def disallowed():
    p = ROOT / "config" / "disallowed_markets.txt"
    return {ln.strip().replace("/", "") for ln in open(p) if ln.strip() and not ln.startswith("#")}


def rnd(x, decimals, up=False):
    f = 10 ** decimals
    return (math.ceil if up else math.floor)(x * f + (-1e-9 if up else 1e-9)) / f


# ---------------------------------------------------------------- brokers
class Paper:
    """Shadow broker: a resting order fills when the opposite best price reaches it (full fill)."""

    def __init__(self, tmn):
        self.bal, self.orders = {"TMN": tmn}, {}

    async def balances(self):
        return dict(self.bal)

    async def place(self, m, side, price, qty, book):
        sym = m.sym
        cid = "P" + uuid.uuid4().hex[:12]
        self.orders[cid] = o = dict(sym=sym, side=side, price=price, qty=qty, filled=0.0, avg=0.0,
                                    fee_base=0.0, fee_tmn=0.0, done=False)
        need = price * qty if side == "BUY" else qty
        if self.bal.get("TMN" if side == "BUY" else sym[:-3], 0) + 1e-9 < need:
            raise WallexError(f"paper: insufficient balance for {side} {qty} {sym}")
        self._match(o, book, taker=True)
        return cid

    def _match(self, o, book, taker=False):
        bid, ask = book
        base = o["sym"][:-3]
        if o["done"]:
            return
        if o["side"] == "BUY" and ask <= o["price"]:
            px, fee = (ask, FEE_TAKER) if taker else (o["price"], FEE_MAKER)
            self.bal["TMN"] -= px * o["qty"]
            o["fee_base"] = o["qty"] * fee
            self.bal[base] = self.bal.get(base, 0) + o["qty"] - o["fee_base"]
        elif o["side"] == "SELL" and bid >= o["price"]:
            px, fee = (bid, FEE_TAKER) if taker else (o["price"], FEE_MAKER)
            self.bal[base] = self.bal.get(base, 0) - o["qty"]
            o["fee_tmn"] = px * o["qty"] * fee
            self.bal["TMN"] += px * o["qty"] - o["fee_tmn"]
        else:
            return
        o.update(filled=o["qty"], avg=px, done=True)

    async def status(self, cid, book):
        o = self.orders[cid]
        self._match(o, book)
        return o

    async def cancel(self, cid, book):
        o = await self.status(cid, book)
        o["done"] = True
        return o


class Live:
    def __init__(self, rest: Rest):
        self.rest = rest

    async def balances(self):
        # total, not free: TMN locked in our resting buys is still ours (free only read equity -90% and tripped the daily stop)
        return {a: free + locked for a, (free, locked) in (await self.rest.balances()).items()}

    async def place(self, m, side, price, qty, book):
        cid = "T" + uuid.uuid4().hex[:16].upper()
        await self.rest.place(m.sym, side, f"{price:.{m.tick}f}", f"{qty:.{m.step}f}", cid)
        return cid

    @staticmethod
    def _parse(r):
        fills = r.get("fills") or []
        quote = r.get("symbol", "")[-3:]
        fee_tmn = sum(float(f["fee"]) for f in fills if f.get("feeAsset") == quote)
        fee_base = sum(float(f["fee"]) for f in fills if f.get("feeAsset") not in (quote, None))
        filled = float(r.get("executedQty") or 0)
        avg = float(r.get("executedSum") or 0) / filled if filled else 0.0
        done = r.get("active") is False or r.get("status") in ("FILLED", "CANCELED", "EXPIRED", "REJECTED")
        return dict(side=r.get("side"), price=float(r.get("price") or 0), qty=float(r.get("origQty") or 0),
                    filled=filled, avg=avg, fee_base=fee_base, fee_tmn=fee_tmn, done=done, nfills=len(fills))

    async def status(self, cid, book):
        return self._parse(await self.rest.order(cid))

    async def cancel(self, cid, book):
        try:
            await self.rest.cancel(cid)
        except WallexError as e:  # already filled/cancelled; the re-read below is the truth
            log.info("cancel %s: %s", cid, e)
        await asyncio.sleep(0.5)
        return self._parse(await self.rest.order(cid))


# ---------------------------------------------------------------- journal
class Journal:
    def __init__(self, path=DB, decisions=DECISIONS):
        self.db = sqlite3.connect(path)
        self.db.executescript("""
        create table if not exists samples(ts real, sym text, bid real, ask real, fair real, prem real, base real);
        create table if not exists orders(ts real, mode text, sym text, cid text, side text, price real, qty real, reason text);
        create table if not exists fills(ts real, mode text, sym text, cid text, side text, price real, qty real,
                                         fee_base real, fee_tmn real);
        create table if not exists trades(ts_open real, ts_close real, mode text, sym text, qty real, entry real,
                                          exit real, pnl_tmn real, pnl_pct real, reason text);
        create table if not exists events(ts real, kind text, msg text);""")
        self.decisions = open(decisions, "a") if decisions else None

    def add(self, table, *row):
        self.db.execute(f"insert into {table} values ({','.join('?' * len(row))})", row)
        self.db.commit()

    def decision(self, rec):
        if self.decisions:
            self.decisions.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.decisions.flush()


# ---------------------------------------------------------------- AI judge
class Judge:
    PROMPT = """You are the risk-aware trade judge of an automated spot bot on the Iranian exchange Wallex.
Strategy: in each approved market the bot keeps a resting limit BUY {edge_bps:.0f} bps below "baseline fair"
(global <COIN>USDT price × Wallex USDT/TMN, adjusted by the market's own 30-min average premium). It only fills if a
seller on Wallex suddenly dumps price that far below the global price; it then sells when the premium returns to
baseline (max hold {hold_h:.0f}h, stop loss {stop_bps:.0f} bps). A dip does NOT need to exist now: you decide whether the
market is in a state where such a sudden dip is likely a temporary liquidity gap that reverts (good), or a real
move that keeps going (bad: the global USDT price trending hard, Wallex premium drifting steadily in one direction, recent
losses in that market). The baseline level itself (positive or negative) is irrelevant because the bot
already prices relative to it; what matters is whether the premium is stable around it. Fees are {fee_bps:.0f} bps per side. All context numbers are in basis points
(1 bp = 0.01%). For scale: premium noise of 5-20 bps and spreads of 3-40 bps are normal on Wallex; a 2h
premium range below ~{edge_bps:.0f} bps is calm. Approve calm, mean-reverting markets; reject only with a concrete
reason. Regime change is a reason: reject when the 6h premium sd is at least twice the 3-day sd (the premium
has stopped behaving like its history), or when the 3-day history is too short to tell. You do NOT size or place orders; hard risk rules are enforced elsewhere. {slots} slots are free.

Context (JSON):
{ctx}

Reply with ONLY a JSON object, no prose, no code fence:
{{"markets": {{"<SYMBOL>": {{"verdict": "approve"|"reject", "confidence": 0.0-1.0, "reason": "<= 20 words"}}}}}}"""

    def __init__(self, enabled=True):
        self.enabled, self.verdicts, self.ts, self.busy = enabled, {}, 0.0, False

    def get(self, sym, now):
        if not self.enabled:
            return dict(verdict="approve", confidence=1.0, reason="AI disabled (backtest): rules only")
        v = self.verdicts.get(sym)
        if v is None or now - self.ts > AI_MAX_AGE:
            return dict(verdict="reject", confidence=0.0, reason="no fresh AI verdict")
        return v

    async def refresh(self, ctx, slots, now):
        self.busy = True
        try:
            prompt = self.PROMPT.format(edge_bps=ENTRY_EDGE * 1e4, stop_bps=STOP_LOSS * 1e4, fee_bps=FEE_MAKER * 1e4,
                                         hold_h=MAX_HOLD / 3600,
                                         slots=slots, ctx=json.dumps(ctx, indent=1))
            p = await asyncio.create_subprocess_exec(
                *shlex.split(AI_CMD), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE, cwd=tempfile.gettempdir())  # neutral cwd: no project context
            out, err = await asyncio.wait_for(p.communicate(prompt.encode()), 120)
            text = out.decode()
            raw = json.loads(text[text.index("{"): text.rindex("}") + 1])["markets"]
            self.verdicts = {s: dict(verdict=str(v.get("verdict", "reject")).lower(),
                                     confidence=float(v.get("confidence", 0)), reason=str(v.get("reason", "")))
                             for s, v in raw.items()}
            self.ts = now
            return self.verdicts
        except Exception as e:  # any AI failure → verdicts age out → reject (fail closed)
            log.warning("AI judge failed: %r", e)
            return None
        finally:
            self.busy = False


# ---------------------------------------------------------------- market state
class Market:
    def __init__(self, sym, tick, step, active=True):
        self.sym, self.base_asset, self.tick, self.step, self.active = sym, sym[:-3], tick, step, active
        self.book = {"bid": (0.0, 0.0), "ask": (0.0, 0.0)}
        self.ref = (0.0, 0.0)
        self.base, self.n = None, 0
        self.order = None
        self.place_fails = 0  # place errors in a row; Wallex answers 422 for a few s after a cancel, until it frees the TMN
        self.pos_qty = self.pos_cost = self.pos_ts = 0.0
        self.hist = deque(maxlen=int(3 * 1440))  # (ts, prem, ref, spread, base, mid) per minute: AI context, qualifier, trail
        self.pnls = deque(maxlen=5)
        self.last_hist = self.last_sample = self.last_step = 0.0
        self.was_stale = False
        self.last_reject = None
        self.fair = 0.0
        self.prox = 1.0  # ask / our buy price - 1: how close a fill is, for ranking slots
        self.entry_base = 0.0  # baseline frozen at entry, so the exit target doesn't drift with the dip
        self.refs = deque(maxlen=400)  # (ts, binance mid) for the shock gate
        self.qual, self.qual_ts, self.qual_why = False, 0.0, "no history yet"
        self.vol_day = float("inf")  # mean TMN/day, set once by warm_history / the backtest
        self.trail_hi = None  # highest bid since the take-profit level was reached; None = not trailing


class Bot:
    def __init__(self, broker, journal, markets, judge=None, alert=lambda msg: None, now=None):
        self.broker, self.j, self.alert = broker, journal, alert
        self.judge = judge or Judge(enabled=False)
        self.m = {m.sym: m for m in markets}
        self.usdt = {"bid": (0.0, 0.0), "ask": (0.0, 0.0)}
        self.usdts = deque(maxlen=400)
        self.bal, self.last_bal, self.adopted = {}, 0.0, False
        self.day, self.day_equity, self.halted, self.peak, self.max_dd = None, None, False, 0.0, 0.0
        self.t0 = now

    # --- feeds
    def on_depth(self, sym, side, levels, now=None):
        if not levels:
            return
        now = now or time.time()
        if sym == "USDTTMN":
            self.usdt[side] = (levels[0][0], now)
        elif sym in self.m:
            self.m[sym].book[side] = (levels[0][0], now)

    def on_usdt_tick(self, now):
        mid = (self.usdt["bid"][0] + self.usdt["ask"][0]) / 2
        if mid and (not self.usdts or now - self.usdts[-1][0] >= 10):
            self.usdts.append((now, mid))

    def on_ref(self, coin, mid, now=None):
        m = self.m.get(coin + "TMN")
        if m:
            m.ref = (mid, now or time.time())

    def stale(self, m, now):
        ts = [m.ref[1], m.book["bid"][1], m.book["ask"][1], self.usdt["bid"][1], self.usdt["ask"][1]]
        return now - min(ts) > STALE

    def equity(self):
        return self.bal.get("TMN", 0) + sum(self.bal.get(m.base_asset, 0) * m.book["bid"][0] for m in self.m.values())

    def phase(self, now):
        left = END_TS - now
        return "flatten" if left <= FLATTEN_BEFORE else "closing" if left <= ENTRY_CUTOFF else "open"

    def shock(self, m, now):
        """Binance falling fast or USDTTMN jumping: Wallex dips then are moves, not gaps."""
        old = [p for t, p in m.refs if now - t <= SHOCK_SEC]
        ou = [p for t, p in self.usdts if now - t <= SHOCK_SEC]
        return bool(old and m.ref[0] < max(old) * (1 - SHOCK_REF)) or bool(ou and (max(ou) / min(ou) - 1) > SHOCK_USDT)

    def qualify(self, m, now):
        """Hourly per-market gate: liquid enough, and with QUALIFY also a calm premium whose dips come back."""
        if m.qual_ts and now - m.qual_ts < 3600:
            return m.qual
        m.qual_ts = now
        if m.vol_day < QUAL_VOL:
            m.qual, m.qual_why = False, f"thin: {m.vol_day / 1e9:.1f}B TMN/day"
            return m.qual
        if not QUALIFY:
            m.qual, m.qual_why = True, "structural check off"
            return m.qual
        h = [x for x in m.hist if now - x[0] <= QUAL_DAYS * 86400]
        if len(h) < QUAL_MIN_HOURS * 60:
            m.qual, m.qual_why = False, f"only {len(h) / 60:.0f}h of history"
            return m.qual
        sd = statistics.pstdev(x[1] for x in h)
        dips = back = 0
        i = 0
        while i < len(h):
            ts, prem, base = h[i][0], h[i][1], h[i][4]
            if prem < base - QUAL_DIP:
                dips += 1
                back += any(y[1] >= base for y in h[i + 1:i + 61] if y[0] - ts <= 3600)
                i += 30  # one dip per half hour, not every minute of the same dip
            i += 1
        rev = back / dips if dips else 1.0
        was, m.qual = m.qual, sd <= QUAL_SD and (dips < 3 or rev >= QUAL_REV)
        m.qual_why = f"premium sd {sd * 1e4:.0f} bps, {dips} dips, {rev:.0%} reverted in 60 min"
        if m.qual != was:
            self.event("qualify", f"{m.sym} {'qualified' if m.qual else 'disqualified'}: {m.qual_why}")
        return m.qual

    def entry_price(self, m, target):
        return min(rnd(target * (1 - ENTRY_EDGE), m.tick), m.book["ask"][0])

    def take_price(self, m, fair, bid, now):
        entry = m.pos_cost / m.pos_qty
        floor_px = entry * (1 + FEE_MAKER + MIN_PROFIT)
        target = max(fair * (1 + m.entry_base) * (1 + EXIT_EDGE), floor_px)
        if EXIT_DECAY and now - m.pos_ts > DECAY_AFTER:
            target -= (target - floor_px) * min(1.0, (now - m.pos_ts - DECAY_AFTER) / (MAX_HOLD - DECAY_AFTER))
        return max(rnd(target, m.tick, up=True), bid)

    def trail_px(self, m, now, px, intrabar=False):
        """TRAIL: one price observation; returns the sell price once the trailing stop is hit, else None.
        intrabar (backtest minute low/high): the stop is assumed hit exactly at its level."""
        if m.trail_hi is None:
            tp = self.take_price(m, m.fair, 0.0, now)
            if px >= tp:  # where take-profit would have filled: start trailing instead
                m.trail_hi = m.trail_ep = px
                m.trail_floor = m.trail_sar = tp * (1 - TRAIL_GIVEBACK)
                m.trail_af, m.trail_t = 0.02, now
            return None
        if TRAIL == "atr":
            mids = [x[5] for x in list(m.hist)[-31:]]
            rets = [b / a - 1 for a, b in zip(mids, mids[1:])]
            stop = m.trail_hi * (1 - TRAIL_K * (statistics.pstdev(rets) if rets else 0.0))
        else:  # sar: classic Parabolic SAR on 1-minute highs, long side only
            if now - m.trail_t >= 60:
                if m.trail_hi > m.trail_ep:
                    m.trail_af = min(m.trail_af + 0.02, 0.2)
                m.trail_ep, m.trail_t = m.trail_hi, now
                m.trail_sar += m.trail_af * (m.trail_ep - m.trail_sar)
            stop = m.trail_sar
        stop = max(stop, m.trail_floor)
        if px <= stop:  # check against the previous high first: order inside a minute is unknown
            return stop if intrabar else px
        m.trail_hi = max(m.trail_hi, px)
        return None

    def busy_slots(self):
        return sum(1 for m in self.m.values() if m.pos_qty > 0 or (m.order and m.order["side"] == "BUY"))

    # --- main step, ~1/s live, 1/min in backtest
    async def tick(self, now):
        self.t0 = self.t0 or now
        if now - self.last_bal >= 15:
            self.last_bal = now
            self.bal = await self.broker.balances()
            if not self.adopted and all(m.book["bid"][0] for m in self.m.values()):
                self.adopt(now)
        self.risk_day(now)
        self.on_usdt_tick(now)
        phase = self.phase(now)
        for m in self.m.values():
            await self.step(m, now, phase)

    def adopt(self, now):
        """After a restart, coins already held in our markets become positions so they still get exited."""
        self.adopted = True
        for m in self.m.values():
            qty, bid = self.bal.get(m.base_asset, 0), m.book["bid"][0]
            if qty * bid >= MIN_NOTIONAL and m.pos_qty == 0:
                m.pos_qty, m.pos_cost, m.pos_ts = qty, qty * bid, now
                self.event("adopt", f"{m.sym}: managing {qty} already on the account at {bid:,.4f}")

    def risk_day(self, now):
        eq = self.equity()
        if eq <= 0:
            return
        day = datetime.fromtimestamp(now, TEHRAN).date()
        if day != self.day:
            self.day, self.day_equity, self.halted = day, eq, False
        self.peak = max(self.peak, eq)
        self.max_dd = max(self.max_dd, 1 - eq / self.peak)
        if not self.halted and eq < self.day_equity * (1 - DAILY_STOP):
            self.halted = True
            self.event("halt", f"daily stop: equity {eq:,.0f} < {self.day_equity:,.0f} -{DAILY_STOP:.0%}; no entries until tomorrow")

    async def step(self, m, now, phase, fresh_only=False):
        """fresh_only: re-price resting orders on new Binance/USDT data without a new Wallex print (backtest)."""
        if self.stale(m, now):
            if not m.was_stale:
                m.was_stale = True
                self.event("stale", f"{m.sym} feed older than {STALE:.0f}s, pausing it")
                if m.order:
                    await self.close_order(m, now)
            return
        m.was_stale = False
        bid, ask = m.book["bid"][0], m.book["ask"][0]
        fair = m.ref[0] * (self.usdt["bid"][0] + self.usdt["ask"][0]) / 2
        prem = (bid + ask) / 2 / fair - 1
        if not fresh_only:
            m.n += 1
            a = max(1 / m.n, min(now - m.last_step, 60) / BASELINE_SEC)  # mean while warming up, then time-based EMA
            m.last_step = now
            m.base = prem if m.base is None else m.base + a * (prem - m.base)
        target = fair * (1 + m.base)
        m.fair = fair
        m.prox = ask / max(self.entry_price(m, target), 1e-12) - 1
        if not m.refs or now - m.refs[-1][0] >= 10:
            m.refs.append((now, m.ref[0]))
        if not fresh_only and now - m.last_hist >= 60:
            m.last_hist = now
            m.hist.append((now, prem, m.ref[0], (ask - bid) / bid, m.base, (bid + ask) / 2))
        if not fresh_only and now - m.last_sample >= 15:
            m.last_sample = now
            self.j.add("samples", now, m.sym, bid, ask, fair, prem, m.base)
        sig = dict(rule="premium_reversion", fair=round(fair, 4), baseline_fair=round(target, 4), bid=bid, ask=ask,
                   prem=round(prem, 5), base=round(m.base, 5))

        if m.order:
            await self.manage_order(m, now, (bid, ask), target, phase)
        if m.order:
            return
        if m.pos_qty * bid >= MIN_NOTIONAL:
            await self.exit(m, now, bid, ask, target, phase, sig)
        elif m.pos_qty:
            m.pos_qty = m.pos_cost = 0.0  # dust below min notional cannot be sold; stop tracking it
        elif (phase == "open" and not self.halted and now - self.t0 >= WARMUP and not self.shock(m, now)
              and self.qualify(m, now)):
            await self.enter(m, now, bid, ask, target, sig)

    def ranked_ok(self, m, now):
        """AI approval and confidence rank within free slots."""
        v = self.judge.get(m.sym, now)
        if v["verdict"] != "approve":
            return False, v
        free = MAX_POS - sum(1 for x in self.m.values() if x.pos_qty > 0)
        # slots go to the most AI-confident markets, then to those whose ask is closest to our buy price
        cands = sorted((-self.judge.get(s, now)["confidence"], x.prox, s) for s, x in self.m.items()
                       if x.active and not x.pos_qty and self.judge.get(s, now)["verdict"] == "approve" and x.qual)
        return m.sym in [s for *_, s in cands[:free]], v

    async def enter(self, m, now, bid, ask, target, sig):
        ok, v = self.ranked_ok(m, now)
        price = self.entry_price(m, target)
        eq = self.equity()
        budget = min(POS_FRAC * eq, self.bal.get("TMN", 0) * 0.995)
        qty = rnd(budget / price, m.step) if price > 0 else 0
        risk = dict(size_tmn=round(qty * price), size_pct=round(qty * price / eq * 100, 1) if eq else 0,
                    stop_pct=-STOP_LOSS * 100, daily_dd_pct=round((1 - eq / self.day_equity) * 100, 2) if self.day_equity else 0,
                    busy_slots=self.busy_slots(), max_pos=MAX_POS)
        if not ok:
            key = (v["verdict"], v["reason"])
            if m.last_reject != key:  # log each distinct rejection once, not every second
                m.last_reject = key
                self.j.decision(dict(ts=iso(now), market=m.sym, signal=sig, ai=v, risk=risk,
                                     decision="skipped", why="AI reject" if v["verdict"] != "approve" else "no free slot",
                                     manual=False))
            return
        m.last_reject = None
        if qty * price < MIN_NOTIONAL:
            return
        await self.place(m, now, "BUY", price, qty, "entry", (bid, ask), sig, v, risk)

    async def exit(self, m, now, bid, ask, target, phase, sig):
        entry = m.pos_cost / m.pos_qty
        held = now - m.pos_ts
        qty = rnd(min(m.pos_qty, self.bal.get(m.base_asset, m.pos_qty)), m.step)
        if phase == "flatten":
            reason, price = "end_of_challenge", bid
        elif bid <= entry * (1 - STOP_LOSS):
            reason, price = "stop_loss", bid
        elif held >= MAX_HOLD:
            reason, price = "max_hold", bid
        elif TRAIL != "off":
            price = self.trail_px(m, now, bid)
            if price is None:
                return
            reason = "trail"
        else:
            reason, price = "take_profit", self.take_price(m, m.fair, bid, now)
        if qty * bid >= MIN_NOTIONAL:
            await self.place(m, now, "SELL", price, qty, reason, (bid, ask), sig, None,
                             dict(entry=round(entry, 4), held_s=round(held), stop_px=round(entry * (1 - STOP_LOSS), 4)))

    async def place(self, m, now, side, price, qty, reason, book, sig, ai, risk):
        rec = dict(ts=iso(now), market=m.sym, signal=sig, ai=ai, risk=risk, decision=f"{side.lower()} ({reason})",
                   order=dict(side=side, type="LIMIT", price=price, qty=qty), manual=False)
        try:
            cid = await self.broker.place(m, side, price, qty, book)
        except (WallexError, httpx.HTTPError) as e:
            rec["order"]["status"] = f"error: {e}"
            self.j.decision(rec)
            m.place_fails += 1
            msg = f"{m.sym} place {side} {qty}@{price}: {e}"
            if m.place_fails == PLACE_ALERT or m.place_fails % 50 == 0:  # a lasting refusal, not the post-cancel 422
                self.event("error", f"{msg} ({m.place_fails} in a row)")
            else:
                log.info("place refused, retrying: %s", msg)
            return
        m.place_fails = 0
        rec["order"].update(cid=cid, status="placed")
        self.j.decision(rec)
        m.order = dict(cid=cid, side=side, price=price, ts=now, reason=reason)
        self.j.add("orders", now, MODE, m.sym, cid, side, price, qty, reason)
        log.info("%s %s %s %s @ %s (%s)", m.sym, side, cid, qty, price, reason)
        await self.manage_order(m, now, book, None, None)

    async def manage_order(self, m, now, book, target, phase):
        o = m.order
        pull = target is not None and o["side"] == "BUY" and o["price"] > self.entry_price(m, target) * (1 + PULL)
        if target is not None and not pull:
            if now - o.get("checked", 0) < POLL:
                return  # stay well under Wallex rate limits
            o["checked"] = now
        try:
            st = await self.broker.status(o["cid"], book)
            urgent = False
            if target is not None and m.pos_qty and o["side"] == "SELL" and o["reason"] == "take_profit":
                entry = m.pos_cost / m.pos_qty
                urgent = book[0] <= entry * (1 - STOP_LOSS) or now - m.pos_ts >= MAX_HOLD or phase == "flatten"
                urgent = urgent or abs(o["price"] / self.take_price(m, m.fair, book[0], now) - 1) > REQUOTE
            if target is not None and o["side"] == "BUY":
                urgent = (phase != "open" or self.halted or self.shock(m, now) or not self.ranked_ok(m, now)[0]
                          or pull or abs(o["price"] / self.entry_price(m, target) - 1) > REQUOTE)
            if not st["done"] and target is not None and (now - o["ts"] >= ORDER_TTL or urgent):
                st = await self.broker.cancel(o["cid"], book)
        except (WallexError, httpx.HTTPError) as e:
            self.event("error", f"{m.sym} order {o['cid']}: {e}")
            return
        if st["done"]:
            m.order = None
            if st["filled"] > 0:
                self.on_fill(m, now, o, st)

    async def check_fills(self, m, now, book):
        """Backtest helper: let resting orders and the trailing stop see an intrabar price."""
        if m.order:
            await self.manage_order(m, now, book, None, None)
        elif TRAIL != "off" and m.pos_qty * book[0] >= MIN_NOTIONAL and m.pos_ts < now:
            px = self.trail_px(m, now, book[0], intrabar=True)
            if px:
                qty = rnd(min(m.pos_qty, self.bal.get(m.base_asset, m.pos_qty)), m.step)
                await self.place(m, now, "SELL", px, qty, "trail", (px, px), None, None, {})

    async def close_order(self, m, now):
        st = await self.broker.cancel(m.order["cid"], (m.book["bid"][0], m.book["ask"][0]))
        o, m.order = m.order, None
        if st["filled"] > 0:
            self.on_fill(m, now, o, st)

    def on_fill(self, m, now, o, st):
        self.j.add("fills", now, MODE, m.sym, o["cid"], o["side"], st["avg"], st["filled"], st["fee_base"], st["fee_tmn"])
        self.last_bal = 0  # refresh balances next tick
        b = m.base_asset
        if o["side"] == "BUY":
            if m.pos_qty == 0:
                m.pos_ts, m.entry_base, m.trail_hi = now, m.base, None
            m.pos_qty += st["filled"] - st["fee_base"]
            m.pos_cost += st["filled"] * st["avg"] + st["fee_tmn"]
            self.bal["TMN"] = self.bal.get("TMN", 0) - st["filled"] * st["avg"] - st["fee_tmn"]
            self.bal[b] = self.bal.get(b, 0) + st["filled"] - st["fee_base"]
            fee_rate = (st["fee_base"] / st["filled"]) if st["filled"] else 0
            if MODE == "live" and st.get("nfills") and fee_rate == 0 and st["fee_tmn"] == 0 and FEE_MAKER > 0:
                self.zero_fee()
            self.event("fill", f"{m.sym} BUY {st['filled']} @ {st['avg']:,.4f} fee {fee_rate:.3%}")
        else:
            share = min(st["filled"] / m.pos_qty, 1.0)
            cost = m.pos_cost * share
            proceeds = st["filled"] * st["avg"] - st["fee_tmn"]
            pnl = proceeds - cost
            self.j.add("trades", m.pos_ts, now, MODE, m.sym, st["filled"], cost / st["filled"], st["avg"], pnl, pnl / cost, o["reason"])
            m.pos_qty -= st["filled"]
            m.pos_cost -= cost
            m.pnls.append(round(pnl / cost * 100, 2))
            self.bal["TMN"] = self.bal.get("TMN", 0) + proceeds
            self.bal[b] = self.bal.get(b, 0) - st["filled"]
            self.j.decision(dict(ts=iso(now), market=m.sym, decision="trade_closed", reason=o["reason"],
                                 pnl_tmn=round(pnl), pnl_pct=round(pnl / cost * 100, 3), manual=False))
            self.event("trade", f"{m.sym} SELL {st['filled']} @ {st['avg']:,.4f} ({o['reason']}) PnL {pnl:+,.0f} TMN ({pnl / cost:+.2%})")

    def zero_fee(self):
        """The API reports 0.25% but Wallex says spot is fee-free; a real fill with fee 0 settles it."""
        global FEE_MAKER, FEE_TAKER, ENTRY_EDGE
        FEE_MAKER = FEE_TAKER = 0.0
        ENTRY_EDGE = ZERO_FEE_EDGE
        for m in self.m.values():
            m.active = True
        self.event("fees", f"real fill charged no fee: fees set to 0, entry edge {ENTRY_EDGE}, now trading "
                           f"{[m.sym for m in self.m.values()]}")

    def ai_context(self, now):
        eq = self.equity()
        ctx = dict(now_tehran=datetime.fromtimestamp(now, TEHRAN).strftime("%Y-%m-%d %H:%M"),
                   hours_to_end=round((END_TS - now) / 3600, 1), equity_tmn=round(eq),
                   day_drawdown_pct=round((1 - eq / self.day_equity) * 100, 2) if self.day_equity else 0,
                   free_slots=MAX_POS - self.busy_slots(), markets={})
        for m in self.m.values():
            h = list(m.hist)
            if len(h) < 10:
                continue
            last30 = [x[1] for x in h[-30:]]
            rets = [h[i][2] / h[i - 1][2] - 1 for i in range(max(1, len(h) - 60), len(h))]
            bp = 1e4
            ctx["markets"][m.sym] = dict(
                minutes_of_history=round((h[-1][0] - h[0][0]) / 60) + 1,  # span, not rows: Wallex has no candle for minutes without trades
                premium_now_bps=round(h[-1][1] * bp, 1), baseline_bps=round(m.base * bp, 1),
                premium_30m_mean_bps=round(statistics.mean(last30) * bp, 1),
                premium_30m_sd_bps=round(statistics.pstdev(last30) * bp, 1),
                premium_6h_sd_bps=round(statistics.pstdev(x[1] for x in h[-360:]) * bp, 1),
                premium_3d_sd_bps=round(statistics.pstdev(x[1] for x in h) * bp, 1),
                qualified=m.qual, qualifier=m.qual_why,
                premium_range_min_max_bps=[round(min(x[1] for x in h[-120:]) * bp, 1), round(max(x[1] for x in h[-120:]) * bp, 1)],
                ref_change_last_60m_bps=round((h[-1][2] / h[max(0, len(h) - 60)][2] - 1) * bp, 1),
                ref_1m_return_sd_bps=round(statistics.pstdev(rets) * bp, 1) if len(rets) > 1 else 0,
                wallex_spread_bps=round(h[-1][3] * bp, 1),
                recent_trade_pnls_bps=[round(x * 100, 1) for x in m.pnls], holding=bool(m.pos_qty))
        return ctx

    async def ai_round(self, now):
        ctx = self.ai_context(now)
        if not ctx["markets"]:
            return
        v = await self.judge.refresh(ctx, MAX_POS, now)
        self.j.decision(dict(ts=iso(now), decision="ai_review", context=ctx, verdicts=v, ok=v is not None, manual=False))
        if v is None:
            self.event("ai", "AI judge failed; no new entries until it answers")
        else:
            log.info("AI: %s", {s: (x["verdict"], x["confidence"]) for s, x in v.items()})

    def event(self, kind, msg):
        log.warning("[%s] %s", kind, msg)
        self.j.add("events", time.time(), kind, msg)
        self.alert(f"TIZ {MODE} {kind}: {msg}")


def warm_from(m, wx, bn, usdt, frm, to):
    """Minute history, baseline and daily volume of one market from candles in [frm, to): live start and backtest."""
    m.vol_day = sum(v[3] * v[4] for t, v in wx.items() if frm <= t < to) / ((to - frm) / 86400)
    u = b = None
    for t in range(frm, to, 60):
        u, b = usdt.get(t, u), bn.get(t, b)
        c = wx.get(t)
        if not (u and b and c):
            continue
        prem = c[3] / (b * u) - 1
        m.n += 1
        m.base = prem if m.base is None else m.base + max(1 / m.n, 60 / BASELINE_SEC) * (prem - m.base)
        m.hist.append((float(t), prem, b, 0.0, m.base, c[3]))


def warm_history(bot, days=QUAL_DAYS, ref="binance"):
    """Fill each market's minute history, baseline and daily volume from Wallex UDF + the reference (Binance klines or
    Wallex <COIN>USDT candles), so the
    qualifier and the AI context work from the first minute instead of after QUAL_MIN_HOURS."""
    from tiz import backtest as T  # lazy: backtest imports this module
    now = int(time.time()) // 60 * 60
    frm = now - int(days * 86400)
    usdt = {t: v[3] for t, v in T.fetch_wallex("USDTTMN", frm, now).items()}
    for m in bot.m.values():
        sym = m.base_asset + "USDT"
        bn = T.fetch_binance(sym, frm, now) if ref == "binance" else {t: v[3] for t, v in T.fetch_wallex(sym, frm, now).items()}
        warm_from(m, T.fetch_wallex(m.sym, frm, now), bn, usdt, frm, now)
        bot.event("warm", f"{m.sym}: {len(m.hist) / 60:.0f}h of history, {m.vol_day / 1e9:.1f}B TMN/day")


def iso(ts):
    return datetime.fromtimestamp(ts, TEHRAN).isoformat(timespec="seconds")


# ---------------------------------------------------------------- live wiring
def ref_source():
    """REF_SOURCE, with auto resolved by one Binance kline request: (source, why)."""
    if REF_SOURCE != "auto":
        return REF_SOURCE, "REF_SOURCE"
    try:
        r = httpx.get("https://data-api.binance.vision/api/v3/klines", params=dict(symbol="BTCUSDT", interval="1m", limit=1), timeout=15)
        return ("binance", "ok") if r.status_code == 200 else ("wallex", f"binance {r.status_code}")
    except httpx.HTTPError as e:
        return "wallex", f"binance {type(e).__name__}"


async def binance_feed(coins, on_ref):
    streams = "/".join(f"{c.lower()}usdt@bookTicker" for c in coins)
    url = f"wss://data-stream.binance.vision/stream?streams={streams}"  # api.binance.com is geo-blocked from Iran
    while True:
        try:
            async with websockets.connect(url) as ws:
                async for msg in ws:
                    d = json.loads(msg)["data"]
                    on_ref(d["s"][:-4], (float(d["b"]) + float(d["a"])) / 2)
        except Exception as e:  # staleness gate pauses trading meanwhile
            log.warning("binance reconnect: %r", e)
            await asyncio.sleep(2)


def telegram(http: httpx.AsyncClient):
    token, chat = os.environ.get("TELEGRAM_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not (token and chat):
        return lambda msg: None

    async def send(msg):
        try:
            await http.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat, "text": msg})
        except httpx.HTTPError as e:
            log.info("telegram: %r", e)

    return lambda msg: asyncio.get_running_loop().create_task(send(msg))


async def main():
    global FEE_MAKER, FEE_TAKER
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    rest = Rest(os.environ.get("WALLEX_API_KEY"))
    syms = [c + "TMN" for c in COINS + ZERO_FEE_COINS]
    if bad := set(syms) & disallowed():
        raise SystemExit(f"not tradable on the challenge account: {bad}")
    info = await rest.markets()
    markets = [Market(s, int(info[s]["tickSize"]), int(info[s]["stepSize"]), s[:-3] in COINS) for s in syms]
    if MODE == "live":
        if not os.environ.get("WALLEX_API_KEY"):
            raise SystemExit("MODE=live needs WALLEX_API_KEY in .env")
        fee = (await rest.fees()).get(syms[0], {})
        FEE_MAKER, FEE_TAKER = float(fee.get("makerFeeRate", FEE_MAKER)), float(fee.get("takerFeeRate", FEE_TAKER))
        broker = Live(rest)
        if open_ := [o for o in await rest.open_orders() if o.get("symbol") in syms]:
            raise SystemExit(f"{len(open_)} open orders on bot markets; cancel them first")
    else:
        broker = Paper(PAPER_TMN)
    bot = Bot(broker, Journal(), markets, Judge(enabled=env("AI_JUDGE", "1") != "0"), telegram(rest.http))  # AI_JUDGE=0: rules only
    ref, why = ref_source()
    log.warning("reference source: %s (%s)", ref, why)
    refb = {c + "USDT": {"bid": (0.0, 0.0), "ask": (0.0, 0.0)} for c in COINS + ZERO_FEE_COINS} if ref == "wallex" else {}

    def on_wallex(sym, side, levels):  # TMN books to the bot; with ref wallex, <COIN>USDT books become the reference
        if sym not in refb:
            return bot.on_depth(sym, side, levels)
        if levels:
            refb[sym][side] = (levels[0][0], time.time())
        b, a = refb[sym]["bid"], refb[sym]["ask"]
        if 0 < b[0] < a[0]:  # empty or crossed book: no ref update, the STALE rule pauses the market
            bot.on_ref(sym[:-4], (b[0] + a[0]) / 2, min(b[1], a[1]))

    try:
        warm_history(bot, ref=ref)
    except Exception as e:  # the qualifier then waits QUAL_MIN_HOURS for live history
        bot.event("error", f"history warm-up failed: {e!r}")
    bot.event("start", f"{MODE} {syms} fees {FEE_MAKER}/{FEE_TAKER} edge {ENTRY_EDGE} pos {POS_FRAC}×{MAX_POS} "
                       f"end {iso(END_TS)} ref {ref}")
    asyncio.create_task(stream_depth(syms + ["USDTTMN"] + list(refb), on_wallex))
    if ref == "binance":
        asyncio.create_task(binance_feed(COINS + ZERO_FEE_COINS, bot.on_ref))
    last_status = next_ai = 0.0
    while True:
        await asyncio.sleep(1)
        now = time.time()
        try:
            for s in syms + ["USDTTMN"] + list(refb):  # socket pushes only on change, no snapshot: top up quiet books via REST
                book = bot.usdt if s == "USDTTMN" else refb.get(s) or bot.m[s].book
                if now - min(book["bid"][1], book["ask"][1]) > 10:
                    d = await rest.depth(s)
                    on_wallex(s, "bid", [(float(x["price"]), 0) for x in d["bid"]])
                    on_wallex(s, "ask", [(float(x["price"]), 0) for x in d["ask"]])
            await bot.tick(now)
            if not bot.judge.busy and now >= next_ai and now - bot.t0 >= WARMUP / 2:
                next_ai = now + 60  # retry in 60 s if this round fails; success pushes it to AI_EVERY
                asyncio.create_task(bot.ai_round(now))
            if bot.judge.ts and next_ai < bot.judge.ts + AI_EVERY and not bot.judge.busy:
                next_ai = bot.judge.ts + AI_EVERY
            if now - last_status >= 300:
                last_status = now
                eq = bot.equity()
                log.info("status equity %s TMN (%+.2f%%) max_dd %.2f%% positions %s halted %s phase %s", f"{eq:,.0f}",
                         (eq / 5_000_000 - 1) * 100, bot.max_dd * 100,
                         {m.sym: m.pos_qty for m in bot.m.values() if m.pos_qty}, bot.halted, bot.phase(now))
        except Exception as e:  # keep running; journal it
            bot.event("error", repr(e))
            log.exception("tick")


if __name__ == "__main__":
    asyncio.run(main())
