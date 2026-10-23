"""Local dashboards: uv run python -m tiz.dash → http://localhost:8787
/ = the running bot (live or shadow), read-only from its journal, decisions and log; no exchange calls.
/backtests = backtest runs (results/live/<tag>.json every 30 simulated minutes) and saved results."""
import asyncio
import json
import os
import re
import secrets
import signal
import sqlite3
import subprocess
import time
from datetime import datetime
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from tiz import bot as B
from tiz.backtest import OUT
from tiz.wallex import Rest

PAGE, BOT_PAGE = Path(__file__).with_name("dash.html"), Path(__file__).with_name("bot.html")
SOURCES = {  # journal, decisions, log of each running bot
    "live": (B.ROOT / "journal.db", B.ROOT / "decisions.jsonl", B.ROOT / "logs" / "live.log"),
    "shadow": (OUT / "shadow_v3.db", OUT / "shadow_v3_decisions.jsonl", B.ROOT / "logs" / "shadow_v3.log"),
}
STATUS = re.compile(r"^(\S+ \S+),\d+ INFO status equity ([\d,]+) TMN \(([-+\d.]+)%\) max_dd ([\d.]+)% positions (\{.*\}) "
                    r"halted (\w+) phase (\w+)")
TOKEN = secrets.token_hex(16)  # put into the page; POSTs without it (e.g. from another website) are refused
PROFILES = {  # what the start button launches; paths match SOURCES so the page reads what it started
    "live": dict(env=dict(MODE="live"), log=SOURCES["live"][2]),
    "shadow": dict(env=dict(MODE="paper", DB=str(SOURCES["shadow"][0]), DECISIONS=str(SOURCES["shadow"][1])), log=SOURCES["shadow"][2]),
}
LIMITS = dict(POS_FRAC=(0.05, 0.34), MAX_POS=(1, 3), ENTRY_EDGE=(0.004, 0.02), STOP_LOSS=(0.01, 0.05), DAILY_STOP=(0.01, 0.05))
AI_CMDS = dict(cli=None, boxapi="uv run python -m tiz.ai_http")
COMMIT = subprocess.run(["git", "-C", str(B.ROOT), "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()


def tail_lines(path, n=4000):
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            f.seek(max(0, f.tell() - 2_000_000))
            return f.read().decode(errors="replace").splitlines()[-n:]
    except OSError:
        return []


def snapshot(src):
    """Everything the bot page shows, from files the bot writes."""
    db_path, dec_path, log_path = SOURCES[src]
    now = time.time()
    out = dict(src=src, now=now, commit=COMMIT, end_ts=B.END_TS, coins=B.COINS, watch=B.ZERO_FEE_COINS,
               params=dict(entry_edge=B.ENTRY_EDGE, stop_loss=B.STOP_LOSS, max_hold=B.MAX_HOLD, daily_stop=B.DAILY_STOP,
                           max_pos=B.MAX_POS, ai_every=B.AI_EVERY, entry_cutoff=B.ENTRY_CUTOFF, flatten_before=B.FLATTEN_BEFORE))
    status, start = [], None
    for ln in tail_lines(log_path):
        if m := STATUS.match(ln):
            ts = datetime.strptime(m[1], "%Y-%m-%d %H:%M:%S").timestamp()
            status.append(dict(ts=ts, equity=int(m[2].replace(",", "")), ret=float(m[3]), dd=float(m[4]),
                               positions=m[5], halted=m[6] == "True", phase=m[7]))
        elif "[start]" in ln:
            start, status = ln.split("[start] ", 1)[1], []  # a restart begins a new curve
    out.update(status=status, start=start)
    if not db_path.exists():
        return out
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    q = lambda sql, *a: db.execute(sql, a).fetchall()
    out["trades"] = [dict(sym=r[0], open=r[1], close=r[2], qty=r[3], entry=r[4], exit=r[5], pnl=r[6], pct=r[7], reason=r[8])
                     for r in q("select sym, ts_open, ts_close, qty, entry, exit, pnl_tmn, pnl_pct, reason from trades order by ts_close")]
    out["events"] = [dict(ts=r[0], kind=r[1], msg=r[2]) for r in q("select ts, kind, msg from events order by ts desc limit 60")]
    filled = {r[0] for r in q("select cid from fills")}
    markets = {}
    for sym in [c + "TMN" for c in B.COINS + B.ZERO_FEE_COINS]:
        rows = q("select ts, bid, ask, fair, prem, base from samples where sym = ? and ts > ? order by ts", sym, now - 7200)
        series = [[round(r[0]), round(r[4] * 1e4, 1), round(r[5] * 1e4, 1)] for r in rows[::4]]  # ~1 per minute, bps
        last = rows[-1] if rows else None
        o = q("select ts, cid, side, price, qty, reason from orders where sym = ? order by ts desc limit 1", sym)
        order = None
        if o and o[0][1] not in filled and now - o[0][0] < B.ORDER_TTL + 15:
            order = dict(ts=o[0][0], side=o[0][2], price=o[0][3], qty=o[0][4], reason=o[0][5])
        pos, entry, opened = 0.0, None, None
        for ts, side, price, qty, fee_base in q("select ts, side, price, qty, fee_base from fills where sym = ? order by ts", sym):
            if side == "BUY":
                pos, entry, opened = pos + qty - fee_base, price, opened or ts
            else:
                pos, opened = max(0.0, pos - qty), None if pos - qty <= 1e-12 else opened
        markets[sym] = dict(active=sym[:-3] in B.COINS, series=series, order=order,
                            last=dict(ts=last[0], bid=last[1], ask=last[2], prem=round(last[4] * 1e4, 1),
                                      base=round(last[5] * 1e4, 1)) if last else None,
                            position=dict(qty=pos, entry=entry, opened=opened, stop=entry * (1 - B.STOP_LOSS)) if pos > 1e-12 else None)
    out["markets"] = markets
    out["heartbeat"] = (q("select max(ts) from samples")[0][0] or 0)
    feed, rounds, skips = [], [], {}
    for ln in tail_lines(dec_path, 20000):
        try:
            r = json.loads(ln)
        except ValueError:
            continue
        d = r.get("decision", "")
        if d == "ai_review":
            rounds.append(dict(ts=r["ts"], ok=r["ok"], verdicts=r["verdicts"]))
            continue
        if d == "skipped":
            skips[r["why"]] = skips.get(r["why"], 0) + 1
        feed.append(dict(ts=r["ts"], market=r.get("market"), decision=d, why=r.get("why"), ai=r.get("ai"),
                         order=r.get("order"), pnl_pct=r.get("pnl_pct"), reason=r.get("reason")))
    out.update(ai=dict(rounds=len(rounds), last=rounds[-1] if rounds else None,
                       reject_pct=round(100 * sum(v["verdict"] != "approve" for x in rounds if x["verdicts"] for v in x["verdicts"].values())
                                        / max(1, sum(len(x["verdicts"]) for x in rounds if x["verdicts"])), 1),
                       failed=sum(not x["ok"] for x in rounds)),
               feed=feed[-60:][::-1], skips=skips)
    return out


def running():
    """Bot processes on this machine, however they were started (web or terminal): {profile: info}."""
    out = {}
    ps = subprocess.run(["ps", "-Ao", "pid=,lstart=,command="], capture_output=True, text=True).stdout
    for ln in ps.splitlines():
        f = ln.split()
        if len(f) < 9 or not ln.rstrip().endswith("python -m tiz.bot") or "/.venv/bin/python" not in ln:
            continue
        pid, started = int(f[0]), datetime.strptime(" ".join(f[1:6]), "%a %b %d %H:%M:%S %Y").timestamp()
        envs = subprocess.run(["ps", "eww", "-o", "command=", "-p", str(pid)], capture_output=True, text=True).stdout
        got = dict(re.findall(r"(?:^| )(MODE|POS_FRAC|MAX_POS|ENTRY_EDGE|STOP_LOSS|DAILY_STOP)=(\S+)", envs))  # whitelist only
        got["AI"] = "boxapi" if "AI_CMD=uv run python -m tiz.ai_http" in envs else "cli"
        out["live" if got.get("MODE") == "live" else "shadow"] = dict(pid=pid, started=started, settings=got)
    return out


def dotenv_keys():
    try:
        return {ln.split("=", 1)[0].strip() for ln in open(B.ROOT / ".env") if "=" in ln and not ln.startswith("#")}
    except OSError:
        return set()


def start(profile, body):
    if profile in running():
        return 409, dict(error=f"{profile} is already running")
    if profile == "live" and body.get("confirm") != "LIVE":
        return 400, dict(error="live needs confirm=LIVE")
    env = {k: v for k, v in os.environ.items() if k not in dotenv_keys()}  # the bot reads .env itself; keep secrets out of ps
    env.update(PROFILES[profile]["env"])
    for k, (lo, hi) in LIMITS.items():
        if body.get(k) is not None:
            v = float(body[k])
            if not lo <= v <= hi:
                return 400, dict(error=f"{k}={v} outside [{lo}, {hi}]")
            env[k] = str(int(v) if k == "MAX_POS" else v)
    if body.get("AI") not in AI_CMDS:
        return 400, dict(error="AI must be cli or boxapi")
    if AI_CMDS[body["AI"]]:
        env["AI_CMD"] = AI_CMDS[body["AI"]]
    log = open(PROFILES[profile]["log"], "a")
    p = subprocess.Popen(["caffeinate", "-ims", "uv", "run", "python", "-m", "tiz.bot"], cwd=B.ROOT, env=env, stdout=log,
                         stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, start_new_session=True)  # survives the dashboard
    return 200, dict(ok=True, pid=p.pid)


async def cancel_open_orders():
    """After a live stop: the bot's resting orders would otherwise stay on the book. Coins held stay as they are."""
    r = Rest(os.environ.get("WALLEX_API_KEY"))
    syms = {c + "TMN" for c in B.COINS + B.ZERO_FEE_COINS}
    done = []
    for o in await r.open_orders():
        if o.get("symbol") in syms and (cid := o.get("clientOrderId") or o.get("client_id")):
            await r.cancel(cid)
            done.append(cid)
    return done


def stop(profile):
    info = running().get(profile)
    if not info:
        return 404, dict(error=f"{profile} is not running")
    os.kill(info["pid"], signal.SIGINT)
    for _ in range(40):
        time.sleep(0.25)
        if profile not in running():
            break
    else:
        os.kill(info["pid"], signal.SIGTERM)
    out = dict(ok=True)
    if profile == "live":
        try:
            out["cancelled"] = asyncio.run(cancel_open_orders())
        except Exception as e:  # say so; the next start refuses while orders are open
            out["cancel_error"] = repr(e)
    return 200, out


_acct = [0.0, None]


def account():
    """Real balances (read-only), cached 15 s; valued at the bot's last recorded bids."""
    if time.time() - _acct[0] < 15:
        return _acct[1]
    bal = asyncio.run(Rest(os.environ.get("WALLEX_API_KEY")).balances())
    bids = {}
    if SOURCES["live"][0].exists():
        db = sqlite3.connect(f"file:{SOURCES['live'][0]}?mode=ro", uri=True)
        bids = {s: b for s, b in db.execute("select sym, bid from samples where ts in (select max(ts) from samples group by sym)")}
    assets = []
    for a, (free, locked) in sorted(bal.items()):
        if free + locked <= 0:
            continue
        px = 1.0 if a == "TMN" else bids.get(a + "TMN", 0.0)
        assets.append(dict(asset=a, free=free, locked=locked, value_tmn=round((free + locked) * px)))
    total = sum(x["value_tmn"] for x in assets)
    _acct[:] = time.time(), dict(ts=time.time(), assets=assets, total_tmn=total, return_pct=round((total / 5_000_000 - 1) * 100, 3))
    return _acct[1]


class Handler(SimpleHTTPRequestHandler):
    def do_POST(self):
        host = self.headers.get("Host", "")
        if self.headers.get("X-TIZ-Token") != TOKEN or self.headers.get("Origin") not in (f"http://{host}", None):
            return self.send(b'{"error":"forbidden"}', "application/json", 403)
        m = re.fullmatch(r"/api/control/(live|shadow)/(start|stop)", self.path)
        if not m:
            return self.send(b'{"error":"not found"}', "application/json", 404)
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        code, res = start(m[1], body) if m[2] == "start" else stop(m[1])
        self.send(json.dumps(res).encode(), "application/json", code)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            return self.send(BOT_PAGE.read_bytes().replace(b"__TIZ_TOKEN__", TOKEN.encode()), "text/html; charset=utf-8")
        if self.path == "/api/control":
            return self.send(json.dumps(dict(running=running(), limits=LIMITS)).encode(), "application/json")
        if self.path == "/api/account":
            try:
                return self.send(json.dumps(account()).encode(), "application/json")
            except Exception as e:
                return self.send(json.dumps(dict(error=repr(e))).encode(), "application/json", 502)
        if self.path in ("/backtests", "/backtests/"):
            return self.send(PAGE.read_bytes(), "text/html; charset=utf-8")
        if self.path.startswith("/api/bot/") and self.path[9:] in SOURCES:
            return self.send(json.dumps(snapshot(self.path[9:]), ensure_ascii=False).encode(), "application/json")
        if self.path == "/index.json":  # running/finished live runs + saved single-run results that have an equity curve
            live = sorted(p.name for p in (OUT / "live").glob("*.json"))
            done = []
            for p in sorted(OUT.glob("*.json")):
                try:
                    if '"equity"' in p.read_text()[:200_000] and isinstance(json.loads(p.read_text()), dict):
                        done.append(p.name)
                except (OSError, ValueError):
                    pass
            return self.send(json.dumps(dict(live=live, results=done)).encode(), "application/json")
        super().do_GET()

    def send(self, body, ctype, code=200):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    (OUT / "live").mkdir(parents=True, exist_ok=True)
    print("http://localhost:8787")
    ThreadingHTTPServer(("127.0.0.1", 8787), partial(Handler, directory=str(OUT))).serve_forever()
