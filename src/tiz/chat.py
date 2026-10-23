"""Operator chat for the panel: the BoxAPI model answers in Persian about the bot, keeps notes it was told to remember,
and may change a bot's start settings. It never places orders; the dashboard applies its settings only inside LIMITS."""
import json
import os
import re

from tiz import ai_http
from tiz.backtest import OUT

MEM = OUT / "chat_memory.json"  # gitignored: notes + recent turns
FACTS = """Strategy: buy a safe-7 coin (TRX BTC BNB ETH XRP DOGE XAUT) ENTRY_EDGE below its Binance-implied fair price, sell at
the frozen baseline (take profit), STOP_LOSS below entry, max hold 4 h, DAILY_STOP halts entries, no entries in the last 3 h,
everything sold 1 h before the end (see "end" in the current state). Fees ~0.25% maker each side, so the round trip costs ~0.5%.
60-day backtest, no AI judge, POS_FRAC 0.3 (pessimistic / optimistic fills): edge 0.4%: 9-17 trades/day, +7%/+10% but the
unseen second half LOST -12%/-7%, drawdown 19%. edge 0.5%: 9-10/day, +17%/+31%, second half -2%/+1%, dd 18-20%.
edge 0.6%: 8-10/day, +24%/+40%, second half +6%/+16%, dd 10-16%. edge 0.7%: 5-8/day, +24%/+51%, second half +10%/+14%, dd 6-9%.
14-day backtest with the Haiku judge, POS_FRAC 0.3: edge 0.8%: 1.3-1.6 trades/day, +1.7%..+2.8%, dd 0.4%; edge 0.7%: 1.9-2.4/day,
+2.1%..+2.9%, dd 0.5%; edge 0.6%: 2.9-4.3/day, +1.8%..+3.0%, dd 0.8%. Today (2026-10-23) the market is calm: in 8 h no safe-7
coin dipped more than 0.32% below fair, so no edge >= 0.6% traded. Turning the judge off loses the AI-role points of the challenge.
Maximum risk inside the limits: POS_FRAC 0.34 x MAX_POS 3 (whole account in play), ENTRY_EDGE 0.006 (lowest edge whose unseen
half stayed profitable), STOP_LOSS 0.03 (only value tested), DAILY_STOP 0.05, AI off. Below 0.006 the edge loses to fees."""
KEYS = ("POS_FRAC", "MAX_POS", "ENTRY_EDGE", "STOP_LOSS", "DAILY_STOP", "AI")


def load():
    try:
        return json.loads(MEM.read_text())
    except (OSError, ValueError):
        return dict(notes=[], history=[])


def save(m):
    tmp = MEM.with_suffix(".tmp")
    tmp.write_text(json.dumps(m, ensure_ascii=False, indent=1))
    os.replace(tmp, MEM)


def clean_set(raw, limits, ais):
    """Keep only known profiles and keys with values inside the limits; say what was dropped."""
    out, dropped = {}, []
    for p, s in (raw or {}).items():
        if p not in ("live", "shadow") or not isinstance(s, dict):
            continue
        for k, v in s.items():
            if k == "AI" and v in ais:
                out.setdefault(p, {})[k] = v
            elif k in limits and isinstance(v, (int, float)) and limits[k][0] <= v <= limits[k][1]:
                out.setdefault(p, {})[k] = int(v) if k == "MAX_POS" else float(v)
            else:
                dropped.append(f"{p}.{k}={v}")
    return out, dropped


def parse(raw):
    m = re.search(r"\{.*\}", raw, re.S)
    try:
        return json.loads(m[0]) if m else dict(reply=raw.strip())
    except ValueError:
        return dict(reply=raw.strip())


def ask(message, state, limits, ais):
    m = load()
    prompt = f"""You are the operator assistant inside «سیگنال آپس» (Signal Ops), a Wallex spot trading bot running in a
hackathon (StarCoach). Talk to the owner in Persian, short and concrete, with numbers. You cannot place or cancel orders and the
rules forbid manual trading; you CAN change the start settings of the live or shadow bot via "set". Settings must stay inside
these limits: {json.dumps(limits)}; AI is one of {list(ais)} ("off" = rules only, no judge). Only use "set" when the owner asks
for a change (or agrees to one you proposed). Be honest about risk; never promise profit or a number of trades.
Put in "remember" short facts worth keeping across conversations (owner preferences, decisions); else [].

Facts and backtests:
{FACTS}

Your saved notes: {json.dumps(m["notes"], ensure_ascii=False)}
Current state (live and shadow bots, account): {json.dumps(state, ensure_ascii=False)}
Recent conversation: {json.dumps(m["history"][-12:], ensure_ascii=False)}

Owner: {message}

Reply with ONLY a JSON object, no prose, no code fence:
{{"reply": "<Persian answer>", "remember": ["..."], "set": {{"live"|"shadow": {{"POS_FRAC": 0.3, "ENTRY_EDGE": 0.007, "AI": "off"}}}} or {{}}}}"""
    j = parse(ai_http.ask(prompt))
    s, dropped = clean_set(j.get("set"), limits, ais)
    reply = str(j.get("reply") or "…")
    if dropped:
        reply += f"\n(خارج از محدوده، اعمال نشد: {', '.join(dropped)})"
    notes = [n for n in j.get("remember") or [] if isinstance(n, str) and n.strip() and n not in m["notes"]]
    m["notes"] = (m["notes"] + notes)[-50:]
    m["history"] = (m["history"] + [dict(role="owner", text=message), dict(role="assistant", text=reply, set=s)])[-40:]
    save(m)
    return dict(reply=reply, set=s, notes=m["notes"])
