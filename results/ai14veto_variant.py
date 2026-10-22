"""Variant for the report only: AI as pure veto, slots ranked by proximity (as with AI off), not by confidence."""
import asyncio, logging, os, sys
logging.disable(logging.CRITICAL)
from tiz import bot as B, backtest as T

def ranked_ok(self, m, now):
    v = self.judge.get(m.sym, now)
    if v["verdict"] != "approve":
        return False, v
    free = B.MAX_POS - sum(1 for x in self.m.values() if x.pos_qty > 0)
    cands = sorted((x.prox, s) for s, x in self.m.items()
                   if x.active and not x.pos_qty and self.judge.get(s, now)["verdict"] == "approve" and x.qual)
    return m.sym in [s for _, s in cands[:free]], v
B.Bot.ranked_ok = ranked_ok
asyncio.run(T.sweep())
