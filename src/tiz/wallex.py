"""Minimal Wallex adapter: REST (orders, balances, fees) + Centrifugo depth stream.
Docs: https://developers.wallex.ir/docs/ (auth header x-api-key)."""
import asyncio
import json
import logging

import httpx
import websockets

log = logging.getLogger("wallex")
BASE = "https://api.wallex.ir"
WS_V1 = "wss://api.wallex.ir/ws"


class WallexError(Exception):
    pass


class Rest:
    def __init__(self, api_key: str | None):
        h = {"Content-Type": "application/json"}
        if api_key:
            h["x-api-key"] = api_key
        self.http = httpx.AsyncClient(base_url=BASE, headers=h, timeout=10)

    async def _call(self, method: str, path: str, **kw) -> dict:
        r = await self.http.request(method, path, **kw)
        try:
            body = r.json()
        except ValueError:
            raise WallexError(f"{method} {path} -> {r.status_code} {r.text[:200]}")
        if r.status_code >= 400 or body.get("success") is False:
            raise WallexError(f"{method} {path} -> {r.status_code} {body.get('code')} {body.get('message')}")
        return body.get("result", body)

    async def markets(self) -> dict:
        return (await self._call("GET", "/v1/markets"))["symbols"]

    async def depth(self, symbol: str) -> dict:
        return await self._call("GET", "/v1/depth", params={"symbol": symbol})

    async def balances(self) -> dict[str, tuple[float, float]]:
        """asset -> (free, locked)"""
        res = (await self._call("GET", "/v1/account/balances"))["balances"]
        return {a: (float(b["value"]) - float(b["locked"]), float(b["locked"])) for a, b in res.items()}

    async def fees(self) -> dict:
        return await self._call("GET", "/v1/account/fee")

    async def place(self, symbol: str, side: str, price: str, qty: str, client_id: str) -> dict:
        body = {"symbol": symbol, "side": side, "type": "LIMIT", "price": price, "quantity": qty, "client_id": client_id}
        return await self._call("POST", "/v1/account/orders", json=body)

    async def order(self, client_id: str) -> dict:
        return await self._call("GET", f"/v1/account/orders/{client_id}")

    async def cancel(self, client_id: str) -> dict:
        return await self._call("DELETE", f"/v1/account/orders/{client_id}")

    async def open_orders(self, symbol: str | None = None) -> list[dict]:
        res = await self._call("GET", "/v1/account/openOrders", params={"symbol": symbol} if symbol else None)
        orders = res.get("orders", res) if isinstance(res, dict) else res
        if isinstance(orders, dict):  # docs show {symbol: [orders]}
            return [o for v in orders.values() if isinstance(v, list) for o in v]
        return orders


async def stream_depth(symbols: list[str], on_side):
    """Calls on_side(symbol, 'bid'|'ask', [(price, qty), ...]) with full snapshots. Reconnects forever."""
    # shortcut: v1 socket (/ws); v2 Centrifugo answered 503 from Iran on 2026-10-22, switch when it works
    while True:
        try:
            async with websockets.connect(WS_V1) as ws:
                for s in symbols:
                    for d in ("buyDepth", "sellDepth"):
                        await ws.send(json.dumps(["subscribe", {"channel": f"{s}@{d}"}]))
                async for frame in ws:
                    msg = json.loads(frame)
                    if isinstance(msg, list) and len(msg) == 2 and "@" in msg[0]:
                        sym, kind = msg[0].split("@")
                        levels = [(float(x["price"]), float(x["quantity"])) for x in msg[1]]
                        on_side(sym, "bid" if kind == "buyDepth" else "ask", levels)
        except Exception as e:  # network drops are expected; staleness gate covers the gap
            log.warning("wallex ws reconnect: %r", e)
            await asyncio.sleep(2)
