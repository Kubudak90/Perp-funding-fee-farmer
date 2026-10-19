"""Shared helpers for the Arcus adapter tests (no tests here).

- fixtures under tests/data/arcus/ are REAL captures (mainnet, 2026-10-19; third-party addresses redacted);
- ``FakeWsServer`` replaces ``websockets.connect`` so the adapter's sockets run without network;
- ``FakeHttp`` is a real aiohttp server on 127.0.0.1 with aioresponses-like routes (aioresponses 0.7.9 is
  incompatible with aiohttp 3.14); ``mock_rest`` registers the reads the adapter performs on connect.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from aiohttp import web
from multidict import CIMultiDict

from farmer.config import ArcusConfig

DATA = Path(__file__).parent / "data" / "arcus"
BASE = "https://arcus.test"
WS_URL = "wss://arcus.test/v1/ws"
ADDR = "0xAbCdEf1234567890AbCdEf1234567890AbCdEf12"
KEY_HEX = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"  # RFC 8032 test 1
API_KEY = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
D = Decimal


def load(name: str) -> Any:
    return json.loads((DATA / name).read_text())


def load_jsonl(name: str) -> list[dict]:
    return [json.loads(line) for line in (DATA / name).read_text().splitlines() if line.strip()]


def make_cfg(*, creds: bool = True, **kw) -> ArcusConfig:
    base = dict(rest_url=BASE, ws_url=WS_URL, dead_mans_switch_s=0, ack_timeout_s=0.5, requote_budget_per_s=50)
    if creds:
        base.update(address=ADDR, api_private_key=KEY_HEX)
    base.update(kw)
    return ArcusConfig(**base)


def markets_payload() -> dict:
    return load("markets.json")


# --------------------------------------------------------------------------- fake HTTP
@dataclass
class Call:
    method: str
    path: str
    query: dict
    headers: CIMultiDict
    text: str

    def json(self) -> Any:
        return json.loads(self.text)


@dataclass
class Resp:
    status: int = 200
    payload: Any = None
    headers: dict = field(default_factory=dict)


@dataclass
class _Route:
    status: int
    payload: Any
    callback: Callable | None
    repeat: bool
    drop: bool
    headers: dict


class FakeHttp:
    """Local aiohttp server. ``get/post(path, payload=, status=, callback=, repeat=, drop=)``; routes for the
    same (method, path) are consumed in registration order (a ``repeat`` route stays)."""

    def __init__(self) -> None:
        self.routes: dict[tuple[str, str], list[_Route]] = {}
        self.log: list[Call] = []
        self._runner: web.AppRunner | None = None
        self.base = ""

    def add(self, method: str, path: str, *, status: int = 200, payload: Any = None, callback: Callable | None = None,
            repeat: bool = False, drop: bool = False, headers: dict | None = None) -> None:
        self.routes.setdefault((method, path), []).append(
            _Route(status, payload if payload is not None else {}, callback, repeat, drop, headers or {}))

    def get(self, path: str, **kw) -> None:
        self.add("GET", path, **kw)

    def post(self, path: str, **kw) -> None:
        self.add("POST", path, **kw)

    def calls(self, method: str, path: str) -> list[Call]:
        return [c for c in self.log if c.method == method and c.path == path]

    async def start(self) -> str:
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]  # noqa: SLF001
        self.base = f"http://127.0.0.1:{port}"
        return self.base

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        text = await request.text()
        call = Call(request.method, request.path, dict(request.query), CIMultiDict(request.headers), text)
        self.log.append(call)
        routes = self.routes.get((request.method, request.path)) or []
        if not routes:
            return web.json_response({"error": f"unmocked {request.method} {request.path}"}, status=418)
        route = routes[0] if routes[0].repeat else routes.pop(0)
        if route.drop:
            request.transport.close()
            return web.Response()
        status, payload, headers = route.status, route.payload, route.headers
        if route.callback is not None:
            res = route.callback(call)
            if inspect.isawaitable(res):
                res = await res
            if isinstance(res, Resp):
                status, payload, headers = res.status, res.payload, res.headers
            elif res is not None:
                payload = res
        return web.json_response(payload, status=status, headers=headers)


def mock_rest(srv: FakeHttp, *, creds: bool = True, whitelisted: bool = True, api_key_status: str = "ACTIVE") -> None:
    """Register GET endpoints used by connect()."""
    srv.get("/v1/time", callback=lambda c: {"timeNs": time.time_ns()}, repeat=True)
    srv.get("/v1/markets", payload=markets_payload(), repeat=True)
    if creds:
        if whitelisted:
            srv.get("/v1/rateLimit", payload=load("rest_ratelimit.json"), repeat=True)
            srv.get("/v1/account/stats", payload={"address": ADDR.lower(), "tradingFeeTier": {"level": 0}},
                    repeat=True)
        else:
            srv.get("/v1/rateLimit", status=403, payload={"error": "address not on access whitelist"}, repeat=True)
        srv.get("/v1/apiKeys", payload={"apiKeys": [
            {"apiKey": API_KEY, "address": ADDR.lower(), "accountIndex": 0, "allSubaccounts": False,
             "apiWalletName": "farmer-d75a9801", "status": api_key_status, "validUntil": 4102444800000,
             "createdAt": 1791990042855}]}, repeat=True)


# --------------------------------------------------------------------------- fake websocket
class FakeSocket:
    def __init__(self, server: "FakeWsServer") -> None:
        self.server = server
        self.sent: list[dict] = []
        self.q: asyncio.Queue = asyncio.Queue()
        self.closed = False

    async def send(self, data: str) -> None:
        if self.closed:
            raise ConnectionError("socket closed")
        msg = json.loads(data)
        self.sent.append(msg)
        self.server.on_send(self, msg)

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.q.put_nowait(None)

    def push(self, obj: dict) -> None:
        if not self.closed:
            self.q.put_nowait(json.dumps(obj))

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        item = await self.q.get()
        if item is None:
            raise StopAsyncIteration
        return item

    def subs(self, channel: str | None = None, kind: str = "subscribe") -> list[dict]:
        return [m for m in self.sent if m.get("type") == kind and (channel is None or m.get("channel") == channel)]


class FakeWsServer:
    """Replaces ``websockets.connect``. Auto-acks subscriptions (``on_subscribe`` may override)."""

    def __init__(self) -> None:
        self.sockets: list[FakeSocket] = []
        self.on_subscribe: Callable[[FakeSocket, dict], dict | None] | None = None
        self.on_rpc: Callable[[FakeSocket, dict], dict | None] | None = None
        self.connect_kwargs: list[dict] = []
        self.fail_connects = 0

    async def connect(self, url: str, **kw) -> FakeSocket:
        self.connect_kwargs.append({"url": url, **kw})
        if self.fail_connects > 0:
            self.fail_connects -= 1
            raise ConnectionRefusedError("fake refused")
        s = FakeSocket(self)
        self.sockets.append(s)
        s.push({"type": "connected", "connection_id": str(len(self.sockets))})
        return s

    def sockets_for(self, channel: str) -> list[FakeSocket]:
        return [s for s in self.sockets if s.subs(channel)]

    def on_send(self, sock: FakeSocket, msg: dict) -> None:
        t = msg.get("type")
        if t == "subscribe":
            ack = self.on_subscribe(sock, msg) if self.on_subscribe else None
            if ack is None:
                ack = default_ack(msg)
            if ack:
                sock.push(ack)
        elif t == "unsubscribe":
            sock.push({"type": "unsubscribed", "channel": msg.get("channel"), "id": msg.get("id", "")})
        elif t in ("get", "post"):
            reply = self.on_rpc(sock, msg) if self.on_rpc else None
            if reply is not None:
                sock.push(reply)


def default_ack(msg: dict) -> dict:
    ch = msg.get("channel")
    contents: dict = {}
    if ch == "orders":
        contents = {"isSnapshot": True, "lastSequenceId": 0, "openOrders": [], "recentClosedOrders": []}
    elif ch == "userFills":
        contents = {"isSnapshot": True, "fills": []}
    elif ch == "oraclePrices":
        contents = {"epoch": 0, "prices": []}
    ack = {"type": "subscribed", "channel": ch, "id": (msg.get("id") or ""), "contents": contents}
    if "accountIndex" in msg:
        ack["accountIndex"] = msg["accountIndex"]
    return ack


async def wait_for(pred: Callable[[], bool], timeout: float = 2.0, interval: float = 0.005) -> bool:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if pred():
            return True
        await asyncio.sleep(interval)
    return pred()


def orders_frame(contents: dict, *, market: str | None = None) -> dict:
    f = {"type": "channel_data", "channel": "orders", "id": ADDR.lower(), "accountIndex": 0, "contents": contents}
    if market:
        f["market"] = market
    return f


def order_row(*, oid: str, cid: str | None, side: str = "BUY", price: str = "60000", size: str = "0.001",
              status: str = "OPEN", state: str | None = None, tif: str = "ALO", seq: int = 1, market_id: int = 1,
              ticker: str = "BTC-USD", filled: str = "0", **extra) -> dict:
    row = {"orderId": oid, "marketId": market_id, "marketDisplayName": ticker, "side": side, "type": "LIMIT",
           "status": status, "price": price, "originalSize": size, "filledSize": filled,
           "remainingSize": str(D(size) - D(filled)), "timeInForce": tif, "goodTilTime": "1796320510381706",
           "state": state or status, "createdAt": 1792432510399814, "updatedAt": 1792432514139591,
           "sequenceNumber": seq}
    if cid:
        row["clientId"] = cid
    row.update(extra)
    return row
