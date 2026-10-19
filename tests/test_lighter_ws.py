"""LighterWS against a local websocket server (no network)."""

import asyncio
import json
from http import HTTPStatus

import pytest
from websockets.asyncio.server import serve

from farmer.exchanges.lighter.helpers import TxNotSent
from farmer.exchanges.lighter.ws import LighterWS


class Server:
    """Scriptable fake of wss://.../stream."""

    def __init__(self) -> None:
        self.received: list[dict] = []
        self.conns: list = []
        self.reject_path: str | None = None  # paths starting with this get HTTP 400 code 20558
        self.silent = False  # do not answer pings / send nothing
        self.tx_mode = "id"  # id | hash | noid_error | ignore | close
        self.server = None
        self.port = 0

    async def _process_request(self, connection, request):
        if self.reject_path and request.path.startswith(self.reject_path) and "readonly=true" not in request.path:
            return connection.respond(HTTPStatus.BAD_REQUEST, json.dumps(
                {"code": 20558, "message": "You are accessing Lighter from a restricted jurisdiction."}))
        return None

    async def handler(self, ws):
        self.conns.append(ws)
        await ws.send(json.dumps({"session_id": f"s{len(self.conns)}", "type": "connected"}))
        try:
            async for raw in ws:
                msg = json.loads(raw)
                self.received.append(msg)
                if self.silent:
                    continue
                t = msg.get("type")
                if t == "ping":
                    await ws.send('{"type":"pong"}')
                elif t == "subscribe":
                    ch = msg["channel"].replace("/", ":")
                    await ws.send(json.dumps({"channel": ch, "type": f"subscribed/{ch.split(':')[0]}", "x": 1}))
                elif t == "jsonapi/sendtx":
                    d = msg["data"]
                    h = d["tx_info"].get("hash", "h" * 80)
                    if self.tx_mode == "id":
                        await ws.send(json.dumps({"id": d["id"], "code": 200, "tx_hash": h}))
                    elif self.tx_mode == "hash":
                        await ws.send(json.dumps({"code": 200, "tx_hash": h}))
                    elif self.tx_mode == "noid_error":
                        await ws.send(json.dumps({"error": {"code": 30007, "message": "Invalid Data:  'tx_type' field not found"}}))
                    elif self.tx_mode == "close":
                        await ws.close()
                        return
        except Exception:
            pass

    async def start(self):
        self.server = await serve(self.handler, "127.0.0.1", 0, process_request=self._process_request)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/stream"

    async def stop(self):
        self.server.close()
        await self.server.wait_closed()

    def of_type(self, t):
        return [m for m in self.received if m.get("type") == t]


async def wait_for(cond, timeout=5.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if cond():
            return True
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met in time")


@pytest.fixture
async def server():
    s = await Server().start()
    yield s
    await s.stop()


def make_client(url, msgs, **kw):
    events = {"connect": 0, "disconnect": []}

    def on_connect():
        events["connect"] += 1

    def on_disconnect(reason):
        events["disconnect"].append(reason)

    ws = LighterWS(url, on_message=msgs.append, on_connect=on_connect, on_disconnect=on_disconnect,
                   backoff_initial_s=0.05, backoff_max_s=0.2, protocol_ping_s=None, **kw)
    return ws, events


async def test_subscribes_after_connected_with_fresh_auth(server):
    msgs: list = []
    tokens = iter(["tok1", "tok2", "tok3"])
    ws, ev = make_client(server.url, msgs)
    ws.subscribe("market_stats/all")
    ws.subscribe("account_all_orders/7", auth=lambda: next(tokens))
    await ws.start()
    try:
        await wait_for(lambda: len(server.of_type("subscribe")) == 2)
        subs = {m["channel"]: m for m in server.of_type("subscribe")}
        assert "auth" not in subs["market_stats/all"] and subs["account_all_orders/7"]["auth"] == "tok1"
        await wait_for(lambda: len(msgs) == 2)
        assert {m["type"] for m in msgs} == {"subscribed/market_stats", "subscribed/account_all_orders"}
        # late subscription is sent immediately
        ws.subscribe("order_book/0")
        await wait_for(lambda: any(m["channel"] == "order_book/0" for m in server.of_type("subscribe")))
        # resubscribe = unsubscribe + subscribe with a NEW token
        ws.resubscribe("account_all_orders/7")
        await wait_for(lambda: len([m for m in server.of_type("subscribe") if m["channel"] == "account_all_orders/7"]) == 2)
        assert server.of_type("unsubscribe")[-1]["channel"] == "account_all_orders/7"
        assert [m["auth"] for m in server.of_type("subscribe") if m["channel"] == "account_all_orders/7"] == ["tok1", "tok2"]
        assert ev["connect"] == 1
    finally:
        await ws.stop()


async def test_reconnects_and_resubscribes(server):
    msgs: list = []
    ws, ev = make_client(server.url, msgs)
    ws.subscribe("order_book/1")
    await ws.start()
    try:
        await wait_for(lambda: len(server.of_type("subscribe")) == 1)
        await server.conns[0].close()
        await wait_for(lambda: len(server.of_type("subscribe")) == 2 and ws.connected)
        assert ev["connect"] == 2 and len(ev["disconnect"]) == 1
        assert ws.stats["connects"] == 2 and ws.stats["disconnects"] == 1
    finally:
        await ws.stop()


async def test_app_ping_and_server_ping(server):
    msgs: list = []
    ws, _ = make_client(server.url, msgs, ping_interval_s=0.1)
    await ws.start()
    try:
        await wait_for(lambda: len(server.of_type("ping")) >= 2)
        await server.conns[0].send('{"type":"ping"}')
        await wait_for(lambda: len(server.of_type("pong")) == 1)
        assert not any(m.get("type") in ("ping", "pong") for m in msgs)  # keepalive frames are not dispatched
    finally:
        await ws.stop()


async def test_shutdown_frame_reconnects_immediately(server):
    msgs: list = []
    ws, ev = make_client(server.url, msgs)
    await ws.start()
    try:
        await wait_for(lambda: ws.connected)
        await server.conns[0].send(json.dumps({"type": "shutdown", "close_in_ms": 5000}))
        await wait_for(lambda: len(server.conns) == 2 and ws.connected)
        assert ws.stats["shutdown_frames"] == 1 and ev["connect"] == 2
    finally:
        await ws.stop()


async def test_watchdog_detects_silent_drop(server):
    msgs: list = []
    ws, ev = make_client(server.url, msgs, watchdog_s=0.3, ping_interval_s=10)
    await ws.start()
    try:
        await wait_for(lambda: ws.connected)
        server.silent = True  # server stops answering anything
        await wait_for(lambda: ws.stats["watchdog_trips"] >= 1, timeout=3)
        assert any(m.get("type") == "ping" for m in server.received)  # probe ping before giving up
        server.silent = False
        await wait_for(lambda: len(server.conns) >= 2 and ws.connected, timeout=3)
    finally:
        await ws.stop()


async def test_geo_block_handshake_falls_back_to_readonly(server):
    server.reject_path = "/stream"
    msgs: list = []
    ws, ev = make_client(server.url, msgs, readonly_fallback_url=server.url + "?readonly=true")
    await ws.start()
    try:
        await wait_for(lambda: ws.connected, timeout=5)
        assert ws.restricted and ws.restricted_mode and ws.url.endswith("?readonly=true")
        assert not ws.can_send_tx  # no trading over the readonly stream
        with pytest.raises(TxNotSent):
            await ws.send_tx(14, {"a": 1}, "h" * 80, 0.1)
    finally:
        await ws.stop()


async def test_geo_block_without_fallback_reports_restricted(server):
    server.reject_path = "/stream"
    ws, _ = make_client(server.url, [])
    await ws.start()
    try:
        assert await ws.wait_first_attempt(5)
        await wait_for(lambda: ws.restricted)
        assert not ws.connected and ws.stats["handshake_rejections"] >= 1
    finally:
        await ws.stop()


@pytest.mark.parametrize("mode", ["id", "hash", "noid_error"])
async def test_send_tx_correlation(server, mode):
    server.tx_mode = mode
    msgs: list = []
    ws, _ = make_client(server.url, msgs)
    await ws.start()
    try:
        await wait_for(lambda: ws.connected)
        h = "ab" * 40
        ack = await ws.send_tx(14, {"hash": h, "Nonce": 1}, h, 2.0)
        if mode == "noid_error":
            assert ack["error"]["code"] == 30007
        else:
            assert ack["tx_hash"] == h and ack["code"] == 200
        sent = server.of_type("jsonapi/sendtx")[0]["data"]
        assert sent["tx_type"] == 14 and isinstance(sent["tx_info"], dict)  # tx_info is a JSON OBJECT
        assert msgs == []  # acks are not dispatched to the data handler
    finally:
        await ws.stop()


async def test_send_tx_timeout_and_close(server):
    msgs: list = []
    ws, _ = make_client(server.url, msgs)
    await ws.start()
    try:
        await wait_for(lambda: ws.connected)
        server.tx_mode = "ignore"
        with pytest.raises(asyncio.TimeoutError):
            await ws.send_tx(14, {"x": 1}, "c" * 80, 0.1)
        server.tx_mode = "close"
        with pytest.raises(ConnectionError):
            await ws.send_tx(14, {"x": 2}, "d" * 80, 2.0)
    finally:
        await ws.stop()


async def test_unrelated_idless_error_not_attributed_to_tx(server):
    msgs: list = []
    ws, _ = make_client(server.url, msgs)
    await ws.start()
    try:
        await wait_for(lambda: ws.connected)
        server.tx_mode = "ignore"
        task = asyncio.create_task(ws.send_tx(14, {"x": 1}, "e" * 80, 0.5))
        await asyncio.sleep(0.05)
        await server.conns[0].send(json.dumps({"error": {"code": 30003, "message": "Already Subscribed to : order_book:0"}}))
        with pytest.raises(asyncio.TimeoutError):
            await task  # a subscription error must never be taken as the tx outcome
        await wait_for(lambda: any("error" in m for m in msgs))
    finally:
        await ws.stop()


async def test_send_tx_not_connected():
    ws, _ = make_client("ws://127.0.0.1:9/stream", [])
    with pytest.raises(TxNotSent):
        await ws.send_tx(14, {}, "f" * 80, 0.1)
