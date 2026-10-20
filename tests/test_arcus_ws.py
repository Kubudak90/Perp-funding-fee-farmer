"""ArcusWsConnection: resubscribe on reconnect, watchdog, lifetime roll, RPC ids, JSON ping."""

import asyncio
import json

import pytest
import websockets

from farmer.exchanges.arcus.ws import ArcusWsConnection, sub_key
from tests.test_arcus_support import FakeWsServer, wait_for


def make(server: FakeWsServer, msgs: list, **kw) -> ArcusWsConnection:
    opts = dict(name="t", on_message=msgs.append, connect=server.connect, backoff_initial_s=0.01,
                backoff_max_s=0.05, watchdog_s=5.0, ping_interval_s=5.0)
    opts.update(kw)
    return ArcusWsConnection("wss://x/v1/ws", **opts)


def test_sub_key_identity():
    assert sub_key({"channel": "orders", "id": "0xABC", "accountIndex": 0}) == ("orders", "0xabc", 0, None)
    assert sub_key({"channel": "bbo", "id": "BTC-USD"}) == ("bbo", "BTC-USD", None, None)
    assert sub_key({"type": "subscribed", "channel": "oraclePrices", "id": ""}) == ("oraclePrices", "", None, None)


async def test_subscribes_and_resubscribes_after_drop():
    server, msgs = FakeWsServer(), []
    disconnects = []
    ws = make(server, msgs, on_disconnect=lambda: disconnects.append(1))
    await ws.subscribe({"channel": "bbo", "id": "BTC-USD"})  # registered before connect
    ws.start()
    assert await wait_for(lambda: ws.connected)
    await ws.subscribe({"channel": "l2OrderbookUpdates", "id": "ETH-USD", "nLevels": 100})
    s1 = server.sockets[0]
    assert [m["channel"] for m in s1.subs()] == ["bbo", "l2OrderbookUpdates"]
    assert await wait_for(lambda: any(m.get("type") == "subscribed" for m in msgs))
    # server drops the socket -> reconnect + resubscribe everything
    await s1.close()
    assert await wait_for(lambda: len(server.sockets) == 2 and ws.connected)
    s2 = server.sockets[1]
    assert sorted(m["channel"] for m in s2.subs()) == ["bbo", "l2OrderbookUpdates"]
    assert s2.subs("l2OrderbookUpdates")[0]["nLevels"] == 100
    assert disconnects == [1] and ws.connects == 2
    await ws.stop()


async def test_resubscribe_sends_unsubscribe_then_subscribe():
    server, msgs = FakeWsServer(), []
    ws = make(server, msgs)
    ws.start()
    assert await wait_for(lambda: ws.connected)
    key = await ws.subscribe({"channel": "l2OrderbookUpdates", "id": "BTC-USD", "nLevels": 100})
    await ws.resubscribe(key)
    kinds = [(m["type"], m["channel"]) for m in server.sockets[0].sent]
    assert kinds[-2:] == [("unsubscribe", "l2OrderbookUpdates"), ("subscribe", "l2OrderbookUpdates")]
    assert "nLevels" not in server.sockets[0].sent[-2]
    await ws.unsubscribe(key)
    assert not ws.has_subscription(key)
    await ws.stop()


async def test_watchdog_reconnects_silent_socket():
    server, msgs = FakeWsServer(), []
    ws = make(server, msgs, watchdog_s=0.15, ping_interval_s=10.0)
    ws.start()
    assert await wait_for(lambda: ws.connected)
    # nothing arrives (fake server never answers pings) -> watchdog closes and reconnects
    assert await wait_for(lambda: len(server.sockets) >= 2, timeout=3.0)
    await ws.stop()


async def test_json_ping_and_lifetime_roll():
    server, msgs = FakeWsServer(), []
    ws = make(server, msgs, ping_interval_s=0.05, max_lifetime_s=0.3, watchdog_s=10.0)
    ws.start()
    assert await wait_for(lambda: any(m == {"type": "ping"} for m in server.sockets[0].sent) if server.sockets
                          else False)
    assert await wait_for(lambda: len(server.sockets) >= 2, timeout=3.0)  # rolled before "24 h"
    await ws.stop()


async def test_rpc_ids_strictly_increasing_and_reset_per_connection():
    server, msgs = FakeWsServer(), []
    server.on_rpc = lambda sock, m: {"method": m["request"]["type"], "id": m["id"], "status": 200, "result": {"ok": 1}}
    ws = make(server, msgs)
    ws.start()
    assert await wait_for(lambda: ws.connected)
    r1 = await ws.get("bbo", {"market": "BTC-USD"})
    r2 = await ws.request("post", "cancelAllOrders", {"address": "0x1"}, envelope={"apiKey": "k", "timestamp": "1",
                                                                                   "signature": "s"})
    assert (r1["id"], r2["id"]) == (1, 2)
    sent = [m for m in server.sockets[0].sent if m["type"] in ("get", "post")]
    assert sent[1]["request"]["apiKey"] == "k" and sent[1]["request"]["payload"] == {"address": "0x1"}
    await server.sockets[0].close()
    assert await wait_for(lambda: len(server.sockets) == 2 and ws.connected)
    r3 = await ws.get("mids")
    assert r3["id"] == 1
    await ws.stop()


async def test_pending_rpc_fails_on_disconnect_and_handler_errors_do_not_kill_reader():
    server = FakeWsServer()
    seen = []

    def handler(m):
        seen.append(m)
        raise RuntimeError("listener bug")

    ws = make(server, [], on_message=handler)
    ws.start()
    assert await wait_for(lambda: ws.connected)
    fut = asyncio.ensure_future(ws.get("bbo", timeout=5.0))
    await asyncio.sleep(0.02)
    await server.sockets[0].close()
    with pytest.raises(ConnectionError):
        await fut
    assert await wait_for(lambda: len(server.sockets) == 2 and ws.connected)
    server.sockets[1].push({"type": "channel_data", "channel": "bbo", "id": "BTC-USD", "contents": {}})
    assert await wait_for(lambda: len([m for m in seen if m.get("type") == "channel_data"]) == 1)
    assert ws.connected
    await ws.stop()


async def test_connect_failures_back_off_and_recover():
    server, msgs = FakeWsServer(), []
    server.fail_connects = 3
    ws = make(server, msgs)
    ws.start()
    assert await wait_for(lambda: ws.connected, timeout=3.0)
    assert len(server.connect_kwargs) == 4
    await ws.stop()
    assert not ws.connected


async def test_real_websockets_server_reconnect_and_resubscribe():
    """Local (127.0.0.1) websockets server: abrupt server close -> client reconnects and resubscribes."""
    received: list[list[dict]] = []

    async def handler(sock):
        mine: list[dict] = []
        received.append(mine)
        await sock.send(json.dumps({"type": "connected", "connection_id": str(len(received))}))
        async for raw in sock:
            m = json.loads(raw)
            mine.append(m)
            if m.get("type") == "subscribe":
                await sock.send(json.dumps({"type": "subscribed", "channel": m["channel"], "id": m.get("id", ""),
                                            "contents": {}}))
                if len(received) == 1:
                    await sock.close(code=1001)  # server restart
            elif m.get("type") == "ping":
                await sock.send('{"type":"pong"}')

    async with websockets.serve(handler, "127.0.0.1", 0) as srv:
        port = srv.sockets[0].getsockname()[1]
        msgs: list = []
        ws = ArcusWsConnection(f"ws://127.0.0.1:{port}", name="local", on_message=msgs.append,
                               backoff_initial_s=0.01, backoff_max_s=0.05, ping_interval_s=0.05)
        await ws.subscribe({"channel": "trades", "id": "BTC-USD"})
        ws.start()
        assert await wait_for(lambda: len(received) >= 2 and ws.connected, timeout=5.0)
        assert await wait_for(lambda: any(m.get("type") == "subscribe" for m in received[1]), timeout=2.0)
        assert await wait_for(lambda: any(m.get("type") == "pong" for m in msgs), timeout=2.0)
        await ws.stop()


async def test_unacknowledged_subscription_is_resent_and_degraded_rearms_timer():
    server, msgs = FakeWsServer(), []
    attempts = {"orders": 0}

    def on_sub(sock, msg):
        if msg["channel"] == "orders":
            attempts["orders"] += 1
            return {} if attempts["orders"] == 1 else None  # the first subscribe is silently lost
        if msg["channel"] == "userFills":
            return {"type": "degraded", "channel": "userFills", "id": msg["id"].lower(), "accountIndex": 0,
                    "reason": "snapshot_stale", "retryAfterMs": 1000}
        return None

    server.on_subscribe = on_sub
    ws = make(server, msgs, subscribe_timeout_s=0.1)
    await ws.subscribe({"channel": "orders", "id": "0xAB", "accountIndex": 0})
    ws.start()
    assert await wait_for(lambda: ws.connected)
    s = server.sockets[0]
    assert await wait_for(lambda: len(s.subs("orders")) == 2, timeout=3.0)  # re-sent (unsubscribe + subscribe)
    assert len(s.subs("orders", kind="unsubscribe")) == 1
    assert await wait_for(lambda: not ws._pending_subs)  # the second attempt was acknowledged
    # degraded = pending on the server: the ack timer restarts after retryAfterMs, no immediate re-send
    key = await ws.subscribe({"channel": "userFills", "id": "0xAB", "accountIndex": 0})
    assert await wait_for(lambda: key in ws.degraded)
    await asyncio.sleep(0.3)
    assert len(s.subs("userFills")) == 1 and key in ws._pending_subs
    s.push({"type": "subscribed", "channel": "userFills", "id": "0xab", "accountIndex": 0,
            "contents": {"isSnapshot": True, "fills": []}})
    assert await wait_for(lambda: key not in ws.degraded and key not in ws._pending_subs)
    assert len(server.sockets) == 1
    await ws.stop()
