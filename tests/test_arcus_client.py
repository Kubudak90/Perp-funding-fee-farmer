"""ArcusExchange end-to-end without external network: local FakeHttp for REST, FakeWsServer for both sockets."""

import asyncio
import time
from decimal import Decimal as D

import pytest

from farmer.exchanges.arcus.auth import legacy_message, modify_payload, place_payload, verify_signature
from farmer.exchanges.arcus.client import ArcusExchange
from farmer.exchanges.base import AuthRequired, ExchangeError
from farmer.models import OrderStatus, Side
from tests.test_arcus_support import (
    ADDR,
    API_KEY,
    FakeHttp,
    FakeWsServer,
    Resp,
    load,
    load_jsonl,
    make_cfg,
    mock_rest,
    order_row,
    orders_frame,
    wait_for,
)

OID = "00000000000000a1"


@pytest.fixture
async def env():
    server = FakeWsServer()
    made: list[ArcusExchange] = []
    m = FakeHttp()
    await m.start()

    async def factory(*, creds: bool = True, whitelisted: bool = True, api_key_status: str = "ACTIVE",
                      before_connect=None, **cfg_kw) -> ArcusExchange:
        mock_rest(m, creds=creds, whitelisted=whitelisted, api_key_status=api_key_status)
        ex = ArcusExchange(make_cfg(creds=creds, rest_url=m.base, **cfg_kw), ws_connect=server.connect)
        made.append(ex)
        if before_connect:
            before_connect(ex)
        await ex.connect()
        return ex

    yield factory, server, m
    for ex in made:
        await ex.close()
    await m.stop()


def acct_sock(server):
    return server.sockets_for("orders")[-1]


def md_sock(server):
    return server.sockets_for("oraclePrices")[-1]


def ack_frame_later(sock, row, delay=0.01):
    asyncio.get_running_loop().call_later(delay, sock.push, orders_frame(row, market=row.get("marketDisplayName")))


async def place_open(ex, server, m, *, price="60000", size="0.001", oid=OID, seq=10, **kw):
    """Place an ALO BUY that the fake venue acks as OPEN; returns (result, captured request)."""
    cap = {}

    def cb(call):
        body = call.json()
        cap.update(body=body, headers=call.headers, query=call.query)
        ack_frame_later(acct_sock(server), order_row(oid=oid, cid=body["clientId"], price=body["price"],
                                                     size=body["quantity"], seq=seq, side=body["orderSide"],
                                                     goodTilTime=body["goodTilTime"]))
        return Resp(202, {"orderId": oid, "status": "ACK", "marketId": 1,
                                                   "rateLimit": {"pool": "order", "remaining": 19990}})

    m.post("/v1/placeOrder", callback=cb)
    r = await ex.place_limit("BTC", Side.BUY, D(size), D(price), **kw)
    return r, cap


# ------------------------------------------------------------------ public mode
async def test_public_only_connect_markets_funding(env):
    factory, server, m = env
    ex = await factory(creds=False)
    assert not ex.has_credentials and ex._acct is None
    assert {"BTC", "ETH", "SOL", "KBONK", "AAPL", "F", "SPY"} <= set(ex.markets)
    assert md_sock(server).subs("oraclePrices") == [{"type": "subscribe", "channel": "oraclePrices"}]
    fund = await ex.fetch_funding(["BTC", "ETH", "F"])
    btc_raw = next(r for r in load("markets.json")["markets"] if r["marketDisplayName"] == "BTC-USD")
    assert set(fund) == {"BTC", "ETH"}  # F is OFFLINE
    assert fund["BTC"].rate_hourly == D(btc_raw["nextFundingRate"])
    assert fund["BTC"].next_funding_ts == float(btc_raw["nextFundingAt"])
    with pytest.raises(AuthRequired):
        await ex.place_limit("BTC", Side.BUY, D("0.001"), D("60000"))
    with pytest.raises(AuthRequired):
        await ex.fetch_positions()
    assert ex.budget_health() == 1.0
    await ex.close()
    await ex.close()  # idempotent


async def test_oracle_and_predicted_funding_frames(env):
    factory, server, m = env
    ex = await factory(creds=False)
    await ex.subscribe_books(["BTC"])
    for f in load("oracle_frames.json") + load("predicted_funding_frames.json"):
        md_sock(server).push(f)
    assert await wait_for(lambda: "BTC" in ex._marks and "BTC" in ex.predicted_funding)
    fund = await ex.fetch_funding(["BTC"])
    assert fund["BTC"].mark_price == ex._marks["BTC"][1]


# ------------------------------------------------------------------ books
async def test_books_stream_gap_resync_and_bbo_fallback(env):
    factory, server, m = env
    ex = await factory(creds=False)
    books = []
    ex.add_book_listener(books.append)
    await ex.subscribe_books(["BTC"])
    md = md_sock(server)
    l2 = md.subs("l2OrderbookUpdates")
    assert l2 == [{"type": "subscribe", "channel": "l2OrderbookUpdates", "id": "BTC-USD", "nLevels": 100}]
    assert md.subs("bbo")[0]["id"] == "BTC-USD" and md.subs("predictedFunding")[0]["id"] == "BTC-USD"
    frames = load_jsonl("book_btc_stream.jsonl")
    for f in frames:
        md.push(f)
    n_l2 = sum(1 for f in frames if f["channel"] == "l2OrderbookUpdates")
    assert await wait_for(lambda: len(books) >= n_l2)
    ob = ex.get_book("BTC")
    assert ob.best_bid < ob.best_ask and len(ob.bids) == 20 and len(ob.asks) == 20
    assert all(b.best_bid < b.best_ask for b in books)
    assert ex.stats()["books"]["BTC"]["resyncs"] == 0
    # a mid-stream gap -> unsubscribe + subscribe (fresh snapshot)
    last = [f for f in frames if f["channel"] == "l2OrderbookUpdates"][-1]["contents"]
    md.push({"type": "channel_data", "channel": "l2OrderbookUpdates", "id": "BTC-USD",
             "contents": {"bids": [], "asks": [], "lastSequenceId": int(last["lastSequenceId"]) + 5}})
    assert await wait_for(lambda: len(md.subs("l2OrderbookUpdates", kind="unsubscribe")) == 1)
    assert await wait_for(lambda: len(md.subs("l2OrderbookUpdates")) == 2)
    assert not ex._books["BTC"].synced
    # while resyncing, bbo frames keep a top-of-book alive
    n = len(books)
    md.push({"type": "channel_data", "channel": "bbo", "id": "BTC-USD",
             "contents": {"bestBid": {"price": "81900.1", "size": "1"}, "bestAsk": {"price": "81900.2", "size": "2"},
                          "lastSequenceId": 1, "timestamp": 1}})
    assert await wait_for(lambda: len(books) > n)
    assert len(ex.get_book("BTC").bids) == 1 and ex.get_book("BTC").best_ask == D("81900.2")
    # the new snapshot restores the full book
    snap = next(f for f in frames if f["type"] == "subscribed" and f["channel"] == "l2OrderbookUpdates")
    md.push(snap)
    assert await wait_for(lambda: ex._books["BTC"].synced and len(ex.get_book("BTC").bids) == 20)


async def test_bbo_mismatch_triggers_resync(env):
    factory, server, m = env
    ex = await factory(creds=False)
    await ex.subscribe_books(["BTC"])
    md = md_sock(server)
    md.push({"type": "subscribed", "channel": "l2OrderbookUpdates", "id": "BTC-USD",
             "contents": {"bids": [["100", "1"]], "asks": [["101", "1"]], "lastSequenceId": 50}})
    assert await wait_for(lambda: ex._books["BTC"].synced)
    md.push({"type": "channel_data", "channel": "l2OrderbookUpdates", "id": "BTC-USD",
             "contents": {"bids": [["100", "1"]], "asks": [], "lastSequenceId": 60}})  # confirms bid 100 @ 1
    # a stale-snapshot style disagreement on an UNCONFIRMED ask is repaired silently ...
    md.push({"type": "channel_data", "channel": "bbo", "id": "BTC-USD",
             "contents": {"bestBid": {"price": "100", "size": "1"}, "bestAsk": {"price": "101", "size": "7"},
                          "lastSequenceId": 60}})
    assert await wait_for(lambda: ex.get_book("BTC").asks[0].size == D("7"))
    assert ex.stats()["books"]["BTC"]["repairs"] == 1 and not md.subs("l2OrderbookUpdates", kind="unsubscribe")
    # ... but a disagreement on a delta-confirmed level forces a fresh snapshot
    md.push({"type": "channel_data", "channel": "bbo", "id": "BTC-USD",
             "contents": {"bestBid": {"price": "100", "size": "3"}, "bestAsk": {"price": "101", "size": "7"},
                          "lastSequenceId": 60}})
    assert await wait_for(lambda: len(md.subs("l2OrderbookUpdates", kind="unsubscribe")) == 1)


async def test_book_heartbeat_keeps_quiet_book_fresh(env):
    factory, server, m = env

    def tweak(ex):
        ex.book_heartbeat_s = 0.25

    ex = await factory(creds=False, before_connect=tweak)
    books = []
    ex.add_book_listener(books.append)
    await ex.subscribe_books(["ETH"])
    md = md_sock(server)
    md.push({"type": "subscribed", "channel": "l2OrderbookUpdates", "id": "ETH-USD",
             "contents": {"bids": [["2000", "1"]], "asks": [["2000.1", "1"]], "lastSequenceId": 5}})
    assert await wait_for(lambda: len(books) == 1)
    t0 = books[0].ts

    async def feed():  # other channels keep the socket demonstrably alive
        for _ in range(40):
            md.push({"type": "channel_data", "channel": "oraclePrices", "id": "", "contents": {"prices": []}})
            await asyncio.sleep(0.025)

    await feed()
    assert len(books) >= 2 and books[-1].ts > t0 and books[-1].best_bid == D("2000")


async def test_fetch_book_rest(env):
    factory, server, m = env
    ex = await factory(creds=False)
    m.get("/v1/l2OrderBook/BTC-USD", payload=load("rest_l2_btc.json"))
    ob = await ex.fetch_book("BTC", 20)
    assert ob.best_bid < ob.best_ask and ob.venue == "arcus" and ob.symbol == "BTC"


# ------------------------------------------------------------------ account reads
async def test_account_reads(env):
    factory, server, m = env
    ex = await factory()
    m.get("/v1/positions", payload=load("rest_positions.json"))
    pos = await ex.fetch_positions()
    raw = load("rest_positions.json")["positions"]
    assert set(pos) == {r["marketDisplayName"].split("-")[0] for r in raw.values()}
    btc = next(r for r in raw.values() if r["marketDisplayName"] == "BTC-USD")
    assert pos["BTC"].size == D(btc["size"]) and pos["BTC"].entry_price == D(btc["averageEntryPrice"])

    acct = load("rest_account.json")
    m.get("/v1/account", payload=acct)
    bal = await ex.fetch_balance()
    assert bal.equity == D(acct["equity"]) and bal.available == D(acct["freeCollateral"])
    assert bal.margin_used == sum(D(p["marginUsed"]) for p in acct["positions"].values())
    mm = sum(abs(D(p["positionValueNotional"])) * ex.markets[p["marketDisplayName"].split("-")[0]].extra["mmf"]
             for p in acct["positions"].values())
    assert bal.maintenance_margin == mm and 0 < bal.margin_ratio < 1

    m.get("/v1/account", status=404, payload={"error": "this account has no activity yet"})
    empty = await ex.fetch_balance()
    assert empty.equity == 0 and empty.available == 0

    m.get("/v1/openOrders", payload=load("rest_open_orders.json"))
    orders = await ex.fetch_open_orders()
    rows = load("rest_open_orders.json")["orders"]
    assert [o.order_id for o in orders] == [r["orderId"] for r in rows]
    assert all(o.status is OrderStatus.OPEN and o.post_only for o in orders if o)
    assert orders[0].symbol == rows[0]["marketDisplayName"].split("-")[0]


async def test_open_order_with_unknown_flags_is_canceled_not_modified(env):
    factory, server, m = env
    ex = await factory()
    row = order_row(oid="1111111111111111", cid="foreign-1", price="60000", seq=3)
    m.get("/v1/openOrders", payload={"orders": [row], "total": 1})
    [o] = await ex.fetch_open_orders("BTC")
    m.post("/v1/cancelOrder", payload={"status": "CANCEL_ACKNOWLEDGED"})
    r = await ex.modify_order(o, price=D("60000.1"))
    assert not r.ok and "canceled" in r.error
    [call] = m.calls("POST", "/v1/cancelOrder")
    assert call.json()["orderId"] == "1111111111111111"
    assert not m.calls("POST", "/v1/modifyOrder")


async def test_funding_payments_paginate(env):
    factory, server, m = env
    ex = await factory()
    base_t = 1792443600000000
    page1 = [{"marketId": 1, "marketDisplayName": "BTC-USD", "fundingRate": "0.0000125", "size": "-1",
              "payment": "1", "time": base_t - i * 3_600_000_000} for i in range(1000)]
    page2 = [dict(page1[-1]), {**page1[-1], "time": page1[-1]["time"] - 3_600_000_000}]
    calls = []

    def cb(call):
        calls.append(dict(call.query))
        return Resp(200, {"fundingPayments": page1 if len(calls) == 1 else page2})

    m.get("/v1/funding", callback=cb, repeat=True)
    pays = await ex.fetch_funding_payments(since_ts=1.7e9)
    assert len(pays) == 1001 and pays[0].ts < pays[-1].ts and all(p.amount == 1 for p in pays)
    assert calls[0]["from"] == str(int(1.7e9 * 1e6)) and calls[1]["to"] == str(page1[-1]["time"])


async def test_whitelist_403_is_explicit(env):
    factory, server, m = env
    ex = await factory()
    m.get("/v1/positions", status=403, payload={"error": "address not on access whitelist"})
    with pytest.raises(ExchangeError) as ei:
        await ex.fetch_positions()
    assert "whitelist" in str(ei.value) and "invite" in str(ei.value)


async def test_connect_with_credentials_not_whitelisted_raises(env):
    factory, server, m = env
    with pytest.raises(ExchangeError) as ei:
        await factory(whitelisted=False)
    assert "whitelist" in str(ei.value) and ei.value.code == "AddressNotOnWhitelist"
    assert all(s.closed for s in server.sockets)  # failed connect cleans up its sockets


async def test_connect_rejects_inactive_api_key(env):
    factory, server, m = env
    with pytest.raises(ExchangeError):
        await factory(api_key_status="DELETED")


# ------------------------------------------------------------------ orders
async def test_place_limit_ack_open_and_signature(env):
    factory, server, m = env
    ex = await factory()
    assert ex.has_credentials and ex._orders_synced.is_set()
    seen = []
    ex.add_order_listener(seen.append)
    r, cap = await place_open(ex, server, m)
    assert r.ok and r.order.status is OrderStatus.OPEN and r.order.order_id == OID and r.order.post_only
    assert seen == [r.order]
    b, h = cap["body"], cap["headers"]
    assert b["orderType"] == "LIMIT" and b["timeInForce"] == "ALO" and b["reduceOnly"] is False
    assert b["timestamp"] == int(h["X-Timestamp"]) and h["X-API-Key"] == API_KEY
    assert cap["query"]["address"] == ADDR
    lead_days = (int(b["goodTilTime"]) / 1e6 - time.time()) / 86400
    assert 179.9 < lead_days <= 180.01
    msg = place_payload(address=ADDR, account_index=0, ct=int(h["X-Timestamp"]), gtt_us=int(b["goodTilTime"]),
                        market_id=1, price_ticks=600000, qty_quantums=100000, reduce_only=False, side="BUY",
                        tif="ALO", client_id=b["clientId"])
    assert verify_signature(API_KEY, msg, h["X-Signature"])
    assert ex._pool_remaining["order"] == 19990


async def test_place_limit_post_only_would_cross(env):
    factory, server, m = env
    ex = await factory()

    def cb(call):
        b = call.json()
        ack_frame_later(acct_sock(server), order_row(oid=OID, cid=b["clientId"], status="REJECTED",
                                                     rejectionReason="POST_ONLY_WOULD_CROSS", seq=3))
        return Resp(202, {"orderId": OID, "status": "ACK"})

    m.post("/v1/placeOrder", callback=cb)
    r = await ex.place_limit("BTC", Side.BUY, D("0.001"), D("60000"))
    assert not r.ok and r.would_cross and not r.retryable and "would cross" in r.error
    assert r.order.status is OrderStatus.REJECTED


async def test_place_limit_pending_when_no_ack(env):
    factory, server, m = env
    ex = await factory(ack_timeout_s=0.1)
    m.post("/v1/placeOrder", status=202, payload={"orderId": OID, "status": "ACK"})
    t0 = time.monotonic()
    r = await ex.place_limit("BTC", Side.SELL, D("0.001"), D("90000"), reduce_only=True, client_id="my-id_1")
    assert r.ok and r.order.status is OrderStatus.PENDING and r.order.client_id == "my-id_1"
    assert time.monotonic() - t0 < 1.0
    # the late WS frame is still applied and emitted
    seen = []
    ex.add_order_listener(seen.append)
    acct_sock(server).push(orders_frame(order_row(oid=OID, cid="my-id_1", side="SELL", price="90000", seq=4)))
    assert await wait_for(lambda: seen and seen[0].status is OrderStatus.OPEN)
    assert seen[0] is r.order and ex._find_rec(OID, None).reduce_only is True


async def test_place_validation_is_local(env):
    factory, server, m = env
    ex = await factory()
    cases = [
        (D("0.001"), D("60000.05")),  # off tick
        (D("0.0000000001"), D("60000")),  # off step
        (D("0.00005"), D("60000")),  # below min size
        (D("0.0001"), D("1000")),  # notional 0.1 < 5
        (D("20000"), D("60000")),  # above max size
        (D("0.001"), D("600000.1")),  # 0.1 is not a multiple of the 0.2 tier tick
    ]
    for size, price in cases:
        r = await ex.place_limit("BTC", Side.BUY, size, price)
        assert not r.ok and not r.retryable, (size, price)
    r = await ex.place_limit("F", Side.BUY, D("1"), D("10"))
    assert not r.ok and "OFFLINE" in r.error
    r = await ex.place_limit("NOPE", Side.BUY, D("1"), D("10"))
    assert not r.ok
    r = await ex.place_limit("BTC", Side.BUY, D("0.001"), D("60000"), client_id="bad id")
    assert not r.ok
    assert not m.calls("POST", "/v1/placeOrder")
    # reduce-only is exempt from the $5 minimum notional
    m.post("/v1/placeOrder", status=202, payload={"orderId": OID, "status": "ACK"})
    r = await ex.place_limit("BTC", Side.SELL, D("0.0001"), D("1000"), reduce_only=True)
    assert r.ok


async def test_place_429_honours_retry_after(env):
    factory, server, m = env
    ex = await factory()
    m.post("/v1/placeOrder", status=429, payload={"error": "rate limited", "reason": "account_empty",
                                                       "retryAfterMs": 850})
    r = await ex.place_limit("BTC", Side.BUY, D("0.001"), D("60000"))
    assert not r.ok and r.retryable
    assert ex.budget_health() == 0.0
    r2 = await ex.place_limit("BTC", Side.BUY, D("0.001"), D("60000"))
    assert not r2.ok and r2.retryable and len(m.calls("POST", "/v1/placeOrder")) == 1
    await asyncio.sleep(0.9)
    assert ex.budget_health() > 0


async def test_timestamp_expired_resyncs_and_retries_once(env):
    factory, server, m = env
    ex = await factory(ack_timeout_s=0.05)
    n_time = len(m.calls("GET", "/v1/time"))
    m.post("/v1/placeOrder", status=401, payload={"error": "unauthorized: timestamp expired"})
    m.post("/v1/placeOrder", status=202, payload={"orderId": OID, "status": "ACK"})
    r = await ex.place_limit("BTC", Side.BUY, D("0.001"), D("60000"))
    assert r.ok
    posts = m.calls("POST", "/v1/placeOrder")
    assert len(posts) == 2 and len(m.calls("GET", "/v1/time")) == n_time + 1
    ts = [int(c.headers["X-Timestamp"]) for c in posts]
    assert ts[1] > ts[0]


async def test_place_unknown_outcome_cancels_by_client_id(env):
    factory, server, m = env
    ex = await factory(ack_timeout_s=0.05)
    m.post("/v1/placeOrder", drop=True)
    m.post("/v1/cancelOrder", payload={"status": "CANCEL_ACKNOWLEDGED"})
    r = await ex.place_limit("BTC", Side.BUY, D("0.001"), D("60000"), client_id="q-77")
    assert not r.ok and r.retryable and r.order.status is OrderStatus.UNKNOWN and r.order.client_id == "q-77"
    assert await wait_for(lambda: len(m.calls("POST", "/v1/cancelOrder")) == 1)
    body = m.calls("POST", "/v1/cancelOrder")[0].json()
    assert body["kind"] == "clientId" and body["clientId"] == "q-77"


async def test_place_market_clamps_to_mark_band_and_ioc_cancel(env):
    factory, server, m = env
    ex = await factory()
    md_sock(server).push({"type": "channel_data", "channel": "oraclePrices", "id": "", "contents": {
        "prices": [{"marketDisplayName": "BTC-USD", "marketId": 1, "price": "80010", "markPrice": "80000"}]}})
    assert await wait_for(lambda: "BTC" in ex._marks)
    cap = {}

    def cb(call):
        b = call.json()
        cap["body"] = b
        ack_frame_later(acct_sock(server), order_row(oid=OID, cid=b["clientId"], tif="IOC", status="REJECTED",
                                                     price=b["price"], rejectionReason="IOC_CANCELED", seq=9))
        return Resp(202, {"orderId": OID, "status": "ACK"})

    m.post("/v1/placeOrder", callback=cb)
    r = await ex.place_market("BTC", Side.BUY, D("0.001"), worst_price=D("100000"))
    b = cap["body"]
    assert b["orderType"] == "MARKET" and b["timeInForce"] == "IOC" and b["price"] == "87600"
    assert r.ok and r.order.ioc and r.order.status is OrderStatus.CANCELED and r.order.filled == 0
    # SELL side clamps the other way and rounds UP onto the tick
    m.post("/v1/placeOrder", callback=cb)
    await ex.place_market("BTC", Side.SELL, D("0.001"), worst_price=D("1"))
    assert cap["body"]["price"] == "72400"


async def test_cancel_order_by_id_and_already_gone(env):
    factory, server, m = env
    ex = await factory()
    r, _ = await place_open(ex, server, m)
    m.post("/v1/cancelOrder", payload={"status": "CANCEL_ACKNOWLEDGED"})
    assert await ex.cancel_order("BTC", client_id=r.order.client_id)
    body = m.calls("POST", "/v1/cancelOrder")[0].json()
    assert body["kind"] == "orderId" and body["orderId"] == OID  # orderId preferred once known
    m.post("/v1/cancelOrder", status=400, payload={"error": "order not found", "rejectionReason": "ORDER_NOT_FOUND"})
    assert await ex.cancel_order("BTC", order_id="ffffffffffffffff")
    m.post("/v1/cancelOrder", status=429, payload={"error": "rate limited", "retryAfterMs": 10})
    assert not await ex.cancel_order("BTC", order_id="eeeeeeeeeeeeeeee")
    # terminal locally -> no request
    acct_sock(server).push(orders_frame(order_row(oid=OID, cid=r.order.client_id, status="CANCELED", seq=99)))
    assert await wait_for(lambda: r.order.status is OrderStatus.CANCELED)
    n = len(m.calls("POST", "/v1/cancelOrder"))
    assert await ex.cancel_order("BTC", order_id=OID)
    assert len(m.calls("POST", "/v1/cancelOrder")) == n


async def test_failed_cancel_placeholder_frame_does_not_touch_order(env):
    factory, server, m = env
    ex = await factory()
    r, _ = await place_open(ex, server, m)
    seen = []
    ex.add_order_listener(seen.append)
    ph = dict(load("orders_frames.json")["reject_not_found"]["contents"], orderId=OID)
    acct_sock(server).push(orders_frame(ph))
    await asyncio.sleep(0.05)
    assert r.order.status is OrderStatus.OPEN and not seen


async def test_cancel_all_is_scheme2_signed(env):
    factory, server, m = env
    ex = await factory()
    m.post("/v1/cancelAllOrders", status=202, payload={"status": "CANCEL_ALL_ACKNOWLEDGED"})
    await ex.cancel_all("ETH")
    [call] = m.calls("POST", "/v1/cancelAllOrders")
    h, raw = call.headers, call.text
    assert call.json() == {"address": ADDR, "accountIndex": 0, "marketId": 2}
    msg = legacy_message(int(h["X-Timestamp"]), "cancelAllOrders", call.json())
    assert msg.decode().endswith(raw)  # the exact bytes sent are the canonical bytes signed
    assert verify_signature(API_KEY, msg, h["X-Signature"])
    m.post("/v1/cancelAllOrders", status=503, payload={"error": "disabled"})
    with pytest.raises(ExchangeError):
        await ex.cancel_all()


async def test_modify_order_amend_flow(env):
    factory, server, m = env
    ex = await factory()
    r, cap = await place_open(ex, server, m)
    seen = []
    ex.add_order_listener(seen.append)
    mcap = {}

    def cb(call):
        b = call.json()
        mcap.update(body=b, headers=call.headers, query=call.query)
        leg = order_row(oid=OID, cid=r.order.client_id, status="CANCELED", cancelReason="MODIFY_CANCELED", seq=11)
        new = order_row(oid=OID, cid=r.order.client_id, price=b["price"], size=b["quantity"], seq=12)
        ack_frame_later(acct_sock(server), leg)
        ack_frame_later(acct_sock(server), new, delay=0.02)
        return Resp(202, {"orderId": OID, "status": "ACK"})

    m.post("/v1/modifyOrder", callback=cb)
    res = await ex.modify_order(r.order, price=D("60000.5"))
    assert res.ok and res.order is r.order and r.order.price == D("60000.5") and r.order.status is OrderStatus.OPEN
    assert [o.status for o in seen] == [OrderStatus.OPEN]  # the MODIFY_CANCELED leg is never emitted
    b, h = mcap["body"], mcap["headers"]
    assert "timestamp" not in b and b["clientTime"] == h["X-Timestamp"] and b["orderId"] == OID
    assert b["goodTilTime"] == cap["body"]["goodTilTime"] and b["timeInForce"] == "ALO" and b["side"] == "BUY"
    assert "address" not in mcap["query"]  # SDK: modify posts address in the body only
    msg = modify_payload(address=ADDR, account_index=0, ct=int(h["X-Timestamp"]), gtt_us=int(b["goodTilTime"]),
                         market_id=1, price_ticks=600005, qty_quantums=100000, reduce_only=False, side="BUY",
                         tif="ALO", order_id=OID)
    assert verify_signature(API_KEY, msg, h["X-Signature"])


async def test_modify_total_size_semantics_after_partial_fill(env):
    factory, server, m = env
    ex = await factory(ack_timeout_s=0.05)
    r, _ = await place_open(ex, server, m, size="0.002")
    acct_sock(server).push(orders_frame(order_row(oid=OID, cid=r.order.client_id, size="0.002", filled="0.0005",
                                                  state="PARTIALLY_FILLED", seq=20)))
    assert await wait_for(lambda: r.order.filled == D("0.0005"))
    m.post("/v1/modifyOrder", status=202, payload={"orderId": OID, "status": "ACK"})
    res = await ex.modify_order(r.order, price=D("60000"), size=D("0.001"))
    assert res.ok and res.order.status is OrderStatus.PENDING  # no frames -> pending copy
    assert m.calls("POST", "/v1/modifyOrder")[0].json()["quantity"] == "0.0015"


async def test_crossing_alo_modify_kills_order(env):
    factory, server, m = env
    ex = await factory()
    r, _ = await place_open(ex, server, m)

    def cb(call):
        b = call.json()
        ack_frame_later(acct_sock(server), order_row(oid=OID, cid=r.order.client_id, status="CANCELED",
                                                     cancelReason="MODIFY_CANCELED", seq=11))
        ack_frame_later(acct_sock(server), order_row(oid=OID, cid=r.order.client_id, status="REJECTED",
                                                     price=b["price"], rejectionReason="POST_ONLY_WOULD_CROSS",
                                                     seq=12), delay=0.02)
        return Resp(202, {"orderId": OID, "status": "ACK"})

    m.post("/v1/modifyOrder", callback=cb)
    res = await ex.modify_order(r.order, price=D("60100"))
    assert not res.ok and res.would_cross and r.order.status is OrderStatus.REJECTED


async def test_modify_action_reject_leaves_order_intact(env):
    factory, server, m = env
    ex = await factory()
    r, _ = await place_open(ex, server, m)
    ph = dict(load("orders_frames.json")["reject_not_found_modify"]["contents"], orderId=OID)

    def cb(call):
        acct_sock(server).push(orders_frame(ph))
        return Resp(202, {"orderId": OID, "status": "ACK"})

    # the venue still lists the order: the placeholder reject never touches it
    m.get(f"/v1/order/{OID}", payload=order_row(oid=OID, cid=r.order.client_id, seq=10))
    m.post("/v1/modifyOrder", callback=cb)
    res = await ex.modify_order(r.order, price=D("60000.2"))
    assert not res.ok and res.order is r.order
    assert await wait_for(lambda: len(m.calls("GET", f"/v1/order/{OID}")) == 1)
    await asyncio.sleep(0.02)
    assert r.order.status is OrderStatus.OPEN and r.order.price == D("60000")


async def test_modify_not_found_for_confirmed_order_is_final_and_verified(env):
    """ORDER_NOT_FOUND_FOR_MODIFY for an order the stream already confirmed = it is gone (e.g. closed during
    a private-stream outage): not retryable (the trader stops amending a phantom) and checked over REST."""
    factory, server, m = env
    ex = await factory()
    r, _ = await place_open(ex, server, m)
    ph = dict(load("orders_frames.json")["reject_not_found_modify"]["contents"], orderId=OID)

    def cb(call):
        acct_sock(server).push(orders_frame(ph))
        return Resp(202, {"orderId": OID, "status": "ACK"})

    m.get(f"/v1/order/{OID}", payload=order_row(oid=OID, cid=r.order.client_id, status="CANCELED", seq=12))
    m.post("/v1/modifyOrder", callback=cb)
    res = await ex.modify_order(r.order, price=D("60000.2"))
    assert not res.ok and not res.retryable and "not found" in res.error.lower()
    assert await wait_for(lambda: r.order.status is OrderStatus.CANCELED)
    # 200 REJECTED response body takes the same path
    r2, _ = await place_open(ex, server, m, oid="00000000000000b2", seq=20)
    m.get("/v1/order/00000000000000b2", payload=order_row(oid="00000000000000b2", cid=r2.order.client_id, seq=20))
    m.post("/v1/modifyOrder", payload={"orderId": "00000000000000b2", "status": "REJECTED",
                                       "rejectionReason": "ORDER_NOT_FOUND_FOR_MODIFY"})
    res2 = await ex.modify_order(r2.order, price=D("60000.2"))
    assert not res2.ok and not res2.retryable and "not found" in res2.error.lower()
    # while a modify replacement is still unsequenced the reject is transient -> retryable
    ex._find_rec("00000000000000b2", None).in_modify = True
    m.post("/v1/modifyOrder", payload={"orderId": "00000000000000b2", "status": "REJECTED",
                                       "rejectionReason": "ORDER_NOT_FOUND_FOR_MODIFY"})
    res3 = await ex.modify_order(r2.order, price=D("60000.3"))
    assert not res3.ok and res3.retryable


async def test_modify_local_cross_guard(env):
    factory, server, m = env
    ex = await factory()
    await ex.subscribe_books(["BTC"])
    md_sock(server).push({"type": "subscribed", "channel": "l2OrderbookUpdates", "id": "BTC-USD",
                          "contents": {"bids": [["59999.9", "1"]], "asks": [["60000.2", "1"]], "lastSequenceId": 9}})
    assert await wait_for(lambda: ex.get_book("BTC") is not None)
    r, _ = await place_open(ex, server, m, price="59999.8")
    res = await ex.modify_order(r.order, price=D("60000.2"))
    assert not res.ok and res.would_cross and r.order.status is OrderStatus.OPEN
    assert not m.calls("POST", "/v1/modifyOrder")


# ------------------------------------------------------------------ fills
async def test_fills_emitted_once_and_recovered_after_reconnect(env):
    factory, server, m = env
    ex = await factory()
    fills = []
    ex.add_fill_listener(fills.append)
    upd = load("userfills_frames.json")["updates"][0]
    sock = acct_sock(server)
    sock.push(upd)
    sock.push(upd)  # duplicate delivery
    assert await wait_for(lambda: len(fills) == 1)
    await asyncio.sleep(0.05)
    assert len(fills) == 1
    f0 = upd["contents"]["fills"][0]
    assert fills[0].trade_id == f0["tradeId"] and fills[0].size == D(f0["size"])
    # reconnect: the userFills snapshot contains an old fill (before this session) and a new missed one
    now_us = int(time.time() * 1e6)
    old = dict(f0, tradeId="1", orderId="a" * 16, createdAt=now_us - 3600 * 10**6)
    missed = dict(f0, tradeId="2", orderId="b" * 16, createdAt=now_us)
    server.on_subscribe = lambda s, msg: ({"type": "subscribed", "channel": "userFills", "id": msg["id"],
                                           "accountIndex": 0, "contents": {"isSnapshot": True,
                                                                           "fills": [old, missed, f0]}}
                                          if msg["channel"] == "userFills" else None)
    await sock.close()
    assert await wait_for(lambda: len(server.sockets_for("userFills")) == 2)
    assert await wait_for(lambda: len(fills) == 2)
    await asyncio.sleep(0.05)
    assert [f.trade_id for f in fills] == [f0["tradeId"], "2"]
    # the replayed fill is flagged: positions may already have been re-read from REST and include it
    assert getattr(fills[1], "recovered", False) is True and not getattr(fills[0], "recovered", False)


async def test_orders_snapshot_after_reconnect_updates_known_orders(env):
    factory, server, m = env
    ex = await factory()
    r, _ = await place_open(ex, server, m)
    filled = order_row(oid=OID, cid=r.order.client_id, status="FILLED", filled="0.001", seq=50, avgFillPrice="60000")
    server.on_subscribe = lambda s, msg: ({"type": "subscribed", "channel": "orders", "id": msg["id"], "accountIndex": 0,
                                           "contents": {"isSnapshot": True, "lastSequenceId": 50, "openOrders": [],
                                                        "recentClosedOrders": [filled]}}
                                          if msg["channel"] == "orders" else None)
    await acct_sock(server).close()
    assert await wait_for(lambda: r.order.status is OrderStatus.FILLED)
    assert r.order.filled == D("0.001")


# ------------------------------------------------------------------ leverage / DMS / health
async def test_set_leverage_clamped_and_rejections(env):
    factory, server, m = env
    ex = await factory()
    m.post("/v1/setLeverage", payload={"status": "APPLIED", "leverage": 40})
    await ex.set_leverage("BTC", 100)
    [call] = m.calls("POST", "/v1/setLeverage")
    b, h = call.json(), call.headers
    assert b == {"address": ADDR, "accountIndex": 0, "marketId": 1, "leverage": 40}
    assert verify_signature(API_KEY, legacy_message(int(h["X-Timestamp"]), "setLeverage", b), h["X-Signature"])
    m.post("/v1/setLeverage", status=422, payload={"status": "REJECTED", "rejectReason": "HAS_OPEN_POSITION"})
    with pytest.raises(ExchangeError) as ei:
        await ex.set_leverage("BTC", 5)
    assert "HAS_OPEN_POSITION" in str(ei.value)


async def test_dead_mans_switch_clamp_and_signature(env):
    factory, server, m = env
    ex = await factory()
    m.post("/v1/scheduleCancel", payload={"status": "scheduled"}, repeat=True)
    for req_s, lo, hi in ((1, 5.0, 6.0), (60, 59.0, 61.0), (10_000, 298.0, 300.0)):
        await ex.arm_dead_mans_switch(req_s)
        call = m.calls("POST", "/v1/scheduleCancel")[-1]
        b, h = call.json(), call.headers
        lead = b["time"] / 1e6 - time.time()
        assert lo <= lead <= hi, (req_s, lead)
        assert verify_signature(API_KEY, legacy_message(int(h["X-Timestamp"]), "scheduleCancel", b), h["X-Signature"])
    await ex.arm_dead_mans_switch(0)
    assert "time" not in m.calls("POST", "/v1/scheduleCancel")[-1].json()


async def test_dms_armed_only_while_orders_rest(env):
    """Every arm/refresh is charged to the lifetime cancel pool: arm before the first resting order, refresh
    while something rests, disarm once when idle; never refresh while the private stream is unhealthy."""
    factory, server, m = env
    m.post("/v1/scheduleCancel", payload={"status": "scheduled"}, repeat=True)
    ex = await factory(dead_mans_switch_s=60)
    await asyncio.sleep(0.05)
    assert not m.calls("POST", "/v1/scheduleCancel")  # nothing rests -> not armed
    assert ex.can_increase_exposure() == (True, "")
    r, _ = await place_open(ex, server, m)
    [arm] = m.calls("POST", "/v1/scheduleCancel")
    assert 58 < arm.json()["time"] / 1e6 - time.time() <= 60
    assert m.log.index(arm) < m.log.index(m.calls("POST", "/v1/placeOrder")[0])  # armed BEFORE the place
    await ex._refresh_dms()  # an order rests -> refresh
    arms = m.calls("POST", "/v1/scheduleCancel")
    assert len(arms) == 2 and "time" in arms[-1].json()
    ex._dms_last_ok = time.monotonic() - 50  # refreshes failing: orders rest without protection
    ok, why = ex.can_increase_exposure()
    assert not ok and "dead man" in why
    ex._dms_last_ok = time.monotonic()
    acct_sock(server).push(orders_frame(order_row(oid=OID, cid=r.order.client_id, status="CANCELED", seq=99)))
    assert await wait_for(lambda: r.order.status is OrderStatus.CANCELED)
    await ex._refresh_dms()  # idle -> disarm once (saves the pool and the 10/day auto-fire quota)
    assert "time" not in m.calls("POST", "/v1/scheduleCancel")[-1].json() and ex._dms_last_ok is None
    n = len(m.calls("POST", "/v1/scheduleCancel"))
    await ex._refresh_dms()
    assert len(m.calls("POST", "/v1/scheduleCancel")) == n
    # the next quote arms again; a dead private stream stops the refresh (the switch fires)
    await place_open(ex, server, m, oid="00000000000000b2", seq=120)
    assert len(m.calls("POST", "/v1/scheduleCancel")) == n + 1
    server.fail_connects = 10**6  # private socket goes down and stays down
    await acct_sock(server).close()
    assert await wait_for(lambda: not ex._acct.connected)
    await ex._refresh_dms()
    assert len(m.calls("POST", "/v1/scheduleCancel")) == n + 1  # not refreshed -> it will fire
    await ex.close()
    assert len(m.calls("POST", "/v1/scheduleCancel")) == n + 1  # an order may rest -> no disarm at close


async def test_quote_placed_during_the_idle_disarm_arms_the_switch_again(env):
    """The idle disarm marks the switch unarmed BEFORE its request: a place racing it waits and re-arms instead of
    trusting the pre-disarm timestamp (and resting unprotected until the next refresh)."""
    factory, server, m = env
    ex = await factory(dead_mans_switch_s=60)
    calls = []
    gate = asyncio.Event()

    async def fake_arm(t):
        calls.append(t)
        if not t:
            await gate.wait()  # the disarm request is slow
        ex._dms_last_ok = time.monotonic() if t else None

    ex.arm_dead_mans_switch = fake_arm
    ex._dms_last_ok = time.monotonic()  # armed a moment ago, nothing rests
    disarm = asyncio.create_task(ex._refresh_dms())
    await wait_for(lambda: calls == [0])
    arm = asyncio.create_task(ex._ensure_dms_armed())  # a quote goes out meanwhile
    await asyncio.sleep(0.02)
    gate.set()
    await asyncio.gather(disarm, arm)
    assert calls == [0, 60.0] and ex._dms_last_ok is not None


async def test_dms_disarmed_at_close_when_nothing_rests(env):
    factory, server, m = env
    m.post("/v1/scheduleCancel", payload={"status": "scheduled"}, repeat=True)
    ex = await factory(dead_mans_switch_s=60)
    await ex.arm_dead_mans_switch(60)
    await ex.close()
    assert "time" not in m.calls("POST", "/v1/scheduleCancel")[-1].json()


async def test_budget_health(env):
    factory, server, m = env
    ex = await factory()
    assert 0.9 < ex.budget_health() <= 1.0  # fixture account has a huge lifetime cap
    ex._rate_limit = {"order": {"used": 15000, "cap": 20000, "nextAvailableMs": 0}}
    assert ex.budget_health() == pytest.approx(0.25)
    ex._rate_limit = {"order": {"used": 25000, "cap": 20000, "nextAvailableMs": 9000}}
    assert ex.budget_health() == 0.0
    # the lifetime cancel pool (cancels + DMS refreshes) counts too
    ex._rate_limit = {"order": {"used": 0, "cap": 20000}, "cancel": {"used": 30000, "cap": 40000}}
    assert ex.budget_health() == pytest.approx(0.25)
    ex._pool_remaining["cancel"] = 4000
    assert ex.budget_health() == pytest.approx(0.1)
    ex._pool_blocked_until["cancel"] = time.monotonic() + 5  # cancel pool 429 (drip)
    assert ex.budget_health() == 0.0


async def test_market_open_and_market_refresh_in_place(env):
    factory, server, m = env
    ex = await factory(creds=False)
    btc = ex.markets["BTC"]
    assert ex.market_open("BTC") and not ex.market_open("F") and not ex.market_open("NOPE")
    rows = load("markets.json")["markets"]
    for r in rows:
        if r["marketDisplayName"] == "BTC-USD":
            r["initialMarginFraction"] = "0.05"
    ex._apply_markets(rows)
    assert ex.markets["BTC"] is btc and btc.max_leverage == D("20")


# ------------------------------------------------------------------ review fixes (private stream, cancels, resync)
async def test_quotes_refused_while_private_stream_down_or_unsynced(env):
    """Acks, fills and rejections only arrive on the private stream: no new resting quotes without it."""
    factory, server, m = env
    ex = await factory()
    ex._acct.backoff_max_s = 0.05
    assert ex.can_increase_exposure() == (True, "")
    server.fail_connects = 10**6
    await acct_sock(server).close()
    assert await wait_for(lambda: not ex._acct.connected)
    ok, why = ex.can_increase_exposure()
    assert not ok and "private stream" in why
    for kw in ({}, {"reduce_only": True}):
        r = await ex.place_limit("BTC", Side.SELL, D("0.001"), D("90000"), **kw)
        assert not r.ok and r.retryable and "private stream" in r.error
    assert not m.calls("POST", "/v1/placeOrder")
    # IOC (hedge / emergency close) orders stay allowed
    m.post("/v1/placeOrder", status=202, payload={"orderId": OID, "status": "ACK"})
    await ex.place_market("BTC", Side.BUY, D("0.001"), worst_price=D("100000"))
    assert len(m.calls("POST", "/v1/placeOrder")) == 1
    # back, but the userFills snapshot never arrives -> still unsynced
    server.on_subscribe = lambda s, msg: {} if msg["channel"] == "userFills" else None
    server.fail_connects = 0
    assert await wait_for(lambda: ex._acct.connected and ex._orders_synced.is_set(), timeout=5.0)
    ok, why = ex.can_increase_exposure()
    assert not ok and "not synced" in why
    acct_sock(server).push({"type": "subscribed", "channel": "userFills", "id": ADDR.lower(), "accountIndex": 0,
                            "contents": {"isSnapshot": True, "fills": []}})
    assert await wait_for(lambda: ex.can_increase_exposure() == (True, ""))
    # a degraded (server-side pending) orders subscription is not healthy either
    acct_sock(server).push({"type": "degraded", "channel": "orders", "id": ADDR.lower(), "accountIndex": 0,
                            "reason": "snapshot_stale", "retryAfterMs": 5000})
    assert await wait_for(lambda: not ex.private_ok()[0])


async def test_cancel_overtaken_by_its_own_unsequenced_place_is_resent(env):
    """Spec 6.5: a cancel may overtake its own queued place (ORDER_NOT_FOUND) and the place rests later."""
    factory, server, m = env
    ex = await factory(ack_timeout_s=0.05)
    ex.cancel_recheck_s = 0.02
    m.post("/v1/placeOrder", drop=True)  # transport error -> outcome unknown -> cancel by clientId
    m.post("/v1/cancelOrder", payload={"status": "REJECTED", "rejectionReason": "ORDER_NOT_FOUND", "marketId": 0})
    m.post("/v1/cancelOrder", payload={"status": "CANCEL_ACKNOWLEDGED"})
    r = await ex.place_limit("BTC", Side.BUY, D("0.001"), D("60000"), client_id="q-88")
    assert r.order.status is OrderStatus.UNKNOWN
    assert await wait_for(lambda: len(m.calls("POST", "/v1/cancelOrder")) == 1)
    assert m.calls("POST", "/v1/cancelOrder")[0].json()["kind"] == "clientId"
    # the async ORDER_NOT_FOUND placeholder carries no usable id and changes nothing
    acct_sock(server).push(orders_frame(load("orders_frames.json")["reject_not_found"]["contents"]))
    # ... then the place is sequenced after all and rests
    acct_sock(server).push(orders_frame(order_row(oid=OID, cid="q-88", seq=30)))
    assert await wait_for(lambda: r.order.status is OrderStatus.OPEN)
    assert await wait_for(lambda: len(m.calls("POST", "/v1/cancelOrder")) == 2)
    body = m.calls("POST", "/v1/cancelOrder")[1].json()
    assert body["kind"] == "orderId" and body["orderId"] == OID
    acct_sock(server).push(orders_frame(order_row(oid=OID, cid="q-88", status="CANCELED", seq=31)))
    assert await wait_for(lambda: r.order.status is OrderStatus.CANCELED)
    await asyncio.sleep(0.05)
    assert len(m.calls("POST", "/v1/cancelOrder")) == 2


async def test_cancel_rejected_for_a_live_order_reports_failure(env):
    factory, server, m = env
    ex = await factory()
    r, _ = await place_open(ex, server, m)
    m.post("/v1/cancelOrder", payload={"status": "REJECTED", "rejectionReason": "ORDER_NOT_FOUND"})
    m.get(f"/v1/order/{OID}", payload=order_row(oid=OID, cid=r.order.client_id, status="FILLED", filled="0.001",
                                                seq=11))
    assert await ex.cancel_order("BTC", order_id=OID) is False  # we still believe it lives: not "done"
    assert await wait_for(lambda: r.order.status is OrderStatus.FILLED)  # the venue says why
    assert await ex.cancel_order("BTC", order_id=OID) is True  # terminal now: nothing to do
    assert len(m.calls("POST", "/v1/cancelOrder")) == 1


async def test_order_missing_from_resync_snapshot_is_verified(env):
    """openOrders + only the last nRecentClosed closed orders: an order that closed during the outage and is
    in neither list must not stay OPEN forever."""
    factory, server, m = env
    ex = await factory()
    r, _ = await place_open(ex, server, m)
    r2, _ = await place_open(ex, server, m, oid="00000000000000b2", seq=11)
    r3, _ = await place_open(ex, server, m, oid="00000000000000b3", seq=12)
    m.get(f"/v1/order/{OID}", payload=order_row(oid=OID, cid=r.order.client_id, status="CANCELED", seq=40))
    m.get("/v1/order/00000000000000b2", status=404, payload={"error": "order not found"})
    still = order_row(oid="00000000000000b3", cid=r3.order.client_id, seq=12)
    server.on_subscribe = lambda s, msg: ({"type": "subscribed", "channel": "orders", "id": msg["id"],
                                           "accountIndex": 0,
                                           "contents": {"isSnapshot": True, "lastSequenceId": 100,
                                                        "openOrders": [still], "recentClosedOrders": []}}
                                          if msg["channel"] == "orders" else None)
    await acct_sock(server).close()
    assert await wait_for(lambda: r.order.status is OrderStatus.CANCELED and r2.order.status is OrderStatus.CANCELED,
                          timeout=4.0)
    assert "missing" in r2.order.error
    assert r3.order.status is OrderStatus.OPEN and not m.calls("GET", "/v1/order/00000000000000b3")
    assert ex.stats()["live_orders"] == 1


async def test_place_ambiguous_5xx_is_unknown_not_forgotten(env):
    """A CDN/LB 502 page (no gateway errorType) may follow a forwarded place: keep tracking + cancel it."""
    factory, server, m = env
    ex = await factory(ack_timeout_s=0.05)
    m.post("/v1/placeOrder", status=502, payload="<html>502 Bad Gateway</html>")
    m.post("/v1/cancelOrder", payload={"status": "CANCEL_ACKNOWLEDGED"})
    r = await ex.place_limit("BTC", Side.BUY, D("0.001"), D("60000"), client_id="q-5")
    assert not r.ok and r.retryable and r.order.status is OrderStatus.UNKNOWN and "q-5" in ex._recs_by_cid
    assert await wait_for(lambda: len(m.calls("POST", "/v1/cancelOrder")) == 1)
    assert m.calls("POST", "/v1/cancelOrder")[0].json()["clientId"] == "q-5"
    # the forwarded order shows up: it is ours (tracked), not a foreign order
    acct_sock(server).push(orders_frame(order_row(oid=OID, cid="q-5", seq=7)))
    assert await wait_for(lambda: r.order.status is OrderStatus.OPEN)
    assert not ex._find_rec(OID, "q-5").foreign
    # the gateway's own "not submitted" marker -> safe to forget and resend
    m.post("/v1/placeOrder", status=502, payload={"error": "not submitted", "errorType": "Transmission"})
    r2 = await ex.place_limit("BTC", Side.BUY, D("0.001"), D("60000"), client_id="q-6")
    assert not r2.ok and r2.retryable and r2.order.status is OrderStatus.REJECTED and "q-6" not in ex._recs_by_cid


async def test_market_attributes_flip_rth_without_waiting_for_rest(env):
    factory, server, m = env
    ex = await factory(creds=False)
    ex.rwa_close_buffer_s = 0  # independent of the wall clock
    md = md_sock(server)
    assert md.subs("marketAttributes") == [{"type": "subscribe", "channel": "marketAttributes"}]
    spy = ex.markets["SPY"]
    entry = {"marketId": spy.market_id, "marketDisplayName": "SPY-USD", "offHoursInitialMarginFraction": "0.03",
             "isOutsideRth": False, "currentSettlementPrice": None, "upperTradingBound": None,
             "lowerTradingBound": None}
    md.push({"type": "subscribed", "channel": "marketAttributes", "id": "",
             "contents": {"isSnapshot": True, "entries": [entry]}})
    assert await wait_for(lambda: ex._attrs.get("SPY") is not None)
    assert ex.market_open("SPY")
    md.push({"type": "channel_data", "channel": "marketAttributes", "id": "", "contents": {
        "isSnapshot": False, "entries": [dict(entry, isOutsideRth=True, currentSettlementPrice="660.1",
                                              upperTradingBound="680", lowerTradingBound="640",
                                              boundEvent="exitRth", boundSide=None)]}})
    assert await wait_for(lambda: not ex.market_open("SPY"))
    assert spy.extra["trading_bounds"]["upperTradingBound"] == "680" and spy.extra["off_hours_imf"] == D("0.03")
    await ex._refresh_markets()  # a (cached) REST read still saying in-RTH does not undo the pushed state
    assert ex.markets["SPY"] is spy and not ex.market_open("SPY")


async def test_bbo_checks_loss_window_and_resync_resubscribes_bbo(env):
    factory, server, m = env
    ex = await factory(creds=False)
    await ex.subscribe_books(["ETH"])
    md = md_sock(server)
    md.push({"type": "subscribed", "channel": "l2OrderbookUpdates", "id": "ETH-USD",
             "contents": {"bids": [["2000", "1"]], "asks": [["2000.5", "1"], ["2001", "2"]], "lastSequenceId": 10}})
    # the ask 2000.5 was lifted inside the loss window; this bbo arrives before any delta (book can't use it yet)
    md.push({"type": "channel_data", "channel": "bbo", "id": "ETH-USD", "contents": {
        "bestBid": {"price": "2000", "size": "1"}, "bestAsk": {"price": "2001", "size": "2"}, "lastSequenceId": 19}})
    md.push({"type": "channel_data", "channel": "l2OrderbookUpdates", "id": "ETH-USD",
             "contents": {"bids": [["1990", "1"]], "asks": [], "lastSequenceId": 20}})  # first delta: deep only
    assert await wait_for(lambda: ex.get_book("ETH") is not None and ex.get_book("ETH").best_ask == D("2001"))
    # a gap -> resync resubscribes the bbo too (a fresh bbo snapshot right after the new l2 snapshot)
    md.push({"type": "channel_data", "channel": "l2OrderbookUpdates", "id": "ETH-USD",
             "contents": {"bids": [], "asks": [], "lastSequenceId": 25}})
    assert await wait_for(lambda: len(md.subs("bbo", kind="unsubscribe")) == 1 and len(md.subs("bbo")) == 2)
    # quiet market: the top is still a snapshot-only level well after the snapshot -> one bbo re-poll
    md.push({"type": "subscribed", "channel": "l2OrderbookUpdates", "id": "ETH-USD",
             "contents": {"bids": [["2000", "1"]], "asks": [["2001", "2"]], "lastSequenceId": 30}})
    md.push({"type": "channel_data", "channel": "l2OrderbookUpdates", "id": "ETH-USD",
             "contents": {"bids": [["1990", "2"]], "asks": [], "lastSequenceId": 40}})
    book = ex._books["ETH"]
    assert await wait_for(lambda: book.applying and book.seq == 40)
    book.snapshot_ts -= 5
    await ex._book_monitor()
    assert await wait_for(lambda: len(md.subs("bbo")) == 3)
    await ex._book_monitor()
    await asyncio.sleep(0.02)
    assert len(md.subs("bbo")) == 3  # once per snapshot
