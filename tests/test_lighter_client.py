"""LighterExchange end-to-end with a fake websocket, a fake signer and mocked REST (no network)."""

import asyncio
import copy
import hashlib
import json
import logging
import time
from decimal import Decimal as D
from pathlib import Path

import pytest
from aiohttp import web

from farmer.config import LighterConfig
from farmer.exchanges.base import AuthRequired, ExchangeError
from farmer.exchanges.lighter import LighterExchange
from farmer.exchanges.lighter.signer import SignedTx
from farmer.models import OrderStatus, Side

DATA = Path(__file__).parent / "data" / "lighter"
ACCT = 313004
LLP = 281474976710654

OBD = "/api/v1/orderBookDetails"
NONCE = "/api/v1/nextNonce"
SENDTX = "/api/v1/sendTx"
ACCOUNT = "/api/v1/account"
ACCOUNT_ORDERS = "/api/v1/accountOrders"
FUNDING_RATES = "/api/v1/funding-rates"
OBO = "/api/v1/orderBookOrders"
TX = "/api/v1/tx"


class FakeRest:
    """Local stand-in for https://mainnet.zklighter.elliot.ai (aiohttp test server)."""

    def __init__(self) -> None:
        self.routes: dict[tuple[str, str], list] = {}
        self.requests: list[dict] = []
        self.runner = None
        self.url = ""

    def get(self, path, payload=None, *, status=200, body=None, repeat=False):
        self.routes.setdefault(("GET", path), []).append((status, payload, body, repeat))

    def post(self, path, payload=None, *, status=200, body=None, repeat=False):
        self.routes.setdefault(("POST", path), []).append((status, payload, body, repeat))

    def calls(self, method, path):
        return [r for r in self.requests if r["method"] == method and r["path"] == path]

    async def _handle(self, request):
        form = dict(await request.post()) if request.method == "POST" else {}
        self.requests.append({"method": request.method, "path": request.path, "query": dict(request.query),
                              "form": form, "headers": dict(request.headers)})
        queue = self.routes.get((request.method, request.path))
        if not queue:
            return web.json_response({"code": 404, "message": "no route"}, status=404)
        status, payload, body, repeat = queue[0] if (len(queue) == 1 and queue[0][3]) else queue.pop(0)
        if body is not None:
            return web.Response(status=status, text=body)
        return web.json_response(payload, status=status)

    async def start(self):
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._handle)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        return self

    async def stop(self):
        await self.runner.cleanup()


def load(name):
    return json.loads((DATA / name).read_text())


# --------------------------------------------------------------------------- fakes
class FakeSigner:
    def __init__(self, account_index: int = ACCT, api_key_index: int = 5) -> None:
        self.account_index = account_index
        self.api_key_index = api_key_index
        self.calls: list[tuple[str, dict]] = []
        self.tokens = 0

    def _tx(self, tx_type: int, kind: str, info: dict, nonce: int) -> SignedTx:
        info = {"AccountIndex": self.account_index, "ApiKeyIndex": self.api_key_index, **info, "Nonce": nonce}
        s = json.dumps(info)
        self.calls.append((kind, info))
        return SignedTx(tx_type, s, hashlib.sha256(s.encode()).hexdigest()[:80].ljust(80, "0"), nonce, kind)

    def create_order(self, *, market_index, client_order_index, base_amount, price, is_ask, order_type, time_in_force,
                     reduce_only, order_expiry, nonce):
        return self._tx(14, "create", {"MarketIndex": market_index, "ClientOrderIndex": client_order_index,
                                       "BaseAmount": base_amount, "Price": price, "IsAsk": int(is_ask), "Type": order_type,
                                       "TimeInForce": time_in_force, "ReduceOnly": int(reduce_only),
                                       "OrderExpiry": order_expiry}, nonce)

    def cancel_order(self, *, market_index, order_index, nonce):
        return self._tx(15, "cancel", {"MarketIndex": market_index, "Index": order_index}, nonce)

    def cancel_all(self, *, market_index=255, time_in_force=0, timestamp_ms=0, nonce):
        return self._tx(16, "cancel_all", {"Market": market_index, "TimeInForce": time_in_force, "Time": timestamp_ms}, nonce)

    def modify_order(self, *, market_index, order_index, base_amount, price, nonce):
        return self._tx(17, "modify", {"MarketIndex": market_index, "Index": order_index, "BaseAmount": base_amount,
                                       "Price": price}, nonce)

    def update_leverage(self, *, market_index, fraction, margin_mode=0, nonce):
        return self._tx(20, "leverage", {"MarketIndex": market_index, "InitialMarginFraction": fraction,
                                         "MarginMode": margin_mode}, nonce)

    def auth_token(self, ttl_s):
        self.tokens += 1
        return f"{int(time.time() + ttl_s)}:{self.account_index}:{self.api_key_index}:tok{self.tokens}", time.time() + ttl_s

    def check_client(self):
        return None

    async def close(self):
        pass


class FakeWS:
    def __init__(self, channels=()) -> None:
        self.subs: dict = {ch: None for ch in channels}
        self.resubs: list[str] = []
        self.connected = True
        self.restricted = False
        self.started = False
        self.sent: list[tuple[int, dict, str]] = []
        self.script: list = []
        self.stats = {}
        self.rx_age = 0.05

    @property
    def can_send_tx(self):
        return self.connected and not self.restricted and self.rx_age < 3.0

    @property
    def channels(self):
        return list(self.subs)

    def subscribe(self, ch, auth=None):
        self.subs[ch] = auth

    def resubscribe(self, ch):
        self.resubs.append(ch)

    def unsubscribe(self, ch):
        self.subs.pop(ch, None)

    async def start(self):
        self.started = True

    async def stop(self):
        self.connected = False

    async def wait_first_attempt(self, timeout):
        return True

    async def wait_connected(self, timeout):
        return self.connected

    def last_rx_age(self):
        return self.rx_age

    async def send_tx(self, tx_type, tx_info, tx_hash, timeout):
        self.sent.append((tx_type, tx_info, tx_hash))
        action = self.script.pop(0) if self.script else None
        if action is None:
            return {"id": "x", "code": 200, "tx_hash": tx_hash}
        if action == "timeout":
            raise asyncio.TimeoutError
        if callable(action):
            return action(tx_type, tx_info, tx_hash)
        return action


def cfg(**kw) -> LighterConfig:
    base = dict(account_index=ACCT, api_key_index=5, api_private_key="0x" + "11" * 40)
    base.update(kw)
    return LighterConfig(**base)


async def make_exchange(m: FakeRest, *, creds: bool = True, account_index: int = ACCT, nonce: int = 10, **kw):
    c = cfg(account_index=account_index, url=m.url, **kw) if creds else LighterConfig(url=m.url)
    signer = FakeSigner(account_index)
    ex = LighterExchange(c, signer_factory=(lambda: signer) if creds else None)
    ex._ws = FakeWS(ex._ws.channels)
    m.get(OBD, payload=load("order_book_details.json"), repeat=True)
    m.get(NONCE, payload={"code": 200, "nonce": nonce}, repeat=True)
    await ex.connect()
    ex._order_ack_timeout_s = 0.05
    ex._fill_wait_s = 0.2
    return ex, signer


@pytest.fixture
async def mocked():
    m = await FakeRest().start()
    yield m
    await m.stop()


def our_trade(coi, oidx, tid, size, price, *, account=ACCT, side="buy", maker=False, tx_hash="ab" * 40, market=0):
    """Trade JSON in the live schema with OUR account on one side."""
    t = {"trade_id": tid, "trade_id_str": str(tid), "tx_hash": tx_hash, "type": "trade", "market_id": market,
         "market_symbol": "ETH", "market_kind": "perps", "size": str(size), "price": str(price),
         "usd_amount": str(D(size) * D(price)), "timestamp": 1792430288798, "transaction_time": 1792430288826504}
    me = {"id": oidx, "client_id": coi, "account_id": account}
    other = {"id": 281478048151728, "client_id": 84244782, "account_id": 685340}
    bid, ask = (me, other) if side == "buy" else (other, me)
    for k, v in (("bid", bid), ("ask", ask)):
        t[f"{k}_id"] = v["id"]
        t[f"{k}_client_id"] = v["client_id"]
        t[f"{k}_account_id"] = v["account_id"]
    t["is_maker_ask"] = (side == "buy") != maker
    return t


def order_json(coi, oidx, *, status, filled="0", quote="0", size="0.1", price="2400.00", market=0, tif="immediate-or-cancel",
               is_ask=False):
    return {"order_index": oidx, "client_order_index": coi, "market_index": market, "initial_base_amount": size,
            "remaining_base_amount": str(D(size) - D(filled)), "filled_base_amount": filled, "filled_quote_amount": quote,
            "price": price, "is_ask": is_ask, "type": "market" if tif == "immediate-or-cancel" else "limit",
            "time_in_force": tif, "reduce_only": False, "status": status, "timestamp": 1792430222}


def frame_account_all(trades=None, positions=None, snapshot=False, account=ACCT):
    return {"account": account, "channel": f"account_all:{account}", "type": "subscribed/account_all" if snapshot else "update/account_all",
            "positions": positions or {}, "trades": trades or {}, "funding_histories": {}, "assets": None}


def frame_orders(orders, snapshot=False, account=ACCT):
    by_m: dict = {}
    for o in orders:
        by_m.setdefault(str(o["market_index"]), []).append(o)
    return {"channel": f"account_all_orders:{account}", "orders": by_m,
            "type": "subscribed/account_all_orders" if snapshot else "update/account_all_orders"}


def collect(ex):
    fills, orders, books = [], [], []
    ex.add_fill_listener(fills.append)
    ex.add_order_listener(lambda o: orders.append((o.status, o.filled)))
    ex.add_book_listener(books.append)
    return fills, orders, books


# --------------------------------------------------------------------------- lifecycle / markets
async def test_public_connect(mocked):
    ex, _ = await make_exchange(mocked, creds=False)
    try:
        assert not ex.has_credentials and ex._ws.started
        assert ex._ws.channels == ["market_stats/all"]
        assert ex.markets["ETH"].market_id == 0 and ex.markets["1000BONK"].venue_symbol == "1000BONK"
        assert ex.market_open("ETH") and not ex.market_open("GME")
        with pytest.raises(AuthRequired):
            await ex.fetch_positions()
        r = await ex.place_market("ETH", Side.BUY, D("0.01"), worst_price=D("2500"))
        assert not r.ok and "credentials" in r.error
    finally:
        await ex.close()
        await ex.close()  # idempotent


async def test_trading_connect_subscribes_account_channels(mocked):
    ex, signer = await make_exchange(mocked)
    try:
        chans = ex._ws.subs
        assert set(chans) >= {"market_stats/all", f"account_all/{ACCT}", f"user_stats/{ACCT}",
                              f"account_all_orders/{ACCT}", f"account_tx/{ACCT}"}
        assert chans[f"account_all/{ACCT}"] is None and chans[f"account_all_orders/{ACCT}"] is not None
        tok = chans[f"account_all_orders/{ACCT}"]()
        assert tok.split(":")[1:3] == [str(ACCT), "5"]
        assert chans[f"account_tx/{ACCT}"]() == tok  # cached until refresh
        assert ex._executor.next_nonce == 10
        assert ex._limiter.available() == ex.cfg.max_requests_per_minute - 2  # markets + nextNonce
    finally:
        await ex.close()


async def test_restricted_handshake_fails_connect(mocked):
    c = cfg(url=mocked.url)
    ex = LighterExchange(c, signer_factory=lambda: FakeSigner())
    ex._ws = FakeWS(ex._ws.channels)
    ex._ws.restricted = True
    mocked.get(OBD, payload=load("order_book_details.json"))
    mocked.get(NONCE, payload={"code": 200, "nonce": 1})
    with pytest.raises(ExchangeError) as ei:
        await ex.connect()
    assert ei.value.code == 20558 and not ei.value.retryable and "permitted" in str(ei.value)
    assert ex.can_increase_exposure()[0] is False


# --------------------------------------------------------------------------- books
async def test_books_from_ws_frames_and_gap_resubscribe(mocked):
    ex, _ = await make_exchange(mocked, creds=False)
    fills, orders, books = collect(ex)
    try:
        await ex.subscribe_books(["ETH"])
        assert "order_book/0" in ex._ws.subs
        frames = [json.loads(l) for l in (DATA / "ws_order_book_eth.jsonl").read_text().splitlines()]
        book_frames = [f for f in frames if f["channel"] == "order_book:0"]
        for f in book_frames[:50]:
            ex._on_ws_message(f)
        assert len(books) == 50 and all(len(b.bids) <= 20 and len(b.asks) <= 20 for b in books)
        b = ex.get_book("ETH")
        assert b is not None and b.best_bid < b.best_ask and b.seq == book_frames[49]["order_book"]["nonce"]
        assert all(b.bids[i].price > b.bids[i + 1].price for i in range(len(b.bids) - 1))
        # gap: skip one update
        ex._on_ws_message(book_frames[51])
        assert ex.get_book("ETH") is None and ex._ws.resubs == ["order_book/0"]
        # new snapshot restores it
        ex._on_ws_message(book_frames[0])
        assert ex.get_book("ETH") is not None
        # a second gap right away is backed off (protects the 200 msg/min WS budget) and retried later
        ex._on_ws_message(book_frames[52])
        assert ex._ws.resubs == ["order_book/0"] and 0 in ex._book_resub_pending
        # a disconnect invalidates every book
        ex._on_ws_disconnect("test")
        assert ex.get_book("ETH") is None
    finally:
        await ex.close()


async def test_fetch_book_rest_aggregates_orders(mocked):
    ex, _ = await make_exchange(mocked, creds=False)
    try:
        raw = load("rest_order_book_orders_btc.json")
        mocked.get(OBO, payload=raw)
        b = await ex.fetch_book("BTC", depth=5)
        assert mocked.calls("GET", OBO)[0]["query"] == {"market_id": "1", "limit": "25"}
        assert len(b.asks) <= 5 and b.asks == sorted(b.asks, key=lambda l: l.price)
        top = D(raw["asks"][0]["price"])
        assert b.best_ask == top and b.asks[0].size == sum(D(a["remaining_base_amount"]) for a in raw["asks"] if D(a["price"]) == top)
    finally:
        await ex.close()


# --------------------------------------------------------------------------- funding / budget
async def test_funding_from_market_stats_merge(mocked):
    ex, _ = await make_exchange(mocked, creds=False)
    try:
        snap, upd = load("ws_market_stats_all.json")
        ex._on_ws_message(snap)
        f = await ex.fetch_funding(["ETH", "BTC", "NVDA", "XXX"])
        assert set(f) == {"ETH", "BTC", "NVDA"}
        assert f["ETH"].rate_hourly == D(snap["market_stats"]["0"]["current_funding_rate"]) / 100
        assert f["NVDA"].rate_hourly == D("0.000004") and f["ETH"].next_funding_ts % 3600 == 0
        # later frames carry only changed markets: merge, don't replace
        upd = copy.deepcopy(upd)
        upd["market_stats"] = {"0": dict(snap["market_stats"]["0"], current_funding_rate="-0.0050")}
        ex._on_ws_message(upd)
        f2 = await ex.fetch_funding(["ETH", "BTC"])
        assert f2["ETH"].rate_hourly == D("-0.00005") and f2["BTC"].rate_hourly == f["BTC"].rate_hourly
    finally:
        await ex.close()


async def test_funding_rest_fallback_only_when_cache_empty(mocked):
    ex, _ = await make_exchange(mocked, creds=False)
    try:
        mocked.get(FUNDING_RATES, payload=load("rest_funding_rates.json"))
        f = await ex.fetch_funding(["BTC", "NVDA"])
        assert f["BTC"].rate_hourly == D("0.000012") and f["NVDA"].rate_hourly == D("0.000004")
        assert f["BTC"].venue == "lighter"  # binance/bybit rows ignored
        f2 = await ex.fetch_funding(["BTC"])  # cached (30 s): no second request
        assert f2["BTC"].rate_hourly == D("0.000012") and len(mocked.calls("GET", FUNDING_RATES)) == 1
    finally:
        await ex.close()


async def test_reads_keep_reserve_for_transactions(mocked):
    ex, _ = await make_exchange(mocked)
    try:
        lim = ex._limiter
        while lim.available() > ex.cfg.read_reserve:
            assert lim.try_acquire()
        with pytest.raises(ExchangeError) as ei:
            await ex.fetch_book("ETH")
        assert ei.value.retryable and ei.value.code == "budget"
        assert ex.budget_health() < 0.5
        # funding: cache empty and no budget -> nothing (no exception), still no REST call
        assert await ex.fetch_funding(["ETH"]) == {}
        # transactions still go through using the reserve
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2500.009"))
        assert r.ok and r.order.status is OrderStatus.PENDING
    finally:
        await ex.close()


# --------------------------------------------------------------------------- trading
async def test_place_market_signs_ioc_and_returns_pending(mocked):
    ex, signer = await make_exchange(mocked)
    try:
        r = await ex.place_market("ETH", Side.BUY, D("0.0500"), worst_price=D("2412.019"), client_id="h1")
        assert r.ok and r.order.status is OrderStatus.PENDING and r.order.client_id == "h1" and r.order.ioc
        kind, info = signer.calls[-1]
        assert kind == "create" and info["MarketIndex"] == 0 and info["BaseAmount"] == 500
        assert info["Price"] == 241201  # BUY worst price rounded DOWN (never above the limit)
        assert info["Type"] == 1 and info["TimeInForce"] == 0 and info["OrderExpiry"] == 0 and info["IsAsk"] == 0
        assert info["Nonce"] == 10 and info["ApiKeyIndex"] == 5
        assert 1 <= info["ClientOrderIndex"] <= 2**48 - 1
        tx_type, tx_info, h = ex._ws.sent[-1]
        assert tx_type == 14 and isinstance(tx_info, dict) and tx_info["ClientOrderIndex"] == info["ClientOrderIndex"]
        assert ex._executor.next_nonce == 11
        r2 = await ex.place_market("ETH", Side.SELL, D("0.0500"), worst_price=D("2390.001"), reduce_only=True)
        assert signer.calls[-1][1]["Price"] == 239001 and signer.calls[-1][1]["ReduceOnly"] == 1  # SELL rounds UP
        assert r2.ok and signer.calls[-1][1]["Nonce"] == 11
        dup = await ex.place_market("ETH", Side.BUY, D("0.05"), worst_price=D("2412"), client_id="h1")
        assert not dup.ok
    finally:
        await ex.close()


async def test_order_validation(mocked):
    ex, signer = await make_exchange(mocked)
    try:
        r = await ex.place_market("ETH", Side.BUY, D("0.00005"), worst_price=D("2400"))
        assert not r.ok and "step" in r.error
        r = await ex.place_market("ETH", Side.BUY, D("0.0010"), worst_price=D("2400"))  # below 0.002 / $10
        assert not r.ok and "minimum" in r.error
        r = await ex.place_market("ETH", Side.SELL, D("0.0010"), worst_price=D("2400"), reduce_only=True)
        assert r.ok  # closing dust is allowed through (venue decides)
        r = await ex.place_limit("ETH", Side.BUY, D("0.0100"), D("2400.001"))
        assert not r.ok and "tick" in r.error
        r = await ex.place_market("NOPE", Side.BUY, D("1"), worst_price=D("1"))
        assert not r.ok
        assert sum(1 for k, _ in signer.calls if k == "create") == 1
    finally:
        await ex.close()


async def test_fills_and_ioc_remainder_canceled(mocked):
    ex, _ = await make_exchange(mocked)
    fills, orders, _ = collect(ex)
    try:
        r = await ex.place_market("ETH", Side.BUY, D("0.1000"), worst_price=D("2410"), client_id="hedge-1")
        coi = ex._cid_to_coi["hedge-1"]
        oidx = 562946773250577
        t1 = our_trade(coi, oidx, 900001, "0.0400", "2400.10")
        t2 = our_trade(coi, oidx, 900002, "0.0200", "2400.30", tx_hash="0000001f" + "0" * 72)  # child tx hash
        ex._on_ws_message(frame_account_all({"0": [t1]}))
        ex._on_ws_message(frame_account_all({"0": [t1, t2]}))  # t1 repeated: must not be re-emitted
        assert [f.trade_id for f in fills] == ["900001", "900002"]
        assert all(f.client_id == "hedge-1" and f.side is Side.BUY and f.is_maker is False and f.fee == 0 for f in fills)
        assert r.order.status is OrderStatus.PARTIALLY_FILLED and r.order.filled == D("0.0600")
        assert r.order.order_id == str(oidx)
        ex._on_ws_message(frame_orders([order_json(coi, oidx, status="canceled", filled="0.0600", quote="144.02",
                                                   size="0.1000")]))
        assert r.order.status is OrderStatus.CANCELED and r.order.filled == D("0.0600")
        assert r.order.avg_fill_price == (D("0.04") * D("2400.10") + D("0.02") * D("2400.30")) / D("0.06")
        assert orders[-1] == (OrderStatus.CANCELED, D("0.0600"))
    finally:
        await ex.close()


async def test_terminal_update_waits_for_fills(mocked):
    ex, _ = await make_exchange(mocked)
    fills, orders, _ = collect(ex)
    try:
        r = await ex.place_market("ETH", Side.SELL, D("0.0500"), worst_price=D("2390"))
        coi = ex._cid_to_coi[r.order.client_id]
        ex._on_ws_message(frame_orders([order_json(coi, 7 * 2**48, status="filled", filled="0.0500", quote="120.0",
                                                   size="0.0500", is_ask=True)]))
        assert not r.order.status.is_terminal and all(not s.is_terminal for s, _ in orders)  # held back
        t = our_trade(coi, 7 * 2**48, 910001, "0.0500", "2400.00", side="sell")
        ex._on_ws_message(frame_account_all({"0": [t]}))
        assert len(fills) == 1 and fills[0].side is Side.SELL
        assert r.order.status is OrderStatus.FILLED and orders[-1] == (OrderStatus.FILLED, D("0.0500"))
    finally:
        await ex.close()


async def test_terminal_released_after_timeout_without_fills(mocked):
    ex, _ = await make_exchange(mocked)
    try:
        r = await ex.place_market("ETH", Side.BUY, D("0.0500"), worst_price=D("2410"))
        coi = ex._cid_to_coi[r.order.client_id]
        ex._on_ws_message(frame_orders([order_json(coi, 5 * 2**48, status="filled", filled="0.0500", quote="120.0",
                                                   size="0.0500")]))
        assert not r.order.status.is_terminal
        await asyncio.sleep(1.5)  # maintenance loop releases it after fill_wait_s
        assert r.order.status is OrderStatus.FILLED and r.order.filled == D("0.0500")
        assert r.order.avg_fill_price == D("2400")
    finally:
        await ex.close()


async def test_multi_maker_sweep_all_fills_emitted(mocked):
    """Replay the live sweep (1 parent + 5 type-21 child fills) as if it were our order."""
    trades = load("sweep_trades.json")
    ex, _ = await make_exchange(mocked, account_index=110400)
    fills, orders, _ = collect(ex)
    try:
        ex._ids.next = lambda: 152116289068  # the live order's client_order_index
        r = await ex.place_market("ETH", Side.BUY, D("19.8176"), worst_price=D("2417.80"))
        assert r.ok
        ex._on_ws_message(frame_account_all({"0": trades[:1]}, account=110400))
        ex._on_ws_message(frame_account_all({"0": trades[1:]}, account=110400))
        assert len(fills) == 6 and sum(f.size for f in fills) == D("19.8176")
        assert len({f.trade_id for f in fills}) == 6
        assert r.order.status is OrderStatus.FILLED and r.order.filled == D("19.8176")
        fee = sum(f.fee for f in fills)  # Premium account in the capture: 280 ppm taker fee
        assert fee == sum(D(t["usd_amount"]) * 280 / 1_000_000 for t in trades)
    finally:
        await ex.close()


async def test_snapshot_trades_not_reemitted_unless_tracked(mocked):
    ex, _ = await make_exchange(mocked)
    fills, _, _ = collect(ex)
    try:
        snap = load("ws_account_all_snapshot_313004.json")
        ex._on_ws_message(snap)
        assert fills == []  # historical fills from before this session
        assert ex._account_ready
        pos = await ex.fetch_positions()
        assert pos["ETH"].size == D(snap["positions"]["0"]["position"]) and "BTC" not in pos
        # a fill of a TRACKED order that happened while disconnected IS emitted from the next snapshot
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        coi = ex._cid_to_coi[r.order.client_id]
        snap2 = copy.deepcopy(snap)
        snap2["trades"]["0"].insert(0, our_trade(coi, 2**49 + 1, 999999999999, "0.0100", "2403.00"))
        ex._on_ws_message(snap2)
        assert [f.trade_id for f in fills] == ["999999999999"]
        # replayed from a snapshot: flagged so the hedger reconciles instead of adding it blindly
        assert fills[0].recovered is True
        # a live update frame is not a replay
        ex._on_ws_message(frame_account_all(trades={"0": [our_trade(coi, 2**49 + 1, 999999999998, "0.0010",
                                                                        "2403.00")]}))
        assert [f.trade_id for f in fills][-1] == "999999999998" and fills[-1].recovered is False
    finally:
        await ex.close()


async def test_positions_and_balance_ws_and_rest(mocked):
    ex, _ = await make_exchange(mocked)
    try:
        # WS not fresh yet -> REST fallback
        mocked.get(ACCOUNT, payload=load("rest_account_313004.json"), repeat=True)
        rest_acct = load("rest_account_313004.json")["accounts"][0]
        pos = await ex.fetch_positions()
        assert pos["ETH"].size == -D("19.0112")
        bal = await ex.fetch_balance()
        assert bal.equity == D(rest_acct["total_asset_value"]) and bal.available == D(rest_acct["available_balance"])
        assert bal.maintenance_margin == D(rest_acct["cross_maintenance_margin_requirement"])
        # WS snapshot + user_stats
        ex._on_ws_message(load("ws_account_all_snapshot_313004.json"))
        ex._on_ws_message(load("ws_user_stats_313004.json"))
        bal2 = await ex.fetch_balance()
        st = load("ws_user_stats_313004.json")["stats"]
        assert bal2.equity == D(st["portfolio_value"]) and bal2.available == D(st["available_balance"])
        p = load("ws_account_all_snapshot_313004.json")["positions"]["0"]
        mark = D(p["position_value"]) / D(p["position"])
        assert bal2.maintenance_margin == D(p["position"]) * mark * 120 / 10_000  # MMF 1.2 %
        assert bal2.margin_used == D(p["position"]) * mark * D("0.05")  # IMF 5 %
        # position updates replace per market; missing markets are not treated as closed
        upd = frame_account_all(positions={"0": dict(p, sign=-1, position="1.0000")})
        ex._on_ws_message(upd)
        assert (await ex.fetch_positions())["ETH"].size == D("-1.0000")
    finally:
        await ex.close()


async def test_restricted_on_send_blocks_trading(mocked):
    ex, _ = await make_exchange(mocked)
    try:
        ex._ws.script = [{"error": {"code": 20558, "message": "You are accessing Lighter from a restricted jurisdiction"}}]
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        assert not r.ok and not r.retryable and "20558" in r.error
        ok, reason = ex.can_increase_exposure()
        assert not ok and "permitted" in reason
        r2 = await ex.place_market("ETH", Side.SELL, D("0.0100"), worst_price=D("2390"), reduce_only=True)
        assert not r2.ok and len(ex._ws.sent) == 1  # nothing else is sent
    finally:
        await ex.close()


async def test_standard_limit_flag(mocked, caplog):
    ex, _ = await make_exchange(mocked)
    try:
        ex._ws.script = [{"error": {"code": 52002, "message": "You've reached the limit of Standard trading"}}]
        with caplog.at_level(logging.CRITICAL, logger="farmer.lighter"):
            r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        assert not r.ok and not r.retryable
        ok, reason = ex.can_increase_exposure()
        assert not ok and "52002" in reason
        assert any(rec.levelno == logging.CRITICAL for rec in caplog.records)
        r2 = await ex.place_market("ETH", Side.SELL, D("0.0100"), worst_price=D("2390"), reduce_only=True)
        assert r2.ok  # reducing is still possible
        ex._std_limit_until = 0
        assert ex.can_increase_exposure() == (True, "")
    finally:
        await ex.close()


async def test_sequencer_error_in_account_tx_rejects_order_and_flags(mocked):
    ex, _ = await make_exchange(mocked)
    _, orders, _ = collect(ex)
    try:
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        h = ex._meta[ex._cid_to_coi[r.order.client_id]].tx_hash
        ex._on_ws_message({"channel": f"account_tx:{ACCT}", "type": "update/account_tx",
                           "txs": [{"hash": h, "type": 14, "status": 2,
                                    "event_info": json.dumps({"ae": "52002 You've reached the limit of Standard trading"})}]})
        assert r.order.status is OrderStatus.REJECTED and "sequencer" in r.order.error
        assert orders[-1][0] is OrderStatus.REJECTED
        assert ex.can_increase_exposure()[0] is False
    finally:
        await ex.close()


async def test_tx_unknown_is_flagged_in_flight(mocked):
    ex, _ = await make_exchange(mocked)
    try:
        ex._executor.lookup_interval_s = 0.0
        ex._ws.script = ["timeout"]
        mocked.get(TX, status=400, payload={"code": 21500, "message": "transaction not found"}, repeat=True)
        mocked.get(NONCE, payload={"code": 200, "nonce": 12}, repeat=True)  # nonce moved: cannot resend safely
        ex._executor.t.fetch_next_nonce = _const_nonce(12)
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        assert r.ok and r.order.status is OrderStatus.UNKNOWN and "unknown" in r.error
        assert ex.stats["tx_unknown"] == 1
    finally:
        await ex.close()


def _const_nonce(n):
    async def f():
        return n
    return f


async def test_rest_fallback_when_ws_down_and_429_cooldown(mocked):
    ex, signer = await make_exchange(mocked)
    try:
        ex._ws.connected = False
        mocked.post(SENDTX, payload={"code": 200, "tx_hash": "x" * 80, "predicted_execution_time_ms": 1, "volume_quota_remaining": 0})
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        assert r.ok and ex._ws.sent == []
        sent = mocked.calls("POST", SENDTX)
        assert len(sent) == 1
        assert sent[0]["form"]["tx_type"] == "14" and json.loads(sent[0]["form"]["tx_info"])["BaseAmount"] == 100
        mocked.post(SENDTX, status=429, body="Too Many Requests")
        r2 = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        assert not r2.ok and r2.retryable and ex.budget_health() == 0.0
        r3 = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        assert not r3.ok and r3.retryable  # cooling down: not even sent
        assert ex._executor.next_nonce == 11
    finally:
        await ex.close()


async def test_place_limit_post_only(mocked):
    ex, signer = await make_exchange(mocked)
    try:
        await ex.subscribe_books(["ETH"])
        ex._on_ws_message({"channel": "order_book:0", "type": "subscribed/order_book",
                           "order_book": {"nonce": 5, "asks": [{"price": "2400.50", "size": "1"}],
                                          "bids": [{"price": "2400.00", "size": "1"}]}})
        r = await ex.place_limit("ETH", Side.BUY, D("0.0100"), D("2400.50"))
        assert not r.ok and r.would_cross and not signer.calls  # rejected locally, no tx spent
        task = asyncio.create_task(ex.place_limit("ETH", Side.BUY, D("0.0100"), D("2400.10"), client_id="mk1"))
        await asyncio.sleep(0.01)
        _, info = signer.calls[-1]
        assert info["TimeInForce"] == 2 and info["Type"] == 0 and info["OrderExpiry"] == -1
        coi = info["ClientOrderIndex"]
        ex._on_ws_message(frame_orders([order_json(coi, 2**49 + 7, status="open", size="0.0100", price="2400.10",
                                                   tif="post-only")]))
        r2 = await task
        assert r2.ok and r2.order.status is OrderStatus.OPEN and r2.order.order_id == str(2**49 + 7)
        # post-only cancelled by the venue -> would_cross
        task = asyncio.create_task(ex.place_limit("ETH", Side.SELL, D("0.0100"), D("2400.60")))
        await asyncio.sleep(0.01)
        coi2 = signer.calls[-1][1]["ClientOrderIndex"]
        ex._on_ws_message(frame_orders([order_json(coi2, 2**48 + 9, status="canceled-post-only", size="0.0100",
                                                   price="2400.60", tif="post-only", is_ask=True)]))
        r3 = await task
        assert not r3.ok and r3.would_cross
        # open orders from the WS cache
        ex._on_ws_message(frame_orders([], snapshot=True) | {"orders": {"0": [order_json(coi, 2**49 + 7, status="open",
                                                                                         size="0.0100", tif="post-only")]}})
        oo = await ex.fetch_open_orders("ETH")
        assert [o.client_id for o in oo] == ["mk1"]
        # cancel uses the exchange order index once known
        assert await ex.cancel_order("ETH", client_id="mk1")
        assert signer.calls[-1] == ("cancel", signer.calls[-1][1]) and signer.calls[-1][1]["Index"] == 2**49 + 7
        # modify: native tx 17 with the remaining size
        res = await ex.modify_order(r2.order, price=D("2400.20"))
        assert res.ok and signer.calls[-1][0] == "modify"
        assert signer.calls[-1][1]["BaseAmount"] == 100 and signer.calls[-1][1]["Price"] == 240020
    finally:
        await ex.close()


async def test_cancel_paths_and_cancel_all(mocked):
    ex, signer = await make_exchange(mocked)
    try:
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"), client_id="c1")
        coi = ex._cid_to_coi["c1"]
        assert await ex.cancel_order("ETH", client_id="c1")
        assert signer.calls[-1][1]["Index"] == coi  # exchange index unknown yet -> COI
        assert await ex.cancel_order("ETH", order_id="562946773250577")
        assert signer.calls[-1][1]["Index"] == 562946773250577
        assert not await ex.cancel_order("ETH", client_id="no-such")
        n = len(signer.calls)
        ex._on_ws_message(frame_orders([], snapshot=True))
        await ex.cancel_all("ETH")  # orders channel fresh and nothing resting -> no tx
        assert len(signer.calls) == n
        ex._orders_ready = False
        await ex.cancel_all("BTC")
        assert signer.calls[-1][0] == "cancel_all" and signer.calls[-1][1]["Market"] == 1
        await ex.cancel_all()
        assert signer.calls[-1][1]["Market"] == 255
        ex._on_ws_message(frame_orders([order_json(coi, 2**49, status="canceled", size="0.0100")]))
        assert r.order.status is OrderStatus.CANCELED
        assert await ex.cancel_order("ETH", client_id="c1")  # already gone -> True without a tx
    finally:
        await ex.close()


async def test_set_leverage(mocked):
    ex, signer = await make_exchange(mocked)
    try:
        await ex.set_leverage("ETH", 5)
        assert signer.calls[-1] == ("leverage", signer.calls[-1][1])
        assert signer.calls[-1][1]["InitialMarginFraction"] == 2000 and signer.calls[-1][1]["MarginMode"] == 0
        n = len(signer.calls)
        await ex.set_leverage("ETH", 5)  # already sent in this session
        assert len(signer.calls) == n
        ex._on_ws_message(load("ws_account_all_snapshot_313004.json"))  # BTC position IMF "5.00" = 20x
        await ex.set_leverage("BTC", 20)
        assert len(signer.calls) == n  # venue already at 20x
        await ex.set_leverage("BTC", 100)  # above max 50x -> capped at min IMF 200
        assert signer.calls[-1][1]["InitialMarginFraction"] == 200
    finally:
        await ex.close()


async def test_funding_payments_from_ws(mocked):
    ex, _ = await make_exchange(mocked)
    try:
        ex._funding_rest_at = time.monotonic() + 3600  # skip the REST backfill
        frame = next(f for f in load("ws_account_all_funding.json") if f["channel"] == f"account_all:{ACCT}")
        ex._on_ws_message(frame)
        ex._on_ws_message(frame)  # deduped by (funding_id, market_id)
        pays = await ex.fetch_funding_payments()
        assert len(pays) == 1 and pays[0].amount == D("-0.218616") and pays[0].symbol == "ETH"
        assert await ex.fetch_funding_payments(since_ts=pays[0].ts + 1) == []
    finally:
        await ex.close()


async def test_funding_backfill_follows_cursor_and_retries_soon_after_failure(mocked):
    ex, _ = await make_exchange(mocked)
    path = "/api/v1/positionFunding"

    def rows(start, n):
        return [{"timestamp": 1792429200 - 3600 * i, "market_id": 0, "funding_id": i, "change": "-0.01",
                 "rate": "0.000012", "position_size": "1", "position_side": "long", "discount": "0"}
                for i in range(start, start + n)]

    try:
        mocked.get(path, payload={"code": 200, "position_fundings": rows(0, 100), "next_cursor": "c1"})
        mocked.get(path, payload={"code": 200, "position_fundings": rows(100, 7), "next_cursor": "c2"})
        pays = await ex.fetch_funding_payments(since_ts=1.0)
        calls = mocked.calls("GET", path)
        assert len(pays) == 107 and len(calls) == 2
        assert "cursor" not in calls[0]["query"] and calls[1]["query"]["cursor"] == "c1"
        assert len(await ex.fetch_funding_payments(since_ts=1.0)) == 107 and len(mocked.calls("GET", path)) == 2
        # a failed backfill is retried after about a minute, not 15
        ex._funding_rest_at = 0.0
        mocked.get(path, status=500, body="oops")
        await ex.fetch_funding_payments(since_ts=1.0)
        assert 30 < ex._funding_rest_at - time.monotonic() <= 60
    finally:
        await ex.close()


async def test_open_orders_llp_capture(mocked):
    ex, _ = await make_exchange(mocked, account_index=LLP)
    try:
        frames = load("ws_account_all_orders_llp.json")
        ex._on_ws_message(frames[0])  # snapshot
        for f in frames[1:]:
            ex._on_ws_message(f)
        oo = await ex.fetch_open_orders()
        statuses = {o.status for o in oo}
        assert oo and statuses <= {OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED}
        assert all(o.client_id for o in oo)
    finally:
        await ex.close()


async def test_auth_refresh_resubscribes(mocked):
    ex, signer = await make_exchange(mocked)
    try:
        tok1 = ex._auth_token()
        t, exp, created = ex._auth
        age = 0.9 * (exp - created)
        ex._auth = (t, exp - age, created - age)  # token issued 90 % of its lifetime ago
        ex._maybe_refresh_auth()
        assert ex._auth_token() != tok1
        assert set(ex._ws.resubs) == {f"account_all_orders/{ACCT}", f"account_tx/{ACCT}"}
        before = ex._ws.resubs.count(f"account_tx/{ACCT}")
        ex._on_ws_message({"error": {"code": 20013, "message": "invalid auth"}})
        assert ex._ws.resubs.count(f"account_tx/{ACCT}") == before + 1
    finally:
        await ex.close()


async def test_stale_pending_order_confirmed_over_rest(mocked):
    ex, _ = await make_exchange(mocked)
    try:
        ex._order_confirm_timeout_s = 0.0
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        coi = ex._cid_to_coi[r.order.client_id]
        mocked.get(ACCOUNT_ORDERS, payload={"code": 200, "orders": [
            order_json(coi, 2**49 + 3, status="canceled-too-much-slippage", size="0.0100")]})
        await asyncio.sleep(1.3)
        assert r.order.status is OrderStatus.CANCELED and r.order.error == "canceled-too-much-slippage"
        req = mocked.calls("GET", ACCOUNT_ORDERS)[0]
        assert req["query"]["client_order_indexes"] == str(coi) and req["query"]["account_index"] == str(ACCT)
        assert req["headers"]["Authorization"].split(":")[1:3] == [str(ACCT), "5"]
    finally:
        await ex.close()


# --------------------------------------------------------------------------- review fixes
def frame_account_tx(h, *, status, ae="", typ=14):
    return {"channel": f"account_tx:{ACCT}", "type": "update/account_tx",
            "txs": [{"hash": h, "type": typ, "status": status, "event_info": json.dumps({"ae": ae} if ae else {})}]}


async def test_rest_positions_refused_while_fills_pending_replay(mocked):
    """WS down: REST positions already include the hedge's fill that the reconnect snapshot re-emits."""
    ex, _ = await make_exchange(mocked)
    fills, _, _ = collect(ex)
    try:
        snap = load("ws_account_all_snapshot_313004.json")
        ex._on_ws_message(snap)
        ex._ws.connected = False
        ex._on_ws_disconnect("test")
        mocked.post(SENDTX, payload={"code": 200, "tx_hash": "x" * 80, "predicted_execution_time_ms": 1})
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        assert r.ok and r.order.status is OrderStatus.PENDING
        mocked.get(ACCOUNT, payload=load("rest_account_313004.json"), repeat=True)  # includes the fill
        with pytest.raises(ExchangeError) as ei:
            await ex.fetch_positions()
        assert ei.value.retryable and "pending replay" in str(ei.value)
        assert mocked.calls("GET", ACCOUNT) == []
        # the stream comes back: the snapshot replays the fill exactly once, positions come from the WS cache
        coi = ex._cid_to_coi[r.order.client_id]
        ex._ws.connected = True
        snap2 = copy.deepcopy(snap)
        snap2["trades"]["0"].insert(0, our_trade(coi, 2**49 + 1, 999999999999, "0.0100", "2403.00"))
        ex._on_ws_message(snap2)
        assert [f.trade_id for f in fills] == ["999999999999"] and r.order.status is OrderStatus.FILLED
        assert (await ex.fetch_positions())["ETH"].size == D("3.9663")
        # nothing left to replay: a later outage may use REST again
        ex._ws.connected = False
        ex._on_ws_disconnect("test")
        assert (await ex.fetch_positions())["ETH"].size == -D("19.0112")
    finally:
        await ex.close()


async def test_rest_positions_replay_guard_is_bounded(mocked):
    ex, _ = await make_exchange(mocked)
    try:
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        ex._ws.connected = False
        mocked.get(ACCOUNT, payload=load("rest_account_313004.json"), repeat=True)
        with pytest.raises(ExchangeError):
            await ex.fetch_positions()
        ex._meta[ex._cid_to_coi[r.order.client_id]].sent_at -= 1000  # long outage: do not block reads forever
        assert (await ex.fetch_positions())["ETH"].size == -D("19.0112")
    finally:
        await ex.close()


async def test_stalled_connected_ws_is_not_venue_truth(mocked):
    """A socket that is 'connected' but silent may be stalled: neither its position cache nor its sendtx is used."""
    ex, _ = await make_exchange(mocked)
    try:
        ex._on_ws_message(load("ws_account_all_snapshot_313004.json"))
        ex._on_ws_message(load("ws_user_stats_313004.json"))
        assert (await ex.fetch_positions())["ETH"].size == D("3.9663")
        ex._ws.rx_age = 15.0  # no frame for 15 s, watchdog not tripped yet
        mocked.get(ACCOUNT, payload=load("rest_account_313004.json"), repeat=True)
        assert (await ex.fetch_positions())["ETH"].size == -D("19.0112")  # REST, not the frozen cache
        assert len(mocked.calls("GET", ACCOUNT)) == 1
        mocked.post(SENDTX, payload={"code": 200, "tx_hash": "x" * 80, "predicted_execution_time_ms": 1})
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        assert r.ok and ex._ws.sent == [] and len(mocked.calls("POST", SENDTX)) == 1  # tx over REST
        # an expired in-flight hedge must not be dropped against the frozen cache: positions are deferred
        with pytest.raises(ExchangeError) as ei:
            await ex.fetch_positions()
        assert ei.value.retryable
    finally:
        await ex.close()


async def test_budget_health_includes_tx_cap(mocked):
    ex, _ = await make_exchange(mocked, max_tx_per_minute=4)
    try:
        h0 = ex.budget_health()
        for _ in range(3):
            assert ex._tx_limiter.try_acquire()
        assert ex.budget_health() == 0.25 < h0  # 1 of 4 txs left although the shared bucket is nearly free
    finally:
        await ex.close()


async def test_private_ok_tracks_account_stream(mocked):
    ex, _ = await make_exchange(mocked)
    try:
        ok, why = ex.private_ok()
        assert not ok and "not synced" in why  # no account/orders snapshot yet
        ex._on_ws_message(load("ws_account_all_snapshot_313004.json"))
        ex._on_ws_message(frame_orders([], snapshot=True))
        assert ex.private_ok() == (True, "")
        ex._ws.rx_age = ex._ws_watchdog_s + 1  # silent socket
        assert not ex.private_ok()[0]
        ex._ws.rx_age = 0.05
        ex._ws.connected = False
        ex._on_ws_disconnect("test")
        assert not ex.private_ok()[0]
    finally:
        await ex.close()
    pub, _ = await make_exchange(mocked, creds=False)
    try:
        assert not pub.private_ok()[0]
    finally:
        await pub.close()


async def test_account_tx_status0_resyncs_nonce_for_any_of_our_txs(mocked):
    ex, signer = await make_exchange(mocked)  # nextNonce route answers 10
    try:
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        h = ex._meta[ex._cid_to_coi[r.order.client_id]].tx_hash
        assert ex._executor.next_nonce == 11
        ex._on_ws_message(frame_account_tx(h, status=0))  # failed before apply: nonce 10 NOT consumed
        assert r.order.status is OrderStatus.REJECTED
        await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        assert signer.calls[-1][1]["Nonce"] == 10
        # same for a non-order tx (cancel)
        assert await ex.cancel_order("ETH", order_id="562946773250577")
        cancel_hash = ex._ws.sent[-1][2]
        assert ex._executor.next_nonce == 12
        ex._on_ws_message(frame_account_tx(cancel_hash, status=0, typ=15))
        await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        assert signer.calls[-1][1]["Nonce"] == 10
        # an applied no-op (status 2 + error) consumed its nonce: no resync
        h2 = ex._ws.sent[-1][2]
        ex._on_ws_message(frame_account_tx(h2, status=2, ae="21738 reduce-only wrong direction"))
        assert not ex._executor._resync
    finally:
        await ex.close()


async def test_stale_order_lookup_status0_resyncs_nonce(mocked):
    ex, signer = await make_exchange(mocked)
    try:
        r = await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        coi = ex._cid_to_coi[r.order.client_id]
        meta = ex._meta[coi]
        mocked.get(ACCOUNT_ORDERS, payload={"code": 200, "orders": []}, repeat=True)
        mocked.get(TX, payload={"code": 200, "hash": meta.tx_hash, "status": 0, "event_info": ""}, repeat=True)
        meta.sent_at -= 60
        for _ in range(2):
            meta.last_check = 0.0
            await ex._confirm_stale_orders()
        assert r.order.status is OrderStatus.REJECTED
        await ex.place_market("ETH", Side.BUY, D("0.0100"), worst_price=D("2410"))
        assert signer.calls[-1][1]["Nonce"] == 10
    finally:
        await ex.close()


async def test_account_tx_failure_before_submit_returns_rejects_order(mocked):
    """The ack is lost; account_tx reports the tx as a no-op while the executor is still resolving it by hash."""
    ex, _ = await make_exchange(mocked)
    try:
        ex._executor.lookup_interval_s = 0.0

        def lost_ack_then_frame(tx_type, tx_info, tx_hash):
            ex._on_ws_message(frame_account_tx(tx_hash, status=2, ae="21738 reduce-only order wrong direction"))
            raise asyncio.TimeoutError

        ex._ws.script = [lost_ack_then_frame]
        mocked.get(TX, payload={"code": 200, "hash": "h", "status": 2, "event_info": ""}, repeat=True)
        r = await ex.place_market("ETH", Side.SELL, D("0.0100"), worst_price=D("2390"), reduce_only=True)
        assert not r.ok and r.order.status is OrderStatus.REJECTED and "sequencer" in r.order.error
    finally:
        await ex.close()


async def test_lookup_receipt_with_applied_error_rejects_order(mocked):
    ex, _ = await make_exchange(mocked)
    try:
        ex._executor.lookup_interval_s = 0.0
        ex._ws.script = ["timeout"]
        mocked.get(TX, payload={"code": 200, "hash": "h", "status": 2,
                                "event_info": json.dumps({"ae": "21738 reduce-only order wrong direction"})}, repeat=True)
        r = await ex.place_market("ETH", Side.SELL, D("0.0100"), worst_price=D("2390"), reduce_only=True)
        assert not r.ok and r.order.status is OrderStatus.REJECTED and "21738" in r.order.error
    finally:
        await ex.close()


async def test_taker_minimum_checked_at_mid_not_worst_price(mocked):
    ex, signer = await make_exchange(mocked)
    try:
        await ex.subscribe_books(["ETH"])
        ex._on_ws_message({"channel": "order_book:0", "type": "subscribed/order_book",
                           "order_book": {"nonce": 5, "asks": [{"price": "2381.05", "size": "1"}],
                                          "bids": [{"price": "2380.95", "size": "1"}]}})
        # 0.0042 x mid 2381.00 = 10.0002 USDC (the hedger's check passes); x worst 2379.04 = 9.99
        r = await ex.place_market("ETH", Side.SELL, D("0.0042"), worst_price=D("2379.04"))
        assert r.ok and signer.calls[-1][1]["Price"] == 237904 and signer.calls[-1][1]["BaseAmount"] == 42
        # genuinely below the minimum at the mid: refused, but retryable (no error-breaker feed)
        r2 = await ex.place_market("ETH", Side.SELL, D("0.0041"), worst_price=D("2379.04"))
        assert not r2.ok and r2.retryable and "minimum" in r2.error
    finally:
        await ex.close()


async def test_auth_token_rotated_by_rest_caller_resubscribes_ws(mocked):
    ex, _ = await make_exchange(mocked)
    try:
        provider = ex._ws.subs[f"account_tx/{ACCT}"]
        tok1 = provider()  # the WS subscribed the auth channels with tok1
        t, exp, created = ex._auth
        age = 0.85 * (exp - created)
        ex._auth = (t, exp - age, created - age)
        tok2 = ex._auth_token()  # a REST auth read (accountOrders, positionFunding...) rotates it first
        assert tok2 != tok1
        ex._ws.resubs.clear()
        ex._maybe_refresh_auth()
        assert set(ex._ws.resubs) == {f"account_all_orders/{ACCT}", f"account_tx/{ACCT}"}
        ex._ws.resubs.clear()
        ex._maybe_refresh_auth()
        assert ex._ws.resubs == []  # once per rotation
    finally:
        await ex.close()
