"""ArcusRest transport against a local aiohttp server: error mapping, retries, Decimal parsing."""

from decimal import Decimal

import pytest

from farmer.exchanges.arcus.rest import ArcusAccessError, ArcusHTTPError, ArcusRest
from farmer.exchanges.base import ExchangeError
from tests.test_arcus_support import FakeHttp, Resp, load


@pytest.fixture
async def srv():
    s = FakeHttp()
    await s.start()
    yield s
    await s.stop()


async def make(srv, **kw) -> ArcusRest:
    r = ArcusRest(srv.base, timeout_s=2.0, **kw)
    await r.start()
    return r


async def test_reads_and_decimal_parsing(srv):
    srv.get("/v1/time", payload={"timeNs": 1792445937523829083})
    srv.get("/v1/markets", payload=load("markets.json"))
    srv.get("/v1/rateLimit", payload={"order": {"used": 1, "cap": 2}, "x": 1.5})
    rest = await make(srv)
    try:
        assert await rest.server_time_ns() == 1792445937523829083
        mk = await rest.markets()
        assert {m["marketDisplayName"] for m in mk} >= {"BTC-USD", "KBONK-USD"}
        rl = await rest.rate_limit("0xabc", 3)
        assert isinstance(rl["x"], Decimal)  # never a binary float
        assert srv.calls("GET", "/v1/rateLimit")[0].query == {"address": "0xabc", "accountIndex": "3"}
    finally:
        await rest.close()


async def test_get_retries_transient_then_succeeds(srv):
    srv.get("/v1/bbo/BTC-USD", status=503, payload={"error": "unavailable"})
    srv.get("/v1/bbo/BTC-USD", status=429, payload={"error": "rate limited"}, headers={"Retry-After": "0"})
    srv.get("/v1/bbo/BTC-USD", payload={"bestBid": None, "bestAsk": None})
    rest = await make(srv)
    try:
        assert await rest.bbo("BTC-USD") == {"bestBid": None, "bestAsk": None}
        assert len(srv.calls("GET", "/v1/bbo/BTC-USD")) == 3
    finally:
        await rest.close()


async def test_error_mapping(srv):
    srv.get("/v1/positions", status=403, payload={"error": "address not on access whitelist"})
    srv.get("/v1/account", status=404, payload={"error": "this account has no activity yet"})
    srv.post("/v1/placeOrder", status=429, payload={"error": "rate limited", "reason": "account_partial",
                                                    "retryAfterMs": 850, "clientId": "q-1"})
    srv.post("/v1/cancelOrder", status=401, payload={"error": "timestamp expired"})
    srv.post("/v1/modifyOrder", status=502, payload={"error": "not submitted", "errorType": "Transmission"})
    srv.post("/v1/setLeverage", status=429, payload={"error": "rate limited"}, headers={"Retry-After": "2"})
    rest = await make(srv, get_retries=0)
    try:
        with pytest.raises(ArcusAccessError) as e1:
            await rest.positions("0x1", 0)
        assert e1.value.is_whitelist and isinstance(e1.value, ExchangeError) and not e1.value.retryable
        with pytest.raises(ArcusHTTPError) as e2:
            await rest.account("0x1", 0)
        assert e2.value.status == 404 and "no activity" in e2.value.error
        with pytest.raises(ArcusHTTPError) as e3:
            await rest.post("/v1/placeOrder", "{}", {})
        assert e3.value.retryable and e3.value.retry_after_ms == 850 and e3.value.rejection_reason == "account_partial"
        with pytest.raises(ArcusHTTPError) as e4:
            await rest.post("/v1/cancelOrder", "{}", {})
        assert e4.value.is_timestamp_expired
        with pytest.raises(ArcusHTTPError) as e5:
            await rest.post("/v1/modifyOrder", "{}", {})
        assert e5.value.is_transmission and e5.value.retryable
        with pytest.raises(ArcusHTTPError) as e6:
            await rest.post("/v1/setLeverage", "{}", {})
        assert e6.value.retry_after_ms == 2000
    finally:
        await rest.close()


async def test_post_sends_exact_body_and_headers(srv):
    srv.post("/v1/cancelAllOrders", callback=lambda c: Resp(202, {"status": "CANCEL_ALL_ACKNOWLEDGED"}))
    rest = await make(srv)
    try:
        st, data = await rest.post("/v1/cancelAllOrders", '{"accountIndex":0,"address":"0xA"}',
                                   {"X-API-Key": "k", "X-Timestamp": "1", "X-Signature": "s"},
                                   params={"address": "0xA"})
        assert st == 202 and data["status"] == "CANCEL_ALL_ACKNOWLEDGED"
        call = srv.calls("POST", "/v1/cancelAllOrders")[0]
        assert call.text == '{"accountIndex":0,"address":"0xA"}' and call.query == {"address": "0xA"}
        assert call.headers["X-Signature"] == "s" and call.headers["Content-Type"] == "application/json"
    finally:
        await rest.close()


async def test_network_failure_on_get_is_retryable_exchange_error():
    rest = ArcusRest("http://127.0.0.1:9", timeout_s=0.5, get_retries=1)  # discard port: connection refused
    await rest.start()
    try:
        with pytest.raises(ExchangeError) as ei:
            await rest.markets()
        assert ei.value.retryable
    finally:
        await rest.close()


async def test_bare_5xx_is_not_transmission(srv):
    """Only the gateway's JSON ``errorType`` says "not submitted"; a CDN 502 page may follow a forwarded write."""
    srv.post("/v1/placeOrder", status=502, payload="<html>502 Bad Gateway</html>")
    srv.post("/v1/placeOrder", status=503, payload={"error": "path temporarily disabled", "errorType": "Unavailable"})
    rest = await make(srv)
    try:
        with pytest.raises(ArcusHTTPError) as e1:
            await rest.post("/v1/placeOrder", "{}", {})
        assert e1.value.status == 502 and e1.value.retryable
        assert not e1.value.is_transmission and not e1.value.is_definitive_5xx
        with pytest.raises(ArcusHTTPError) as e2:
            await rest.post("/v1/placeOrder", "{}", {})
        assert e2.value.is_definitive_5xx and not e2.value.is_transmission
    finally:
        await rest.close()
