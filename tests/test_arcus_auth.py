"""Arcus signing: spec test vectors (reproduced with the official SDK layout), clock, encoders."""

from decimal import Decimal as D

import pytest

from farmer.exchanges.arcus.auth import (
    ArcusSigner,
    Ed25519Signer,
    NsClock,
    auth_headers,
    cancel_payload,
    canonical_json,
    dec_str,
    legacy_message,
    modify_payload,
    place_payload,
    submit_tif,
    to_units,
    verify_signature,
)
from farmer.models import Side
from tests.test_arcus_support import API_KEY, KEY_HEX

ADDR = "0xAbCdEf1234567890AbCdEf1234567890AbCdEf12"
CT = 1713825891591000000
GTT = 4102444800000000
TICK, STEP = D("0.1"), D("0.00000001")


@pytest.fixture
def signer() -> ArcusSigner:
    return ArcusSigner(Ed25519Signer(KEY_HEX), ADDR, 0, NsClock())


def test_public_key_derivation():
    assert Ed25519Signer(KEY_HEX).api_key == API_KEY
    assert Ed25519Signer("0x" + KEY_HEX).api_key == API_KEY
    # NaCl-style 64-byte secret key (seed || pub)
    assert Ed25519Signer(KEY_HEX + API_KEY).api_key == API_KEY
    with pytest.raises(ValueError):
        Ed25519Signer(KEY_HEX + "00" * 32)
    with pytest.raises(ValueError):
        Ed25519Signer("zz" * 32)
    assert KEY_HEX not in repr(Ed25519Signer(KEY_HEX))


def test_place_vector(signer):
    r = signer.place(market_id=1, tick_size=TICK, step_size=STEP, side=Side.BUY, price=D("60000.0"), size=D("0.001"),
                     tif="ALO", gtt_us=GTT, client_id="mk-1", ts_ns=CT)
    assert r.message.decode() == (
        '{"ad":"0xabcdef1234567890abcdef1234567890abcdef12","ai":0,"c":"mk-1","ct":1713825891591000000,'
        '"g":4102444800000000000,"m":1,"op":1,"p":600000,"q":100000,"r":0,"s":0,"t":3,"v":1}')
    assert r.signature == ("bf56ff5b17b2d169159f7204a38a97b7f130b510423a3f92ad812e2aaeb84d5c"
                           "0207e00128dee0a72b0b5ad45efd339ff778b182e133558aab21ae8d80078103")
    assert r.body == {"address": ADDR, "accountIndex": 0, "marketId": 1, "orderSide": "BUY", "orderType": "LIMIT",
                      "quantity": "0.001", "price": "60000", "timeInForce": "ALO", "goodTilTime": str(GTT),
                      "reduceOnly": False, "timestamp": CT, "clientId": "mk-1"}
    assert r.headers(signer.api_key) == {"X-API-Key": API_KEY, "X-Timestamp": str(CT), "X-Signature": r.signature}
    assert len(r.signature) == 128 and r.signature == r.signature.lower()


def test_cancel_by_client_id_vector(signer):
    r = signer.cancel(market_id=1, client_id="mk-1", ts_ns=CT)
    assert r.message.decode() == ('{"ad":"0xabcdef1234567890abcdef1234567890abcdef12","ai":0,"c":"mk-1",'
                                  '"ct":1713825891591000000,"m":1,"op":2,"v":1}')
    assert r.signature == ("ee233bae0479ce60038ceb29af0312d1dcc63b3902d13ad1e3a1123edc303381"
                           "1ecccf290a977e215f4336bfb4e656f3fcf26586baff6c6e5a1ab79f1a7ae303")
    assert r.body["kind"] == "clientId" and r.body["clientId"] == "mk-1" and "orderId" not in r.body


def test_cancel_by_order_id_payload(signer):
    r = signer.cancel(market_id=1, order_id="a1b2c3d4e5f67890", ts_ns=CT)
    assert r.message.decode() == ('{"ad":"0xabcdef1234567890abcdef1234567890abcdef12","ai":0,'
                                  '"ct":1713825891591000000,"id":"a1b2c3d4e5f67890","m":1,"op":2,"v":1}')
    assert verify_signature(API_KEY, r.message, r.signature)
    assert r.body["kind"] == "orderId" and r.body["orderId"] == "a1b2c3d4e5f67890"
    with pytest.raises(ValueError):
        cancel_payload(address=ADDR, account_index=0, ct=CT, market_id=1, order_id="x", client_id="y")
    with pytest.raises(ValueError):
        cancel_payload(address=ADDR, account_index=0, ct=CT, market_id=1)


def test_modify_by_order_id_payload(signer):
    r = signer.modify(market_id=1, tick_size=TICK, step_size=STEP, side="BUY", price=D("60000.5"), size=D("0.001"),
                      tif="ALO", reduce_only=False, gtt_us=GTT, order_id="a1b2c3d4e5f67890", ts_ns=CT)
    assert r.message.decode() == (
        '{"ad":"0xabcdef1234567890abcdef1234567890abcdef12","ai":0,"ct":1713825891591000000,'
        '"g":4102444800000000000,"id":"a1b2c3d4e5f67890","m":1,"op":3,"p":600005,"q":100000,"r":0,"s":0,"t":3,"v":1}')
    assert verify_signature(API_KEY, r.message, r.signature)
    # ModifyOrderRequest: additionalProperties false -> no "timestamp", ct travels as clientTime string
    assert "timestamp" not in r.body and r.body["clientTime"] == str(CT)
    assert r.body["side"] == "BUY" and r.body["goodTilTime"] == str(GTT) and r.body["orderId"] == "a1b2c3d4e5f67890"


def test_modify_by_client_id_mirrors_body():
    p = modify_payload(address=ADDR, account_index=0, ct=CT, gtt_us=GTT, market_id=1, price_ticks=600005,
                       qty_quantums=100000, reduce_only=True, side="SELL", tif="GTC", client_id="q-1")
    assert '"c":"q-1"' in p and '"id"' not in p and '"r":1' in p and '"s":1' in p and '"t":0' in p


def test_legacy_cancel_all_vector(signer):
    body = {"address": ADDR, "accountIndex": 0, "marketId": 1}
    r = signer.legacy("cancelAllOrders", body, ts_ns=CT)
    assert r.message.decode() == ('1713825891591000000cancelAllOrders{"accountIndex":0,'
                                  '"address":"0xAbCdEf1234567890AbCdEf1234567890AbCdEf12","marketId":1}')
    assert r.signature == ("1c96018a00ea491834ea460ead755a8f21be087df7ff1b06803d5a5222a00fa9"
                           "ed21a6c01db07c26af661f44ecb1f47c03834a81b80ce4635598fcdbf0e8b60d")


def test_legacy_schedule_cancel_message(signer):
    body = {"address": ADDR, "accountIndex": 0, "time": 1734567890123000}
    r = signer.legacy("scheduleCancel", body, ts_ns=CT)
    assert r.message.decode() == ('1713825891591000000scheduleCancel{"accountIndex":0,'
                                  '"address":"0xAbCdEf1234567890AbCdEf1234567890AbCdEf12","time":1734567890123000}')
    assert verify_signature(API_KEY, r.message, r.signature)
    assert legacy_message(CT, "scheduleCancel", body) == r.message


def test_place_rejects_off_grid_and_bad_ids(signer):
    with pytest.raises(ValueError):  # price not a multiple of top-level tickSize
        signer.place(market_id=1, tick_size=TICK, step_size=STEP, side="BUY", price=D("60000.05"), size=D("0.001"),
                     tif="ALO", gtt_us=GTT)
    with pytest.raises(ValueError):  # size not a multiple of step
        signer.place(market_id=1, tick_size=TICK, step_size=STEP, side="BUY", price=D("60000"), size=D("0.000000001"),
                     tif="ALO", gtt_us=GTT)
    for bad in ("has space", "x" * 37, "quote\"", ""):
        with pytest.raises(ValueError):
            signer.place(market_id=1, tick_size=TICK, step_size=STEP, side="BUY", price=D("60000"), size=D("0.001"),
                         tif="ALO", gtt_us=GTT, client_id=bad)
    with pytest.raises(ValueError):
        ArcusSigner(Ed25519Signer(KEY_HEX), "0x123", 0, NsClock())
    with pytest.raises(ValueError):
        ArcusSigner(Ed25519Signer(KEY_HEX), ADDR, 10, NsClock())


def test_place_payload_codes():
    p = place_payload(address=ADDR, account_index=3, ct=1, gtt_us=2, market_id=7, price_ticks=5, qty_quantums=6,
                      reduce_only=True, side="SELL", tif="IOC")
    assert p == '{"ad":"0xabcdef1234567890abcdef1234567890abcdef12","ai":3,"ct":1,"g":2000,"m":7,"op":1,"p":5,' \
                '"q":6,"r":1,"s":1,"t":2,"v":1}'
    assert '"t":1' in place_payload(address=ADDR, account_index=0, ct=1, gtt_us=2, market_id=1, price_ticks=1,
                                    qty_quantums=1, reduce_only=False, side="BUY", tif="FOK")
    assert submit_tif("GTC") == "GTT" and submit_tif("alo") == "ALO"
    with pytest.raises(ValueError):
        submit_tif("DAY")


def test_tick_tiers_never_change_the_divisor(signer):
    # BTC band >= 500000 uses tick 0.2 but p is still price / top-level tickSize 0.1
    r = signer.place(market_id=1, tick_size=TICK, step_size=STEP, side="SELL", price=D("600000.2"), size=D("0.001"),
                     tif="ALO", gtt_us=GTT, ts_ns=CT)
    assert '"p":6000002' in r.message.decode()


def test_nsclock_strictly_increasing_and_sync():
    now = [1_000_000_000_000]
    clk = NsClock(time_ns=lambda: now[0])
    a, b, c = clk.now_ns(), clk.now_ns(), clk.now_ns()
    assert a < b < c
    # server 2 s ahead; request took 10 ms -> offset from the midpoint
    off = clk.sync(now[0] + 2_000_000_000 + 5_000_000, now[0], now[0] + 10_000_000)
    assert off == 2_000_000_000
    assert clk.now_ns() >= now[0] + 2_000_000_000
    # offset moving backwards never produces a repeated / decreasing stamp
    last = clk.now_ns()
    clk.sync(now[0] - 5_000_000_000, now[0], now[0])
    assert clk.now_ns() > last
    assert clk.now_us() == (now[0] + clk.offset_ns) // 1000


def test_encoders():
    assert to_units(D("60000.0"), D("0.1")) == 600000
    with pytest.raises(ValueError):
        to_units(D("1.05"), D("0.1"))
    assert dec_str(D("60000.0")) == "60000" and dec_str(D("0.00010")) == "0.0001" and dec_str(D("1E-8")) == "0.00000001"
    assert canonical_json({"b": 1, "a": {"d": True, "c": None}}) == '{"a":{"c":null,"d":true},"b":1}'
    with pytest.raises(TypeError):
        canonical_json({"x": 1.5})
    with pytest.raises(ValueError):
        canonical_json({"x": "é"})
    assert auth_headers("k", 5, "s") == {"X-API-Key": "k", "X-Timestamp": "5", "X-Signature": "s"}
    assert not verify_signature(API_KEY, b"x", "00" * 64)
