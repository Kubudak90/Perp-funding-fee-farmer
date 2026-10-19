"""LighterSigner against the REAL lighter-sdk 1.1.7 native signer, offline (throwaway key, dead URL, no network)."""

import json
import re
import time

import pytest

from farmer.exchanges.lighter import signer as sg
from farmer.exchanges.lighter.helpers import MAX_PRICE_INT
from farmer.exchanges.lighter.keys import generate_api_key

DEAD_URL = "http://127.0.0.1:9"  # CreateClient does no network I/O; explicit-nonce signing never calls out


@pytest.fixture(scope="module")
def keypair():
    return generate_api_key()


@pytest.fixture
async def signer(keypair):
    s = sg.LighterSigner(DEAD_URL, 123456, 7, keypair[0], 304)
    yield s
    await s.close()


def test_constructor_requires_running_loop(keypair):
    with pytest.raises(RuntimeError):
        sg.LighterSigner(DEAD_URL, 1, 7, keypair[0], 304)


def test_generated_key_format(keypair):
    priv, pub = keypair
    assert re.fullmatch(r"0x[0-9a-f]{80}", priv) and re.fullmatch(r"0x[0-9a-f]{80}", pub)


async def test_market_ioc_order_fields(signer):
    t0 = time.time()
    tx = signer.create_order(market_index=0, client_order_index=42, base_amount=500, price=241201, is_ask=False,
                             order_type=sg.ORDER_TYPE_MARKET, time_in_force=sg.TIF_IOC, reduce_only=False,
                             order_expiry=sg.DEFAULT_IOC_EXPIRY, nonce=7)
    assert tx.tx_type == sg.TX_CREATE_ORDER and re.fullmatch(r"[0-9a-f]{80}", tx.tx_hash)
    info = tx.info
    assert info["AccountIndex"] == 123456 and info["ApiKeyIndex"] == 7  # explicit key index, never the default 255
    assert (info["MarketIndex"], info["ClientOrderIndex"], info["BaseAmount"], info["Price"]) == (0, 42, 500, 241201)
    assert (info["IsAsk"], info["Type"], info["TimeInForce"], info["OrderExpiry"], info["Nonce"]) == (0, 1, 0, 0, 7)
    assert info["L2TxAttributes"] is None and info["Sig"]
    assert 590 < info["ExpiredAt"] / 1000 - t0 < 610  # signer sets ExpiredAt = now + ~10 min


async def test_post_only_and_other_txs(signer):
    po = signer.create_order(market_index=1, client_order_index=2**48 - 1, base_amount=100, price=820000, is_ask=True,
                             order_type=sg.ORDER_TYPE_LIMIT, time_in_force=sg.TIF_POST_ONLY, reduce_only=True,
                             order_expiry=sg.DEFAULT_28_DAY_ORDER_EXPIRY, nonce=8).info
    assert po["TimeInForce"] == 2 and po["ReduceOnly"] == 1 and po["IsAsk"] == 1
    assert 27.9 < (po["OrderExpiry"] / 1000 - time.time()) / 86400 < 28.1  # 28-day expiry substituted by the signer
    c = signer.cancel_order(market_index=0, order_index=42, nonce=9)
    assert c.tx_type == sg.TX_CANCEL_ORDER and c.info["Index"] == 42
    ca = signer.cancel_all(nonce=10)
    assert ca.tx_type == sg.TX_CANCEL_ALL and ca.info["TimeInForce"] == 0 and ca.info["Time"] == 0
    assert ca.info["L2TxAttributes"] is None  # all markets
    assert signer.cancel_all(market_index=1, nonce=11).info["L2TxAttributes"] == {"5": 1}
    m = signer.modify_order(market_index=0, order_index=42, base_amount=600, price=240100, nonce=12)
    assert m.tx_type == sg.TX_MODIFY_ORDER and (m.info["BaseAmount"], m.info["Price"]) == (600, 240100)
    lv = signer.update_leverage(market_index=0, fraction=2000, nonce=13)
    assert lv.tx_type == sg.TX_UPDATE_LEVERAGE and lv.info["InitialMarginFraction"] == 2000 and lv.info["MarginMode"] == 0
    assert len({t.tx_hash for t in (c, ca, m, lv)}) == 4


async def test_same_payload_same_hash_new_nonce_new_hash(signer):
    def mk(nonce):
        return signer.create_order(market_index=0, client_order_index=5, base_amount=10, price=100, is_ask=False,
                                   order_type=sg.ORDER_TYPE_LIMIT, time_in_force=sg.TIF_IOC, reduce_only=False,
                                   order_expiry=0, nonce=nonce)
    assert mk(1).tx_hash != mk(2).tx_hash
    assert json.loads(mk(1).tx_info)["Nonce"] == 1


@pytest.mark.parametrize("kw", [
    dict(price=MAX_PRICE_INT + 1),  # ctypes would silently wrap above the C int range
    dict(price=2**32 + 5),
    dict(price=0),
    dict(price=240000.5),  # floats never reach the native call
    dict(base_amount=0),
    dict(base_amount=2**48),
    dict(client_order_index=0),
    dict(client_order_index=2**48),
    dict(market_index=255),
    dict(order_expiry=-1),  # IOC needs expiry 0
    dict(nonce=-1),  # -1 would make the Go signer fetch the nonce with a BLOCKING HTTP call
])
async def test_range_checks_before_native_call(signer, kw):
    args = dict(market_index=0, client_order_index=1, base_amount=10, price=100, is_ask=False,
                order_type=sg.ORDER_TYPE_MARKET, time_in_force=sg.TIF_IOC, reduce_only=False, order_expiry=0, nonce=1)
    args.update(kw)
    with pytest.raises(sg.SignError):
        signer.create_order(**args)


async def test_native_validation_error_surfaces(signer):
    with pytest.raises(sg.SignError):  # MARKET + POST_ONLY is refused by the native signer itself
        signer.create_order(market_index=0, client_order_index=1, base_amount=10, price=100, is_ask=False,
                            order_type=sg.ORDER_TYPE_MARKET, time_in_force=sg.TIF_POST_ONLY, reduce_only=False,
                            order_expiry=0, nonce=1)


async def test_auth_token_format_and_cap(signer):
    tok, exp = signer.auth_token(6 * 3600)
    expiry, acct, key, sig = tok.split(":")
    assert (acct, key) == ("123456", "7") and re.fullmatch(r"[0-9a-f]{160}", sig)
    assert int(expiry) == int(exp) and 6 * 3600 - 5 < exp - time.time() <= 6 * 3600
    tok2, exp2 = signer.auth_token(48 * 3600)  # capped below the 8 h server maximum
    assert exp2 - time.time() <= sg.MAX_AUTH_TTL_S


async def test_executor_with_real_signer(signer):
    """Full sign -> send path with the real signer and an in-memory transport."""

    class T:
        def __init__(self):
            self.sent = []

        def ws_ready(self):
            return True

        async def send_ws(self, tx, timeout):
            self.sent.append(json.loads(tx.tx_info))
            return {"code": 200, "tx_hash": tx.tx_hash}

        async def send_rest(self, tx, timeout):
            raise AssertionError

        async def lookup_tx(self, h):
            return None

        async def fetch_next_nonce(self):
            return 41

    t = T()
    ex = sg.TxExecutor(t)
    for coi in (1, 2):
        await ex.submit(lambda n, coi=coi: signer.create_order(
            market_index=0, client_order_index=coi, base_amount=10, price=100, is_ask=True,
            order_type=sg.ORDER_TYPE_MARKET, time_in_force=sg.TIF_IOC, reduce_only=False, order_expiry=0, nonce=n))
    assert [s["Nonce"] for s in t.sent] == [41, 42] and ex.next_nonce == 43
