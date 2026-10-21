"""keys.py pure parts + account lookup against a local server (registration itself is never executed)."""

import json
from pathlib import Path

import pytest
from aiohttp import web

from farmer.exchanges.lighter import keys

DATA = Path(__file__).parent / "data" / "lighter"


def test_validate_api_key_index():
    for ok in (4, 5, 156, 158, 254):
        assert keys.validate_api_key_index(ok) == ok
    for bad in (0, 1, 2, 3, 255, -1):
        with pytest.raises(ValueError):
            keys.validate_api_key_index(bad)
    assert keys.validate_api_key_index(157) == 157  # mainnet
    with pytest.raises(ValueError):
        keys.validate_api_key_index(157, rh=True)  # reserved on Lighter RH
    with pytest.raises(ValueError):
        keys.validate_api_key_index(True)  # type: ignore[arg-type]
    assert keys.is_rh_url("https://api.rh.lighter.xyz") and not keys.is_rh_url("https://mainnet.zklighter.elliot.ai")


def test_normalize_pubkey():
    assert keys.normalize_pubkey("0xABcd") == "abcd" and keys.normalize_pubkey("abcd") == "abcd"
    assert keys.normalize_pubkey(None) == ""


def test_pick_master_account_index():
    payload = json.loads((DATA / "rest_accounts_by_l1.json").read_text())
    assert keys.pick_master_account_index(payload) == 313004
    multi = {"sub_accounts": [{"index": 281474976710600}, {"index": 4242}, {"index": "281474976710601"}]}
    assert keys.pick_master_account_index(multi) == 4242  # masters count up, sub-accounts count down from 2^48-2
    with pytest.raises(keys.KeySetupError):
        keys.pick_master_account_index({"sub_accounts": []})


def test_generate_api_key_unique():
    a, b = keys.generate_api_key(), keys.generate_api_key()
    assert a != b and all(len(k) == 82 and k.startswith("0x") for k in (*a, *b))


def test_address_from_eth_key():
    # well-known test vector (private key 0x...01)
    addr = keys.address_from_eth_key("0x" + "00" * 31 + "01")
    assert addr == "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"


@pytest.fixture
async def server():
    responses = {}

    async def handle(request):
        status, payload = responses[request.path]
        return web.json_response(payload, status=status)

    app = web.Application()
    app.router.add_route("GET", "/{tail:.*}", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}", responses
    await runner.cleanup()


async def test_lookup_account_index(server):
    url, responses = server
    responses["/api/v1/accountsByL1Address"] = (200, json.loads((DATA / "rest_accounts_by_l1.json").read_text()))
    assert await keys.lookup_account_index("0xa522fe38eF16E2aDB68a589D11cD13F8A76AB1c1", url=url) == 313004
    responses["/api/v1/accountsByL1Address"] = (400, {"code": 21100, "message": "account not found"})
    with pytest.raises(keys.KeySetupError):
        await keys.lookup_account_index("0x0000000000000000000000000000000000000001", url=url)


async def test_fetch_registered_pubkey(server):
    import aiohttp

    url, responses = server
    responses["/api/v1/apikeys"] = (200, {"code": 200, "api_keys": [
        {"account_index": 1, "api_key_index": 4, "nonce": 7, "public_key": "AB" * 40}]})
    async with aiohttp.ClientSession() as s:
        assert await keys.fetch_registered_pubkey(1, 4, url=url, session=s) == "ab" * 40
        assert await keys.fetch_registered_pubkey(1, 5, url=url, session=s) is None


ERR = ("private key does not match the one on Lighter. ownPubKey: " + "af" * 40 + " response: " + "49" * 40
       + " on api key 4")


async def test_explain_key_mismatch_finds_the_right_slot(server):
    url, responses = server
    responses["/api/v1/apikeys"] = (200, {"code": 200, "api_keys": [
        {"account_index": 7, "api_key_index": 2, "public_key": "AF" * 40},
        {"account_index": 7, "api_key_index": 4, "public_key": "49" * 40}]})
    hint = await keys.explain_key_mismatch(ERR, account_index=7, api_key_index=4, url=url)
    assert "LIGHTER_API_KEY_INDEX=2" in hint


async def test_explain_key_mismatch_not_registered(server):
    url, responses = server
    responses["/api/v1/apikeys"] = (200, {"code": 200, "api_keys": [
        {"account_index": 7, "api_key_index": 0, "public_key": "11" * 40},
        {"account_index": 7, "api_key_index": 4, "public_key": "49" * 40}]})
    hint = await keys.explain_key_mismatch(ERR, account_index=7, api_key_index=4, url=url)
    assert "not registered on account 7" in hint and "slots: 0, 4" in hint and "farmer keygen lighter" in hint
    assert "af" * 40 not in hint  # only a short prefix of the public key, never anything secret


async def test_explain_key_mismatch_unknown_inputs(server):
    url, responses = server
    assert await keys.explain_key_mismatch("some other error", account_index=7, api_key_index=4, url=url) is None
    responses["/api/v1/apikeys"] = (500, {"code": 500})
    assert await keys.explain_key_mismatch(ERR, account_index=7, api_key_index=4, url=url) is None
