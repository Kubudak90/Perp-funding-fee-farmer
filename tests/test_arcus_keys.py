"""API-key management: Ed25519 keygen + EIP-712 CreateApiKey (deterministic keys, no network)."""

import time

import pytest
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak

from farmer.exchanges.arcus.auth import Ed25519Signer
from farmer.exchanges.arcus.keys import (
    DAY_MS,
    build_create_api_key_request,
    build_create_api_key_typed_data,
    compute_valid_until_ms,
    default_key_name,
    generate_ed25519_keypair,
    public_key_from_private,
    recover_typed_data_signer,
    register_api_key,
    sign_typed_data,
)
from tests.test_arcus_support import API_KEY, KEY_HEX, FakeHttp

WALLET_KEY = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
WALLET = Account.from_key(WALLET_KEY).address
# exact type strings compiled into the official TS SDK (app.arcus.xyz bundle)
SDK_DOMAIN_TYPE = "EIP712Domain(string name,string version,uint256 chainId)"
SDK_CREATE_TYPE = "CreateApiKey(string apiWalletName,string apiWalletPublicKey,uint256 validUntil,string nonce,uint8 accountIndex)"


def typed(**kw):
    args = dict(chain_id=4663, name="farmer-d75a9801", public_key_hex=API_KEY, valid_until_ms=1_800_000_000_000,
                nonce="1792428749875437199", account_index=0)
    args.update(kw)
    return build_create_api_key_typed_data(**args)


def test_generate_keypair():
    priv, pub = generate_ed25519_keypair()
    assert len(priv) == len(pub) == 64
    assert public_key_from_private(priv) == pub == Ed25519Signer(priv).api_key
    assert generate_ed25519_keypair()[0] != priv
    assert public_key_from_private(KEY_HEX) == API_KEY


def test_eip712_digest_matches_sdk_type_strings():
    td = typed()
    m = td["message"]
    enc_str = lambda s: keccak(text=s)  # noqa: E731
    u256 = lambda n: int(n).to_bytes(32, "big")  # noqa: E731
    domain_hash = keccak(keccak(text=SDK_DOMAIN_TYPE) + enc_str("Arcus API Key") + enc_str("1") + u256(4663))
    struct_hash = keccak(keccak(text=SDK_CREATE_TYPE) + enc_str(m["apiWalletName"]) + enc_str(m["apiWalletPublicKey"])
                         + u256(m["validUntil"]) + enc_str(m["nonce"]) + u256(m["accountIndex"]))
    digest = keccak(b"\x19\x01" + domain_hash + struct_hash)
    signable = encode_typed_data(full_message=td)
    assert keccak(b"\x19" + signable.version + signable.header + signable.body) == digest


def test_sign_is_deterministic_and_recovers():
    td = typed()
    s1, s2 = sign_typed_data(WALLET_KEY, td), sign_typed_data(WALLET_KEY, td)
    assert s1 == s2 and len(s1["r"]) == len(s1["s"]) == 66 and s1["v"] in ("0x1b", "0x1c")
    assert recover_typed_data_signer(td, s1) == WALLET
    assert recover_typed_data_signer(typed(account_index=1), s1) != WALLET  # scope is signed
    assert recover_typed_data_signer(typed(chain_id=46630), s1) != WALLET  # chain is signed


def test_request_body():
    body = build_create_api_key_request(chain_id=4663, wallet_private_key=WALLET_KEY, public_key_hex=API_KEY.upper(),
                                        account_index=3, valid_until_ms=1_800_000_000_000, nonce="42")
    assert body["address"] == WALLET and body["publicKey"] == API_KEY and body["accountIndex"] == 3
    assert body["apiWalletName"] == default_key_name(API_KEY) == "farmer-d75a9801"
    assert body["validUntil"] == 1_800_000_000_000 and body["nonce"] == "42"
    td = typed(name=body["apiWalletName"], valid_until_ms=body["validUntil"], nonce="42", account_index=3)
    assert recover_typed_data_signer(td, body["signature"]) == WALLET


def test_validation():
    with pytest.raises(ValueError):
        typed(name="")
    with pytest.raises(ValueError):
        typed(name="x" * 65)
    with pytest.raises(ValueError):
        typed(public_key_hex="0x" + API_KEY)
    with pytest.raises(ValueError):
        typed(account_index=10)
    typed(account_index=255)  # all-subaccounts scope is allowed


def test_valid_until_window():
    now = 1_790_000_000_000
    assert compute_valid_until_ms(180, now_ms=now) == now + 180 * DAY_MS - 60_000
    assert compute_valid_until_ms(1000, now_ms=now) == now + 180 * DAY_MS - 60_000
    assert compute_valid_until_ms(0.5, now_ms=now) == now + DAY_MS + 60_000
    assert compute_valid_until_ms(30, now_ms=now) == now + 30 * DAY_MS


async def test_register_api_key_flow():
    srv = FakeHttp()
    await srv.start()
    posted = {}

    def created(call):
        posted.update(call.json())
        return {"apiKey": posted["publicKey"], "address": posted["address"].lower(), "accountIndex": 0,
                "validUntil": posted["validUntil"], "createdAt": int(time.time() * 1e6)}

    srv.post("/v1/createApiKey", callback=created)
    srv.get("/v1/apiKeys", payload={"apiKeys": []})  # not visible yet on the first poll
    srv.get("/v1/apiKeys", callback=lambda c: {"apiKeys": [{"apiKey": posted["publicKey"], "status": "ACTIVE"}]},
            repeat=True)
    try:
        res = await register_api_key(srv.base, 4663, WALLET_KEY, 0, poll_interval_s=0.01)
    finally:
        await srv.stop()
    assert res.status == "ACTIVE" and res.address == WALLET and res.api_key == posted["publicKey"]
    assert public_key_from_private(res.api_private_key) == res.api_key
    assert res.name == posted["apiWalletName"] == f"farmer-{res.api_key[:8]}"
    assert res.api_private_key not in repr(res)
    lead_days = (posted["validUntil"] / 1000 - time.time()) / 86400
    assert 179.9 < lead_days < 180
    td = typed(name=posted["apiWalletName"], public_key_hex=posted["publicKey"], valid_until_ms=posted["validUntil"],
               nonce=posted["nonce"], account_index=0)
    assert recover_typed_data_signer(td, posted["signature"]) == WALLET


async def test_register_api_key_timeout_and_reject():
    srv = FakeHttp()
    await srv.start()
    srv.post("/v1/createApiKey", payload={"apiKey": API_KEY})
    srv.get("/v1/apiKeys", payload={"apiKeys": []}, repeat=True)
    srv.post("/v1/createApiKey", status=409, payload={"error": "public key already registered"})
    try:
        with pytest.raises(TimeoutError):
            await register_api_key(srv.base, 4663, WALLET_KEY, 0, api_private_key=KEY_HEX, poll_timeout_s=0.05,
                                   poll_interval_s=0.01)
        from farmer.exchanges.arcus.rest import ArcusHTTPError

        with pytest.raises(ArcusHTTPError) as ei:
            await register_api_key(srv.base, 4663, WALLET_KEY, 0, api_private_key=KEY_HEX)
        assert ei.value.status == 409
    finally:
        await srv.stop()
