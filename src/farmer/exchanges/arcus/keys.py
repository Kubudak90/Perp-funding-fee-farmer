"""Arcus API-key management: Ed25519 key generation and EIP-712 ``CreateApiKey`` registration.

The API key is an Ed25519 public key (64 hex). It is registered once with ``POST /v1/createApiKey``,
signed by the master ETH wallet (EIP-712, domain ``{"name":"Arcus API Key","version":"1","chainId"}``,
no verifyingContract). Keys are trade-only. IMPORTANT: re-creating a key with the SAME
``apiWalletName`` revokes the previous key, so every key gets a unique name by default.

Pure builders are separated from I/O so they can be unit-tested with deterministic keys.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field

import aiohttp
from cryptography.hazmat.primitives.asymmetric import ed25519
from eth_account import Account
from eth_account.messages import encode_typed_data

from farmer.exchanges.arcus.rest import ArcusHTTPError, ArcusRest

log = logging.getLogger("farmer.arcus.keys")

EIP712_DOMAIN_NAME = "Arcus API Key"
EIP712_DOMAIN_VERSION = "1"
MAINNET_CHAIN_ID = 4663
TESTNET_CHAIN_ID = 46630
DAY_MS = 86_400_000
MAX_VALID_DAYS = 180
_PUBKEY_RE = re.compile(r"^[0-9a-fA-F]{64}$")

CREATE_API_KEY_TYPES = {
    "CreateApiKey": [
        {"name": "apiWalletName", "type": "string"},
        {"name": "apiWalletPublicKey", "type": "string"},
        {"name": "validUntil", "type": "uint256"},
        {"name": "nonce", "type": "string"},
        {"name": "accountIndex", "type": "uint8"},
    ]
}


def generate_ed25519_keypair() -> tuple[str, str]:
    """New Ed25519 key. Returns ``(private_seed_hex, public_key_hex)`` (both 64 hex, no 0x)."""
    sk = ed25519.Ed25519PrivateKey.generate()
    return sk.private_bytes_raw().hex(), sk.public_key().public_bytes_raw().hex()


def public_key_from_private(private_seed_hex: str) -> str:
    h = private_seed_hex.strip().removeprefix("0x").removeprefix("0X")
    sk = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(h)[:32])
    return sk.public_key().public_bytes_raw().hex()


def default_key_name(public_key_hex: str) -> str:
    return f"farmer-{public_key_hex[:8].lower()}"


def compute_valid_until_ms(valid_days: float = MAX_VALID_DAYS, *, now_ms: int | None = None) -> int:
    """Expiry (epoch ms) clamped into the accepted window [now+1d, now+180d] with a 60 s margin."""
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    want = now + int(float(valid_days) * DAY_MS)
    lo, hi = now + DAY_MS + 60_000, now + MAX_VALID_DAYS * DAY_MS - 60_000
    return max(lo, min(hi, want))


def build_create_api_key_typed_data(*, chain_id: int, name: str, public_key_hex: str, valid_until_ms: int,
                                    nonce: str, account_index: int) -> dict:
    """EIP-712 typed data (``eth_signTypedData_v4`` full-message form) for ``CreateApiKey``."""
    if not 1 <= len(name) <= 64:
        raise ValueError("apiWalletName must be 1..64 chars")
    if not _PUBKEY_RE.match(public_key_hex):
        raise ValueError("public key must be 64 hex chars (no 0x)")
    if not (0 <= int(account_index) <= 9 or int(account_index) == 255):
        raise ValueError("accountIndex must be 0..9 or 255 (all subaccounts)")
    if not nonce or len(nonce) > 64:
        raise ValueError("nonce must be 1..64 chars")
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
            ],
            **CREATE_API_KEY_TYPES,
        },
        "primaryType": "CreateApiKey",
        "domain": {"name": EIP712_DOMAIN_NAME, "version": EIP712_DOMAIN_VERSION, "chainId": int(chain_id)},
        "message": {
            "apiWalletName": name,
            "apiWalletPublicKey": public_key_hex,
            "validUntil": int(valid_until_ms),
            "nonce": str(nonce),
            "accountIndex": int(account_index),
        },
    }


def sign_typed_data(wallet_private_key: str, typed_data: dict) -> dict:
    """Sign with eth-account; returns ``{"r","s","v"}`` hex strings (r/s zero-padded to 64 hex)."""
    signable = encode_typed_data(full_message=typed_data)
    signed = Account.sign_message(signable, private_key=wallet_private_key)
    return {"r": f"0x{signed.r:064x}", "s": f"0x{signed.s:064x}", "v": hex(signed.v)}


def recover_typed_data_signer(typed_data: dict, sig: dict) -> str:
    """Address that produced ``sig`` over ``typed_data`` (for self-checks / tests)."""
    signable = encode_typed_data(full_message=typed_data)
    r, s, v = int(sig["r"], 16), int(sig["s"], 16), int(sig["v"], 16)
    return Account.recover_message(signable, vrs=(v, r, s))


def build_create_api_key_request(*, chain_id: int, wallet_private_key: str, public_key_hex: str, account_index: int,
                                 name: str | None = None, valid_until_ms: int | None = None,
                                 nonce: str | None = None) -> dict:
    """Signed ``POST /v1/createApiKey`` body (pure apart from the default clock/nonce)."""
    wallet = Account.from_key(wallet_private_key)
    pub = public_key_hex.lower()
    name = name or default_key_name(pub)
    valid_until_ms = compute_valid_until_ms() if valid_until_ms is None else int(valid_until_ms)
    nonce = str(time.time_ns()) if nonce is None else str(nonce)
    typed = build_create_api_key_typed_data(chain_id=chain_id, name=name, public_key_hex=pub,
                                            valid_until_ms=valid_until_ms, nonce=nonce, account_index=account_index)
    sig = sign_typed_data(wallet_private_key, typed)
    if recover_typed_data_signer(typed, sig).lower() != wallet.address.lower():  # pragma: no cover - sanity
        raise RuntimeError("EIP-712 signature does not recover to the wallet address")
    return {
        "address": wallet.address,
        "publicKey": pub,
        "apiWalletName": name,
        "validUntil": valid_until_ms,
        "nonce": nonce,
        "accountIndex": int(account_index),
        "signature": sig,
    }


@dataclass
class RegisteredKey:
    """Result of ``register_api_key``. ``api_private_key`` is the secret to store (ARCUS_API_PRIVATE_KEY)."""

    address: str
    account_index: int
    api_key: str
    api_private_key: str = field(repr=False)
    name: str
    valid_until_ms: int
    response: dict = field(default_factory=dict)
    status: str | None = None


async def register_api_key(rest_url: str, chain_id: int, wallet_private_key: str, account_index: int,
                           name: str | None = None, valid_days: float = MAX_VALID_DAYS, *,
                           api_private_key: str | None = None, poll_timeout_s: float = 60.0,
                           poll_interval_s: float = 1.0, session: aiohttp.ClientSession | None = None,
                           timeout_s: float = 10.0) -> RegisteredKey:
    """Register an Ed25519 API key for ``(wallet, account_index)`` and wait until it is ACTIVE.

    Generates a fresh keypair unless ``api_private_key`` is given. The wallet key is only used for the
    EIP-712 signature and never sent. Raises ``ArcusHTTPError`` on rejection, ``TimeoutError`` if the
    key does not show up as ACTIVE within ``poll_timeout_s``.
    """
    if api_private_key is None:
        api_private_key, pub = generate_ed25519_keypair()
    else:
        pub = public_key_from_private(api_private_key)
    valid_until = compute_valid_until_ms(valid_days)
    body = build_create_api_key_request(chain_id=chain_id, wallet_private_key=wallet_private_key,
                                        public_key_hex=pub, account_index=account_index, name=name,
                                        valid_until_ms=valid_until)
    rest = ArcusRest(rest_url, timeout_s=timeout_s, session=session)
    await rest.start()
    try:
        _, created = await rest.post("/v1/createApiKey", json.dumps(body, separators=(",", ":")), {}, weight=20)
        log.info("createApiKey accepted for %s idx=%s key=%s...", body["address"], account_index, pub[:8])
        deadline = time.monotonic() + poll_timeout_s
        status = None
        while True:
            try:
                keys = await rest.api_keys(body["address"])
            except ArcusHTTPError as exc:
                log.warning("apiKeys poll failed: %s", exc)
                keys = []
            row = next((k for k in keys if str(k.get("apiKey", "")).lower() == pub), None)
            status = row.get("status") if row else None
            if status == "ACTIVE":
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"API key {pub[:8]}... not ACTIVE after {poll_timeout_s:.0f}s (status={status})")
            await asyncio.sleep(poll_interval_s)
    finally:
        await rest.close()
    return RegisteredKey(address=body["address"], account_index=int(account_index), api_key=pub,
                         api_private_key=api_private_key, name=body["apiWalletName"], valid_until_ms=valid_until,
                         response=created if isinstance(created, dict) else {}, status=status)


__all__ = [
    "CREATE_API_KEY_TYPES",
    "MAINNET_CHAIN_ID",
    "RegisteredKey",
    "TESTNET_CHAIN_ID",
    "build_create_api_key_request",
    "build_create_api_key_typed_data",
    "compute_valid_until_ms",
    "default_key_name",
    "generate_ed25519_keypair",
    "public_key_from_private",
    "recover_typed_data_signer",
    "register_api_key",
    "sign_typed_data",
]
