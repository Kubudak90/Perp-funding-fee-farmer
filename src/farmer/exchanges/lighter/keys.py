"""One-time Lighter API-key setup helpers (used by ``farmer keygen``-style tooling, never by the hot path).

Flow (specs/lighter-trading.md §3):
1. ``lookup_account_index(l1_address)`` -> master account index (``min`` of ``sub_accounts[].index``; an account
   only exists after the first deposit, unknown addresses return HTTP 400 code 21100);
2. ``generate_api_key()`` -> (private, public) key pair from the native signer (no network);
3. ``register_api_key(...)`` signs ``ChangePubKey`` (tx type 8) with the API key AND an L1 (ETH) signature and
   submits it; then polls ``/api/v1/apikeys`` until the new public key is visible.

API key index rules: 0-3 are reserved for Lighter's own apps (and 157 on Lighter RH); use 4..254.
Registration must run from a permitted jurisdiction (sendTx is geo-blocked with code 20558 elsewhere).
Keys are secrets: never log them.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import aiohttp

from farmer.exchanges.lighter.helpers import ERR_RESTRICTED, RESTRICTED_MESSAGE, parse_ack

log = logging.getLogger("farmer.lighter.keys")

MAINNET_URL = "https://mainnet.zklighter.elliot.ai"
API_KEY_INDEX_MIN = 4
API_KEY_INDEX_MAX = 254
RESERVED_RH_INDEXES = frozenset({157})


class KeySetupError(Exception):
    pass


def validate_api_key_index(idx: int, *, rh: bool = False) -> int:
    """Return ``idx`` if usable for a bot key, else raise ValueError."""
    if isinstance(idx, bool) or not isinstance(idx, int):
        raise ValueError("api_key_index must be an int")
    if not API_KEY_INDEX_MIN <= idx <= API_KEY_INDEX_MAX:
        raise ValueError(f"api_key_index must be in {API_KEY_INDEX_MIN}..{API_KEY_INDEX_MAX} (0-3 are reserved), got {idx}")
    if rh and idx in RESERVED_RH_INDEXES:
        raise ValueError(f"api_key_index {idx} is reserved on Lighter RH")
    return idx


def is_rh_url(url: str) -> bool:
    return "rh.lighter" in url or "rh-testnet.lighter" in url


def normalize_pubkey(pk: str | None) -> str:
    """Public keys: ``create_api_key`` returns ``0x``-prefixed hex, ``/apikeys`` returns it without prefix."""
    s = (pk or "").strip().lower()
    return s[2:] if s.startswith("0x") else s


def pick_master_account_index(payload: dict) -> int:
    """``accountsByL1Address`` response -> master account index (smallest index; sub-accounts count down)."""
    subs = payload.get("sub_accounts") or []
    idxs = []
    for a in subs:
        try:
            idxs.append(int(a.get("index")))
        except (TypeError, ValueError, AttributeError):
            continue
    if not idxs:
        raise KeySetupError("no Lighter account for this L1 address (deposit first)")
    return min(idxs)


def generate_api_key() -> tuple[str, str]:
    """New (private_key, public_key), both ``0x`` + 80 hex. Uses the native signer; no network."""
    from farmer.exchanges.lighter.signer import import_lighter

    lighter = import_lighter()
    priv, pub, err = lighter.create_api_key()
    if err or not priv or not pub:
        raise KeySetupError(f"create_api_key failed: {err}")
    return priv, pub


def address_from_eth_key(eth_private_key: str) -> str:
    from eth_account import Account

    return Account.from_key(eth_private_key).address


async def _get_json(session: aiohttp.ClientSession, url: str, params: dict) -> tuple[int, Any]:
    async with session.get(url, params=params) as r:
        text = await r.text()
        try:
            return r.status, json.loads(text)
        except ValueError:
            return r.status, {"code": -r.status, "message": text[:200]}


async def lookup_account_index(l1_address: str, *, url: str = MAINNET_URL, session: aiohttp.ClientSession | None = None) -> int:
    own = session is None
    s = session or aiohttp.ClientSession(trust_env=True, timeout=aiohttp.ClientTimeout(total=10))
    try:
        status, j = await _get_json(s, f"{url}/api/v1/accountsByL1Address", {"l1_address": l1_address})
        if status != 200 or j.get("code") != 200:
            raise KeySetupError(f"account lookup failed: HTTP {status} {j}")
        return pick_master_account_index(j)
    finally:
        if own:
            await s.close()


async def fetch_registered_pubkey(account_index: int, api_key_index: int, *, url: str = MAINNET_URL,
                                  session: aiohttp.ClientSession) -> str | None:
    status, j = await _get_json(session, f"{url}/api/v1/apikeys", {"account_index": account_index, "api_key_index": api_key_index})
    if status != 200:
        return None
    for k in j.get("api_keys") or []:
        if int(k.get("api_key_index", -1)) == api_key_index:
            return normalize_pubkey(k.get("public_key"))
    return None


async def fetch_next_nonce(account_index: int, api_key_index: int, *, url: str, session: aiohttp.ClientSession) -> int | None:
    status, j = await _get_json(session, f"{url}/api/v1/nextNonce", {"account_index": account_index, "api_key_index": api_key_index})
    if status == 200 and j.get("code") == 200:
        return int(j["nonce"])
    return None


async def register_api_key(
    *,
    eth_private_key: str,
    api_private_key: str,
    api_public_key: str,
    account_index: int,
    api_key_index: int,
    url: str = MAINNET_URL,
    chain_id: int = 304,
    wait_s: float = 30.0,
    session: aiohttp.ClientSession | None = None,
) -> str:
    """Register ``api_public_key`` at ``api_key_index`` (replaces any key there). Returns the tx hash.

    NOT exercised by unit tests (needs a funded account, an ETH key and a permitted jurisdiction).
    """
    validate_api_key_index(api_key_index, rh=is_rh_url(url))
    from farmer.exchanges.lighter.signer import import_lighter

    lighter = import_lighter()
    own = session is None
    s = session or aiohttp.ClientSession(trust_env=True, timeout=aiohttp.ClientTimeout(total=15))
    client = None
    try:
        client = lighter.SignerClient(
            url=url,
            account_index=account_index,
            api_private_keys={api_key_index: api_private_key},
            nonce_management_type=lighter.nonce_manager.NonceManagerType.NONE,
            chain_id=chain_id,
        )
        nonce = await fetch_next_nonce(account_index, api_key_index, url=url, session=s)
        if nonce is None:
            nonce = -1  # let the Go signer fetch it (blocking call -> thread)
            tx_type, tx_info, tx_hash, err = await asyncio.to_thread(
                client.sign_change_api_key, eth_private_key, api_public_key, 0, nonce, api_key_index
            )
        else:
            tx_type, tx_info, tx_hash, err = client.sign_change_api_key(
                eth_private_key, api_public_key, 0, nonce, api_key_index
            )
        if err:
            raise KeySetupError(f"sign ChangePubKey failed: {err}")
        async with s.post(f"{url}/api/v1/sendTx", data={"tx_type": str(tx_type), "tx_info": tx_info}) as r:
            text = await r.text()
        try:
            resp = json.loads(text)
        except ValueError:
            raise KeySetupError(f"sendTx: non-JSON HTTP {r.status}: {text[:200]}") from None
        ok, code, msg = parse_ack(resp)
        if not ok:
            if code == ERR_RESTRICTED:
                raise KeySetupError(RESTRICTED_MESSAGE)
            raise KeySetupError(f"ChangePubKey rejected ({code}): {msg}")
        want = normalize_pubkey(api_public_key)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_s
        while loop.time() < deadline:
            await asyncio.sleep(2.0)
            if await fetch_registered_pubkey(account_index, api_key_index, url=url, session=s) == want:
                log.info("lighter API key %d registered for account %d (tx %s)", api_key_index, account_index, tx_hash)
                return str(tx_hash)
        raise KeySetupError(f"key not visible after {wait_s}s; check GET /api/v1/tx?by=hash&value={tx_hash}")
    finally:
        if client is not None:
            await client.close()
        if own:
            await s.close()
