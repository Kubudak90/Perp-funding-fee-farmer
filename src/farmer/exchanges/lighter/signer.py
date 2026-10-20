"""Signing (lighter-sdk 1.1.7 ``SignerClient``) and the robust tx executor.

Rules enforced here (specs/lighter-trading.md §1, §4, §7, §11.1):
- ``SignerClient`` must be constructed INSIDE a running event loop (aiohttp objects in its ``__init__``);
- the SDK nonce manager is disabled (``NonceManagerType.NONE``); every ``sign_*`` call gets an explicit
  ``nonce`` AND ``api_key_index`` (``nonce=-1`` would block the loop on a Go HTTP call; ``api_key_index=255``
  would sign with whichever key was created last);
- integers are range-checked before they cross the ctypes boundary (``c_int`` silently wraps);
- one in-flight tx per API key (nonce lock); the tx hash is known at signing time;
- outcome unknown (timeout / socket died / ambiguous 29500-29501 answer / a non-deterministic rejection of a copy
  sent after the socket died) -> look the hash up, compare ``nextNonce``, re-send the IDENTICAL signed payload when
  the nonce is still free (it cannot execute twice), otherwise raise ``TxUnknown``. ``TxRejected`` is reserved for
  answers proving the tx did not run. A tx is never re-signed with a new nonce while an earlier copy may still execute.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import warnings
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from farmer.exchanges.base import ExchangeError
from farmer.exchanges.lighter.helpers import (
    DETERMINISTIC_TX_CODES,
    ERR_INVALID_NONCE,
    MAX_BASE_AMOUNT,
    MAX_CLIENT_ORDER_INDEX,
    MAX_PRICE_INT,
    TxNotSent,
    TxRejected,
    TxUnknown,
    is_ambiguous_tx_code,
    is_retryable,
    parse_ack,
)

log = logging.getLogger("farmer.lighter.signer")

# SignerClient constants (verbatim values from lighter/signer_client.py 1.1.7; duplicated so pure code and tests
# do not need the native library)
ORDER_TYPE_LIMIT = 0
ORDER_TYPE_MARKET = 1
TIF_IOC = 0
TIF_GTT = 1
TIF_POST_ONLY = 2
CANCEL_ALL_IMMEDIATE = 0
CANCEL_ALL_SCHEDULED = 1
CANCEL_ALL_ABORT = 2
NIL_TRIGGER_PRICE = 0
DEFAULT_28_DAY_ORDER_EXPIRY = -1
DEFAULT_IOC_EXPIRY = 0
CROSS_MARGIN_MODE = 0
ISOLATED_MARGIN_MODE = 1
NIL_MARKET_INDEX = 255
MAX_AUTH_TTL_S = 8 * 3600 - 120  # server maximum is 8 h (the signer itself does not enforce it)

TX_CREATE_ORDER = 14
TX_CANCEL_ORDER = 15
TX_CANCEL_ALL = 16
TX_MODIFY_ORDER = 17
TX_UPDATE_LEVERAGE = 20


class SignError(ExchangeError):
    """The signer refused the parameters (nothing was sent; nonce untouched)."""


def import_lighter():
    """Import the SDK lazily (public mode never needs the native signer). Silences its websockets warning."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        import lighter  # noqa: WPS433
        from lighter import nonce_manager  # noqa: F401
    return lighter


@dataclass(frozen=True)
class SignedTx:
    tx_type: int
    tx_info: str  # JSON string exactly as signed
    tx_hash: str
    nonce: int
    label: str = ""

    @property
    def info(self) -> dict:
        return json.loads(self.tx_info)


def _check_int(name: str, v: Any, lo: int, hi: int) -> int:
    if isinstance(v, bool) or not isinstance(v, int):
        raise SignError(f"{name} must be int, got {type(v).__name__}")
    if not lo <= v <= hi:
        raise SignError(f"{name}={v} out of range [{lo}, {hi}]")
    return v


class LighterSigner:
    """Thin, explicit-nonce wrapper around ``lighter.SignerClient`` for ONE (account, api key)."""

    def __init__(self, url: str, account_index: int, api_key_index: int, api_private_key: str, chain_id: int,
                 *, client: Any = None) -> None:
        asyncio.get_running_loop()  # fail fast: SignerClient creates aiohttp objects in __init__
        self.account_index = int(account_index)
        self.api_key_index = _check_int("api_key_index", int(api_key_index), 0, 254)
        self.chain_id = int(chain_id)
        if client is None:
            lighter = import_lighter()
            client = lighter.SignerClient(
                url=url,
                account_index=self.account_index,
                api_private_keys={self.api_key_index: api_private_key},
                nonce_management_type=lighter.nonce_manager.NonceManagerType.NONE,
                chain_id=self.chain_id,
            )
        self._c = client

    # ------------------------------------------------------------------ helpers
    def _wrap(self, res: tuple, nonce: int, label: str) -> SignedTx:
        tx_type, tx_info, tx_hash, err = res
        if err is not None or tx_info is None:
            raise SignError(f"lighter signer: {err}")
        return SignedTx(tx_type=int(tx_type), tx_info=tx_info, tx_hash=str(tx_hash), nonce=nonce, label=label)

    def _nonce(self, nonce: int) -> int:
        return _check_int("nonce", nonce, 0, 2**48 - 1)

    # ------------------------------------------------------------------ sign_*
    def create_order(self, *, market_index: int, client_order_index: int, base_amount: int, price: int, is_ask: bool,
                     order_type: int, time_in_force: int, reduce_only: bool, order_expiry: int, nonce: int) -> SignedTx:
        _check_int("market_index", market_index, 0, 65535)
        if market_index == NIL_MARKET_INDEX:
            raise SignError("market_index 255 is not a market")
        _check_int("client_order_index", client_order_index, 1, MAX_CLIENT_ORDER_INDEX)
        _check_int("base_amount", base_amount, 1, MAX_BASE_AMOUNT)
        _check_int("price", price, 1, MAX_PRICE_INT)
        if order_type == ORDER_TYPE_MARKET and (time_in_force != TIF_IOC or order_expiry != DEFAULT_IOC_EXPIRY):
            raise SignError("market orders must be IOC with order_expiry 0")
        if time_in_force == TIF_IOC and order_expiry != DEFAULT_IOC_EXPIRY:
            raise SignError("IOC orders need order_expiry 0")
        nonce = self._nonce(nonce)
        res = self._c.sign_create_order(
            market_index,
            client_order_index,
            base_amount,
            price,
            bool(is_ask),
            order_type,
            time_in_force,
            bool(reduce_only),
            NIL_TRIGGER_PRICE,
            order_expiry,
            nonce=nonce,
            api_key_index=self.api_key_index,
        )
        return self._wrap(res, nonce, f"create coi={client_order_index}")

    def cancel_order(self, *, market_index: int, order_index: int, nonce: int) -> SignedTx:
        _check_int("order_index", order_index, 1, 2**63 - 1)
        nonce = self._nonce(nonce)
        res = self._c.sign_cancel_order(market_index, order_index, nonce=nonce, api_key_index=self.api_key_index)
        return self._wrap(res, nonce, f"cancel {order_index}")

    def cancel_all(self, *, market_index: int = NIL_MARKET_INDEX, time_in_force: int = CANCEL_ALL_IMMEDIATE,
                   timestamp_ms: int = 0, nonce: int) -> SignedTx:
        nonce = self._nonce(nonce)
        res = self._c.sign_cancel_all_orders(time_in_force, timestamp_ms, market_index, nonce=nonce,
                                             api_key_index=self.api_key_index)
        return self._wrap(res, nonce, f"cancel_all m={market_index}")

    def modify_order(self, *, market_index: int, order_index: int, base_amount: int, price: int, nonce: int) -> SignedTx:
        _check_int("order_index", order_index, 1, 2**63 - 1)
        _check_int("base_amount", base_amount, 1, MAX_BASE_AMOUNT)
        _check_int("price", price, 1, MAX_PRICE_INT)
        nonce = self._nonce(nonce)
        res = self._c.sign_modify_order(market_index, order_index, base_amount, price, NIL_TRIGGER_PRICE,
                                        nonce=nonce, api_key_index=self.api_key_index)
        return self._wrap(res, nonce, f"modify {order_index}")

    def update_leverage(self, *, market_index: int, fraction: int, margin_mode: int = CROSS_MARGIN_MODE,
                        nonce: int) -> SignedTx:
        _check_int("fraction", fraction, 1, 10_000)
        nonce = self._nonce(nonce)
        res = self._c.sign_update_leverage(market_index, fraction, margin_mode, nonce=nonce,
                                           api_key_index=self.api_key_index)
        return self._wrap(res, nonce, f"leverage m={market_index} imf={fraction}")

    # ------------------------------------------------------------------ auth / misc
    def auth_token(self, ttl_s: float) -> tuple[str, float]:
        """Auth token valid for ``ttl_s`` (capped below the 8 h server maximum). Pure, no network."""
        ttl = int(max(60, min(float(ttl_s), MAX_AUTH_TTL_S)))
        now = int(time.time())
        tok, err = self._c.create_auth_token_with_expiry(ttl, timestamp=now, api_key_index=self.api_key_index)
        if err or not tok:
            raise SignError(f"lighter auth token: {err}")
        return tok, float(now + ttl)

    def check_client(self) -> str | None:
        """BLOCKING (Go HTTP): verify the key is registered. Call via ``asyncio.to_thread``."""
        return self._c.check_client()

    async def close(self) -> None:
        try:
            await self._c.close()
        except Exception:  # pragma: no cover - best effort
            pass


# --------------------------------------------------------------------------- executor
class TxTransport(Protocol):
    """I/O used by the executor. Implementations enforce the shared REST/tx rate budget."""

    def ws_ready(self) -> bool: ...

    async def send_ws(self, tx: SignedTx, timeout: float) -> dict: ...

    async def send_rest(self, tx: SignedTx, timeout: float) -> dict: ...

    async def lookup_tx(self, tx_hash: str) -> dict | None: ...

    async def fetch_next_nonce(self) -> int: ...


@dataclass
class TxReceipt:
    tx_hash: str
    nonce: int
    tx_type: int
    via: str  # ws | rest | lookup
    resent: bool = False
    ack: dict = field(default_factory=dict)


class TxExecutor:
    """Single-writer nonce stream for one API key + safe submission (spec §11.1)."""

    def __init__(self, transport: TxTransport, *, ack_timeout_s: float = 1.5, rest_timeout_s: float = 3.0,
                 lookup_tries: int = 4, lookup_interval_s: float = 0.5, name: str = "farmer.lighter.tx") -> None:
        self.t = transport
        self.ack_timeout_s = ack_timeout_s
        self.rest_timeout_s = rest_timeout_s
        self.lookup_tries = lookup_tries
        self.lookup_interval_s = lookup_interval_s
        self.log = logging.getLogger(name)
        self.next_nonce: int | None = None
        self._resync = False
        self._lock = asyncio.Lock()
        self.stats = {"sent": 0, "accepted": 0, "rejected": 0, "unknown": 0, "resent": 0, "nonce_resync": 0}

    @property
    def busy(self) -> bool:
        return self._lock.locked()

    def invalidate_nonce(self) -> None:
        self.next_nonce = None

    def request_resync(self) -> None:
        """Re-read ``nextNonce`` before the next tx (e.g. one of our txs failed in the sequencer: status 0 does
        not consume its nonce). A flag, not ``next_nonce = None``: a concurrent ``_accepted`` would overwrite it."""
        self._resync = True

    async def sync_nonce(self) -> int:
        n = int(await self.t.fetch_next_nonce())
        self.stats["nonce_resync"] += 1
        self.next_nonce = n
        return n

    async def submit(self, build: Callable[[int], SignedTx]) -> TxReceipt:
        """Sign with the next nonce and send. Raises TxRejected (nonce not consumed), TxUnknown or SignError."""
        async with self._lock:
            if self.next_nonce is None or self._resync:
                self._resync = False
                try:
                    await self.sync_nonce()
                except Exception as exc:
                    self.next_nonce = None
                    raise TxRejected(-1, f"nextNonce unavailable: {exc}", retryable=True) from exc
            resynced = False
            sent_any = False
            try:
                while True:
                    nonce = int(self.next_nonce)  # type: ignore[arg-type]
                    tx = build(nonce)
                    try:
                        ack, maybe_dup, via = await self._transmit(tx)
                    except TxNotSent as exc:
                        raise TxRejected(-1, f"not sent: {exc}", retryable=True) from exc
                    sent_any = True
                    self.stats["sent"] += 1
                    if ack is not None:
                        ok, code, msg = parse_ack(ack)
                        if ok:
                            return self._accepted(tx, via, ack)
                        if is_ambiguous_tx_code(code):
                            # internal error / process timeout: does NOT prove the tx was dropped (spec §12)
                            self.log.warning("lighter sendTx %s answered %s (%s): outcome unknown", tx.label, code, msg)
                        elif maybe_dup and code not in DETERMINISTIC_TX_CODES:
                            pass  # an earlier copy may have landed: resolve below, never re-sign
                        elif code == ERR_INVALID_NONCE and not resynced:
                            resynced = True
                            self.log.warning("lighter invalid nonce %s (%s); resyncing nextNonce", nonce, tx.label)
                            try:
                                await self.sync_nonce()
                            except Exception as exc:
                                self.next_nonce = None
                                raise TxRejected(code, f"{msg}; nextNonce resync failed: {exc}", retryable=True) from exc
                            continue
                        else:
                            self.stats["rejected"] += 1
                            if code == ERR_INVALID_NONCE:
                                # still invalid after a resync: nextNonce lags while an earlier tx sits in the
                                # speed bump (spec §7.4) -> transient; resync again on the next attempt
                                self.next_nonce = None
                                raise TxRejected(code, msg, retryable=True)
                            raise TxRejected(code, msg)
                    return await self._resolve_unknown(tx)
            except (TxRejected, TxUnknown):
                raise
            except Exception as exc:
                if sent_any:
                    self.next_nonce = None
                    self.stats["unknown"] += 1
                    raise TxUnknown("?", f"unexpected error after send: {exc!r}") from exc
                raise

    def _accepted(self, tx: SignedTx, via: str, ack: dict | None = None, *, resent: bool = False) -> TxReceipt:
        self.next_nonce = tx.nonce + 1
        self.stats["accepted"] += 1
        h = (ack or {}).get("tx_hash")
        if isinstance(h, str) and h and h != tx.tx_hash:
            self.log.warning("lighter ack hash %s differs from signed hash %s", h, tx.tx_hash)
        return TxReceipt(tx_hash=tx.tx_hash, nonce=tx.nonce, tx_type=tx.tx_type, via=via, resent=resent, ack=ack or {})

    async def _transmit(self, tx: SignedTx) -> tuple[dict | None, bool, str]:
        """(ack or None if the outcome is unknown, maybe_duplicate, channel). TxNotSent if nothing left."""
        maybe_dup = False
        if self.t.ws_ready():
            try:
                return await self.t.send_ws(tx, self.ack_timeout_s), False, "ws"
            except TxNotSent:
                pass
            except (asyncio.TimeoutError, TimeoutError):
                return None, False, "ws"  # sent, no ack
            except Exception as exc:  # socket died after the frame may have left
                self.log.warning("lighter ws send failed (%s): %s; falling back to REST", tx.label, exc)
                maybe_dup = True
        try:
            return await self.t.send_rest(tx, self.rest_timeout_s), maybe_dup, "rest"
        except TxNotSent:
            if maybe_dup:
                return None, True, "rest"
            raise
        except Exception as exc:  # timeout / network: may have been accepted
            self.log.warning("lighter REST sendTx failed (%s): %r", tx.label, exc)
            return None, True, "rest"

    async def _lookup(self, tx_hash: str, tries: int | None = None) -> dict | None:
        for _ in range(self.lookup_tries if tries is None else tries):
            await asyncio.sleep(self.lookup_interval_s)
            try:
                res = await self.t.lookup_tx(tx_hash)
            except TxNotSent:
                return None
            except Exception as exc:
                self.log.debug("lighter tx lookup failed: %r", exc)
                continue
            if res is not None:
                return res
        return None

    async def _resolve_unknown(self, tx: SignedTx) -> TxReceipt:
        """Docs 'Retrying safely': look up by hash, compare nextNonce, resend the identical payload if safe."""
        self.log.warning("lighter tx %s (%s) outcome unknown; resolving", tx.tx_hash[:16], tx.label)
        found = await self._lookup(tx.tx_hash)
        if found is not None:
            return await self._from_lookup(tx, found)
        try:
            srv = int(await self.t.fetch_next_nonce())
        except Exception as exc:
            self.next_nonce = None
            self.stats["unknown"] += 1
            raise TxUnknown(tx.tx_hash, f"not found and nextNonce unavailable ({exc!r})") from exc
        if srv != tx.nonce:
            # consumed (maybe by this very tx, not yet indexed) or lagging: give the index one more chance
            found = await self._lookup(tx.tx_hash, tries=2)
            if found is not None:
                return await self._from_lookup(tx, found)
            self.next_nonce = srv if srv > tx.nonce else None
            self.stats["unknown"] += 1
            raise TxUnknown(tx.tx_hash, f"not found; nextNonce={srv} vs ours={tx.nonce}")
        # nonce still free and tx unseen: re-send the IDENTICAL signed tx (same nonce -> at most one executes)
        self.stats["resent"] += 1
        self.log.warning("lighter tx %s not seen and nonce %s still free: re-sending identical payload", tx.tx_hash[:16], tx.nonce)
        try:
            ack, _, via = await self._transmit(tx)
        except TxNotSent as exc:
            self.next_nonce = None
            self.stats["unknown"] += 1
            raise TxUnknown(tx.tx_hash, f"resend not possible: {exc}") from exc
        if ack is None:
            self.next_nonce = None
            self.stats["unknown"] += 1
            raise TxUnknown(tx.tx_hash, "no ack for the identical resend")
        ok, code, msg = parse_ack(ack)
        if ok:
            return self._accepted(tx, via, ack, resent=True)
        if code == ERR_INVALID_NONCE:  # the first copy may have landed after all
            found = await self._lookup(tx.tx_hash, tries=2)
            if found is not None:
                return await self._from_lookup(tx, found)
            self.next_nonce = None
            self.stats["unknown"] += 1
            raise TxUnknown(tx.tx_hash, "invalid nonce on identical resend")
        if is_ambiguous_tx_code(code) or is_retryable(code):
            # a transient / ambiguous answer to the resend says nothing about the first copy (still unresolved)
            self.next_nonce = None
            self.stats["unknown"] += 1
            raise TxUnknown(tx.tx_hash, f"identical resend answered {code} ({msg})")
        self.stats["rejected"] += 1
        raise TxRejected(code, msg)

    async def _from_lookup(self, tx: SignedTx, found: dict) -> TxReceipt:
        status = found.get("status")
        if status == 0:
            self.stats["rejected"] += 1
            try:
                await self.sync_nonce()
            except Exception:
                self.next_nonce = None
            raise TxRejected(0, f"tx {tx.tx_hash} failed in the sequencer", retryable=False)
        self.next_nonce = tx.nonce + 1
        self.stats["accepted"] += 1
        return TxReceipt(tx_hash=tx.tx_hash, nonce=tx.nonce, tx_type=tx.tx_type, via="lookup", ack=found)
