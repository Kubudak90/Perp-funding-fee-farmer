"""Async REST transport for Arcus (aiohttp).

- Public/account reads are plain GETs (account reads only need ``?address=``; mainnet gates them
  per queried address with ``403 address not on access whitelist``).
- Writes are POSTs whose body is the exact canonical JSON that was signed.
- A local token bucket keeps us under the per-IP weight budget (1,500 weight, refill 25/s);
  order writes cost 0 IP weight.
- JSON is parsed with ``parse_float=Decimal`` so no binary float ever reaches order math.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from decimal import Decimal
from typing import Any

import aiohttp

from farmer.exchanges.base import ExchangeError
from farmer.utils.ratelimit import TokenBucket

log = logging.getLogger("farmer.arcus.rest")

WHITELIST_HINT = (
    "Arcus mainnet is access-gated: address {address} is not on the access whitelist. Get the address "
    "whitelisted or redeem an invite code (web app / POST /v1/affiliate/redeemInvite) before trading or "
    "reading account data."
)

# IP weights (spec arcus-trading §9 / arcus-data §10)
W_TIME = 1
W_LIGHT = 2  # bbo, mids, account, positions, order, feetiers, leverages, account/stats, rateLimit
W_HEAVY = 20  # markets, prices, openOrders, orders, fills, funding, fundingRates, apiKeys, createApiKey
W_SET_LEVERAGE = 125


def loads(text: str) -> Any:
    return json.loads(text, parse_float=Decimal) if text else {}


class ArcusHTTPError(ExchangeError):
    """Non-2xx Arcus HTTP response."""

    def __init__(self, status: int, message: str, *, path: str = "", body: Any = None,
                 retry_after_ms: int | None = None, error_type: str | None = None) -> None:
        retryable = status == 429 or status >= 500
        super().__init__(f"arcus HTTP {status} {path}: {message}", retryable=retryable,
                         code=error_type or status)
        self.status = status
        self.error = message
        self.path = path
        self.body = body
        self.retry_after_ms = retry_after_ms
        self.error_type = error_type

    @property
    def is_whitelist(self) -> bool:
        return self.status == 403 and (
            "whitelist" in (self.error or "").lower() or self.error_type == "AddressNotOnWhitelist")

    @property
    def is_timestamp_expired(self) -> bool:
        return self.status == 401 and "timestamp expired" in (self.error or "").lower()

    @property
    def is_transmission(self) -> bool:
        """``errorType: Transmission``: the request never reached the engine; safe to resend unchanged.

        Only the gateway's explicit marker counts: a bare 502/52x (e.g. an HTML page from the CDN in front
        of the API) may come back AFTER the request was forwarded, so its outcome is unknown.
        """
        return self.error_type == "Transmission"

    @property
    def is_definitive_5xx(self) -> bool:
        """A 5xx the Arcus gateway itself answered with a JSON ``errorType`` saying nothing was submitted
        (``Transmission``, or ``Unavailable`` = path temporarily disabled)."""
        return self.status >= 500 and self.error_type in ("Transmission", "Unavailable")

    @property
    def rejection_reason(self) -> str | None:
        if isinstance(self.body, dict):
            return self.body.get("rejectionReason") or self.body.get("reason")
        return None


class ArcusAccessError(ArcusHTTPError):
    """403 AddressNotOnWhitelist (mainnet access gate)."""


def _error_from(status: int, path: str, text: str, headers: Any) -> ArcusHTTPError:
    try:
        body = loads(text)
    except ValueError:
        body = text
    msg, etype, retry_ms = text[:300], None, None
    if isinstance(body, dict):
        msg = str(body.get("error") or body.get("message") or msg)
        etype = body.get("errorType") or body.get("code")
        if body.get("retryAfterMs") is not None:
            try:
                retry_ms = int(body["retryAfterMs"])
            except (TypeError, ValueError):
                retry_ms = None
    if retry_ms is None and status == 429:
        ra = headers.get("Retry-After") if headers is not None else None
        try:
            retry_ms = int(float(ra) * 1000) if ra is not None else None
        except (TypeError, ValueError):
            retry_ms = None
    cls = ArcusHTTPError
    err = cls(status, msg, path=path, body=body, retry_after_ms=retry_ms, error_type=etype)
    if err.is_whitelist:
        err = ArcusAccessError(status, msg, path=path, body=body, retry_after_ms=retry_ms,
                               error_type=etype or "AddressNotOnWhitelist")
    return err


class ArcusRest:
    """Thin async REST client. Create with ``start()`` (or pass a session) and ``close()``."""

    def __init__(self, base_url: str, *, timeout_s: float = 10.0, session: aiohttp.ClientSession | None = None,
                 ip_weight_per_s: float = 20.0, ip_weight_burst: float = 1000.0, get_retries: int = 2) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self._session = session
        self._own_session = session is None
        self._bucket = TokenBucket(rate=ip_weight_per_s, capacity=ip_weight_burst)
        self.get_retries = get_retries

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout_s),
                trust_env=True,  # honour HTTPS_PROXY / SSL_CERT_FILE like any CLI tool
                json_serialize=json.dumps,
            )
            self._own_session = True

    async def close(self) -> None:
        if self._session is not None and self._own_session and not self._session.closed:
            await self._session.close()
        self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            raise ExchangeError("arcus REST session not started", retryable=True)
        return self._session

    # ------------------------------------------------------------------ core
    async def request(self, method: str, path: str, *, params: dict | None = None, data: str | None = None,
                      headers: dict | None = None, weight: float = 1.0) -> tuple[int, Any]:
        """One HTTP request. Returns ``(status, json)`` for 2xx, raises ``ArcusHTTPError`` otherwise."""
        if weight > 0:
            await self._bucket.acquire(weight)
        hdrs = {"Accept": "application/json"}
        if data is not None:
            hdrs["Content-Type"] = "application/json"
        if headers:
            hdrs.update(headers)
        clean = {k: (str(v).lower() if isinstance(v, bool) else str(v)) for k, v in (params or {}).items()
                 if v is not None}
        async with self.session.request(method, self.base_url + path, params=clean or None, data=data,
                                        headers=hdrs) as resp:
            text = await resp.text()
            if 200 <= resp.status < 300:
                try:
                    return resp.status, loads(text)
                except ValueError as exc:
                    raise ArcusHTTPError(resp.status, f"invalid JSON: {text[:200]}", path=path) from exc
            raise _error_from(resp.status, path, text, resp.headers)

    async def get(self, path: str, params: dict | None = None, *, weight: float = W_LIGHT) -> Any:
        """GET with bounded retries on transient failures (network, 5xx, 429)."""
        attempt = 0
        while True:
            try:
                _, data = await self.request("GET", path, params=params, weight=weight)
                return data
            except ArcusHTTPError as exc:
                if not exc.retryable or attempt >= self.get_retries:
                    raise
                delay = (exc.retry_after_ms / 1000.0) if exc.retry_after_ms else 0.5 * 2 ** attempt
            except (TimeoutError, aiohttp.ClientError) as exc:
                if attempt >= self.get_retries:
                    raise ExchangeError(f"arcus GET {path} failed: {exc!r}", retryable=True) from exc
                delay = 0.5 * 2 ** attempt
            attempt += 1
            await asyncio.sleep(min(delay, 10.0) + random.random() * 0.1)

    async def post(self, path: str, body_text: str, headers: dict, *, params: dict | None = None,
                   weight: float = 0.0) -> tuple[int, Any]:
        """Single signed POST (never retried here: a write may have been accepted)."""
        return await self.request("POST", path, params=params, data=body_text, headers=headers, weight=weight)

    # ------------------------------------------------------------------ public reads
    async def server_time_ns(self) -> int:
        data = await self.get("/v1/time", weight=W_TIME)
        return int(data["timeNs"])

    async def markets(self) -> list[dict]:
        data = await self.get("/v1/markets", weight=W_HEAVY)
        return list(data.get("markets") or [])

    async def l2_orderbook(self, ticker: str, n_levels: int = 20) -> dict:
        n = max(1, min(int(n_levels), 100))
        return await self.get(f"/v1/l2OrderBook/{ticker}", {"nLevels": n}, weight=2 + n // 20)

    async def bbo(self, ticker: str) -> dict:
        return await self.get(f"/v1/bbo/{ticker}", weight=W_LIGHT)

    async def fee_tiers(self) -> dict:
        return await self.get("/v1/feetiers", weight=W_LIGHT)

    # ------------------------------------------------------------------ account reads (no auth)
    async def account(self, address: str, account_index: int) -> dict:
        return await self.get("/v1/account", {"address": address, "accountIndex": account_index}, weight=W_LIGHT)

    async def positions(self, address: str, account_index: int, market: str | None = None) -> dict:
        return await self.get("/v1/positions", {"address": address, "accountIndex": account_index, "market": market},
                              weight=W_LIGHT)

    async def open_orders(self, address: str, account_index: int, market: str | None = None,
                          limit: int = 1000) -> list[dict]:
        data = await self.get("/v1/openOrders", {"address": address, "accountIndex": account_index,
                                                 "market": market, "limit": limit}, weight=W_HEAVY + limit // 50)
        return list(data.get("orders") or [])

    async def order(self, address: str, account_index: int, order_id: str) -> dict:
        return await self.get(f"/v1/order/{order_id}", {"address": address, "accountIndex": account_index},
                              weight=W_LIGHT)

    async def fills(self, address: str, account_index: int, *, market: str | None = None,
                    from_us: int | None = None, to_us: int | None = None, limit: int = 1000) -> list[dict]:
        data = await self.get("/v1/fills", {"address": address, "accountIndex": account_index, "market": market,
                                            "from": from_us, "to": to_us, "limit": limit},
                              weight=W_HEAVY + limit // 20)
        return list(data.get("fills") or [])

    async def funding_payments(self, address: str, account_index: int, *, market: str | None = None,
                               from_us: int | None = None, to_us: int | None = None,
                               limit: int = 1000) -> list[dict]:
        data = await self.get("/v1/funding", {"address": address, "accountIndex": account_index, "market": market,
                                              "from": from_us, "to": to_us, "limit": limit},
                              weight=W_HEAVY + limit // 20)
        return list(data.get("fundingPayments") or [])

    async def rate_limit(self, address: str, account_index: int) -> dict:
        return await self.get("/v1/rateLimit", {"address": address, "accountIndex": account_index}, weight=W_LIGHT)

    async def account_stats(self, address: str, include: str = "feeTier") -> dict:
        return await self.get("/v1/account/stats", {"address": address, "include": include}, weight=W_LIGHT)

    async def leverages(self, address: str, account_index: int, market: str | None = None) -> dict:
        return await self.get("/v1/leverages", {"address": address, "accountIndex": account_index, "market": market},
                              weight=W_LIGHT)

    async def api_keys(self, address: str, account_index: int | None = None) -> list[dict]:
        data = await self.get("/v1/apiKeys", {"address": address, "accountIndex": account_index}, weight=W_HEAVY)
        return list(data.get("apiKeys") or [])


__all__ = [
    "ArcusAccessError",
    "ArcusHTTPError",
    "ArcusRest",
    "WHITELIST_HINT",
    "W_HEAVY",
    "W_LIGHT",
    "W_SET_LEVERAGE",
    "loads",
]
