"""LighterExchange: Lighter mainnet adapter (taker/hedge leg; Standard account = 0 fees).

Data flow
- ONE websocket (``?readonly=true`` without credentials) carries ``market_stats/all`` (funding + mark/index,
  first frame = all markets, then only changed ones -> merged cache), ``order_book/{id}`` (snapshot + deltas with
  nonce continuity, see book.py), and with credentials ``account_all/{acct}`` (positions, trades, funding
  payments; public), ``user_stats/{acct}`` (balances; public), ``account_all_orders/{acct}`` and
  ``account_tx/{acct}`` (auth token). Transactions go over the same socket (``jsonapi/sendtx``) with a REST
  ``sendTx`` fallback.
- REST is the scarce resource: Standard accounts have ONE bucket of 60 requests per rolling minute per IP and L1
  address shared by every REST call AND every sendTx (REST or WS). A single ``SlidingWindowLimiter`` guards all of
  them; REST *reads* only run while more than ``cfg.read_reserve`` slots remain (otherwise WS caches serve them
  or a retryable ``ExchangeError`` is raised); transactions always have priority.

Order model
- ``place_market`` returns ``OrderResult(ok=True, order=PENDING)`` once the API accepted the tx. Fills arrive on
  ``account_all`` trades (deduped by trade id; matched by account + client/order index so multi-maker sweeps whose
  later fills sit in type-21 child txs are all reported) and order updates on ``account_all_orders``. A terminal
  order update is held back (up to ``fill_wait_s``) until the fills that make it up were emitted.
- If the outcome of a sent tx cannot be established (``TxUnknown``) the result is ``ok=True`` with
  ``order.status == UNKNOWN`` and ``order.error`` set: treat it as IN FLIGHT and reconcile from positions; never
  blindly re-send (that could double the hedge).
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

import aiohttp

from farmer.config import LighterConfig
from farmer.exchanges.base import AuthRequired, Exchange, ExchangeError
from farmer.exchanges.lighter.book import LocalBook
from farmer.exchanges.lighter.helpers import (
    ACTIVE_ORDER_STATUSES,
    ERR_INVALID_AUTH,
    ERR_RESTRICTED,
    ERR_STANDARD_LIMIT,
    ERR_TOO_MANY_REQUESTS,
    ERR_TX_NOT_FOUND,
    FATAL_CODES,
    RESTRICTED_MESSAGE,
    STANDARD_LIMIT_MESSAGE,
    VENUE,
    TxNotSent,
    TxRejected,
    TxUnknown,
    check_market_invariants,
    dec,
    funding_from_market_stats,
    hourly_from_funding_rates,
    is_retryable,
    leverage_to_fraction,
    map_order_status,
    next_funding_ts,
    order_from_json,
    parse_event_info,
    parse_funding_payment,
    parse_markets,
    parse_position,
    position_imf,
    price_to_int,
    readonly_url,
    size_to_int,
    trade_id,
    trade_side_for,
    trade_to_fill,
    worst_price_to_int,
)
from farmer.exchanges.lighter.signer import (
    CANCEL_ALL_IMMEDIATE,
    CROSS_MARGIN_MODE,
    DEFAULT_28_DAY_ORDER_EXPIRY,
    DEFAULT_IOC_EXPIRY,
    NIL_MARKET_INDEX,
    ORDER_TYPE_LIMIT,
    ORDER_TYPE_MARKET,
    TIF_GTT,
    TIF_IOC,
    TIF_POST_ONLY,
    LighterSigner,
    SignedTx,
    SignError,
    TxExecutor,
    TxReceipt,
)
from farmer.exchanges.lighter.ws import LighterWS
from farmer.models import (
    ZERO,
    Balance,
    Fill,
    FundingInfo,
    FundingPayment,
    MarketInfo,
    Order,
    OrderBook,
    OrderResult,
    OrderStatus,
    Position,
    Side,
)
from farmer.utils.ids import IntIdGenerator
from farmer.utils.ratelimit import SlidingWindowLimiter

BOOK_DEPTH = 20
_UNKNOWN_AFTER_S = 30.0  # a PENDING order nobody can confirm becomes UNKNOWN after this long


@dataclass
class _OrderMeta:
    coi: int
    market_id: int
    sent_at: float = field(default_factory=time.monotonic)
    tx_hash: str | None = None
    fills_base: Decimal = ZERO
    fills_quote: Decimal = ZERO
    venue_filled: Decimal = ZERO
    venue_quote: Decimal = ZERO
    venue_seen: bool = False
    pending_terminal: OrderStatus | None = None
    pending_since: float = 0.0
    last_check: float = 0.0
    checks: int = 0
    terminal_at: float | None = None
    event: asyncio.Event = field(default_factory=asyncio.Event)


class _Transport:
    """TxTransport for the executor: WS ``sendtx`` + REST fallback, all under the shared budget."""

    def __init__(self, ex: "LighterExchange") -> None:
        self.ex = ex

    def ws_ready(self) -> bool:
        ws = self.ex._ws
        return ws is not None and ws.can_send_tx

    async def _budget(self, *, tx: bool) -> None:
        ex = self.ex
        if time.monotonic() < ex._cooldown_until:
            raise TxNotSent("lighter REST/tx budget cooling down after HTTP 429")
        wait = ex._tx_budget_wait_s
        try:
            if tx:
                await asyncio.wait_for(ex._tx_limiter.acquire(), wait)
            await asyncio.wait_for(ex._limiter.acquire(), wait)
        except asyncio.TimeoutError as exc:
            raise TxNotSent("lighter rate budget exhausted") from exc

    async def send_ws(self, tx: SignedTx, timeout: float) -> dict:
        ws = self.ex._ws
        if ws is None or not ws.can_send_tx:
            raise TxNotSent("websocket not ready")
        await self._budget(tx=True)
        return await ws.send_tx(tx.tx_type, json.loads(tx.tx_info), tx.tx_hash, timeout)

    async def send_rest(self, tx: SignedTx, timeout: float) -> dict:
        ex = self.ex
        if ex._http is None:
            raise TxNotSent("http session closed")
        await self._budget(tx=True)
        url = ex._url("/api/v1/sendTx")
        async with ex._http.post(url, data={"tx_type": str(tx.tx_type), "tx_info": tx.tx_info},
                                 timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            text = await r.text()
            status = r.status
        if status in (429, 405):
            ex._enter_cooldown(status)
            return {"error": {"code": ERR_TOO_MANY_REQUESTS, "message": f"HTTP {status} rate limited"}}
        try:
            j = json.loads(text)
        except ValueError:
            if status == 403:  # CDN/WAF page: request blocked before reaching the API
                return {"error": {"code": -403, "message": text[:200]}}
            raise ConnectionError(f"sendTx HTTP {status} non-JSON body: {text[:120]}")
        if status >= 500:
            raise ConnectionError(f"sendTx HTTP {status}: {text[:200]}")  # may have been accepted -> resolve
        return j if isinstance(j, dict) else {"error": {"code": -status, "message": text[:200]}}

    async def lookup_tx(self, tx_hash: str) -> dict | None:
        ex = self.ex
        if ex._http is None:
            raise TxNotSent("http session closed")
        await self._budget(tx=False)
        async with ex._http.get(ex._url("/api/v1/tx"), params={"by": "hash", "value": tx_hash}) as r:
            text = await r.text()
            status = r.status
        if status in (429, 405):
            ex._enter_cooldown(status)
            raise TxNotSent("rate limited")
        try:
            j = json.loads(text)
        except ValueError as exc:
            raise ConnectionError(f"tx lookup HTTP {status} non-JSON") from exc
        if status == 200 and isinstance(j, dict) and j.get("code") == 200:
            return j
        if isinstance(j, dict) and j.get("code") == ERR_TX_NOT_FOUND:
            return None
        raise ConnectionError(f"tx lookup HTTP {status}: {text[:200]}")

    async def fetch_next_nonce(self) -> int:
        ex = self.ex
        if ex._http is None:
            raise TxNotSent("http session closed")
        await self._budget(tx=False)
        params = {"account_index": ex.cfg.account_index, "api_key_index": ex.cfg.api_key_index}
        async with ex._http.get(ex._url("/api/v1/nextNonce"), params=params) as r:
            text = await r.text()
            status = r.status
        if status in (429, 405):
            ex._enter_cooldown(status)
            raise TxNotSent("rate limited")
        j = json.loads(text)
        if status != 200 or j.get("code") != 200:
            code = j.get("code") if isinstance(j, dict) else None
            if code in FATAL_CODES:
                ex._on_tx_code(int(code), str(j.get("message")))
            raise ExchangeError(f"lighter nextNonce failed: HTTP {status} {text[:200]}", retryable=True, code=code)
        return int(j["nonce"])


class LighterExchange(Exchange):
    """Lighter adapter. ``LighterExchange(cfg)``; call ``await connect()`` inside the running loop."""

    name = "lighter"

    def __init__(self, cfg: LighterConfig, *, signer_factory: Callable[[], LighterSigner] | None = None) -> None:
        super().__init__()
        self.cfg = cfg
        self._signer_factory = signer_factory
        g = lambda k, d: getattr(cfg, k, d)  # noqa: E731 - optional config fields (see module docs)
        self._ws_watchdog_s = float(g("ws_watchdog_s", 20.0))
        self._tx_ack_timeout_s = float(g("tx_ack_timeout_s", 1.5))
        self._tx_rest_timeout_s = float(g("tx_rest_timeout_s", 3.0))
        self._tx_budget_wait_s = float(g("tx_budget_wait_s", 10.0))
        self._order_ack_timeout_s = float(g("order_ack_timeout_s", 2.0))
        self._order_confirm_timeout_s = float(g("order_confirm_timeout_s", 6.0))
        self._fill_wait_s = float(g("fill_wait_s", 2.0))
        self._markets_refresh_s = float(g("markets_refresh_s", 600.0))
        self._std_limit_cooldown_s = float(g("standard_limit_cooldown_s", 3600.0))
        self._book_quiet_grace_s = float(g("book_quiet_grace_s", 10.0))
        self._verify_api_key = bool(g("verify_api_key", True))
        # ONE bucket for every REST call and every tx (Standard: 60/min per IP and L1 address)
        self._limiter = SlidingWindowLimiter(max(1, int(cfg.max_requests_per_minute)), 60.0)
        # per tx-type cap ("Default 40 requests/minute"); keeps order/cancel bursts inside it
        self._tx_limiter = SlidingWindowLimiter(max(1, int(g("max_tx_per_minute", 40))), 60.0)
        self._cooldown_until = 0.0
        self._http: aiohttp.ClientSession | None = None
        self._connected = False
        self._closing = False
        self._tasks: list[asyncio.Task] = []
        # websocket (created eagerly so subscriptions can be registered before connect)
        creds = cfg.has_credentials
        self._ws = LighterWS(
            readonly_url(cfg.ws_url) if not creds else cfg.ws_url,
            on_message=self._on_ws_message,
            on_connect=self._on_ws_connect,
            on_disconnect=self._on_ws_disconnect,
            readonly_fallback_url=readonly_url(cfg.ws_url) if creds else None,
            ping_interval_s=float(cfg.ws_ping_interval_s),
            watchdog_s=self._ws_watchdog_s,
            max_msgs_per_min=int(g("ws_max_msgs_per_min", 180)),
        )
        self._ws.subscribe("market_stats/all")
        # markets / data caches
        self._by_id: dict[int, MarketInfo] = {}
        self._books: dict[int, LocalBook] = {}
        self._last_books: dict[int, OrderBook] = {}
        self._book_resub_at: dict[int, tuple[float, int]] = {}  # market -> (last resubscribe, streak)
        self._book_resub_pending: set[int] = set()
        self._ms: dict[int, dict] = {}
        self._ms_ts: dict[int, float] = {}
        self._fr_cache: tuple[float, list[dict]] | None = None
        # account caches (WS)
        self._positions_raw: dict[int, dict] = {}
        self._account_ready = False
        self._user_stats: dict | None = None
        self._stats_ready = False
        self._venue_orders: dict[int, dict] = {}  # order_index -> active Order JSON
        self._orders_ready = False
        self._rest_account_cache: tuple[float, dict] | None = None
        self._funding: dict[tuple[int, int], FundingPayment] = {}
        self._funding_rest_at = 0.0
        # our orders
        self._ids = IntIdGenerator(1, 2**48 - 1)
        self._orders: dict[int, Order] = {}  # client_order_index -> Order
        self._meta: dict[int, _OrderMeta] = {}
        self._cid_to_coi: dict[str, int] = {}
        self._oidx_to_coi: dict[int, int] = {}
        self._tx_to_coi: dict[str, int] = {}
        self._seen_trades: collections.OrderedDict[str, None] = collections.OrderedDict()
        self._leverage_sent: dict[int, int] = {}
        # trading
        self._signer: LighterSigner | None = None
        self._executor: TxExecutor | None = None
        self._transport = _Transport(self)
        self._auth: tuple[str, float, float] | None = None  # token, expiry, created
        self._auth_error_at = 0.0
        # venue-level flags
        self._restricted: str | None = None
        self._fatal: str | None = None
        self._std_limit_until = 0.0
        self._std_limit_reason = ""
        self.stats: dict[str, int] = collections.defaultdict(int)

    # ================================================================== lifecycle
    @property
    def has_credentials(self) -> bool:
        return self.cfg.has_credentials

    def _url(self, path: str) -> str:
        return self.cfg.url.rstrip("/") + path

    async def connect(self) -> None:
        if self._connected:
            return
        self._closing = False
        if self._http is None:
            self._http = aiohttp.ClientSession(trust_env=True, timeout=aiohttp.ClientTimeout(total=float(self.cfg.request_timeout_s)))
        try:
            await self.load_markets(_startup=True)
            if self.has_credentials:
                await self._init_trading()
        except BaseException:
            await self.close()
            raise
        if self.has_credentials:
            acct = int(self.cfg.account_index)  # type: ignore[arg-type]
            self._ws.subscribe(f"account_all/{acct}")
            self._ws.subscribe(f"user_stats/{acct}")
            self._ws.subscribe(f"account_all_orders/{acct}", auth=self._auth_token)
            self._ws.subscribe(f"account_tx/{acct}", auth=self._auth_token)
        await self._ws.start()
        await self._ws.wait_first_attempt(timeout=15.0)
        if self.has_credentials and self._ws.restricted:
            self._set_restricted()
            await self.close()
            raise ExchangeError(RESTRICTED_MESSAGE, retryable=False, code=ERR_RESTRICTED)
        if not self._ws.connected:
            self.log.warning("lighter ws not connected yet; continuing (reconnect runs in the background)")
        self._tasks = [
            asyncio.create_task(self._markets_loop(), name="lighter-markets"),
            asyncio.create_task(self._maintenance_loop(), name="lighter-maintenance"),
        ]
        self._connected = True
        self.log.info("lighter connected: %d markets, mode=%s", len(self.markets), "trading" if self.has_credentials else "public")

    async def _init_trading(self) -> None:
        cfg = self.cfg
        if self._signer is None:
            if self._signer_factory is not None:
                self._signer = self._signer_factory()
            else:
                self._signer = LighterSigner(cfg.url, int(cfg.account_index), int(cfg.api_key_index),  # type: ignore[arg-type]
                                             str(cfg.api_private_key), int(cfg.chain_id))
        if self._verify_api_key and self._signer_factory is None:
            await self._limiter.acquire()
            err = await asyncio.to_thread(self._signer.check_client)  # blocking Go HTTP -> thread
            if err:
                code = ERR_RESTRICTED if "20558" in str(err) else None
                if code == ERR_RESTRICTED:
                    self._set_restricted()
                    raise ExchangeError(RESTRICTED_MESSAGE, retryable=False, code=ERR_RESTRICTED)
                raise ExchangeError(f"lighter API key check failed: {err} (register the key; see keys.py)", retryable=False)
        if self._executor is None:
            self._executor = TxExecutor(self._transport, ack_timeout_s=self._tx_ack_timeout_s,
                                        rest_timeout_s=self._tx_rest_timeout_s)
        try:
            await self._executor.sync_nonce()
        except Exception as exc:
            self.log.warning("lighter nextNonce fetch failed at startup (%s); will retry on first tx", exc)

    async def close(self) -> None:
        self._closing = True
        tasks, self._tasks = self._tasks, []
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await self._ws.stop()
        except Exception:  # pragma: no cover
            self.log.exception("lighter ws stop failed")
        if self._signer is not None:
            await self._signer.close()
            self._signer = None
        if self._http is not None:
            await self._http.close()
            self._http = None
        self._executor = None
        self._connected = False

    # ================================================================== REST plumbing
    def _read_budget_ok(self) -> bool:
        return time.monotonic() >= self._cooldown_until and self._limiter.available() > int(self.cfg.read_reserve)

    def _enter_cooldown(self, status: int) -> None:
        self._cooldown_until = time.monotonic() + 60.0
        self.stats["http_429"] += 1
        self.log.error("lighter REST rate limited (HTTP %s): pausing REST/tx for 60 s", status)

    async def _rest_get(self, path: str, params: dict | None = None, *, priority: bool = False, auth: bool = False) -> dict:
        if self._http is None:
            raise ExchangeError("lighter: not connected", retryable=True)
        if time.monotonic() < self._cooldown_until:
            raise ExchangeError("lighter REST cooling down after HTTP 429", retryable=True, code=ERR_TOO_MANY_REQUESTS)
        headers = {"Authorization": self._auth_token()} if auth else None
        if priority:
            await self._limiter.acquire()
        elif not self._read_budget_ok() or not self._limiter.try_acquire():
            self.stats["reads_deferred"] += 1
            raise ExchangeError(f"lighter REST read budget exhausted ({path}); reserved for transactions", retryable=True, code="budget")
        self.stats["rest_reads"] += 1
        try:
            async with self._http.get(self._url(path), params=params, headers=headers) as r:
                text = await r.text()
                status = r.status
        except (asyncio.TimeoutError, aiohttp.ClientError, OSError) as exc:
            raise ExchangeError(f"lighter GET {path} failed: {exc!r}", retryable=True) from exc
        return self._parse_rest(path, status, text)

    def _parse_rest(self, path: str, status: int, text: str) -> dict:
        if status in (429, 405):
            self._enter_cooldown(status)
            raise ExchangeError(f"lighter GET {path}: HTTP {status} rate limited", retryable=True, code=ERR_TOO_MANY_REQUESTS)
        try:
            j = json.loads(text)
        except ValueError as exc:
            raise ExchangeError(f"lighter GET {path}: HTTP {status} non-JSON body {text[:120]!r}",
                                retryable=status >= 500 or status == 403, code=-status) from exc
        if not isinstance(j, dict):
            raise ExchangeError(f"lighter GET {path}: unexpected body", retryable=True)
        code = j.get("code")
        if status != 200 or code not in (None, 200):
            try:
                c = int(code)
            except (TypeError, ValueError):
                c = -status
            if c == ERR_RESTRICTED:
                self._set_restricted()
            raise ExchangeError(f"lighter GET {path}: HTTP {status} code {c} {j.get('message', '')}",
                                retryable=status >= 500 or is_retryable(c), code=c)
        return j

    # ================================================================== markets
    async def load_markets(self, _startup: bool = False) -> dict[str, MarketInfo]:
        data = await self._rest_get("/api/v1/orderBookDetails", {"filter": "perp"}, priority=_startup or not self.markets)
        markets = parse_markets(data)
        if not markets:
            raise ExchangeError("lighter orderBookDetails returned no perp markets", retryable=True)
        for m in markets.values():
            problems = check_market_invariants(m)
            if problems:
                self.log.warning("lighter market %s violates encoding invariants: %s", m.symbol, problems)
        old_ids = {s: m.market_id for s, m in self.markets.items()}
        self.markets.clear()  # in place: callers may hold a reference to the dict
        self.markets.update(markets)
        self._by_id = {m.market_id: m for m in markets.values()}
        for s, mid in old_ids.items():
            if s in markets and markets[s].market_id != mid:
                self.log.error("lighter market id of %s changed %s -> %s", s, mid, markets[s].market_id)
        return self.markets

    async def _markets_loop(self) -> None:
        while not self._closing:
            await asyncio.sleep(self._markets_refresh_s)
            try:
                if self._read_budget_ok():
                    await self.load_markets()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning("lighter market refresh failed: %s", exc)

    # ================================================================== websocket routing
    def _on_ws_connect(self) -> None:
        self.stats["ws_connects"] += 1

    def _on_ws_disconnect(self, reason: str) -> None:
        self.stats["ws_disconnects"] += 1
        for book in self._books.values():
            book.invalidate()
        self._last_books.clear()
        self._account_ready = False
        self._stats_ready = False
        self._orders_ready = False

    def _on_ws_message(self, msg: dict) -> None:
        if "error" in msg:
            self._on_ws_error(msg.get("error"))
            return
        ch = str(msg.get("channel") or "")
        typ = str(msg.get("type") or "")
        name = ch.split(":", 1)[0]
        snapshot = typ.startswith("subscribed")
        if name == "order_book":
            self._on_book(ch, msg, snapshot)
        elif name == "market_stats":
            self._on_market_stats(msg)
        elif name == "account_all":
            self._on_account_all(msg, snapshot)
        elif name == "user_stats":
            self._on_user_stats(msg)
        elif name == "account_all_orders":
            self._on_orders_frame(msg, snapshot)
        elif name == "account_tx":
            self._on_account_tx(msg)

    def _on_ws_error(self, err: Any) -> None:
        code = None
        msg = ""
        if isinstance(err, dict):
            msg = str(err.get("message", ""))
            try:
                code = int(err.get("code"))
            except (TypeError, ValueError):
                code = None
        if code == 30003:  # already subscribed
            self.log.debug("lighter ws: %s", msg)
            return
        self.log.warning("lighter ws error frame: %s %s", code, msg)
        if code == ERR_INVALID_AUTH and time.monotonic() - self._auth_error_at > 60:
            self._auth_error_at = time.monotonic()
            self._auth = None
            self._resubscribe_auth()

    # ---------------------------------------------------------------- books
    def _on_book(self, ch: str, msg: dict, snapshot: bool) -> None:
        try:
            mid = int(ch.split(":", 1)[1])
        except (IndexError, ValueError):
            return
        book = self._books.get(mid)
        ob = msg.get("order_book")
        if book is None or not isinstance(ob, dict):
            return
        now = time.time()
        if snapshot:
            book.apply_snapshot(ob, ts=now)
        elif not book.apply_update(ob, ts=now):
            self.log.warning("lighter book %s: nonce gap (begin_nonce %s); resubscribing", book.symbol, ob.get("begin_nonce"))
            self._last_books.pop(mid, None)
            self._resubscribe_book(mid)
            return
        if not book.valid:
            return
        if book.crossed():
            self.log.warning("lighter book %s crossed (%s >= %s); resubscribing", book.symbol, book.best_bid, book.best_ask)
            book.invalidate()
            self._last_books.pop(mid, None)
            self._resubscribe_book(mid)
            return
        snap = book.to_orderbook(BOOK_DEPTH)
        self._last_books[mid] = snap
        self._emit_book(snap)

    def _resubscribe_book(self, mid: int) -> None:
        """Unsubscribe+subscribe for a fresh snapshot, backing off when a market keeps gapping."""
        now = time.monotonic()
        last, streak = self._book_resub_at.get(mid, (0.0, 0))
        streak = streak + 1 if now - last < 60.0 else 0
        if now - last < min(30.0, float(2 ** min(streak, 5))) and last:
            self._book_resub_pending.add(mid)
            return
        self._book_resub_at[mid] = (now, streak)
        self._book_resub_pending.discard(mid)
        self.stats["book_resubscribes"] += 1
        self._ws.resubscribe(f"order_book/{mid}")

    # ---------------------------------------------------------------- market stats
    def _on_market_stats(self, msg: dict) -> None:
        ms = msg.get("market_stats")
        if not isinstance(ms, dict):
            return
        now = time.time()
        items = [(ms.get("market_id"), ms)] if "market_id" in ms else list(ms.items())
        for k, v in items:
            if not isinstance(v, dict):
                continue
            try:
                mid = int(v.get("market_id", k))
            except (TypeError, ValueError):
                continue
            self._ms[mid] = v
            self._ms_ts[mid] = now

    # ---------------------------------------------------------------- account
    def _symbol_for(self, mid: int, fallback: Any = None) -> str:
        m = self._by_id.get(mid)
        return m.symbol if m else str(fallback or mid)

    def _on_account_all(self, msg: dict, snapshot: bool) -> None:
        positions = msg.get("positions")
        if snapshot:
            self._positions_raw = {}
        if isinstance(positions, dict):
            for k, p in positions.items():
                try:
                    self._positions_raw[int(k)] = p
                except (TypeError, ValueError):
                    continue
        if snapshot:
            self._account_ready = True
        trades = msg.get("trades")
        if isinstance(trades, dict):
            flat = [t for lst in trades.values() if isinstance(lst, list) for t in lst if isinstance(t, dict)]
            flat.sort(key=lambda t: int(t.get("trade_id") or 0))
            for t in flat:
                self._on_trade(t, historical=snapshot)
        fh = msg.get("funding_histories")
        if isinstance(fh, dict):
            for lst in fh.values():
                for f in lst or []:
                    self._on_funding_row(f)

    def _on_user_stats(self, msg: dict) -> None:
        st = msg.get("stats")
        if isinstance(st, dict):
            self._user_stats = st
            self._stats_ready = True

    def _on_funding_row(self, f: dict) -> None:
        try:
            key = (int(f.get("funding_id")), int(f.get("market_id")))
        except (TypeError, ValueError):
            return
        if key in self._funding:
            return
        fp = parse_funding_payment(f, self._symbol_for(key[1]))
        if fp is not None:
            self._funding[key] = fp

    # ---------------------------------------------------------------- fills
    def _client_id_for(self, coi: int) -> str | None:
        o = self._orders.get(coi)
        return o.client_id if o is not None else None

    def _remember_trade(self, tid: str) -> bool:
        if tid in self._seen_trades:
            return False
        self._seen_trades[tid] = None
        while len(self._seen_trades) > 50_000:
            self._seen_trades.popitem(last=False)
        return True

    def _on_trade(self, t: dict, *, historical: bool = False) -> None:
        acct = self.cfg.account_index
        if acct is None:
            return
        who = trade_side_for(t, int(acct))
        if who is None:
            return
        tid = trade_id(t)
        if tid in self._seen_trades:
            return
        _, oidx, coi = who
        if not coi and oidx:
            coi = self._oidx_to_coi.get(oidx, 0)
        tracked = coi in self._orders
        if historical and not tracked:
            self._remember_trade(tid)  # old fill from before this session: never re-emit
            return
        try:
            mid = int(t.get("market_id"))
        except (TypeError, ValueError):
            return
        fill = trade_to_fill(t, int(acct), self._symbol_for(mid, t.get("market_symbol")), self._client_id_for)
        if fill is None:
            return
        self._remember_trade(tid)
        if tracked and fill.client_id != self._orders[coi].client_id:
            fill.client_id = self._orders[coi].client_id
        self.stats["fills"] += 1
        self._emit_fill(fill)
        if tracked:
            self._apply_fill_to_order(coi, fill, oidx)

    def _apply_fill_to_order(self, coi: int, fill: Fill, oidx: int) -> None:
        order = self._orders[coi]
        meta = self._meta.get(coi)
        if meta is None:
            return
        if oidx and order.order_id is None:
            order.order_id = str(oidx)
            self._oidx_to_coi[oidx] = coi
        meta.fills_base += fill.size
        meta.fills_quote += fill.size * fill.price
        changed = False
        if meta.fills_base > order.filled:
            order.filled = meta.fills_base
            order.avg_fill_price = meta.fills_quote / meta.fills_base
            changed = True
        if meta.pending_terminal is not None and meta.fills_base >= meta.venue_filled:
            self._release_terminal(coi)
            return
        if order.status.is_terminal:
            if changed:
                order.updated_ts = time.time()
                self._emit_order(order)
            return
        if not order.status.is_terminal:
            if order.filled >= order.size > 0:
                new = OrderStatus.FILLED
            elif order.filled > 0:
                new = OrderStatus.PARTIALLY_FILLED
            else:
                new = order.status
            if new is not order.status:
                order.status = new
                changed = True
                if new.is_terminal:
                    meta.terminal_at = time.monotonic()
        if changed:
            order.updated_ts = time.time()
            meta.event.set()
            self._emit_order(order)

    # ---------------------------------------------------------------- orders
    def _on_orders_frame(self, msg: dict, snapshot: bool) -> None:
        orders = msg.get("orders")
        if snapshot:
            self._venue_orders = {}
        if isinstance(orders, dict):
            for lst in orders.values():
                for o in lst or []:
                    if isinstance(o, dict):
                        self._on_order_json(o)
        if snapshot:
            self._orders_ready = True
            now = time.monotonic()
            live = {int(o.get("order_index") or 0) for o in self._venue_orders.values()}
            for coi, meta in self._meta.items():
                order = self._orders[coi]
                if order.status.is_terminal or not meta.venue_seen or meta.pending_terminal is not None:
                    continue
                if order.order_id and order.order_id.isdigit() and int(order.order_id) not in live:
                    # finished while we were disconnected: confirm over REST on the next maintenance tick
                    meta.venue_seen = False
                    meta.last_check = 0.0
                    meta.sent_at = min(meta.sent_at, now - self._order_confirm_timeout_s)

    def _on_order_json(self, o: dict) -> None:
        try:
            oidx = int(o.get("order_index"))
            mid = int(o.get("market_index"))
        except (TypeError, ValueError):
            return
        coi = int(o.get("client_order_index") or 0)
        status_s = str(o.get("status") or "")
        if status_s in ACTIVE_ORDER_STATUSES:
            self._venue_orders[oidx] = o
        else:
            self._venue_orders.pop(oidx, None)
        if not coi:
            coi = self._oidx_to_coi.get(oidx, 0)
        if not coi or coi not in self._orders:
            return  # not ours (placed elsewhere / previous session): visible via fetch_open_orders only
        order = self._orders[coi]
        meta = self._meta[coi]
        self._oidx_to_coi[oidx] = coi
        order.order_id = str(oidx)
        meta.venue_seen = True
        filled = dec(o.get("filled_base_amount"), ZERO) or ZERO
        fq = dec(o.get("filled_quote_amount"), ZERO) or ZERO
        if filled >= meta.venue_filled:
            meta.venue_filled, meta.venue_quote = filled, fq
        before = (order.status, order.filled, order.price, order.size)
        # order.filled tracks the fills already EMITTED (fills arrive on account_all); the venue amount is
        # applied when the terminal update is released
        px = dec(o.get("price"))
        if px is not None and px > 0 and not order.ioc:
            order.price = px
        init = dec(o.get("initial_base_amount"))
        if init is not None and init > 0:
            order.size = init
        new = map_order_status(status_s, order.filled)
        if status_s.startswith("canceled") and status_s != "canceled":
            order.error = status_s
        if new.is_terminal and meta.fills_base < meta.venue_filled:
            # hold the terminal update until its fills were emitted (fills arrive on another channel)
            if meta.pending_terminal is None:
                meta.pending_since = time.monotonic()
            meta.pending_terminal = new
            if order.status in (OrderStatus.PENDING, OrderStatus.OPEN) and order.filled > 0:
                order.status = OrderStatus.PARTIALLY_FILLED
        elif not order.status.is_terminal or new.is_terminal:
            if new is not OrderStatus.UNKNOWN:
                order.status = new
            if new.is_terminal and meta.terminal_at is None:
                meta.terminal_at = time.monotonic()
        if (order.status, order.filled, order.price, order.size) != before:
            order.updated_ts = time.time()
            self._emit_order(order)
        if order.status is not OrderStatus.PENDING or meta.pending_terminal is not None:
            meta.event.set()

    def _release_terminal(self, coi: int, *, timed_out: bool = False) -> None:
        order = self._orders.get(coi)
        meta = self._meta.get(coi)
        if order is None or meta is None or meta.pending_terminal is None:
            return
        if timed_out:
            self.log.warning("lighter order %s: terminal status without all fills (%s of %s) after %.1fs",
                             order.client_id, meta.fills_base, meta.venue_filled, self._fill_wait_s)
        if meta.venue_filled > order.filled:  # fills missing: fall back to the venue's cumulative amounts
            order.filled = meta.venue_filled
            order.avg_fill_price = meta.venue_quote / meta.venue_filled if meta.venue_filled > 0 else None
        order.status = meta.pending_terminal
        meta.pending_terminal = None
        meta.terminal_at = time.monotonic()
        order.updated_ts = time.time()
        meta.event.set()
        self._emit_order(order)

    def _reject_order(self, coi: int, error: str) -> None:
        order = self._orders.get(coi)
        meta = self._meta.get(coi)
        if order is None or meta is None or order.status.is_terminal:
            return
        if order.filled > 0 or meta.fills_base > 0:
            order.status = OrderStatus.CANCELED
        else:
            order.status = OrderStatus.REJECTED
        order.error = error
        order.updated_ts = time.time()
        meta.terminal_at = time.monotonic()
        meta.event.set()
        self._emit_order(order)

    def _on_account_tx(self, msg: dict) -> None:
        for tx in msg.get("txs") or []:
            if not isinstance(tx, dict):
                continue
            h = str(tx.get("hash") or "")
            status = tx.get("status")
            ev = parse_event_info(tx)
            ae = str(ev.get("ae") or "")
            if ae and (str(ERR_STANDARD_LIMIT) in ae or "Standard trading" in ae or "limit of Standard" in ae):
                self._set_standard_limit(ae)
            coi = self._tx_to_coi.get(h)
            if coi is None:
                if ae:
                    self.log.warning("lighter tx %s (type %s) applied with error: %s", h[:16], tx.get("type"), ae)
                continue
            if status == 0 or ae:
                self.log.warning("lighter order tx %s failed in the sequencer: status=%s ae=%s", h[:16], status, ae)
                self._reject_order(coi, f"sequencer: {ae or 'failed'}")

    # ================================================================== flags
    def _set_restricted(self) -> None:
        if self._restricted is None:
            self._restricted = RESTRICTED_MESSAGE
            self.log.critical(RESTRICTED_MESSAGE)

    def _set_standard_limit(self, reason: str) -> None:
        if time.monotonic() >= self._std_limit_until:
            self.log.critical("%s (%s); new Lighter exposure blocked for %.0fs", STANDARD_LIMIT_MESSAGE, reason, self._std_limit_cooldown_s)
        self._std_limit_until = time.monotonic() + self._std_limit_cooldown_s
        self._std_limit_reason = f"{STANDARD_LIMIT_MESSAGE}: {reason}"

    def _on_tx_code(self, code: int, message: str) -> None:
        if code == ERR_RESTRICTED:
            self._set_restricted()
        elif code == ERR_STANDARD_LIMIT:
            self._set_standard_limit(message)
        elif code in FATAL_CODES and self._fatal is None:
            self._fatal = f"lighter fatal error {code}: {message}"
            self.log.critical("%s - trading disabled", self._fatal)

    def can_increase_exposure(self) -> tuple[bool, str]:
        if self._restricted:
            return False, self._restricted
        if self._fatal:
            return False, self._fatal
        if time.monotonic() < self._std_limit_until:
            return False, self._std_limit_reason
        return True, ""

    def budget_health(self) -> float:
        if time.monotonic() < self._cooldown_until:
            return 0.0
        return max(0.0, min(1.0, self._limiter.available() / self._limiter.max_calls))

    # ================================================================== auth
    def _auth_token(self) -> str:
        if self._signer is None:
            raise AuthRequired("lighter: no signer (credentials missing)")
        now = time.time()
        ttl = float(self.cfg.auth_token_ttl_s)
        if self._auth is None or now >= self._auth[1] - max(60.0, 0.2 * ttl):
            tok, exp = self._signer.auth_token(ttl)
            self._auth = (tok, exp, now)
        return self._auth[0]

    def _auth_channels(self) -> list[str]:
        acct = self.cfg.account_index
        return [f"account_all_orders/{acct}", f"account_tx/{acct}"]

    def _resubscribe_auth(self) -> None:
        for ch in self._auth_channels():
            self._ws.resubscribe(ch)

    def _maybe_refresh_auth(self) -> None:
        if self._signer is None or self._auth is None:
            return
        tok, exp, created = self._auth
        ttl = max(60.0, exp - created)
        if time.time() - created >= 0.8 * ttl:
            self._auth = None
            self._auth_token()
            self.log.info("lighter auth token refreshed; resubscribing auth channels")
            self._resubscribe_auth()

    # ================================================================== maintenance
    async def _maintenance_loop(self) -> None:
        while not self._closing:
            await asyncio.sleep(1.0)
            try:
                self._maybe_refresh_auth()
                for mid in list(self._book_resub_pending):  # deferred (backed-off) book resubscribes
                    book = self._books.get(mid)
                    if book is not None and not book.valid and self._ws.connected:
                        self._resubscribe_book(mid)
                    elif book is None or book.valid:
                        self._book_resub_pending.discard(mid)
                self._release_stale_terminals()
                await self._confirm_stale_orders()
                self._cleanup_orders()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("lighter maintenance step failed")

    def _release_stale_terminals(self) -> None:
        now = time.monotonic()
        for coi, meta in list(self._meta.items()):
            if meta.pending_terminal is not None and now - meta.pending_since >= self._fill_wait_s:
                self._release_terminal(coi, timed_out=True)

    async def _confirm_stale_orders(self) -> None:
        """Orders without a venue status after ``order_confirm_timeout_s``: REST by COI, then tx lookup."""
        if not self.has_credentials or self._signer is None:
            return
        now = time.monotonic()
        stale = [
            (coi, m) for coi, m in self._meta.items()
            if not self._orders[coi].status.is_terminal and m.pending_terminal is None and m.tx_hash
            and now - m.sent_at >= self._order_confirm_timeout_s and now - m.last_check >= 5.0
            and (not m.venue_seen or self._orders[coi].ioc) and m.checks < 8
        ]
        if not stale or not self._read_budget_ok():
            return
        batch = stale[:20]
        for _, m in batch:
            m.last_check = now
            m.checks += 1
        found: set[int] = set()
        try:
            j = await self._rest_get("/api/v1/accountOrders", {
                "account_index": self.cfg.account_index,
                "client_order_indexes": ",".join(str(c) for c, _ in batch),
            }, auth=True)
            for o in j.get("orders") or []:
                if isinstance(o, dict):
                    found.add(int(o.get("client_order_index") or 0))
                    self._on_order_json(o)
        except ExchangeError as exc:
            self.log.debug("lighter accountOrders reconcile failed: %s", exc)
        for coi, m in batch:
            if coi in found:
                continue
            order = self._orders[coi]
            if m.checks >= 2 and m.tx_hash and self._read_budget_ok():
                try:
                    tx = await self._transport.lookup_tx(m.tx_hash)
                except Exception:
                    tx = None
                if tx is not None:
                    ev = parse_event_info(tx)
                    ae = str(ev.get("ae") or "")
                    if tx.get("status") == 0 or ae:
                        if str(ERR_STANDARD_LIMIT) in ae or "Standard trading" in ae:
                            self._set_standard_limit(ae)
                        self._reject_order(coi, f"sequencer: {ae or 'failed'}")
                        continue
            if order.status is OrderStatus.PENDING and now - m.sent_at >= _UNKNOWN_AFTER_S:
                order.status = OrderStatus.UNKNOWN
                order.error = order.error or "no venue confirmation"
                order.updated_ts = time.time()
                self.log.error("lighter order %s (tx %s) unconfirmed after %.0fs -> UNKNOWN; reconcile positions",
                               order.client_id, (m.tx_hash or "")[:16], _UNKNOWN_AFTER_S)
                self._emit_order(order)

    def _cleanup_orders(self, keep_s: float = 900.0) -> None:
        now = time.monotonic()
        for coi, meta in list(self._meta.items()):
            if meta.terminal_at is not None and now - meta.terminal_at > keep_s:
                order = self._orders.pop(coi, None)
                self._meta.pop(coi, None)
                if order is not None:
                    self._cid_to_coi.pop(order.client_id, None)
                    if order.order_id and order.order_id.isdigit():
                        self._oidx_to_coi.pop(int(order.order_id), None)
                if meta.tx_hash:
                    self._tx_to_coi.pop(meta.tx_hash, None)

    # ================================================================== market data API
    async def subscribe_books(self, symbols: list[str]) -> None:
        for s in symbols:
            m = self.market(s)
            if m.market_id not in self._books:
                self._books[m.market_id] = LocalBook(m.symbol, m.market_id, VENUE)
            self._ws.subscribe(f"order_book/{m.market_id}")

    def get_book(self, symbol: str) -> OrderBook | None:
        m = self.markets.get(symbol)
        if m is None:
            return None
        book = self._books.get(m.market_id)
        snap = self._last_books.get(m.market_id)
        if book is None or snap is None or not book.valid:
            return None
        # A quiet book on a live, gap-free stream is still current: extend its timestamp by up to
        # ``book_quiet_grace_s`` while the connection keeps delivering frames.
        if self._ws.connected and self._ws.last_rx_age() < 2.0:
            fresh_ts = min(time.time(), snap.ts + self._book_quiet_grace_s)
            if fresh_ts > snap.ts:
                return OrderBook(venue=snap.venue, symbol=snap.symbol, bids=snap.bids, asks=snap.asks, ts=fresh_ts, seq=snap.seq)
        return snap

    async def fetch_book(self, symbol: str, depth: int = 20) -> OrderBook:
        m = self.market(symbol)
        if not self._read_budget_ok():
            book = self.get_book(symbol)
            if book is not None:
                return book
        limit = max(1, min(250, depth * 5))
        j = await self._rest_get("/api/v1/orderBookOrders", {"market_id": m.market_id, "limit": limit})
        return _aggregate_rest_book(j, symbol, depth)

    async def fetch_funding(self, symbols: list[str] | None = None) -> dict[str, FundingInfo]:
        syms = list(symbols) if symbols is not None else list(self.markets)
        out: dict[str, FundingInfo] = {}
        for s in syms:
            m = self.markets.get(s)
            if m is None:
                continue
            ms = self._ms.get(m.market_id)
            if ms is not None:
                fi = funding_from_market_stats(ms, s, ts=self._ms_ts.get(m.market_id))
                if fi is not None:
                    out[s] = fi
        stale = bool(self._ms_ts) and time.time() - max(self._ms_ts.values()) > 300
        missing = [s for s in syms if s in self.markets and (s not in out or stale)]
        if missing and (not self._ms or stale):
            try:
                rows = await self._funding_rates_rest()
            except ExchangeError as exc:
                self.log.debug("lighter funding REST fallback skipped: %s", exc)
                return out
            by_id = {self.markets[s].market_id: s for s in missing}
            for r in rows:
                if str(r.get("exchange", "")).lower() != "lighter":
                    continue
                try:
                    s = by_id.get(int(r.get("market_id")))
                except (TypeError, ValueError):
                    continue
                rate = hourly_from_funding_rates(r.get("rate"))
                if s is None or rate is None:
                    continue
                out[s] = FundingInfo(venue=VENUE, symbol=s, rate_hourly=rate, next_funding_ts=next_funding_ts(None),
                                     is_predicted=True, ts=self._fr_cache[0] if self._fr_cache else time.time())
        return out

    async def _funding_rates_rest(self) -> list[dict]:
        if self._fr_cache is not None and time.time() - self._fr_cache[0] < 30.0:
            return self._fr_cache[1]
        j = await self._rest_get("/api/v1/funding-rates")
        rows = [r for r in j.get("funding_rates") or [] if isinstance(r, dict)]
        self._fr_cache = (time.time(), rows)
        return rows

    def market_stats(self, symbol: str) -> dict | None:
        """Latest raw ``market_stats`` object (mark/index/OI/funding) for ``symbol``."""
        m = self.markets.get(symbol)
        return self._ms.get(m.market_id) if m else None

    # ================================================================== account API
    def _require_creds(self) -> None:
        if not self.has_credentials:
            raise AuthRequired("lighter: credentials required (LIGHTER_API_PRIVATE_KEY/ACCOUNT_INDEX/API_KEY_INDEX)")

    def _account_fresh(self) -> bool:
        return self._ws.connected and self._account_ready

    async def _rest_account(self) -> dict:
        j = await self._rest_get("/api/v1/account", {"by": "index", "value": self.cfg.account_index})
        accts = j.get("accounts") or []
        if not accts:
            raise ExchangeError("lighter account not found", retryable=False)
        self._rest_account_cache = (time.time(), accts[0])
        return accts[0]

    def _mark_for(self, mid: int, p: dict | None = None) -> Decimal | None:
        ms = self._ms.get(mid)
        mark = dec(ms.get("mark_price")) if ms else None
        if mark:
            return mark
        if p is not None:
            size = dec(p.get("position"), ZERO) or ZERO
            value = dec(p.get("position_value"))
            if size and value is not None:
                return abs(value) / size
        return None

    def _positions_from_raw(self, raw: dict[int, dict], ts: float) -> dict[str, Position]:
        out: dict[str, Position] = {}
        for mid, p in raw.items():
            pos = parse_position(p, self._symbol_for(mid, p.get("symbol")), ts=ts)
            if pos.size == 0:
                continue
            mark = self._mark_for(mid)
            if mark:
                pos.mark_price = mark
            out[pos.symbol] = pos
        return out

    async def fetch_positions(self) -> dict[str, Position]:
        self._require_creds()
        if self._account_fresh():
            return self._positions_from_raw(dict(self._positions_raw), time.time())
        acct = await self._rest_account()
        raw = {}
        for p in acct.get("positions") or []:
            try:
                raw[int(p.get("market_id"))] = p
            except (TypeError, ValueError):
                continue
        return self._positions_from_raw(raw, time.time())

    def _maintenance_margin(self, raw: dict[int, dict]) -> tuple[Decimal, Decimal]:
        mm = ZERO
        im = ZERO
        for mid, p in raw.items():
            size = abs(dec(p.get("position"), ZERO) or ZERO)
            if size == 0:
                continue
            mark = self._mark_for(mid, p) or ZERO
            m = self._by_id.get(mid)
            mmf = Decimal(m.extra.get("mmf") or 0) if m else ZERO
            mm += size * mark * mmf / 10_000
            imf_pct = dec(p.get("initial_margin_fraction"))
            if imf_pct:
                im += size * mark * imf_pct / 100
        return mm, im

    async def fetch_balance(self) -> Balance:
        self._require_creds()
        if self._account_fresh() and self._stats_ready and self._user_stats:
            st = self._user_stats
            equity = dec(st.get("portfolio_value"), ZERO) or ZERO
            avail = dec(st.get("available_balance"), ZERO) or ZERO
            mm, im = self._maintenance_margin(dict(self._positions_raw))
            if im == 0:
                usage = dec(st.get("margin_usage"), ZERO) or ZERO
                im = equity * usage / 100
            return Balance(venue=VENUE, equity=equity, available=avail, margin_used=im, maintenance_margin=mm)
        acct = await self._rest_account()
        equity = dec(acct.get("total_asset_value")) or dec(acct.get("collateral"), ZERO) or ZERO
        return Balance(
            venue=VENUE,
            equity=equity,
            available=dec(acct.get("available_balance"), ZERO) or ZERO,
            margin_used=dec(acct.get("cross_initial_margin_requirement"), ZERO) or ZERO,
            maintenance_margin=dec(acct.get("cross_maintenance_margin_requirement"), ZERO) or ZERO,
        )

    def _order_obj(self, o: dict) -> Order:
        coi = int(o.get("client_order_index") or 0)
        if coi in self._orders:
            return self._orders[coi]
        try:
            mid = int(o.get("market_index"))
        except (TypeError, ValueError):
            mid = -1
        return order_from_json(o, self._symbol_for(mid, o.get("market_symbol")))

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        self._require_creds()
        mid = self.market(symbol).market_id if symbol is not None else None
        if self._ws.connected and self._orders_ready:
            return [self._order_obj(o) for o in self._venue_orders.values()
                    if mid is None or int(o.get("market_index", -1)) == mid]
        if mid is not None:
            mids = [mid]
        else:  # markets where we know of live orders or positions
            ids = {self.markets[o.symbol].market_id for o in self._orders.values() if o.is_open and o.symbol in self.markets}
            ids |= {k for k, p in self._positions_raw.items() if (dec(p.get("position"), ZERO) or ZERO) != 0}
            mids = sorted(ids)
        out: list[Order] = []
        for m_id in mids:
            j = await self._rest_get("/api/v1/accountActiveOrders",
                                     {"account_index": self.cfg.account_index, "market_id": m_id}, auth=True)
            out.extend(self._order_obj(o) for o in j.get("orders") or [] if isinstance(o, dict))
        return out

    async def fetch_funding_payments(self, since_ts: float | None = None) -> list[FundingPayment]:
        if not self.has_credentials:
            return []
        if time.monotonic() - self._funding_rest_at > 900 and self._signer is not None and self._read_budget_ok():
            self._funding_rest_at = time.monotonic()
            params: dict[str, Any] = {"account_index": self.cfg.account_index, "limit": 100}
            if since_ts:
                params["start_timestamp"] = int(since_ts)
            try:
                j = await self._rest_get("/api/v1/positionFunding", params, auth=True)
                for f in j.get("position_fundings") or []:
                    if isinstance(f, dict):
                        self._on_funding_row(f)
            except ExchangeError as exc:
                self.log.debug("lighter positionFunding fetch failed: %s", exc)
        rows = sorted(self._funding.values(), key=lambda f: f.ts)
        return [f for f in rows if since_ts is None or f.ts >= since_ts]

    # ================================================================== trading
    def _require_trading(self) -> None:
        self._require_creds()
        if self._restricted:
            raise ExchangeError(self._restricted, retryable=False, code=ERR_RESTRICTED)
        if self._fatal:
            raise ExchangeError(self._fatal, retryable=False)
        if self._executor is None or self._signer is None:
            raise ExchangeError("lighter: trading not initialised (connect first)", retryable=True)

    async def _send_tx(self, build: Callable[[int], SignedTx]) -> TxReceipt:
        self._require_trading()
        assert self._executor is not None
        try:
            return await self._executor.submit(build)
        except TxRejected as exc:
            self._on_tx_code(exc.tx_code, exc.tx_message)
            raise

    def _track(self, order: Order, coi: int, market_id: int) -> _OrderMeta:
        meta = _OrderMeta(coi=coi, market_id=market_id)
        self._orders[coi] = order
        self._meta[coi] = meta
        self._cid_to_coi[order.client_id] = coi
        return meta

    def _new_coi(self) -> int:
        for _ in range(5):
            coi = self._ids.next()
            if coi not in self._orders:
                return coi
        raise ExchangeError("lighter: could not allocate a client order index")  # pragma: no cover

    def _validate_order(self, m: MarketInfo, side: Side, size: Decimal, price: Decimal, *, reduce_only: bool,
                        taker: bool) -> tuple[int, int]:
        if not isinstance(size, Decimal) or not isinstance(price, Decimal):
            raise ValueError("size and price must be Decimal")
        if size <= 0 or price <= 0:
            raise ValueError("size and price must be positive")
        assert m.size_decimals is not None and m.price_decimals is not None
        base = size_to_int(size, m.size_decimals, exact=True)
        px = worst_price_to_int(price, side, m.price_decimals) if taker else price_to_int(price, m.price_decimals)
        if not reduce_only and not m.is_tradeable_size(size, price):
            raise ValueError(f"order below Lighter minimum (size {size} >= {m.min_size}, notional >= {m.min_notional})")
        limit = m.extra.get("order_quote_limit")
        if isinstance(limit, Decimal) and limit > 0 and size * price > limit:
            raise ValueError(f"order notional {size * price} above the market limit {limit}")
        return base, px

    async def _submit_order(self, order: Order, meta: _OrderMeta, build: Callable[[int], SignedTx], *,
                            wait_s: float) -> OrderResult:
        try:
            rcpt = await self._send_tx(build)
        except TxUnknown as exc:
            order.status = OrderStatus.UNKNOWN
            order.error = str(exc)
            order.updated_ts = time.time()
            if exc.tx_hash and exc.tx_hash != "?":
                meta.tx_hash = exc.tx_hash
                self._tx_to_coi[exc.tx_hash] = meta.coi
            self.stats["tx_unknown"] += 1
            self.log.error("lighter %s %s %s: %s -> treat as IN FLIGHT and reconcile", order.symbol, order.side.value,
                           order.size, exc)
            self._emit_order(order)
            return OrderResult(ok=True, order=order, error=str(exc), retryable=False)
        except (TxRejected, SignError, ExchangeError) as exc:
            order.status = OrderStatus.REJECTED
            order.error = str(exc)
            order.updated_ts = time.time()
            meta.terminal_at = time.monotonic()
            self._emit_order(order)
            return OrderResult(ok=False, order=order, error=str(exc), retryable=bool(getattr(exc, "retryable", False)))
        meta.tx_hash = rcpt.tx_hash
        meta.sent_at = time.monotonic()
        self._tx_to_coi[rcpt.tx_hash] = meta.coi
        if wait_s > 0 and order.status is OrderStatus.PENDING:
            try:
                await asyncio.wait_for(meta.event.wait(), wait_s)
            except asyncio.TimeoutError:
                pass
        if order.status is OrderStatus.CANCELED and order.error == "canceled-post-only":
            return OrderResult(ok=False, order=order, error="post-only order would cross", would_cross=True)
        if order.status is OrderStatus.REJECTED:
            return OrderResult(ok=False, order=order, error=order.error)
        return OrderResult(ok=True, order=order)

    async def place_market(self, symbol: str, side: Side, size: Decimal, *, worst_price: Decimal,
                           reduce_only: bool = False, client_id: str | None = None) -> OrderResult:
        try:
            self._require_trading()
            m = self.market(symbol)
            base, px = self._validate_order(m, side, size, worst_price, reduce_only=reduce_only, taker=True)
        except (ValueError, ExchangeError) as exc:
            return OrderResult(ok=False, error=str(exc), retryable=bool(getattr(exc, "retryable", False)))
        if client_id is not None and client_id in self._cid_to_coi:
            return OrderResult(ok=False, error=f"duplicate client_id {client_id}")
        coi = self._new_coi()
        order = Order(venue=VENUE, symbol=symbol, side=side, price=worst_price, size=size,
                      client_id=client_id or f"lt{coi}", status=OrderStatus.PENDING, ioc=True, reduce_only=reduce_only)
        meta = self._track(order, coi, m.market_id)
        signer = self._signer
        assert signer is not None

        def build(nonce: int) -> SignedTx:
            return signer.create_order(market_index=m.market_id, client_order_index=coi, base_amount=base, price=px,
                                       is_ask=side is Side.SELL, order_type=ORDER_TYPE_MARKET, time_in_force=TIF_IOC,
                                       reduce_only=reduce_only, order_expiry=DEFAULT_IOC_EXPIRY, nonce=nonce)

        return await self._submit_order(order, meta, build, wait_s=0.0)

    def _would_cross(self, m: MarketInfo, side: Side, price: Decimal) -> bool:
        book = self._last_books.get(m.market_id)
        b = self._books.get(m.market_id)
        if book is None or b is None or not b.valid:
            return False
        if side is Side.BUY:
            return book.best_ask is not None and price >= book.best_ask
        return book.best_bid is not None and price <= book.best_bid

    async def place_limit(self, symbol: str, side: Side, size: Decimal, price: Decimal, *, post_only: bool = True,
                          reduce_only: bool = False, client_id: str | None = None) -> OrderResult:
        try:
            self._require_trading()
            m = self.market(symbol)
            base, px = self._validate_order(m, side, size, price, reduce_only=reduce_only, taker=False)
        except (ValueError, ExchangeError) as exc:
            return OrderResult(ok=False, error=str(exc), retryable=bool(getattr(exc, "retryable", False)))
        if post_only and self._would_cross(m, side, price):
            return OrderResult(ok=False, error="post-only order would cross (local book)", would_cross=True)
        if client_id is not None and client_id in self._cid_to_coi:
            return OrderResult(ok=False, error=f"duplicate client_id {client_id}")
        coi = self._new_coi()
        order = Order(venue=VENUE, symbol=symbol, side=side, price=price, size=size, client_id=client_id or f"lt{coi}",
                      status=OrderStatus.PENDING, post_only=post_only, reduce_only=reduce_only)
        meta = self._track(order, coi, m.market_id)
        signer = self._signer
        assert signer is not None
        tif = TIF_POST_ONLY if post_only else TIF_GTT

        def build(nonce: int) -> SignedTx:
            return signer.create_order(market_index=m.market_id, client_order_index=coi, base_amount=base, price=px,
                                       is_ask=side is Side.SELL, order_type=ORDER_TYPE_LIMIT, time_in_force=tif,
                                       reduce_only=reduce_only, order_expiry=DEFAULT_28_DAY_ORDER_EXPIRY, nonce=nonce)

        return await self._submit_order(order, meta, build, wait_s=self._order_ack_timeout_s)

    def _resolve_order_ref(self, order_id: str | None, client_id: str | None) -> tuple[int | None, Order | None]:
        """-> (index to cancel/modify with: exchange order_index if known, else our COI; tracked order)."""
        order: Order | None = None
        if client_id is not None and client_id in self._cid_to_coi:
            order = self._orders.get(self._cid_to_coi[client_id])
        if order is None and order_id is not None and str(order_id).isdigit():
            coi = self._oidx_to_coi.get(int(order_id))
            order = self._orders.get(coi) if coi else None
        if order_id is not None and str(order_id).isdigit():
            return int(order_id), order
        if order is not None:
            if order.order_id and order.order_id.isdigit():
                return int(order.order_id), order
            return self._cid_to_coi[order.client_id], order
        if client_id is not None and client_id.isdigit():
            return int(client_id), None  # COI of an order from another session (cancel accepts the COI)
        return None, None

    async def cancel_order(self, symbol: str, *, order_id: str | None = None, client_id: str | None = None) -> bool:
        self._require_trading()
        m = self.market(symbol)
        idx, order = self._resolve_order_ref(order_id, client_id)
        if order is not None and order.status.is_terminal:
            return True
        if idx is None:
            self.log.warning("lighter cancel %s: unknown order (order_id=%s client_id=%s)", symbol, order_id, client_id)
            return False
        signer = self._signer
        assert signer is not None
        try:
            await self._send_tx(lambda nonce: signer.cancel_order(market_index=m.market_id, order_index=idx, nonce=nonce))
        except TxUnknown as exc:
            self.log.error("lighter cancel %s %s outcome unknown: %s", symbol, idx, exc)
            return False
        except (TxRejected, SignError, ExchangeError) as exc:
            self.log.warning("lighter cancel %s %s rejected: %s", symbol, idx, exc)
            return False
        return True

    async def cancel_all(self, symbol: str | None = None) -> None:
        self._require_trading()
        mid = self.market(symbol).market_id if symbol is not None else NIL_MARKET_INDEX
        if self._ws.connected and self._orders_ready:
            live = [o for o in self._venue_orders.values() if mid == NIL_MARKET_INDEX or int(o.get("market_index", -1)) == mid]
            if not live:
                return  # nothing resting: save a tx from the 60/min budget
        signer = self._signer
        assert signer is not None
        try:
            await self._send_tx(lambda nonce: signer.cancel_all(market_index=mid, time_in_force=CANCEL_ALL_IMMEDIATE,
                                                                timestamp_ms=0, nonce=nonce))
        except TxUnknown as exc:
            raise ExchangeError(f"lighter cancel_all outcome unknown: {exc}", retryable=True) from exc

    async def modify_order(self, order: Order, *, price: Decimal, size: Decimal | None = None) -> OrderResult:
        """Native ModifyOrder (tx 17). ``size`` is the new REMAINING size (default: current remaining)."""
        try:
            self._require_trading()
            m = self.market(order.symbol)
        except ExchangeError as exc:
            return OrderResult(ok=False, order=order, error=str(exc), retryable=exc.retryable)
        tracked = self._orders.get(self._cid_to_coi.get(order.client_id, -1))
        target = tracked or order
        if not target.is_open:
            return OrderResult(ok=False, order=target, error="order not open")
        new_size = size if size is not None else target.remaining
        try:
            base, px = self._validate_order(m, target.side, new_size, price, reduce_only=True, taker=False)
        except ValueError as exc:
            return OrderResult(ok=False, order=target, error=str(exc))
        if target.post_only and self._would_cross(m, target.side, price):
            return OrderResult(ok=False, order=target, error="post-only order would cross (local book)", would_cross=True)
        idx, _ = self._resolve_order_ref(target.order_id, target.client_id)
        if idx is None:
            return OrderResult(ok=False, order=target, error="unknown order index")
        signer = self._signer
        assert signer is not None
        meta = self._meta.get(self._cid_to_coi.get(target.client_id, -1))
        if meta is not None:
            meta.event.clear()
        try:
            await self._send_tx(lambda nonce: signer.modify_order(market_index=m.market_id, order_index=idx,
                                                                  base_amount=base, price=px, nonce=nonce))
        except TxUnknown as exc:
            return OrderResult(ok=True, order=target, error=str(exc))
        except (TxRejected, SignError, ExchangeError) as exc:
            return OrderResult(ok=False, order=target, error=str(exc), retryable=bool(getattr(exc, "retryable", False)))
        if meta is not None and self._order_ack_timeout_s > 0:
            try:
                await asyncio.wait_for(meta.event.wait(), self._order_ack_timeout_s)
            except asyncio.TimeoutError:
                pass
        if target.status is OrderStatus.CANCELED and target.error == "canceled-post-only":
            return OrderResult(ok=False, order=target, error="post-only order would cross", would_cross=True)
        return OrderResult(ok=True, order=target)

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        self._require_trading()
        m = self.market(symbol)
        min_imf = int(m.extra.get("min_imf") or 0)
        frac = leverage_to_fraction(leverage, min_imf)
        if min_imf and frac == min_imf and leverage > (m.max_leverage or leverage):
            self.log.warning("lighter %s: leverage %sx above market max %sx; using the max", symbol, leverage, m.max_leverage)
        current = None
        p = self._positions_raw.get(m.market_id)
        if p is not None and self._account_fresh():
            current = position_imf(p)
        if current == frac or self._leverage_sent.get(m.market_id) == frac:
            return
        signer = self._signer
        assert signer is not None
        try:
            await self._send_tx(lambda nonce: signer.update_leverage(market_index=m.market_id, fraction=frac,
                                                                     margin_mode=CROSS_MARGIN_MODE, nonce=nonce))
        except TxUnknown as exc:
            raise ExchangeError(f"lighter set_leverage outcome unknown: {exc}", retryable=True) from exc
        self._leverage_sent[m.market_id] = frac


def _aggregate_rest_book(j: dict, symbol: str, depth: int) -> OrderBook:
    """``orderBookOrders`` (one entry per ORDER) -> aggregated price levels."""
    from farmer.models import BookLevel

    def agg(rows: list, reverse: bool) -> list[BookLevel]:
        levels: dict[Decimal, Decimal] = {}
        for r in rows or []:
            px = dec(r.get("price"))
            sz = dec(r.get("remaining_base_amount"))
            if px is None or sz is None or sz <= 0:
                continue
            levels[px] = levels.get(px, ZERO) + sz
        return [BookLevel(p, levels[p]) for p in sorted(levels, reverse=reverse)[:depth]]

    return OrderBook(venue=VENUE, symbol=symbol, bids=agg(j.get("bids"), True), asks=agg(j.get("asks"), False))


__all__ = ["LighterExchange"]
