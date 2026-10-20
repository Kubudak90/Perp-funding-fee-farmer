"""ArcusExchange: the Exchange adapter for Arcus perps (maker leg).

Data flow:
- REST (``ArcusRest``): markets (refreshed every 60 s, MarketInfo objects updated IN PLACE), funding,
  account reads (public, by ``address`` + ``accountIndex``; mainnet whitelist-gated), signed writes.
- Market-data socket: ``l2OrderbookUpdates`` (nLevels 100 seed + full-depth deltas, sequence-checked),
  ``bbo`` (integrity cross-check + top-of-book fallback while resyncing), ``predictedFunding`` and one
  global ``oraclePrices`` subscription (mark/oracle for all markets).
- Private socket (only with an address): ``orders`` + ``userFills`` for (address, accountIndex). It is
  the source of truth for acks, rejections (e.g. POST_ONLY_WOULD_CROSS) and fills.

Order writes return HTTP 202; ``place_*``/``modify_order`` wait up to ``cfg.ack_timeout_s`` for the
first ``orders`` frame and otherwise return PENDING (see ``farmer.exchanges.base``). Fills are emitted
exactly once per (tradeId, orderId) from ``userFills`` only.
"""

from __future__ import annotations

import asyncio
import collections
import dataclasses
import time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Awaitable, Callable

import aiohttp

from farmer.config import ArcusConfig
from farmer.exchanges.arcus.auth import (
    CLIENT_ID_RE,
    US_PER_DAY,
    ArcusSigner,
    Ed25519Signer,
    NsClock,
    SignedRequest,
    canonical_json,
    submit_tif,
)
from farmer.exchanges.arcus.book import DeltaResult, L2Book, bbo_to_order_book, levels_to_order_book
from farmer.exchanges.arcus.parse import (
    WOULD_CROSS,
    apply_order_row,
    canonical_symbol,
    classify_order_frame,
    dec,
    fill_from_row,
    fill_key,
    funding_info,
    funding_payment_from_row,
    is_market_open,
    map_order_status,
    order_from_row,
    parse_market,
    position_from_row,
    update_market,
)
from farmer.exchanges.arcus.rest import (
    W_SET_LEVERAGE,
    WHITELIST_HINT,
    ArcusAccessError,
    ArcusHTTPError,
    ArcusRest,
)
from farmer.exchanges.arcus.ws import ArcusWsConnection, SubKey
from farmer.exchanges.base import AuthRequired, Exchange, ExchangeError
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
from farmer.utils.ids import new_client_id
from farmer.utils.ratelimit import TokenBucket

TRANSPORT_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, OSError, ConnectionError)


@dataclasses.dataclass(eq=False)
class _OrderRec:
    """Local knowledge about one order (reads never return reduceOnly / submit TIF / our goodTilTime)."""

    order: Order
    market_id: int
    tif: str  # submit TIF: ALO / GTT / IOC / FOK
    reduce_only: bool | None  # None = unknown (order placed outside this session)
    gtt_us: int | None
    acked: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    would_cross: bool = False
    in_modify: bool = False  # MODIFY_CANCELED leg seen, replacement outcome pending
    modify_fut: asyncio.Future | None = None
    modify_target: tuple[Decimal, Decimal] | None = None
    last_seq: int | None = None
    last_action_error: str | None = None
    terminal_mono: float | None = None
    foreign: bool = False
    # a cancel was accepted for this order; if the venue later reports it live anyway (the cancel
    # overtook its own unsequenced place -> ORDER_NOT_FOUND), it is re-sent (bounded, then cancelAll)
    cancel_requested_mono: float | None = None
    cancel_attempts: int = 0
    cancel_followup: bool = False
    verifying: bool = False  # a REST /v1/order/{id} check is running


class ArcusExchange(Exchange):
    """Arcus adapter. ``ArcusExchange(cfg)``; public data works without credentials."""

    name = "arcus"

    def __init__(self, cfg: ArcusConfig, *, rest: ArcusRest | None = None,
                 ws_connect: Callable[..., Any] | None = None) -> None:
        super().__init__()
        self.cfg = cfg
        self.address: str | None = cfg.address
        self.account_index = int(cfg.account_index)
        self.clock = NsClock()
        self.rest = rest or ArcusRest(cfg.rest_url, timeout_s=cfg.request_timeout_s)
        self._ws_connect = ws_connect
        self._signer: ArcusSigner | None = None
        if cfg.has_credentials:
            self._signer = ArcusSigner(Ed25519Signer(cfg.api_private_key or ""), cfg.address or "",
                                       self.account_index, self.clock)

        # tunables (see ArcusConfig)
        self.markets_refresh_s = float(cfg.markets_refresh_s)
        self.clock_resync_s = float(cfg.clock_resync_s)
        self.ratelimit_poll_s = float(cfg.ratelimit_poll_s)
        self.book_depth = int(cfg.book_depth)
        self.book_seed_levels = int(cfg.book_seed_levels)
        self.book_heartbeat_s = float(cfg.book_heartbeat_s)
        self.book_resync_timeout_s = float(cfg.book_resync_timeout_s)
        self.bbo_fallback = bool(cfg.bbo_fallback)
        self.subscribe_predicted_funding = bool(cfg.subscribe_predicted_funding)
        self.ws_watchdog_s = float(cfg.ws_watchdog_s)
        self.ws_ping_interval_s = float(cfg.ws_ping_interval_s)
        self.ws_max_lifetime_s = float(cfg.ws_max_lifetime_s)
        self.market_price_band = Decimal(str(cfg.market_price_band))
        self.min_modify_gtt_lead_days = float(cfg.min_modify_gtt_lead_days)
        self.private_ready_timeout_s = float(cfg.private_ready_timeout_s)
        self.fills_snapshot_n = int(cfg.fills_snapshot_n)
        # private stream counts as dead after this long without any frame (JSON pong every <= 10 s)
        self.private_stale_s = float(cfg.private_stale_s)
        # RWA: stop reporting market_open this long before the regular-trading-hours close
        self.rwa_close_buffer_s = float(cfg.rwa_close_buffer_s)
        self.cancel_recheck_s = 0.5  # a live frame this long after an accepted cancel -> re-send it
        self.max_cancel_attempts = 3

        # goodTilTime must be >= ~1 month ahead, also when echoed on modify -> never below 35 days
        self.good_til_days = max(35, int(cfg.good_til_days))
        if self.good_til_days != int(cfg.good_til_days):
            self.log.warning("arcus good_til_days %s < 35 is rejected by the venue; using 35", cfg.good_til_days)

        rate = float(cfg.requote_budget_per_s)
        self._order_bucket: TokenBucket | None = TokenBucket(rate, max(1.0, rate * 2)) if rate > 0 else None

        self._md: ArcusWsConnection | None = None
        self._acct: ArcusWsConnection | None = None
        self._tasks: list[asyncio.Task] = []
        self._bg_tasks: set[asyncio.Future] = set()  # one-shot helpers (order checks, cancel follow-ups)
        self._connected = False
        self._closing = False
        self._session_start_us = 0

        # markets
        self._by_id: dict[int, str] = {}
        self._by_ticker: dict[str, str] = {}
        self._market_rows: dict[str, dict] = {}
        self._markets_ts = 0.0
        self._fee_level: int | None = None
        self._fee_discount_ppm = 0

        # books / prices
        self._books: dict[str, L2Book] = {}
        self._book_keys: dict[str, SubKey] = {}
        self._book_out: dict[str, OrderBook] = {}
        self._book_emit_mono: dict[str, float] = {}
        self._book_wait_since: dict[str, float] = {}
        self._resync_mono: dict[str, float] = {}
        self._resync_deferred: set[str] = set()
        self._bbo_ahead_since: dict[str, float] = {}
        self._bbo_pending: dict[str, dict] = {}  # sym -> latest bbo frame the book could not use yet
        self._bbo_keys: dict[str, SubKey] = {}
        self._bbo_repolled: set[str] = set()  # unconfirmed top already re-checked with a fresh bbo snapshot
        self._attrs_synced = False  # marketAttributes snapshot received on the current md socket
        self._attrs: dict[str, dict] = {}  # sym -> latest marketAttributes entry (complete block)
        self._marks: dict[str, tuple[Decimal | None, Decimal | None, float]] = {}  # sym -> (oracle, mark, ts)
        self.predicted_funding: dict[str, tuple[Decimal, float]] = {}  # sym -> (rate1h, ts_s)

        # orders / fills
        self._recs_by_cid: dict[str, _OrderRec] = {}
        self._recs_by_oid: dict[str, _OrderRec] = {}
        self._seen_fills: set[tuple[str, str]] = set()
        self._seen_fills_q: collections.deque[tuple[str, str]] = collections.deque()
        self._orders_synced = asyncio.Event()
        self._fills_synced = asyncio.Event()
        self.access_error: str | None = None

        # rate limits
        self._pool_blocked_until: dict[str, float] = {}
        self._pool_remaining: dict[str, int] = {}
        self._rate_limit: dict | None = None
        self._dms_last_ok: float | None = None
        self._dms_lock = asyncio.Lock()
        self._writes_inflight = 0  # order POSTs (place/modify) in flight: keep the DMS armed

    # ================================================================== lifecycle
    @property
    def has_credentials(self) -> bool:
        return self.cfg.has_credentials

    async def connect(self) -> None:
        if self._connected:
            return
        try:
            await self._connect()
        except BaseException:
            await self.close()
            raise

    async def _connect(self) -> None:
        self._closing = False
        await self.rest.start()
        await self._sync_clock()
        await self.load_markets()
        self._session_start_us = self.clock.now_us()

        self._md = self._make_ws("md", self._on_md_message, on_disconnect=self._on_md_disconnect,
                                 ping_interval_s=self.ws_ping_interval_s)
        await self._md.subscribe({"channel": "oraclePrices"})
        # RWA regime (isOutsideRth, bands, off-hours IM): pushed on every exitRth/enterRth crossing
        await self._md.subscribe({"channel": "marketAttributes"})
        self._md.start()
        if not await self._md.wait_connected(self.private_ready_timeout_s):
            self.log.warning("arcus market-data socket not connected yet (it keeps retrying in the background)")

        private_ok = False
        if self.address:
            private_ok = await self._check_access()
            if private_ok:
                await self._refresh_fee_tier()
                if self.has_credentials:
                    await self._check_api_key()
                self._acct = self._make_ws("acct", self._on_acct_message, on_disconnect=self._on_acct_disconnect,
                                           ping_interval_s=min(10.0, self.ws_ping_interval_s))
                addr = self.address.lower()
                await self._acct.subscribe({"channel": "orders", "id": addr, "accountIndex": self.account_index,
                                            "nRecentClosed": 20})
                await self._acct.subscribe({"channel": "userFills", "id": addr, "accountIndex": self.account_index,
                                            "nFills": self.fills_snapshot_n})
                self._acct.start()
                try:
                    await asyncio.wait_for(asyncio.gather(self._orders_synced.wait(), self._fills_synced.wait()),
                                           self.private_ready_timeout_s)
                except asyncio.TimeoutError:
                    self.log.warning("arcus private stream not ready after %.0fs (continuing)",
                                     self.private_ready_timeout_s)

        self._spawn(self._periodic("markets", self.markets_refresh_s, self._refresh_markets,
                                   initial_delay=self.markets_refresh_s))
        self._spawn(self._periodic("books", max(0.25, min(self.book_heartbeat_s or 1.0, 1.0)), self._book_monitor))
        self._spawn(self._periodic("gc", 60.0, self._gc, initial_delay=60.0))
        if self.has_credentials and private_ok:
            self._spawn(self._periodic("clock", self.clock_resync_s, self._sync_clock,
                                       initial_delay=self.clock_resync_s))
            self._spawn(self._periodic("ratelimit", self.ratelimit_poll_s, self._poll_rate_limit,
                                       initial_delay=self.ratelimit_poll_s))
            dms = float(self.cfg.dead_mans_switch_s or 0)
            if dms > 0:
                self._spawn(self._periodic("dms", max(1.0, dms / 3.0), self._refresh_dms))
        self._connected = True
        self.log.info("arcus connected: %d markets, credentials=%s, private=%s", len(self.markets),
                      self.has_credentials, private_ok)

    async def close(self) -> None:
        if self._closing and not self._tasks and not self._connected:
            return
        self._closing = True
        tasks, self._tasks = self._tasks + list(self._bg_tasks), []
        self._bg_tasks.clear()
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        if (self._connected and self.has_credentials and float(self.cfg.dead_mans_switch_s or 0) > 0
                and self._dms_last_ok is not None and not self._live_recs(include_foreign=True)):
            try:  # nothing of ours rests -> disarm instead of burning one of the 10 daily auto-fires
                await asyncio.wait_for(self.arm_dead_mans_switch(0), 3.0)
            except Exception as exc:
                self.log.warning("arcus DMS disarm failed: %s", exc)
        for ws in (self._md, self._acct):
            if ws is not None:
                await ws.stop()
        self._md = self._acct = None
        # books belong to the closed socket; a later connect() starts clean (call subscribe_books again)
        self._books.clear()
        self._book_keys.clear()
        self._bbo_keys.clear()
        self._bbo_pending.clear()
        self._bbo_repolled.clear()
        self._attrs_synced = False
        self._book_wait_since.clear()
        self._resync_deferred.clear()
        self._orders_synced.clear()
        self._fills_synced.clear()
        await self.rest.close()
        self._connected = False

    def _make_ws(self, name: str, handler: Callable[[dict], None], *, on_disconnect: Callable[[], None],
                 ping_interval_s: float) -> ArcusWsConnection:
        return ArcusWsConnection(self.cfg.ws_url, name=name, on_message=handler, on_disconnect=on_disconnect,
                                 watchdog_s=self.ws_watchdog_s, ping_interval_s=ping_interval_s,
                                 max_lifetime_s=self.ws_max_lifetime_s, connect=self._ws_connect)

    def _spawn(self, coro: Awaitable) -> None:
        self._tasks.append(asyncio.ensure_future(coro))

    async def _periodic(self, name: str, interval: float, fn: Callable[[], Awaitable[Any]], *,
                        initial_delay: float = 0.0) -> None:
        """Run ``fn`` forever; exceptions are logged and retried with backoff, never fatal."""
        if initial_delay > 0:
            await asyncio.sleep(initial_delay)
        errors = 0
        while not self._closing:
            try:
                await fn()
                errors = 0
                delay = interval
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors += 1
                delay = min(interval, 2.0 ** min(errors, 6))
                self.log.warning("arcus %s task failed (%d): %r", name, errors, exc)
            await asyncio.sleep(delay)

    # ================================================================== clock / access / keys
    async def _sync_clock(self) -> None:
        try:
            t0 = time.time_ns()
            server = await self.rest.server_time_ns()
            t1 = time.time_ns()
        except Exception as exc:
            self.log.warning("arcus clock sync failed: %r", exc)
            return
        off = self.clock.sync(server, t0, t1)
        if abs(off) > 5_000_000_000:
            self.log.warning("arcus clock offset %.2fs (local clock skew)", off / 1e9)

    async def _check_access(self) -> bool:
        """Whitelist gate check via GET /v1/rateLimit. Raises with credentials, else warns."""
        try:
            rl = await self.rest.rate_limit(self.address or "", self.account_index)
        except ArcusAccessError as exc:
            msg = WHITELIST_HINT.format(address=self.address)
            self.access_error = msg
            if self.has_credentials:
                raise ExchangeError(msg, code="AddressNotOnWhitelist") from exc
            self.log.warning(msg)
            return False
        except Exception as exc:  # transient: proceed, the private stream will tell
            self.log.warning("arcus access check failed: %r", exc)
            return True
        if isinstance(rl, dict):
            if rl.get("accountIndex", self.account_index) != self.account_index:
                self.log.warning("arcus rateLimit echoed accountIndex %s != %s", rl.get("accountIndex"),
                                 self.account_index)
            self._rate_limit = rl
        return True

    async def _check_api_key(self) -> None:
        assert self._signer is not None
        try:
            keys = await self.rest.api_keys(self.address or "")
        except Exception as exc:
            self.log.warning("arcus apiKeys check failed: %r", exc)
            return
        me = self._signer.api_key
        row = next((k for k in keys if str(k.get("apiKey", "")).lower() == me), None)
        if row is None:
            raise ExchangeError(f"arcus API key {me[:8]}... is not registered for {self.address} "
                                "(register it with farmer keygen / the web app)", code="ApiKeyNotFound")
        if row.get("status") != "ACTIVE":
            raise ExchangeError(f"arcus API key {me[:8]}... status {row.get('status')}", code="ApiKeyInactive")
        if not row.get("allSubaccounts") and row.get("accountIndex", 0) != self.account_index:
            raise ExchangeError(f"arcus API key is scoped to accountIndex {row.get('accountIndex')}, "
                                f"not {self.account_index}", code="ApiKeyScope")
        vu = int(row.get("validUntil") or 0)
        if vu and vu < 10**14:  # ms
            days = (vu / 1000 - time.time()) / 86400
            if days < 7:
                self.log.warning("arcus API key expires in %.1f days - renew it", days)

    async def _refresh_fee_tier(self) -> None:
        try:
            stats = await self.rest.account_stats(self.address or "", "feeTier")
        except Exception as exc:
            self.log.debug("arcus fee tier unavailable: %r", exc)
            return
        tier = (stats or {}).get("tradingFeeTier") or {}
        if "level" in tier:
            self._fee_level = int(tier["level"])
            self._fee_discount_ppm = int(tier.get("feeDiscountPpm") or 0)
            for sym, row in self._market_rows.items():
                if sym in self.markets:
                    update_market(self.markets[sym], row, self._fee_level, self._fee_discount_ppm)

    async def _poll_rate_limit(self) -> None:
        rl = await self.rest.rate_limit(self.address or "", self.account_index)
        if isinstance(rl, dict):
            self._rate_limit = rl

    # ================================================================== markets
    async def load_markets(self) -> dict[str, MarketInfo]:
        rows = await self.rest.markets()
        self._apply_markets(rows)
        return self.markets

    async def _refresh_markets(self) -> None:
        self._apply_markets(await self.rest.markets())

    def _apply_markets(self, rows: list[dict]) -> None:
        for row in rows:
            try:
                sym = canonical_symbol(row)
                mid = int(row["marketId"])
                cur = self.markets.get(sym)
                if cur is not None and cur.market_id != mid:
                    # two markets with one base asset: keep the ONLINE one
                    if not (str(row.get("status")).upper() == "ONLINE" and not cur.active):
                        continue
                    self.markets[sym] = parse_market(row, self._fee_level, self._fee_discount_ppm)
                elif cur is not None:
                    update_market(cur, row, self._fee_level, self._fee_discount_ppm)
                else:
                    self.markets[sym] = parse_market(row, self._fee_level, self._fee_discount_ppm)
                self._by_id[mid] = sym
                self._by_ticker[str(row.get("marketDisplayName"))] = sym
                self._market_rows[sym] = row
                attrs = self._attrs.get(sym)
                if attrs is not None and self._attrs_synced and self.markets[sym].market_id == mid \
                        and attrs.get("marketDisplayName") == row.get("marketDisplayName"):
                    # the pushed RTH state is never older than a (possibly cached) REST read
                    self._apply_attrs(self.markets[sym], attrs)
            except Exception as exc:
                self.log.warning("arcus: skipping bad market row %s: %r", row.get("marketDisplayName"), exc)
        self._markets_ts = time.time()

    @staticmethod
    def _apply_attrs(m: MarketInfo, e: dict) -> None:
        """Overlay one ``marketAttributes`` block (RTH flag, bands, off-hours IM) onto ``m.extra`` in place."""
        ext = m.extra
        if "isOutsideRth" in e:
            ext["is_outside_rth"] = e.get("isOutsideRth")
        if "offHoursInitialMarginFraction" in e:
            ext["off_hours_imf"] = dec(e.get("offHoursInitialMarginFraction"))
        bounds = dict(ext.get("trading_bounds") or {})
        for k in ("upperTradingBound", "lowerTradingBound", "currentSettlementPrice"):
            if k in e:
                bounds[k] = e.get(k)
        ext["trading_bounds"] = bounds
        if e.get("boundEvent"):
            ext["bound_event"] = e.get("boundEvent")

    def _on_market_attrs(self, c: dict) -> None:
        for e in c.get("entries") or []:
            if not isinstance(e, dict) or e.get("type") == "feeTierConfig":
                continue
            sym = self._by_ticker.get(str(e.get("marketDisplayName"))) or self._symbol_of(e)
            m = self.markets.get(sym) if sym else None
            if m is None or m.venue_symbol != e.get("marketDisplayName"):
                continue  # unknown, or the other (e.g. OFFLINE) market sharing this base asset
            prev = m.extra.get("is_outside_rth")
            self._attrs[sym] = e
            self._apply_attrs(m, e)
            if e.get("boundEvent") in ("exitRth", "enterRth") or (prev is not None and prev != e.get("isOutsideRth")):
                self.log.info("arcus %s: %s (isOutsideRth=%s)", sym, e.get("boundEvent") or "RTH change",
                              e.get("isOutsideRth"))
        if c.get("isSnapshot"):
            self._attrs_synced = True

    def market_open(self, symbol: str) -> bool:
        return is_market_open(self.markets.get(symbol), close_buffer_s=self.rwa_close_buffer_s)

    def private_ok(self) -> tuple[bool, str]:
        """Is the private stream (the only source of acks, fills and rejections) connected and synced?"""
        acct = self._acct
        if acct is None:
            return False, "arcus private stream not started"
        if not acct.is_healthy(self.private_stale_s):
            return False, "arcus private stream down/stale"
        if not (self._orders_synced.is_set() and self._fills_synced.is_set()):
            return False, "arcus private stream not synced (orders/userFills snapshot pending)"
        if any(k[0] in ("orders", "userFills") for k in getattr(acct, "degraded", {})):
            return False, "arcus private stream degraded (server-side snapshot pending)"
        return True, ""

    def _dms_armed(self) -> bool:
        dms = float(self.cfg.dead_mans_switch_s or 0)
        if dms <= 0:
            return True
        return self._dms_last_ok is not None and time.monotonic() - self._dms_last_ok <= dms * 2.0 / 3.0

    def can_increase_exposure(self) -> tuple[bool, str]:
        if self.access_error:
            return False, self.access_error
        if self.has_credentials:
            ok, why = self.private_ok()
            if not ok:
                return False, why
            if not self._dms_armed() and self._live_recs(include_foreign=True):
                return False, "arcus dead man's switch is not armed while orders rest"
        return True, ""

    def symbol_for_ticker(self, ticker: str) -> str | None:
        return self._by_ticker.get(ticker)

    def _symbol_of(self, c: dict) -> str | None:
        mid = c.get("marketId")
        if mid is not None:
            try:
                sym = self._by_id.get(int(mid))
            except (TypeError, ValueError):
                sym = None
            if sym:
                return sym
        t = c.get("marketDisplayName") or c.get("market")
        if t:
            return self._by_ticker.get(str(t)) or str(t).split("-")[0].upper()
        return None

    # ================================================================== funding
    async def fetch_funding(self, symbols: list[str] | None = None) -> dict[str, FundingInfo]:
        if time.time() - self._markets_ts > 5.0 or not self._market_rows:
            self._apply_markets(await self.rest.markets())
        out: dict[str, FundingInfo] = {}
        now = time.time()
        for sym, row in self._market_rows.items():
            if symbols is not None and sym not in symbols:
                continue
            if str(row.get("status") or "").upper() != "ONLINE":
                continue
            oracle = mark = None
            ws_px = self._marks.get(sym)
            if ws_px is not None and now - ws_px[2] < 10.0:
                oracle, mark = ws_px[0], ws_px[1]
            out[sym] = funding_info(row, mark=mark, oracle=oracle)
        return out

    # ================================================================== books
    async def subscribe_books(self, symbols: list[str]) -> None:
        if self._md is None:
            raise ExchangeError("arcus: connect() before subscribe_books()")
        for sym in symbols:
            m = self.market(sym)
            if sym in self._books:
                continue
            if not m.active:
                self.log.warning("arcus %s is %s - subscribing anyway", sym, m.extra.get("status"))
            ticker = m.venue_symbol  # exact-case uppercase ticker (WS rejects lowercase/numeric ids)
            self._books[sym] = L2Book(sym, ticker, venue=self.name, depth=self.book_depth)
            self._book_wait_since[sym] = time.monotonic()
            self._book_keys[sym] = await self._md.subscribe(
                {"channel": "l2OrderbookUpdates", "id": ticker, "nLevels": self.book_seed_levels})
            self._bbo_keys[sym] = await self._md.subscribe({"channel": "bbo", "id": ticker})
            if self.subscribe_predicted_funding:
                await self._md.subscribe({"channel": "predictedFunding", "id": ticker})
        n_subs = 2 + len(self._books) * (3 if self.subscribe_predicted_funding else 2)
        if n_subs > 100:
            self.log.warning("arcus md socket has %d subscriptions (server cap 100/connection)", n_subs)

    def get_book(self, symbol: str) -> OrderBook | None:
        return self._book_out.get(symbol)

    async def fetch_book(self, symbol: str, depth: int = 20) -> OrderBook:
        m = self.market(symbol)
        data = await self.rest.l2_orderbook(m.venue_symbol, depth)
        return levels_to_order_book(symbol, data, venue=self.name, depth=depth)

    def _on_md_message(self, msg: dict) -> None:
        t = msg.get("type")
        if t not in ("subscribed", "channel_data"):
            return
        ch = msg.get("channel")
        c = msg.get("contents")
        if not isinstance(c, dict):
            return
        if ch == "l2OrderbookUpdates":
            self._on_l2(str(msg.get("id")), t == "subscribed", c)
        elif ch == "bbo":
            self._on_bbo(str(msg.get("id")), c)
        elif ch == "oraclePrices":
            self._on_oracle(c)
        elif ch == "predictedFunding":
            self._on_predicted(str(msg.get("id")), c)
        elif ch == "marketAttributes":
            self._on_market_attrs(dict(c, isSnapshot=True) if t == "subscribed" else c)

    def _on_l2(self, ticker: str, snapshot: bool, c: dict) -> None:
        sym = self._by_ticker.get(ticker)
        book = self._books.get(sym) if sym else None
        if book is None:
            return
        now = time.time()
        if snapshot:
            book.apply_snapshot(c, now=now)
            if not book.synced:  # e.g. an empty ack without lastSequenceId: keep waiting / monitor resyncs
                self.log.warning("arcus %s: snapshot without lastSequenceId ignored", sym)
                return
            self._book_wait_since.pop(sym, None)
            self._resync_deferred.discard(sym)
            self._bbo_ahead_since.pop(sym, None)
            self._bbo_repolled.discard(sym)
            if book.is_crossed():
                self._request_resync(sym, "crossed snapshot")
                return
            self._apply_pending_bbo(sym, book)
            self._publish(book, now)
            return
        res = book.apply_delta(c, now=now)
        if res is DeltaResult.APPLIED:
            if self._apply_pending_bbo(sym, book) is False:
                self._request_resync(sym, f"bbo disagrees with confirmed levels at seq {book.seq}")
                return
            self._publish(book, now)
        elif res in (DeltaResult.GAP, DeltaResult.CROSSED):
            self._request_resync(sym, f"{res.value} at seq {c.get('lastSequenceId')} (book seq {book.seq})")

    def _apply_pending_bbo(self, sym: str, book: L2Book) -> bool | None:
        """Use the latest bbo frame that arrived before the book could check it (book behind it, or no
        delta applied yet) once the book has caught up with its sequence."""
        c = self._bbo_pending.get(sym)
        if c is None or book.seq is None:
            return None
        raw = c.get("lastSequenceId")
        if raw is not None and int(raw) > book.seq:
            return None  # the book has not reached it yet: keep it
        self._bbo_pending.pop(sym, None)
        if raw is None:
            return None
        repairs = book.repairs
        res = book.prune_with_bbo(c)
        if res and book.repairs != repairs:
            self.log.debug("arcus %s: repaired %d stale snapshot level(s) from a buffered bbo", sym,
                           book.repairs - repairs)
        return res

    def _publish(self, book: L2Book, now: float) -> None:
        ob = book.to_order_book(self.book_depth, ts=now)
        self._book_out[book.symbol] = ob
        self._book_emit_mono[book.symbol] = time.monotonic()
        self._emit_book(ob)

    def _request_resync(self, sym: str, reason: str) -> None:
        """Drop the local book and ask for a fresh snapshot (unsubscribe + subscribe), at most 1/s."""
        book = self._books.get(sym)
        if book is None or self._md is None:
            return
        book.reset()
        mono = time.monotonic()
        self._book_wait_since.setdefault(sym, mono)
        self.log.warning("arcus %s book resync: %s", sym, reason)
        if mono - self._resync_mono.get(sym, 0.0) < 1.0:
            self._resync_deferred.add(sym)  # the book monitor sends it shortly
            return
        self._send_resync(sym, mono)

    def _send_resync(self, sym: str, mono: float) -> None:
        book, key = self._books.get(sym), self._book_keys.get(sym)
        self._resync_deferred.discard(sym)
        if book is None or key is None or self._md is None:
            return
        self._resync_mono[sym] = mono
        self._book_wait_since[sym] = mono
        book.resyncs += 1
        keys = [key]
        bbo_key = self._bbo_keys.get(sym)
        if bbo_key is not None:  # a fresh bbo snapshot (taken after the new l2 one) checks the loss window
            keys.append(bbo_key)
        fut = asyncio.ensure_future(self._resubscribe(keys))
        fut.add_done_callback(_log_future_error(self.log, f"resubscribe {sym}"))

    async def _resubscribe(self, keys: list[SubKey]) -> None:
        for key in keys:
            if self._md is not None:
                await self._md.resubscribe(key)

    def _on_bbo(self, ticker: str, c: dict) -> None:
        sym = self._by_ticker.get(ticker)
        book = self._books.get(sym) if sym else None
        if book is None:
            return
        now = time.time()
        raw = c.get("lastSequenceId")
        if book.synced:
            repairs = book.repairs
            chk = book.reconcile_bbo(c)  # same-sequence bbo is authoritative for the top of book
            if chk is None and raw is not None and book.seq is not None:
                if int(raw) > book.seq:  # book behind: check it once the book got there
                    self._bbo_pending[sym] = c
                    if book.applying:
                        since = self._bbo_ahead_since.setdefault(sym, now)
                        if now - since > 2.0:
                            self._request_resync(sym, "book lags bbo for > 2s")
                    return
                # an older bbo from past the loss window still exposes stale snapshot levels better than it
                chk = book.prune_with_bbo(c)
            if chk is False:
                self._request_resync(sym, f"bbo disagrees with confirmed levels at seq {raw}")
                return
            self._bbo_ahead_since.pop(sym, None)
            if book.repairs != repairs:
                self.log.debug("arcus %s: repaired %d stale snapshot level(s) from bbo", sym,
                               book.repairs - repairs)
                self._publish(book, now)
            return
        if raw is not None:
            self._bbo_pending[sym] = c  # may be newer than the snapshot that is on its way
        if self.bbo_fallback and c.get("bestBid") and c.get("bestAsk"):
            ob = bbo_to_order_book(sym, c, venue=self.name, ts=now)
            if ob.best_bid is not None and ob.best_ask is not None and ob.best_bid < ob.best_ask:
                self._book_out[sym] = ob
                self._book_emit_mono[sym] = time.monotonic()
                self._emit_book(ob)

    def _on_oracle(self, c: dict) -> None:
        now = time.time()
        for p in c.get("prices") or []:
            sym = self._symbol_of(p)
            if sym is None:
                continue
            oracle, mark = dec(p.get("price")), dec(p.get("markPrice"))
            self._marks[sym] = (oracle if oracle and oracle > 0 else None, mark if mark and mark > 0 else None, now)

    def _on_predicted(self, ticker: str, c: dict) -> None:
        sym = self._by_ticker.get(ticker)
        rate = dec(c.get("rate1h"))
        if sym is None or rate is None:
            return
        ts = c.get("timestampMs")
        self.predicted_funding[sym] = (rate, int(ts) / 1000.0 if ts else time.time())

    def _on_md_disconnect(self) -> None:
        for sym, book in self._books.items():
            book.reset()
            self._book_wait_since[sym] = time.monotonic()
        self._bbo_ahead_since.clear()
        self._bbo_pending.clear()
        self._bbo_repolled.clear()
        self._attrs_synced = False  # REST is the RTH source again until the next snapshot

    async def _book_monitor(self) -> None:
        """Heartbeat quiet-but-healthy books (fresh ts) and resubscribe books stuck without a snapshot."""
        if self._md is None:
            return
        mono, now = time.monotonic(), time.time()
        hb = self.book_heartbeat_s
        for sym, book in self._books.items():
            if book.synced:
                if (book.applying and sym not in self._bbo_repolled and book.top_unconfirmed()
                        and book.snapshot_ts is not None and now - book.snapshot_ts > 2.0
                        and self._bbo_keys.get(sym) is not None and self._md.connected):
                    # quiet market: the top is still a snapshot-only level that may have changed inside the
                    # loss window -> one fresh bbo snapshot (now past the window) confirms or prunes it
                    self._bbo_repolled.add(sym)
                    fut = asyncio.ensure_future(self._resubscribe([self._bbo_keys[sym]]))
                    fut.add_done_callback(_log_future_error(self.log, f"bbo re-poll {sym}"))
                last = self._book_emit_mono.get(sym, 0.0)
                if hb > 0 and mono - last >= hb and self._md.is_healthy(max(2.0, 2 * hb + 1)):
                    ob = self._book_out.get(sym)
                    if ob is not None:
                        fresh = OrderBook(venue=ob.venue, symbol=ob.symbol, bids=list(ob.bids), asks=list(ob.asks),
                                          ts=now, seq=ob.seq)
                        self._book_out[sym] = fresh
                        self._book_emit_mono[sym] = mono
                        self._emit_book(fresh)
            elif self._md.connected:
                since = self._book_wait_since.setdefault(sym, mono)
                if sym in self._resync_deferred and mono - self._resync_mono.get(sym, 0.0) >= 1.0:
                    self._send_resync(sym, mono)
                elif mono - since > self.book_resync_timeout_s:
                    self.log.warning("arcus %s: no book snapshot for %.0fs -> resubscribe", sym, mono - since)
                    self._send_resync(sym, mono)

    def _mark_price(self, sym: str) -> Decimal | None:
        ws_px = self._marks.get(sym)
        if ws_px is not None and ws_px[1] and time.time() - ws_px[2] < 10.0:
            return ws_px[1]
        m = self.markets.get(sym)
        mk = m.extra.get("mark_price") if m else None
        if mk and mk > 0:
            return mk
        book = self._book_out.get(sym)
        return book.mid if book is not None else None

    # ================================================================== account reads
    def _require_address(self) -> str:
        if not self.address:
            raise AuthRequired("arcus: account reads require ARCUS_ADDRESS")
        return self.address

    async def _account_call(self, fn: Callable[..., Awaitable[Any]], *args, **kw) -> Any:
        try:
            return await fn(*args, **kw)
        except ArcusAccessError as exc:
            raise ExchangeError(WHITELIST_HINT.format(address=self.address), code="AddressNotOnWhitelist") from exc

    async def fetch_positions(self) -> dict[str, Position]:
        addr = self._require_address()
        data = await self._account_call(self.rest.positions, addr, self.account_index)
        out: dict[str, Position] = {}
        for key, row in (data.get("positions") or {}).items():
            sym = self._symbol_of(row) or self._by_id.get(int(key))
            if sym is None:
                continue
            pos = position_from_row(row, sym)
            if pos is not None:
                out[sym] = pos
        return out

    async def fetch_balance(self) -> Balance:
        addr = self._require_address()
        try:
            acct = await self._account_call(self.rest.account, addr, self.account_index)
        except ArcusHTTPError as exc:
            if exc.status == 404:  # "this account has no activity yet"
                return Balance(venue=self.name, equity=ZERO, available=ZERO)
            raise
        margin = maint = ZERO
        for key, p in (acct.get("positions") or {}).items():
            margin += dec(p.get("marginUsed"), ZERO)
            sym = self._symbol_of(p) or self._by_id.get(int(key))
            m = self.markets.get(sym) if sym else None
            mmf = m.extra.get("mmf") if m else None
            if mmf:
                maint += abs(dec(p.get("positionValueNotional"), ZERO)) * mmf
        return Balance(venue=self.name, equity=dec(acct.get("equity"), ZERO),
                       available=dec(acct.get("freeCollateral"), ZERO), margin_used=margin, maintenance_margin=maint)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        addr = self._require_address()
        ticker = self.market(symbol).venue_symbol if symbol else None
        rows = await self._account_call(self.rest.open_orders, addr, self.account_index, ticker)
        out: list[Order] = []
        for row in rows:
            sym = self._symbol_of(row)
            if sym is None or (symbol is not None and sym != symbol):
                continue
            rec = self._find_rec(row.get("orderId"), row.get("clientId"))
            seq = row.get("sequenceNumber")
            if rec is None:
                rec = self._register_foreign(row, sym)
            elif seq is not None and (rec.last_seq is None or int(seq) > rec.last_seq) \
                    and classify_order_frame(row) == "update":
                apply_order_row(rec.order, row, rec.tif)
                rec.last_seq = int(seq)
                self._emit_order(rec.order)
            out.append(rec.order)
        return out

    async def fetch_funding_payments(self, since_ts: float | None = None) -> list[FundingPayment]:
        addr = self._require_address()
        from_us = int(since_ts * 1e6) if since_ts and since_ts > 1e8 else None
        seen: set[tuple] = set()
        out: list[FundingPayment] = []
        to_us: int | None = None
        for _ in range(50):
            page = await self._account_call(self.rest.funding_payments, addr, self.account_index,
                                            from_us=from_us, to_us=to_us, limit=1000)
            new = 0
            for r in page:
                k = (r.get("marketId"), r.get("time"))
                if k in seen:
                    continue
                seen.add(k)
                new += 1
                sym = self._symbol_of(r)
                if sym is not None:
                    out.append(funding_payment_from_row(r, sym))
            if len(page) < 1000 or new == 0:
                break
            to_us = min(int(r["time"]) for r in page if r.get("time") is not None)
        out.sort(key=lambda p: p.ts)
        return out

    # ================================================================== private stream
    def _on_acct_message(self, msg: dict) -> None:
        t = msg.get("type")
        if t == "error":
            text = str(msg.get("message") or "")
            if "whitelist" in text.lower():
                self.access_error = WHITELIST_HINT.format(address=self.address)
                self.log.error(self.access_error)
            return
        if t not in ("subscribed", "channel_data"):
            return
        ch, c = msg.get("channel"), msg.get("contents")
        if not isinstance(c, dict):
            return
        if ch == "orders":
            if t == "subscribed" or c.get("isSnapshot"):
                self._on_orders_snapshot(c)
            else:
                self._on_order_frame(c)
        elif ch == "userFills":
            snap = t == "subscribed" or bool(c.get("isSnapshot"))
            for f in c.get("fills") or []:
                self._on_fill_row(f, snapshot=snap)
            if snap:
                self._fills_synced.set()

    def _on_acct_disconnect(self) -> None:
        self._orders_synced.clear()
        self._fills_synced.clear()

    def _on_orders_snapshot(self, c: dict) -> None:
        rows = list(c.get("openOrders") or []) + list(c.get("recentClosedOrders") or [])
        rows.sort(key=lambda r: int(r.get("sequenceNumber") or 0))
        oids: set[str] = set()
        cids: set[str] = set()
        for r in rows:
            if r.get("orderId"):
                oids.add(str(r["orderId"]))
            if r.get("clientId"):
                cids.add(str(r["clientId"]))
            self._on_order_frame(r, from_snapshot=True)
        self._orders_synced.set()
        # Local live orders absent from the snapshot (openOrders + only the last nRecentClosed closed ones)
        # may have closed while the stream was down: verify them over REST instead of trusting them forever.
        raw = c.get("lastSequenceId")
        snap_seq = int(raw) if raw is not None else None
        with_id: list[_OrderRec] = []
        without_id: list[_OrderRec] = []
        for rec in self._live_recs(include_foreign=True):
            o = rec.order
            if (o.order_id and o.order_id in oids) or (o.client_id and o.client_id in cids):
                continue
            if o.order_id:
                if rec.last_seq is not None and snap_seq is not None and rec.last_seq >= snap_seq:
                    continue  # we already know a state at least as new as the snapshot
                with_id.append(rec)
            elif o.status is OrderStatus.UNKNOWN:
                without_id.append(rec)
        if (with_id or without_id) and self.address:
            self.log.info("arcus: %d local live order(s) missing from the orders snapshot -> verifying",
                          len(with_id) + len(without_id))
            self._background(self._resolve_missing(with_id, without_id), "resolve missing orders")

    def _background(self, coro: Awaitable, what: str) -> None:
        fut = asyncio.ensure_future(coro)
        self._bg_tasks.add(fut)
        fut.add_done_callback(self._bg_tasks.discard)
        fut.add_done_callback(_log_future_error(self.log, what))

    def _mark_gone(self, rec: _OrderRec, reason: str) -> None:
        o = rec.order
        o.status = OrderStatus.CANCELED
        o.error = reason
        o.updated_ts = time.time()
        rec.terminal_mono = time.monotonic()
        rec.cancel_requested_mono = None
        rec.acked.set()
        if rec.modify_fut is not None and not rec.modify_fut.done():
            rec.modify_fut.set_result(("update", o.status))
        self._emit_order(o)

    async def _verify_rec(self, rec: _OrderRec, why: str) -> None:
        """Ask ``GET /v1/order/{id}`` for the truth about one order and apply it (404 + once sequenced = gone)."""
        oid = rec.order.order_id
        if rec.verifying or not oid or not self.address or rec.order.status.is_terminal:
            return
        rec.verifying = True
        try:
            row = await self.rest.order(self.address, self.account_index, oid)
        except ArcusHTTPError as exc:
            if exc.status == 404 and rec.last_seq is not None and not rec.order.status.is_terminal:
                self.log.warning("arcus order %s (%s) unknown to the venue -> marking it gone", oid, why)
                self._mark_gone(rec, f"missing after resync ({why})")
            else:
                self.log.warning("arcus order %s check (%s) failed: %s", oid, why, exc)
            return
        except Exception as exc:
            self.log.warning("arcus order %s check (%s) failed: %r", oid, why, exc)
            return
        finally:
            rec.verifying = False
        if not isinstance(row, dict) or not row.get("orderId"):
            return
        # a terminal REST state is final even if its sequenceNumber is not newer than what we saw
        force = map_order_status(row, rec.tif)[0].is_terminal and classify_order_frame(row) == "update"
        self._on_order_frame(row, from_snapshot=True, force=force)
        if rec.order.status.is_terminal:
            self.log.info("arcus order %s (%s): venue says %s", oid, why, rec.order.status.value)

    async def _resolve_missing(self, with_id: list[_OrderRec], without_id: list[_OrderRec]) -> None:
        for rec in with_id:
            await self._verify_rec(rec, "missing from the orders snapshot")
        if not without_id:
            return
        try:  # unknown-outcome places (no orderId): resting ones are listed by clientId
            rows = await self.rest.open_orders(self.address or "", self.account_index)
        except Exception as exc:
            self.log.warning("arcus openOrders check after resync failed: %r", exc)
            return
        listed = {str(r["clientId"]): r for r in rows if r.get("clientId")}
        for rec in without_id:
            if rec.order.status.is_terminal or rec.order.order_id:
                continue
            row = listed.get(rec.order.client_id)
            if row is not None:
                self._on_order_frame(row)
            else:
                self._mark_gone(rec, "unknown-outcome order not resting after resync")

    def _find_rec(self, oid: Any, cid: Any) -> _OrderRec | None:
        rec = self._recs_by_oid.get(str(oid)) if oid else None
        if rec is None and cid:
            rec = self._recs_by_cid.get(str(cid))
            if rec is not None and oid and rec.order.order_id not in (None, str(oid)):
                return None  # clientId reused by a newer order
        return rec

    def _index(self, rec: _OrderRec) -> None:
        if rec.order.client_id:
            self._recs_by_cid[rec.order.client_id] = rec
        if rec.order.order_id:
            self._recs_by_oid[rec.order.order_id] = rec

    def _register_foreign(self, c: dict, sym: str) -> _OrderRec:
        ro = bool(c["reduceOnly"]) if "reduceOnly" in c else None
        order = order_from_row(c, sym, reduce_only=ro if ro is not None else False)
        try:
            tif = submit_tif(c.get("timeInForce") or "GTT")
        except ValueError:
            tif = "GTT"
        gtt = c.get("goodTilTime")
        rec = _OrderRec(order=order, market_id=int(c.get("marketId") or 0), tif=tif, reduce_only=ro,
                        gtt_us=int(gtt) if gtt else None, foreign=True)
        seq = c.get("sequenceNumber")
        rec.last_seq = int(seq) if seq is not None else None
        rec.acked.set()
        if order.status.is_terminal:
            rec.terminal_mono = time.monotonic()
        self._index(rec)
        return rec

    def _on_order_frame(self, c: dict, *, from_snapshot: bool = False, force: bool = False) -> None:
        oid, cid = c.get("orderId"), c.get("clientId")
        rec = self._find_rec(oid, cid)
        kind = classify_order_frame(c, in_modify=bool(rec and rec.in_modify))
        if kind == "action_reject":
            reason = c.get("rejectionReason")
            if rec is not None:
                rec.last_action_error = reason
                if rec.modify_fut is not None and not rec.modify_fut.done():
                    rec.modify_fut.set_result(("action_reject", reason))
            self.log.debug("arcus cancel/modify of %s failed: %s", oid, reason)
            return
        seq_raw = c.get("sequenceNumber")
        seq = int(seq_raw) if seq_raw is not None else None
        if rec is None:
            sym = self._symbol_of(c)
            if sym is None:
                return
            rec = self._register_foreign(c, sym)
            if kind == "modify_leg":  # still live: the replacement follows under the same orderId
                rec.in_modify = True
                rec.order.status = OrderStatus.OPEN
                rec.order.error = None
                rec.terminal_mono = None
            elif not from_snapshot:
                self._emit_order(rec.order)
            return
        if seq is not None and rec.last_seq is not None and seq <= rec.last_seq and not force:
            return  # stale / replayed
        if kind == "modify_leg":
            rec.in_modify = True
            filled = dec(c.get("filledSize"))
            if filled is not None and filled > rec.order.filled:
                rec.order.filled = filled
            rec.last_seq = seq if seq is not None else rec.last_seq
            return
        was_in_modify = rec.in_modify
        apply_order_row(rec.order, c, rec.tif)
        rec.in_modify = False
        if c.get("rejectionReason") == WOULD_CROSS:
            rec.would_cross = True
        if c.get("goodTilTime"):
            try:
                rec.gtt_us = int(c["goodTilTime"])
            except (TypeError, ValueError):
                pass
        if "reduceOnly" in c:
            rec.reduce_only = bool(c["reduceOnly"])
        if seq is not None and (rec.last_seq is None or seq > rec.last_seq):
            rec.last_seq = seq
        self._index(rec)
        if not rec.order.status.is_terminal:
            rec.terminal_mono = None
            if rec.cancel_requested_mono is not None and rec.order.order_id and not rec.cancel_followup:
                # live after an accepted cancel: the cancel may have overtaken its own unsequenced place
                # (ORDER_NOT_FOUND) -> make sure it really goes
                rec.cancel_followup = True
                self._background(self._cancel_followup(rec), "cancel follow-up")
        else:
            rec.cancel_requested_mono = None
            if rec.terminal_mono is None:
                rec.terminal_mono = time.monotonic()
        fut = rec.modify_fut
        if fut is not None and not fut.done():
            tgt = rec.modify_target
            if was_in_modify or rec.order.status.is_terminal or (
                    tgt is not None and rec.order.price == tgt[0] and rec.order.size == tgt[1]):
                fut.set_result(("update", rec.order.status))
        rec.acked.set()
        self._emit_order(rec.order)

    def _on_fill_row(self, f: dict, *, snapshot: bool) -> None:
        key = fill_key(f)
        if key in self._seen_fills:
            return
        self._seen_fills.add(key)
        self._seen_fills_q.append(key)
        while len(self._seen_fills_q) > 50_000:
            self._seen_fills.discard(self._seen_fills_q.popleft())
        if snapshot and int(f.get("createdAt") or 0) < self._session_start_us:
            return  # history from before this session (positions are reconciled from REST)
        sym = self._symbol_of(f)
        if sym is None:
            self.log.warning("arcus fill for unknown market: %s", f)
            return
        fill = fill_from_row(f, sym)
        if fill.size <= 0:
            return
        if snapshot:
            # Missed while the private socket was down. Positions may already have been re-read from REST
            # (hedger reconcile) and include it, so it is flagged: consumers must not apply it blindly.
            self.log.warning("arcus: recovered fill missed during reconnect: %s %s %s @ %s", sym, fill.side.value,
                             fill.size, fill.price)
            fill = _mark_recovered(fill)
        self._emit_fill(fill)

    def _live_recs(self, *, include_foreign: bool = False) -> list[_OrderRec]:
        seen: set[int] = set()
        out = []
        for rec in list(self._recs_by_cid.values()) + list(self._recs_by_oid.values()):
            if id(rec) in seen:
                continue
            seen.add(id(rec))
            if not rec.order.status.is_terminal and (include_foreign or not rec.foreign):
                out.append(rec)
        return out

    async def _gc(self) -> None:
        cutoff = time.monotonic() - 900.0
        for d in (self._recs_by_cid, self._recs_by_oid):
            for k in [k for k, r in d.items() if r.terminal_mono is not None and r.terminal_mono < cutoff]:
                del d[k]
        # unknown-outcome places that never showed up: resolve them (they would keep the DMS armed forever)
        old = time.time() - 60.0
        stuck = [r for r in self._live_recs(include_foreign=True)
                 if r.order.status is OrderStatus.UNKNOWN and not r.order.order_id and r.order.created_ts < old]
        if stuck and self.address and self.private_ok()[0]:
            await self._resolve_missing([], stuck)

    # ================================================================== trading helpers
    def _require_trading(self) -> ArcusSigner:
        if self._signer is None:
            raise AuthRequired("arcus: trading requires ARCUS_ADDRESS and ARCUS_API_PRIVATE_KEY")
        return self._signer

    def _pool_blocked(self, pool: str) -> float:
        return max(0.0, self._pool_blocked_until.get(pool, 0.0) - time.monotonic())

    def _note_rate_limit(self, data: Any) -> None:
        rl = data.get("rateLimit") if isinstance(data, dict) else None
        if isinstance(rl, dict) and rl.get("pool") in ("order", "cancel"):
            try:
                self._pool_remaining[rl["pool"]] = int(rl.get("remaining"))
            except (TypeError, ValueError):
                pass

    async def _post_signed(self, path: str, build: Callable[[], SignedRequest], *, query_address: bool = True,
                           pool: str | None = None, weight: float = 0.0) -> tuple[int, Any]:
        """POST a freshly signed request; re-sync the clock and retry ONCE on 'timestamp expired'."""
        signer = self._require_trading()
        for attempt in (0, 1):
            req = build()
            try:
                status, data = await self.rest.post(path, canonical_json(req.body), req.headers(signer.api_key),
                                                    params={"address": self.address} if query_address else None,
                                                    weight=weight)
            except ArcusHTTPError as exc:
                if exc.is_timestamp_expired and attempt == 0:
                    self.log.warning("arcus %s: timestamp expired -> clock re-sync + retry", path)
                    await self._sync_clock()
                    continue
                if exc.status == 429 and pool:
                    wait_s = (exc.retry_after_ms or 1000) / 1000.0
                    self._pool_blocked_until[pool] = time.monotonic() + wait_s
                    self.log.warning("arcus %s rate limited (%s pool) for %.1fs: %s", path, pool, wait_s,
                                     (exc.body or {}).get("reason") if isinstance(exc.body, dict) else exc.error)
                raise
            self._note_rate_limit(data)
            return status, data
        raise ExchangeError(f"arcus {path}: timestamp expired twice", retryable=True)  # pragma: no cover

    def _validate(self, m: MarketInfo, size: Decimal, price: Decimal, *, reduce_only: bool,
                  notional_price: Decimal | None = None) -> str | None:
        if not m.active:
            return f"market {m.symbol} is {m.extra.get('status') or 'not active'}"
        if not isinstance(size, Decimal) or not isinstance(price, Decimal):
            return "size and price must be Decimal"
        if size <= 0:
            return "size must be positive"
        if price <= 0:
            return "price must be positive"
        if size % m.step_size != 0:
            return f"size {size} is not a multiple of step {m.step_size}"
        tick = m.tick_for(price)
        if price % tick != 0 or price % m.tick_size != 0:
            return f"price {price} is not a multiple of tick {tick}"
        if m.max_size is not None and size > m.max_size:
            return f"size {size} exceeds maxOrderSize {m.max_size}"
        if not reduce_only:
            if size < m.min_size:
                return f"size {size} below minOrderSize {m.min_size}"
            px = notional_price if notional_price is not None else price
            if size * px < m.min_notional:
                return f"notional {size * px} below minOrderNotional {m.min_notional}"
        return None

    @staticmethod
    def _round_valid(m: MarketInfo, price: Decimal, side: Side) -> Decimal:
        """Round passively (BUY down, SELL up) onto the tick of the band the result lands in."""
        mode = ROUND_FLOOR if side is Side.BUY else ROUND_CEILING
        p = price
        for _ in range(3):
            tick = m.tick_for(p)
            q = (p / tick).to_integral_value(rounding=mode) * tick
            if m.tick_for(q) == tick:
                return q
            p = q
        return q

    async def _await_ack(self, rec: _OrderRec, deadline: float) -> bool:
        if rec.acked.is_set():
            return True
        if self._acct is None or not self._acct.connected:
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        try:
            await asyncio.wait_for(rec.acked.wait(), remaining)
            return True
        except asyncio.TimeoutError:
            return False

    def _apply_write_response(self, rec: _OrderRec, data: Any, http_status: int) -> None:
        if not isinstance(data, dict):
            return
        oid = data.get("orderId")
        if oid and not rec.order.order_id:
            rec.order.order_id = str(oid)
            self._index(rec)
        st = str(data.get("status") or "").upper()
        if http_status == 200 and not rec.acked.is_set() and st in ("OPEN", "FILLED", "CANCELED", "REJECTED",
                                                                   "PARTIALLY_FILLED"):
            row = dict(data)
            row.setdefault("state", st)
            if "originalSize" not in row and "quantity" in row:
                row["originalSize"] = row["quantity"]
            apply_order_row(rec.order, row, rec.tif)
            if row.get("rejectionReason") == WOULD_CROSS:
                rec.would_cross = True
            rec.acked.set()
            self._emit_order(rec.order)

    @staticmethod
    def _result(rec: _OrderRec) -> OrderResult:
        o = rec.order
        if o.status is OrderStatus.REJECTED:
            return OrderResult(ok=False, order=o, error=o.error, would_cross=rec.would_cross, retryable=False)
        if o.status is OrderStatus.EXPIRED:
            return OrderResult(ok=False, order=o, error=o.error)
        return OrderResult(ok=True, order=o, error=o.error)

    def _http_failure(self, rec: _OrderRec | None, exc: ArcusHTTPError) -> OrderResult:
        text = exc.error or str(exc)
        if exc.is_whitelist:
            text = WHITELIST_HINT.format(address=self.address)
        reason = exc.rejection_reason
        would_cross = reason == WOULD_CROSS or "would cross" in text.lower()
        if rec is not None:
            rec.order.status = OrderStatus.REJECTED
            rec.order.error = f"{exc.status}: {text}"
        return OrderResult(ok=False, order=rec.order if rec else None, error=f"arcus {exc.status}: {text}",
                           retryable=exc.retryable or exc.is_transmission, would_cross=would_cross)

    def _forget(self, rec: _OrderRec) -> None:
        if self._recs_by_cid.get(rec.order.client_id) is rec:
            del self._recs_by_cid[rec.order.client_id]
        if rec.order.order_id and self._recs_by_oid.get(rec.order.order_id) is rec:
            del self._recs_by_oid[rec.order.order_id]

    async def _cancel_unknown(self, rec: _OrderRec) -> None:
        try:
            await self.cancel_order(rec.order.symbol, client_id=rec.order.client_id)
        except Exception as exc:  # pragma: no cover - best effort
            self.log.warning("arcus cancel of unknown-outcome order %s failed: %r", rec.order.client_id, exc)

    # ================================================================== trading
    async def place_limit(self, symbol: str, side: Side, size: Decimal, price: Decimal, *, post_only: bool = True,
                          reduce_only: bool = False, client_id: str | None = None) -> OrderResult:
        self._require_trading()
        m = self.markets.get(symbol)
        if m is None:
            return OrderResult(ok=False, error=f"arcus: unknown market {symbol!r}")
        err = self._validate(m, size, price, reduce_only=reduce_only)
        if err:
            return OrderResult(ok=False, error=err)
        ok, why = self.private_ok()
        if not ok:  # a resting quote whose acks/fills we cannot see: refuse (IOC hedges stay allowed)
            return OrderResult(ok=False, retryable=True, error=why)
        return await self._submit(m, side, size, price, tif="ALO" if post_only else "GTT", order_type="LIMIT",
                                  reduce_only=reduce_only, client_id=client_id, wait_for_budget=False)

    async def place_market(self, symbol: str, side: Side, size: Decimal, *, worst_price: Decimal,
                           reduce_only: bool = False, client_id: str | None = None) -> OrderResult:
        self._require_trading()
        m = self.markets.get(symbol)
        if m is None:
            return OrderResult(ok=False, error=f"arcus: unknown market {symbol!r}")
        ref = self._mark_price(symbol)
        px = Decimal(worst_price)
        if ref is not None and ref > 0:  # MARKET price must be within 10% of mark
            px = min(px, ref * (1 + self.market_price_band)) if side is Side.BUY else \
                max(px, ref * (1 - self.market_price_band))
        if px <= 0:
            return OrderResult(ok=False, error=f"invalid worst price {worst_price}")
        px = self._round_valid(m, px, side)
        err = self._validate(m, size, px, reduce_only=reduce_only, notional_price=ref or px)
        if err:
            return OrderResult(ok=False, error=err)
        return await self._submit(m, side, size, px, tif="IOC", order_type="MARKET", reduce_only=reduce_only,
                                  client_id=client_id, wait_for_budget=True)

    async def _submit(self, m: MarketInfo, side: Side, size: Decimal, price: Decimal, *, tif: str, order_type: str,
                      reduce_only: bool, client_id: str | None, wait_for_budget: bool) -> OrderResult:
        signer = self._require_trading()
        cid = client_id or new_client_id("ff")
        if not CLIENT_ID_RE.match(cid):
            return OrderResult(ok=False, error=f"invalid clientId {cid!r} (must match [A-Za-z0-9_-]{{1,36}})")
        prev = self._recs_by_cid.get(cid)
        if prev is not None and not prev.order.status.is_terminal and prev.order.status is not OrderStatus.UNKNOWN:
            return OrderResult(ok=False, error=f"duplicate live clientId {cid}")
        blocked = self._pool_blocked("order")
        if blocked > 0 and not wait_for_budget:
            return OrderResult(ok=False, retryable=True, error=f"arcus order pool rate limited ({blocked:.1f}s)")
        if blocked > 0:
            await asyncio.sleep(min(blocked, 10.0))
        if self._order_bucket is not None:
            if wait_for_budget:
                try:
                    await asyncio.wait_for(self._order_bucket.acquire(), 10.0)
                except asyncio.TimeoutError:
                    return OrderResult(ok=False, retryable=True, error="arcus local order budget exhausted")
            elif not self._order_bucket.try_acquire():
                return OrderResult(ok=False, retryable=True, error="arcus local order budget exhausted")
        gtt_us = self.clock.now_us() + self.good_til_days * US_PER_DAY
        order = Order(venue=self.name, symbol=m.symbol, side=side, price=price, size=size, client_id=cid,
                      status=OrderStatus.PENDING, post_only=tif == "ALO", reduce_only=reduce_only,
                      ioc=tif in ("IOC", "FOK"))
        resting = tif not in ("IOC", "FOK")
        if resting:
            await self._ensure_dms_armed()  # a resting order must never sit without the dead man's switch
        rec = _OrderRec(order=order, market_id=m.market_id, tif=tif, reduce_only=reduce_only, gtt_us=gtt_us)
        self._recs_by_cid[cid] = rec
        deadline = time.monotonic() + float(self.cfg.ack_timeout_s)

        def build() -> SignedRequest:
            return signer.place(market_id=m.market_id, tick_size=m.tick_size, step_size=m.step_size, side=side,
                                price=price, size=size, tif=tif, gtt_us=gtt_us, order_type=order_type,
                                reduce_only=reduce_only, client_id=cid)

        self._writes_inflight += 1
        try:
            status, data = await self._post_signed("/v1/placeOrder", build, pool="order")
        except ValueError as exc:  # local encoding validation
            self._forget(rec)
            return OrderResult(ok=False, error=f"arcus: {exc}")
        except ArcusHTTPError as exc:
            if _outcome_unknown(exc):  # 500/504, or a 5xx without the gateway's "not submitted" marker
                return await self._place_unknown(rec, exc, deadline)
            self._forget(rec)
            self.log.warning("arcus place %s %s %s@%s failed: %s", cid, side.value, size, price, exc)
            return self._http_failure(rec, exc)
        except TRANSPORT_ERRORS as exc:
            return await self._place_unknown(rec, exc, deadline)
        finally:
            self._writes_inflight -= 1
        self._apply_write_response(rec, data, status)
        await self._await_ack(rec, deadline)
        return self._result(rec)

    async def _place_unknown(self, rec: _OrderRec, exc: BaseException, deadline: float) -> OrderResult:
        self.log.warning("arcus place %s outcome unknown: %r", rec.order.client_id, exc)
        if await self._await_ack(rec, deadline):
            return self._result(rec)
        rec.order.status = OrderStatus.UNKNOWN
        rec.order.error = f"transport error: {exc!r}"
        if not rec.order.ioc:  # never leave an orphan quote behind: cancel by clientId
            fut = asyncio.ensure_future(self._cancel_unknown(rec))
            fut.add_done_callback(_log_future_error(self.log, "cancel unknown"))
        return OrderResult(ok=False, order=rec.order, error=rec.order.error, retryable=True)

    async def cancel_order(self, symbol: str, *, order_id: str | None = None, client_id: str | None = None) -> bool:
        signer = self._require_trading()
        m = self.markets.get(symbol)
        if m is None:
            self.log.warning("arcus cancel: unknown market %s", symbol)
            return False
        rec = self._find_rec(order_id, None) if order_id else None
        if rec is None and client_id:
            rec = self._recs_by_cid.get(client_id)
        oid = order_id or (rec.order.order_id if rec else None)
        cid = None if oid else client_id
        if not oid and not cid:
            raise ValueError("cancel_order needs order_id or client_id")
        if rec is not None and rec.order.status.is_terminal:
            return True
        try:
            _, data = await self._post_signed(
                "/v1/cancelOrder", lambda: signer.cancel(market_id=m.market_id, order_id=oid, client_id=cid),
                pool="cancel")
        except ArcusHTTPError as exc:
            if exc.rejection_reason == "ORDER_NOT_FOUND" or "not found" in (exc.error or "").lower():
                return self._cancel_not_found(rec, exc.rejection_reason or "ORDER_NOT_FOUND")
            self.log.warning("arcus cancel %s failed: %s", oid or cid, exc)
            return False
        except ValueError as exc:
            self.log.warning("arcus cancel %s invalid: %s", oid or cid, exc)
            return False
        except TRANSPORT_ERRORS as exc:
            self.log.warning("arcus cancel %s transport error: %r", oid or cid, exc)
            return False
        if isinstance(data, dict) and data.get("orderId") and rec is not None and not rec.order.order_id:
            rec.order.order_id = str(data["orderId"])
            self._index(rec)
        if isinstance(data, dict) and str(data.get("status") or "").upper() == "REJECTED":
            return self._cancel_not_found(rec, str(data.get("rejectionReason") or data.get("error") or "REJECTED"))
        if rec is not None:
            rec.cancel_requested_mono = time.monotonic()
            rec.cancel_attempts += 1
        return True

    def _cancel_not_found(self, rec: _OrderRec | None, reason: str) -> bool:
        """The venue rejected a cancel (normally ORDER_NOT_FOUND). Fine if the order is gone; if we still
        believe it is live, report failure: the cancel may have overtaken its own unsequenced place."""
        if rec is None or rec.order.status.is_terminal:
            return True
        rec.cancel_requested_mono = time.monotonic()  # a later live frame triggers a follow-up cancel
        rec.cancel_attempts += 1
        self.log.info("arcus cancel of %s rejected (%s) while it is %s locally", rec.order.order_id or
                      rec.order.client_id, reason, rec.order.status.value)
        if rec.order.order_id and rec.last_seq is not None:
            self._background(self._verify_rec(rec, f"cancel {reason}"), "order check")
        return False

    async def _cancel_followup(self, rec: _OrderRec) -> None:
        try:
            t0 = rec.cancel_requested_mono
            if t0 is None:
                return
            wait = t0 + self.cancel_recheck_s - time.monotonic()
            if wait > 0:  # frames right after the cancel may predate it (e.g. a partial fill)
                await asyncio.sleep(wait)
            if rec.order.status.is_terminal or rec.cancel_requested_mono is None or self._closing:
                return
            sym, oid = rec.order.symbol, rec.order.order_id
            if rec.cancel_attempts >= self.max_cancel_attempts:
                self.log.warning("arcus order %s still live after %d cancels -> cancelAllOrders(%s)", oid,
                                 rec.cancel_attempts, sym)
                rec.cancel_requested_mono = None
                await self.cancel_all(sym)
                return
            self.log.warning("arcus order %s is live after an accepted cancel -> re-sending the cancel", oid)
            await self.cancel_order(sym, order_id=oid)
        finally:
            rec.cancel_followup = False

    async def cancel_all(self, symbol: str | None = None) -> None:
        signer = self._require_trading()
        body: dict[str, Any] = {"address": self.address, "accountIndex": self.account_index}
        if symbol is not None:
            body["marketId"] = self.market(symbol).market_id
        try:
            await self._post_signed("/v1/cancelAllOrders", lambda: signer.legacy("cancelAllOrders", body),
                                    pool="cancel")
        except TRANSPORT_ERRORS as exc:
            raise ExchangeError(f"arcus cancelAllOrders transport error: {exc!r}", retryable=True) from exc

    async def modify_order(self, order: Order, *, price: Decimal, size: Decimal | None = None) -> OrderResult:
        signer = self._require_trading()
        rec = self._find_rec(order.order_id, order.client_id) if (order.order_id or order.client_id) else None
        m = self.markets.get(order.symbol)
        if m is None:
            return OrderResult(ok=False, order=order, error=f"arcus: unknown market {order.symbol!r}")
        if rec is None or rec.reduce_only is None or rec.tif not in ("ALO", "GTT"):
            # flags unknown (placed outside this session): never amend blind -> cancel it
            if not order.order_id and not order.client_id:
                return OrderResult(ok=False, order=order, error="order has no id")
            await self.cancel_order(order.symbol, order_id=order.order_id, client_id=order.client_id or None)
            return OrderResult(ok=False, order=order, error="order flags unknown; canceled instead of modified")
        if rec.order.status.is_terminal:
            return OrderResult(ok=False, order=rec.order, error=f"order is {rec.order.status.value}")
        deadline = time.monotonic() + float(self.cfg.ack_timeout_s)
        if not rec.order.order_id:
            await self._await_ack(rec, deadline)
            if not rec.order.order_id:
                return OrderResult(ok=False, order=rec.order, retryable=True, error="order not acknowledged yet")
        if rec.modify_fut is not None and not rec.modify_fut.done():
            return OrderResult(ok=False, order=rec.order, retryable=True, error="a modify is already in flight")
        remaining = size if size is not None else rec.order.remaining
        total = rec.order.filled + remaining  # quantity = TOTAL order size (incl. filled)
        if remaining <= 0:
            return OrderResult(ok=False, order=rec.order, error="new size must leave a positive remainder")
        err = self._validate(m, total, price, reduce_only=bool(rec.reduce_only))
        if err is None and not rec.reduce_only and remaining * price < m.min_notional:
            err = f"remaining notional {remaining * price} below minOrderNotional {m.min_notional}"
        if err:
            return OrderResult(ok=False, order=rec.order, error=err)
        if rec.tif == "ALO":  # a crossing ALO modify KILLS the order on Arcus -> refuse locally
            book = self._book_out.get(m.symbol)
            if book is not None and time.time() - book.ts < 2.0:
                if (rec.order.side is Side.BUY and book.best_ask is not None and price >= book.best_ask) or (
                        rec.order.side is Side.SELL and book.best_bid is not None and price <= book.best_bid):
                    return OrderResult(ok=False, order=rec.order, would_cross=True,
                                       error="post-only would cross (local check; order unchanged)")
        if rec.gtt_us is None or rec.gtt_us - self.clock.now_us() < self.min_modify_gtt_lead_days * US_PER_DAY:
            # cannot echo a goodTilTime < ~1 month ahead -> cancel + replace with a fresh expiry
            await self.cancel_order(m.symbol, order_id=rec.order.order_id)
            return await self.place_limit(m.symbol, rec.order.side, remaining, price, post_only=rec.tif == "ALO",
                                          reduce_only=bool(rec.reduce_only))
        if self._pool_blocked("order") > 0:
            return OrderResult(ok=False, order=rec.order, retryable=True, error="arcus order pool rate limited")
        if self._order_bucket is not None and not self._order_bucket.try_acquire():
            return OrderResult(ok=False, order=rec.order, retryable=True, error="arcus local order budget exhausted")
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        rec.modify_fut = fut
        rec.modify_target = (price, total)
        oid = rec.order.order_id
        in_modify = rec.in_modify
        self._writes_inflight += 1
        try:
            try:
                _, data = await self._post_signed(
                    "/v1/modifyOrder",
                    lambda: signer.modify(market_id=m.market_id, tick_size=m.tick_size, step_size=m.step_size,
                                          side=rec.order.side, price=price, size=total, tif=rec.tif,
                                          reduce_only=bool(rec.reduce_only), gtt_us=int(rec.gtt_us or 0),
                                          order_id=oid),
                    query_address=False, pool="order")
            except ArcusHTTPError as exc:
                if not _outcome_unknown(exc):
                    r = self._http_failure(None, exc)
                    return dataclasses.replace(r, order=rec.order)
                # the modify may have been applied: wait for the stream like after a 202
                self.log.warning("arcus modify %s outcome unknown: %s", oid, exc)
                data = None
            except ValueError as exc:
                return OrderResult(ok=False, order=rec.order, error=f"arcus: {exc}")
            except TRANSPORT_ERRORS as exc:
                return OrderResult(ok=False, order=rec.order, retryable=True, error=f"transport error: {exc!r}")
            finally:
                self._writes_inflight -= 1
            if isinstance(data, dict) and str(data.get("status") or "").upper() == "REJECTED" and not fut.done():
                reason = data.get("rejectionReason") or data.get("error") or "REJECTED"
                if reason == "ORDER_NOT_FOUND_FOR_MODIFY":
                    return self._modify_not_found(rec, in_modify)
                return OrderResult(ok=False, order=rec.order, error=str(reason), would_cross=reason == WOULD_CROSS)
            remaining_s = deadline - time.monotonic()
            if self._acct is None or not self._acct.connected or remaining_s <= 0:
                return OrderResult(ok=True, order=dataclasses.replace(rec.order, status=OrderStatus.PENDING))
            try:
                kind, val = await asyncio.wait_for(asyncio.shield(fut), remaining_s)
            except asyncio.TimeoutError:
                return OrderResult(ok=True, order=dataclasses.replace(rec.order, status=OrderStatus.PENDING))
            if kind == "action_reject":
                if val == "ORDER_NOT_FOUND_FOR_MODIFY":
                    return self._modify_not_found(rec, in_modify)
                return OrderResult(ok=False, order=rec.order, error=str(val))
            return self._result(rec) if rec.order.status is not OrderStatus.CANCELED else OrderResult(
                ok=False, order=rec.order, error=rec.order.error or "order canceled")
        finally:
            if rec.modify_fut is fut:
                rec.modify_fut = None
                rec.modify_target = None

    def _modify_not_found(self, rec: _OrderRec, in_modify: bool) -> OrderResult:
        """ORDER_NOT_FOUND_FOR_MODIFY: transient only while the order (or a modify replacement) is not
        sequenced yet; for an order the stream already confirmed it means the order is gone."""
        if in_modify or rec.last_seq is None or rec.order.status.is_terminal:
            return OrderResult(ok=False, order=rec.order, error="ORDER_NOT_FOUND_FOR_MODIFY", retryable=True)
        self._background(self._verify_rec(rec, "ORDER_NOT_FOUND_FOR_MODIFY"), "order check")
        return OrderResult(ok=False, order=rec.order, retryable=False,
                           error="order not found (ORDER_NOT_FOUND_FOR_MODIFY)")

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        signer = self._require_trading()
        m = self.market(symbol)
        lev = max(1, int(leverage))
        if m.max_leverage is not None:
            lev = min(lev, max(1, int(m.max_leverage)))
        body = {"address": self.address, "accountIndex": self.account_index, "marketId": m.market_id,
                "leverage": lev}
        try:
            _, data = await self._post_signed("/v1/setLeverage", lambda: signer.legacy("setLeverage", body),
                                              weight=W_SET_LEVERAGE)
        except ArcusHTTPError as exc:
            reason = exc.body.get("rejectReason") if isinstance(exc.body, dict) else None
            raise ExchangeError(f"arcus setLeverage {symbol} x{lev} failed: {reason or exc.error}",
                                code=reason or exc.status, retryable=exc.retryable) from exc
        if isinstance(data, dict) and str(data.get("status") or "").upper() == "REJECTED":
            raise ExchangeError(f"arcus setLeverage {symbol} x{lev} rejected: {data.get('rejectReason')}",
                                code=data.get("rejectReason"))

    async def arm_dead_mans_switch(self, timeout_s: float) -> None:
        """Arm/refresh the account-wide scheduleCancel (deadline clamped to 5..300 s); <= 0 disarms."""
        signer = self._require_trading()
        body: dict[str, Any] = {"address": self.address, "accountIndex": self.account_index}
        if timeout_s and timeout_s > 0:
            t = min(max(float(timeout_s), 5.0), 300.0)
            t = min(max(t, 5.5), 299.5)  # keep inside the inclusive window despite network latency
            body["time"] = self.clock.now_us() + int(t * 1_000_000)
        try:
            await self._post_signed("/v1/scheduleCancel", lambda: signer.legacy("scheduleCancel", body),
                                    pool="cancel")
        except ArcusHTTPError as exc:
            if exc.status == 503:  # spec: "treat the switch as not armed and retry"
                self._dms_last_ok = None
            raise
        except TRANSPORT_ERRORS as exc:
            raise ExchangeError(f"arcus scheduleCancel transport error: {exc!r}", retryable=True) from exc
        self._dms_last_ok = time.monotonic() if "time" in body else None

    async def _refresh_dms(self) -> None:
        """Keep the switch armed only while something of ours may rest: every arm/refresh is charged to the
        lifetime cancel pool, and a switch that fires with nothing resting still burns a daily auto-fire."""
        healthy = self._acct is not None and self._acct.is_healthy(self.private_stale_s) \
            and self._orders_synced.is_set()
        if not healthy:
            self.log.warning("arcus private stream unhealthy: NOT refreshing the dead man's switch")
            return
        async with self._dms_lock:
            if self._live_recs(include_foreign=True) or self._writes_inflight:
                await self.arm_dead_mans_switch(float(self.cfg.dead_mans_switch_s))
            elif self._dms_last_ok is not None:
                self.log.info("arcus: nothing resting -> disarming the dead man's switch")
                await self.arm_dead_mans_switch(0)

    async def _ensure_dms_armed(self) -> None:
        """Arm the switch before a resting order goes out if it is not (freshly) armed. Best effort: a
        failure is logged and ``can_increase_exposure`` refuses new exposure while orders rest unprotected."""
        dms = float(self.cfg.dead_mans_switch_s or 0)
        if dms <= 0 or not self.has_credentials:
            return
        if self._dms_last_ok is not None and time.monotonic() - self._dms_last_ok < dms / 3.0:
            return
        if self._pool_blocked("cancel") > 0:  # the arm would only be rejected again (cancel pool 429)
            return

        async def arm_if_stale() -> None:
            async with self._dms_lock:  # a concurrent refresh may just have armed it
                if self._dms_last_ok is None or time.monotonic() - self._dms_last_ok >= dms / 3.0:
                    await self.arm_dead_mans_switch(dms)

        try:
            await asyncio.wait_for(arm_if_stale(), 3.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.log.warning("arcus: arming the dead man's switch before placing failed: %r", exc)

    # ================================================================== health
    def _pool_health(self, pool: str) -> float:
        """Remaining fraction of a lifetime per-subaccount pool (order / cancel); 0 while it is 429-blocked."""
        if self._pool_blocked(pool) > 0:
            return 0.0
        h = 1.0
        rl = (self._rate_limit or {}).get(pool) if isinstance(self._rate_limit, dict) else None
        cap = 0
        if isinstance(rl, dict):
            try:
                cap, used = int(rl.get("cap") or 0), int(rl.get("used") or 0)
            except (TypeError, ValueError):
                cap, used = 0, 0
            if cap > 0:
                h = min(h, max(0.0, (cap - used) / cap))
        rem = self._pool_remaining.get(pool)
        if rem is not None and rem >= 0 and cap > 0:
            h = min(h, max(0.0, rem / cap))
        return h

    def budget_health(self) -> float:
        if not self.has_credentials:
            return 1.0
        # cancels (and DMS refreshes) draw on their own lifetime pool: running it dry means stale quotes
        # can no longer be pulled, so it limits the requote pace just like the order pool
        h = min(self._pool_health("order"), self._pool_health("cancel"))
        if h <= 0:
            return 0.0
        if self._order_bucket is not None:
            b = self._order_bucket
            b._refill()  # noqa: SLF001 - local helper class
            h = min(h, max(0.0, b._tokens / b.capacity))  # noqa: SLF001
        return max(0.0, min(1.0, h))

    def stats(self) -> dict:
        """Diagnostics for monitoring."""
        return {
            "books": {s: {"synced": b.synced, "seq": b.seq, "gaps": b.gaps, "resyncs": b.resyncs,
                          "repairs": b.repairs, "levels": b.levels()} for s, b in self._books.items()},
            "md": None if self._md is None else {"connected": self._md.connected, "connects": self._md.connects,
                                                  "frame_age": self._md.frame_age()},
            "acct": None if self._acct is None else {"connected": self._acct.connected,
                                                      "connects": self._acct.connects,
                                                      "frame_age": self._acct.frame_age()},
            "live_orders": len(self._live_recs()),
            "private": (self.private_ok()[1] or "ok") if self.has_credentials else None,
            "dms_armed": self._dms_armed() if self.has_credentials else None,
            "rate_limit": self._rate_limit,
            "budget_health": self.budget_health(),
            "clock_offset_ms": self.clock.offset_ns / 1e6,
            "access_error": self.access_error,
        }


def _outcome_unknown(exc: ArcusHTTPError) -> bool:
    """A failed write whose effect is unknown: any 5xx except a gateway JSON error saying it was not
    submitted (``errorType`` Transmission / Unavailable). CDN/LB 502/52x pages can follow a forwarded request."""
    return exc.status >= 500 and not exc.is_definitive_5xx


def _mark_recovered(fill: Fill) -> Fill:
    """Flag a fill replayed from the ``userFills`` snapshot after a reconnect (``Fill.recovered``)."""
    return dataclasses.replace(fill, recovered=True)


def _log_future_error(logger, what: str) -> Callable[[asyncio.Future], None]:
    def cb(fut: asyncio.Future) -> None:
        if fut.cancelled():
            return
        exc = fut.exception()
        if exc is not None:
            logger.warning("arcus %s failed: %r", what, exc)
    return cb


__all__ = ["ArcusExchange"]
