"""Hedger: keeps ``maker_pos + taker_pos (+ in-flight) == 0`` for one pair with taker IOC orders.

Positions are tracked from fill listeners (the ONLY source of fills, see ``exchanges.base``) and
re-anchored to venue truth by reconciles. In-flight hedges are keyed by our client id and expire after
``hedge_timeout_s``; an expired hedge is never re-sent blindly (Lighter: an unknown-outcome tx may still
execute) - the venue positions are read instead.

Hedging is risk-reducing and therefore ALWAYS runs, also while quoting is paused or the risk manager halted.

Fills replayed by an adapter after a private-stream reconnect (``Fill.recovered``) are never added blindly: a
reconcile during the outage may already have adopted them into the position. They trigger a two-read reconcile
instead (unless they belong to one of our in-flight orders), and so does every private-stream resync.
"""

from __future__ import annotations

import asyncio
import collections
import logging
import random
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

from farmer.config import RiskConfig, StrategyConfig
from farmer.exchanges.base import Exchange
from farmer.models import ZERO, Fill, MarketInfo, Order, OrderBook, OrderResult, OrderStatus, Side
from farmer.strategy.mapping import PairSpec
from farmer.strategy.pricing import worst_taker_price
from farmer.utils.ids import new_client_id

log = logging.getLogger("farmer.hedger")

TAKER = "taker"
MAKER = "maker"
# venue cancel reasons of an unfilled IOC that a plain retry cannot fix (Lighter: canceled-margin-not-allowed, ...)
_PERMANENT_CANCEL_HINTS = ("margin-not-allowed", "invalid-balance", "position-not-allowed")


def _sign(x: Decimal) -> int:
    return 1 if x > 0 else (-1 if x < 0 else 0)


def min_trade_size(m: MarketInfo, price: Decimal | None) -> Decimal:
    """Smallest base size ``m`` accepts at ``price`` (rounded UP to the step)."""
    need = max(m.min_size, m.step_size)
    if price is not None and price > 0 and m.min_notional > 0:
        need = max(need, m.min_notional / price)
    n = (need / m.step_size).to_integral_value(rounding="ROUND_CEILING")
    return n * m.step_size


def private_stream_ok(ex: Exchange) -> tuple[bool, str]:
    """Health of ``ex``'s private (orders / fills) stream; True when the adapter offers no such hint.

    Adapters may expose ``private_ok() -> (bool, reason)`` or ``private_stream_ok()`` /
    ``fill_stream_healthy() -> bool``. A failing check counts as unhealthy.
    """
    for name in ("private_ok", "private_stream_ok", "fill_stream_healthy"):
        fn = getattr(ex, name, None)
        if not callable(fn):
            continue
        try:
            r = fn()
        except Exception as exc:
            return False, f"{ex.name} private stream check failed: {exc!r}"
        if isinstance(r, tuple):
            ok, why = bool(r[0]), str(r[1] or "") if len(r) > 1 else ""
        else:
            ok, why = bool(r), ""
        return ok, ("" if ok else (why or f"{ex.name} private stream down"))
    return True, ""


@dataclass
class _Inflight:
    client_id: str
    leg: str  # TAKER (hedge venue) | MAKER (emergency / close IOC on the maker venue)
    side: Side
    size: Decimal  # abs order size
    remaining: Decimal  # SIGNED part not yet filled (counts toward delta)
    deadline: float
    role: str = "hedge"  # store role of its fills: hedge | emergency | close
    reduce_only: bool = False
    filled: Decimal = ZERO  # abs size of fills seen
    sent_ts: float = field(default_factory=time.time)
    unknown: bool = False  # outcome of the send is unknown (transport error / UNKNOWN status)
    terminal: bool = False  # venue reported the order finished; only its unseen fills remain in flight
    terminal_ts: float = 0.0
    sending: bool = False  # place_market has not returned yet (never expired / dropped meanwhile)
    finish_err: tuple[bool, str, str] | None = None  # failure seen by the order listener while sending


class Hedger:
    """One per pair. ``run()`` until ``stop()``; positions are always tracked (listeners)."""

    def __init__(self, pair: PairSpec, maker: Exchange, taker: Exchange, scfg: StrategyConfig, rcfg: RiskConfig,
                 risk: Any | None = None, store: Any | None = None, *, notifier: Any | None = None,
                 clock: Callable[[], float] = time.time) -> None:
        self.pair = pair
        self.maker = maker
        self.taker = taker
        self.scfg = scfg
        self.rcfg = rcfg
        self.risk = risk
        self.store = store
        self.notifier = notifier
        self.clock = clock
        self.symbol = pair.symbol
        self.ms = pair.maker_symbol
        self.ts = pair.taker_symbol
        self.aggregate_s = max(0.0, float(scfg.hedge_aggregate_ms) / 1000.0)
        self.timeout_s = float(scfg.hedge_timeout_s)
        self.retry_base_s = max(0.01, float(scfg.hedge_retry_backoff_s))
        self.retry_max_s = float(scfg.hedge_retry_max_s)
        self.tick_s = max(0.01, min(1.0, float(scfg.loop_interval_s)))
        self.reconcile_interval_s = float(scfg.reconcile_interval_s)
        self.reconcile_confirm_s = float(scfg.reconcile_confirm_s)
        self.quiet_s = float(scfg.reconcile_quiet_s)
        self.max_hedge_notional = scfg.max_hedge_notional
        self.unhedged_grace_s = float(rcfg.unhedged_grace_s)
        self.io_timeout_s = float(scfg.hedge_io_timeout_s)
        self.fill_wait_s = float(scfg.hedge_fill_wait_s)  # wait for fills a terminal update announced
        self.reconcile_retry_s = float(scfg.reconcile_retry_s)
        # an in-flight entry whose send is still pending is never expired; give up on that after this long
        self.send_stuck_s = 4 * self.io_timeout_s
        # a venue-reported fill the position reads do not show yet is waited for at most this long
        self.terminal_max_s = max(3 * self.timeout_s, self.fill_wait_s)
        # after a maker-leg emergency flatten: no quoting for this long; halt after N emergencies per hour
        self.emergency_cooldown_s = float(rcfg.emergency_cooldown_s)
        self.max_emergencies_per_hour = int(rcfg.max_emergencies_per_hour)

        self.maker_pos = ZERO
        self.taker_pos = ZERO
        self._last_px: Decimal | None = None
        self._inflight: dict[str, _Inflight] = {}  # both legs, keyed by client id
        # orders whose venue-reported fills were adopted from a position read before their fill frames arrived
        self._settled_cids: collections.OrderedDict[str, None] = collections.OrderedDict()
        self._wake = asyncio.Event()
        self._fill_evt = asyncio.Event()
        self._stop = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._maker_fill_pending = False
        self._fill_epoch = 0
        self._last_maker_fill = 0.0
        self._last_taker_fill = 0.0
        self._last_reconcile = clock()
        self._periodic_gap = self.reconcile_interval_s * random.uniform(0.8, 1.2)
        self._reconcile_requested: str | None = None
        self._reconcile_requested_ts = 0.0
        self._reconciling = False
        self._reconcile_fails = 0
        self._reconcile_retry_at = 0.0
        self._fail_count = 0
        self._retry_at = 0.0
        self._last_error: str | None = None
        self._unhedged_since: float | None = None
        self.escalated = False
        self._escalated_reason: str | None = None
        self._last_emergency = 0.0
        self._emergency_ts: collections.deque[float] = collections.deque()
        self._emergency_block_until = 0.0
        self._stream_ok: dict[str, bool] = {}
        self._halt_requested = False
        self.close_mode = False  # only reduce-only hedges (close_all taker mode)
        self._running = False
        self._bg: set[asyncio.Future] = set()
        self._warned: dict[str, float] = {}
        self.stats: dict[str, Any] = {
            "hedges_sent": 0, "hedge_fills": 0, "hedge_volume": ZERO, "maker_fills": 0, "maker_volume": ZERO,
            "hedge_failures": 0, "reconciles": 0, "adoptions": 0, "emergencies": 0, "expired": 0,
        }

        maker.add_fill_listener(self._on_maker_fill)
        taker.add_fill_listener(self._on_taker_fill)
        maker.add_order_listener(self._on_maker_order)
        taker.add_order_listener(self._on_taker_order)

    # ------------------------------------------------------------------ state
    def set_positions(self, maker_pos: Decimal, taker_pos: Decimal) -> None:
        """Seed positions from venue reads (startup)."""
        self.maker_pos = Decimal(maker_pos)
        self.taker_pos = Decimal(taker_pos)
        self._fill_epoch += 1
        self._wake.set()

    def _sum(self, leg: str | None = None) -> Decimal:
        return sum((e.remaining for e in self._inflight.values() if leg is None or e.leg == leg), ZERO)

    @property
    def inflight(self) -> Decimal:
        """Signed size of hedges/flatten orders sent but not yet filled or finished."""
        return self._sum()

    @property
    def taker_inflight(self) -> Decimal:
        return self._sum(TAKER)

    @property
    def maker_inflight(self) -> Decimal:
        return self._sum(MAKER)

    @property
    def delta(self) -> Decimal:
        return self.maker_pos + self.taker_pos + self.inflight

    @property
    def unhedged(self) -> Decimal:
        """Delta that is actually on the books (excludes in-flight orders)."""
        return self.maker_pos + self.taker_pos

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    def has_inflight(self) -> bool:
        return bool(self._inflight)

    def ref_price(self) -> Decimal | None:
        for ex, s in ((self.taker, self.ts), (self.maker, self.ms)):
            try:
                b = ex.get_book(s)
            except Exception:
                b = None
            if b is not None and b.mid is not None and b.mid > 0:
                return b.mid
        return self._last_px

    def hedge_min(self, price: Decimal | None = None) -> Decimal:
        return min_trade_size(self.pair.taker_market, price if price is not None else self.ref_price())

    def is_flat(self, maker_min: Decimal | None = None) -> bool:
        """Both legs below their venue minimums and nothing in flight.

        ``maker_min`` raises the maker-leg dust threshold (e.g. to the pair minimum, below which a forced-close
        trader stops quoting)."""
        px = self.ref_price()
        mmin = min_trade_size(self.pair.maker_market, px)
        if maker_min is not None:
            mmin = max(mmin, maker_min)
        return (not self._inflight and abs(self.maker_pos) < mmin
                and abs(self.taker_pos) < min_trade_size(self.pair.taker_market, px))

    def is_hedged(self) -> bool:
        """No in-flight orders and the residual delta is below the taker minimum (dust)."""
        return not self._inflight and abs(self.delta) < self.hedge_min()

    def quoting_blocked(self) -> str | None:
        """Reason the trader must not quote (None = ok)."""
        if self.escalated:
            return f"hedger escalated: {self._escalated_reason}"
        left = self._emergency_block_until - self.clock()
        if left > 0:
            return f"hedger: cooling down after an emergency flatten ({left:.0f}s left)"
        return None

    # ------------------------------------------------------------------ listeners
    def _on_recovered_fill(self, f: Fill, leg: str) -> bool:
        """A fill replayed after a private-stream reconnect (``Fill.recovered``). A reconcile during the outage
        may already have adopted it into the position (venue reads include it): never add it blindly, let a
        two-read reconcile set the position. An in-flight order of ours cannot have been adopted (reconcile
        needs an empty in-flight set or drops expired entries first), so its fills are applied as usual.
        Late fills of an order whose venue-reported fills were adopted on expiry are treated the same way.
        Returns True when handled here."""
        settled = bool(f.client_id) and f.client_id in self._settled_cids
        if not settled and not f.recovered:
            return False
        e = self._inflight.get(f.client_id or "")
        if e is not None and e.leg == leg:
            return False
        if leg == MAKER:
            self.stats["maker_fills"] += 1
            self.stats["maker_volume"] += f.notional
            self._record(f, "maker" if f.is_maker is not False else "manual")
        else:
            self.stats["hedge_fills"] += 1
            self.stats["hedge_volume"] += f.notional
            self._record(f, "hedge")
        log.warning("%s: %s %s fill %s %s @ %s (already in an adopted position?) -> reconcile with the venue",
                    self.symbol, "late" if settled else "recovered", leg, f.side.value, f.size, f.price)
        self.request_reconcile("recovered fills after reconnect")
        return True

    def _on_maker_fill(self, f: Fill) -> None:
        if f.symbol != self.ms:
            return
        if self._on_recovered_fill(f, MAKER):
            return
        self.maker_pos += f.signed_size
        self._last_px = f.price
        self._fill_epoch += 1
        now = self.clock()
        e = self._inflight.get(f.client_id or "")
        role = "maker" if f.is_maker is not False else "manual"
        if e is not None and e.leg == MAKER:
            self._apply_fill(e, f)
            role = e.role
        else:
            self._last_maker_fill = now
            self._maker_fill_pending = True
            self.stats["maker_fills"] += 1
            self.stats["maker_volume"] += f.notional
        self._record(f, role)
        self._fill_evt.set()
        self._wake.set()

    def _on_taker_fill(self, f: Fill) -> None:
        if f.symbol != self.ts:
            return
        if self._on_recovered_fill(f, TAKER):
            return
        self.taker_pos += f.signed_size
        self._last_px = self._last_px or f.price
        self._fill_epoch += 1
        self._last_taker_fill = self.clock()
        e = self._inflight.get(f.client_id or "")
        role = "hedge"
        if e is not None and e.leg == TAKER:
            self._apply_fill(e, f)
            role = e.role
        self.stats["hedge_fills"] += 1
        self.stats["hedge_volume"] += f.notional
        self._fail_count = 0
        self._retry_at = 0.0
        self._record(f, role)
        self._fill_evt.set()
        self._wake.set()

    def _apply_fill(self, e: _Inflight, f: Fill) -> None:
        e.filled += f.size
        rem = e.remaining - f.signed_size
        if _sign(rem) != _sign(e.remaining):  # over-fill / crossed zero: nothing left in flight
            rem = ZERO
        e.remaining = rem
        if e.terminal and rem == 0:  # the late fills announced by the terminal update have all arrived
            self._inflight.pop(e.client_id, None)

    def _on_maker_order(self, o: Order) -> None:
        if o.symbol == self.ms:
            self._on_order(o, MAKER)

    def _on_taker_order(self, o: Order) -> None:
        if o.symbol == self.ts:
            self._on_order(o, TAKER)

    def _on_order(self, o: Order, leg: str) -> None:
        e = self._inflight.get(o.client_id)
        if e is None or e.leg != leg or e.terminal:
            return
        if o.status is OrderStatus.UNKNOWN:
            e.unknown = True
            return
        if not o.status.is_terminal:
            return
        self._finish(e, o)
        self._wake.set()

    def _finish(self, e: _Inflight, o: Order | None) -> None:
        """Terminal update: clear the unfilled remainder.

        If the venue reports more filled than the fills we received (terminal frame overtook its fills, or the
        adapter gave up waiting for them), the unseen part STAYS in flight: it is either confirmed by the late
        fills or, once it expires, by a position reconcile - never hedged a second time.
        """
        if o is not None and o.filled > e.filled:
            missing = min(o.filled, e.size) - e.filled
            if missing > 0:
                log.info("%s %s order %s ended %s: %s filled, %s not yet seen as fills -> waiting", self.symbol,
                         e.leg, e.client_id, o.status.value, o.filled, missing)
                e.terminal = True
                e.terminal_ts = self.clock()
                e.remaining = missing * e.side.sign
                e.deadline = max(e.deadline, e.terminal_ts + self.fill_wait_s)
                return
        self._inflight.pop(e.client_id, None)
        if o is None:
            return
        if o.filled <= 0 and e.filled <= 0:
            reason = o.error or o.status.value
            # a cancel for lack of margin / balance repeats on every retry: count it toward the circuit breaker
            permanent = o.status is OrderStatus.REJECTED or any(h in reason for h in _PERMANENT_CANCEL_HINTS)
            failure = (not permanent, f"{e.leg} {e.client_id}: {reason}",
                       self.taker.name if e.leg == TAKER else self.maker.name)
            if e.sending:  # the send call is still pending: it accounts the failure once (with its retryable flag)
                e.finish_err = failure
                return
            self._note_failure(retryable=failure[0], err=failure[1], venue=failure[2])

    # ------------------------------------------------------------------ loop
    async def run(self) -> None:
        """Hedge loop until ``stop()``. Exceptions are logged and the loop continues with backoff."""
        self._running = True
        errors = 0
        try:
            while not self._stop.is_set():
                try:
                    await self._wait(self.tick_s)
                    if self._stop.is_set():
                        break
                    if self._maker_fill_pending and self.aggregate_s > 0:
                        await asyncio.sleep(self.aggregate_s)  # coalesce bursts of maker fills
                    self._maker_fill_pending = False
                    await self.step()
                    errors = 0
                except asyncio.CancelledError:
                    raise
                except Exception:
                    errors += 1
                    log.exception("%s hedger step failed (%d)", self.symbol, errors)
                    await asyncio.sleep(min(10.0, self.retry_base_s * 2 ** min(errors, 6)))
        finally:
            self._running = False

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def resume(self) -> None:
        """Allow ``run()`` again after ``stop()``."""
        self._stop.clear()

    def request_reconcile(self, reason: str = "requested") -> None:
        self._reconcile_requested = reason
        self._reconcile_requested_ts = self.clock()
        self._wake.set()

    async def _wait(self, timeout: float) -> None:
        if not self._wake.is_set():
            try:
                await asyncio.wait_for(self._wake.wait(), timeout)
            except asyncio.TimeoutError:
                pass
        self._wake.clear()

    def _poll_streams(self) -> None:
        """Re-confirm positions after a private stream comes back (fills during the gap are replayed or lost)."""
        for leg, ex in ((MAKER, self.maker), (TAKER, self.taker)):
            ok, why = private_stream_ok(ex)
            prev = self._stream_ok.get(leg)
            self._stream_ok[leg] = ok
            if prev is True and not ok:
                log.warning("%s: %s private stream unhealthy (%s)", self.symbol, ex.name, why)
            elif prev is False and ok:
                log.info("%s: %s private stream back -> reconcile positions", self.symbol, ex.name)
                self.request_reconcile(f"{ex.name} private stream resynced")

    def _send_pending(self, e: _Inflight, now: float) -> bool:
        """True while the place call of ``e`` has not returned (it may still be queued for the venue)."""
        return e.sending and now - e.sent_ts < self.send_stuck_s

    async def step(self) -> None:
        """One evaluation: expiry/reconcile, escalation, hedge, emergency, periodic reconcile."""
        self._poll_streams()
        now = self.clock()
        hold = False
        if self._inflight:
            expired = [e for e in self._inflight.values() if e.deadline <= now and not self._send_pending(e, now)]
            if expired and len(expired) == len(self._inflight) and now >= self._reconcile_retry_at:
                log.warning("%s: %d in-flight order(s) expired without terminal update/fills -> reconcile",
                            self.symbol, len(expired))
                if await self._throttled_reconcile("in-flight expired", drop_inflight=True):
                    self.stats["expired"] += len(expired)
        elif self._reconcile_requested and now >= self._reconcile_retry_at:
            ok = await self._throttled_reconcile(self._reconcile_requested)
            # positions are known to be off: don't hedge on stale numbers for a while
            hold = not ok and now - self._reconcile_requested_ts < self.timeout_s
        now = self.clock()
        self._update_escalation(now)
        if not hold:
            await self._maybe_hedge(now)
        await self._maybe_emergency(self.clock())
        now = self.clock()
        quiet = (not self._inflight and now - self._last_maker_fill >= self.quiet_s
                 and now - self._last_taker_fill >= self.quiet_s)
        if quiet and self.reconcile_interval_s > 0 and now - self._last_reconcile >= self._periodic_gap:
            # jittered so pairs don't all read positions at once (Lighter: one shared 60 req/min budget)
            self._periodic_gap = self.reconcile_interval_s * random.uniform(0.8, 1.2)
            await self.reconcile("periodic")

    # ------------------------------------------------------------------ escalation
    def _update_escalation(self, now: float) -> None:
        unhedged = self.unhedged
        px = self.ref_price()
        if unhedged != 0 and abs(unhedged) >= self.hedge_min(px):
            if self._unhedged_since is None:
                self._unhedged_since = now
        else:
            self._unhedged_since = None
        if self._unhedged_since is None:
            if self.escalated:
                log.info("%s: unhedged delta resolved; quoting may resume", self.symbol)
            self.escalated = False
            self._escalated_reason = None
            return
        age = now - self._unhedged_since
        notional = abs(unhedged) * px if px is not None else None
        reason = None
        if age > float(self.rcfg.unhedged_timeout_s):
            reason = f"unhedged {unhedged} for {age:.1f}s"
        elif notional is not None and notional > self.rcfg.max_unhedged_notional and age >= self.unhedged_grace_s:
            reason = f"unhedged notional {notional:.2f} > {self.rcfg.max_unhedged_notional}"
        if reason and not self.escalated:
            log.warning("%s: ESCALATE (%s): pausing quotes, hedging with 2x slippage", self.symbol, reason)
            self._event("hedge_escalated", reason=reason, unhedged=unhedged)
            self._retry_at = 0.0  # retry immediately with the wider limit
        self.escalated = reason is not None
        self._escalated_reason = reason

    # ------------------------------------------------------------------ hedging
    def _note_failure(self, *, retryable: bool, err: str, venue: str | None = None) -> None:
        self._fail_count += 1
        self.stats["hedge_failures"] += 1
        self._last_error = err
        delay = min(self.retry_max_s, self.retry_base_s * 2 ** min(self._fail_count - 1, 10))
        self._retry_at = self.clock() + delay
        if retryable:
            log.warning("%s hedge failed (retry in %.2fs): %s", self.symbol, delay, err)
        else:
            log.error("%s hedge failed permanently (retry in %.2fs): %s", self.symbol, delay, err)
            if self.risk is not None:
                try:
                    self.risk.record_error(venue or self.taker.name, err)
                except Exception:  # pragma: no cover
                    log.exception("risk.record_error failed")

    async def _book(self, ex: Exchange, sym: str) -> OrderBook | None:
        """Fresh WS book, else a REST snapshot, else the stale WS book (better than not hedging)."""
        try:
            b = ex.get_book(sym)
        except Exception:
            b = None
        if b is not None and b.bids and b.asks and b.age() <= float(self.rcfg.stale_book_s):
            return b
        try:
            fb = await asyncio.wait_for(ex.fetch_book(sym, depth=20), self.io_timeout_s)
            if fb is not None and (fb.bids or fb.asks):
                return fb
        except Exception as exc:
            self._warn(f"book-{ex.name}", f"{self.symbol}: REST book on {ex.name} failed: {exc!r}")
        return b

    def _cap(self, size: Decimal, m: MarketInfo, px: Decimal) -> Decimal:
        caps = []
        if m.max_size is not None and m.max_size > 0:
            caps.append(m.max_size)
        lim = m.extra.get("order_quote_limit") if isinstance(m.extra, dict) else None
        if isinstance(lim, Decimal) and lim > 0 and px > 0:
            caps.append(lim / px)
        if self.max_hedge_notional is not None and px > 0:
            caps.append(Decimal(str(self.max_hedge_notional)) / px)
        if caps:
            size = min(size, m.round_size(min(caps)))
        return size

    async def _maybe_hedge(self, now: float) -> None:
        if now < self._retry_at:
            return
        async with self._send_lock:
            delta = self.delta
            if delta == 0:
                return
            tm = self.pair.taker_market
            q = -delta
            size = tm.round_size(abs(q))  # toward zero on the TAKER step
            if size <= 0:
                return
            px = self.ref_price()
            if px is None:
                self._warn("nopx", f"{self.symbol}: delta {delta} but no price to size the hedge")
                return
            if not tm.is_tradeable_size(size, px):
                return  # residual below the venue minimum: wait for more fills
            side = Side.BUY if q > 0 else Side.SELL
            size = self._cap(size, tm, px)
            if size <= 0 or not tm.is_tradeable_size(size, px):
                return
            # reduce-only only if it strictly reduces the taker leg INCLUDING hedges already in flight
            tp_eff = self.taker_pos + self.taker_inflight
            reduce_only = tp_eff != 0 and _sign(tp_eff) == -side.sign and size <= abs(tp_eff)
            if self.close_mode and not reduce_only:
                return  # flatten mode: never open on the taker leg
            book = await self._book(self.taker, self.ts)
            if book is None:
                self._note_failure(retryable=True, err="no taker book")
                return
            mult = Decimal(2) if self.escalated else Decimal(1)
            slip = self.pair.cfg.max_slippage_bps * mult
            worst = worst_taker_price(side, book, size, slip, tm)
            if worst is None:
                self._note_failure(retryable=True, err="taker book has no touch")
                return
            if not reduce_only and not tm.is_tradeable_size(size, worst):
                # the venue/adapter checks min notional at the protective (worst) price: a SELL just above the
                # minimum at mid fails there on every retry -> over-hedge by the few steps needed (dust) instead
                bumped = self._cap(min_trade_size(tm, worst), tm, px)
                if bumped > size and bumped - abs(q) < self.hedge_min(px):
                    size = bumped
                    worst = worst_taker_price(side, book, size, slip, tm)
                if worst is None or not tm.is_tradeable_size(size, worst):
                    self._warn("min-worst", f"{self.symbol}: hedge {side.value} {size} below the taker minimum at "
                                            f"its worst price {worst}; treating delta {delta} as dust")
                    return
            await self._send(TAKER, side, size, worst, reduce_only=reduce_only, role="hedge")

    async def _send(self, leg: str, side: Side, size: Decimal, worst: Decimal, *, reduce_only: bool,
                    role: str) -> bool:
        """Register in-flight BEFORE sending (fills may arrive before the call returns), then place.

        The expiry deadline counts from the RETURN of the place call (venue acceptance), not from registration:
        a send can queue behind the venue's rate budget for longer than ``hedge_timeout_s``. The call is bounded
        by ``hedge_io_timeout_s`` but never cancelled (shielded): on timeout the entry stays in flight (outcome
        unknown) and the late result is applied when it arrives; until then the entry is never expired.
        """
        ex, sym = (self.taker, self.ts) if leg == TAKER else (self.maker, self.ms)
        cid = new_client_id("fh" if leg == TAKER else "fe")
        now = self.clock()
        e = _Inflight(client_id=cid, leg=leg, side=side, size=size, remaining=size * side.sign,
                      deadline=now + self.timeout_s, role=role, reduce_only=reduce_only, sent_ts=now, sending=True)
        self._inflight[cid] = e
        self.stats["hedges_sent"] += 1
        log.info("%s %s %s %s %s worst=%s reduce_only=%s (delta before %s)", self.symbol, role, ex.name, side.value,
                 size, worst, reduce_only, self.delta - e.remaining)
        task = asyncio.ensure_future(ex.place_market(sym, side, size, worst_price=worst, reduce_only=reduce_only,
                                                     client_id=cid))
        try:
            res = await asyncio.wait_for(asyncio.shield(task), self.io_timeout_s if self.io_timeout_s > 0 else None)
        except asyncio.CancelledError:  # e.g. shutdown: the venue call itself is not interrupted
            e.unknown = True
            task.add_done_callback(lambda t, e=e, ex=ex: self._late_send_result(e, ex, t))
            raise
        except asyncio.TimeoutError:
            e.unknown = True  # still sending: may execute any time -> stays in flight, never re-sent blindly
            task.add_done_callback(lambda t, e=e, ex=ex: self._late_send_result(e, ex, t))
            self._note_failure(retryable=True, venue=ex.name,
                               err=f"{ex.name} place_market still pending after {self.io_timeout_s:.0f}s")
            return False
        except Exception as exc:  # outcome unknown: keep in flight until it expires, then reconcile
            e.sending = False
            e.unknown = True
            self._restart_deadline(e)
            self._note_failure(retryable=True, err=f"{ex.name} place_market raised {exc!r}", venue=ex.name)
            return False
        return self._on_send_result(e, ex, res)

    def _restart_deadline(self, e: _Inflight) -> None:
        if not e.terminal and self._inflight.get(e.client_id) is e:
            e.deadline = max(e.deadline, self.clock() + self.timeout_s)

    def _late_send_result(self, e: _Inflight, ex: Exchange, task: asyncio.Future) -> None:
        """Done-callback of a place call that outlived ``_send`` (timeout / cancellation)."""
        e.sending = False
        try:
            if task.cancelled():
                raise asyncio.CancelledError()
            res = task.result()
        except BaseException as exc:
            e.unknown = True
            self._restart_deadline(e)
            log.warning("%s late %s place_market %s ended with %r (outcome unknown)", self.symbol, ex.name,
                        e.client_id, exc)
            self._wake.set()
            return
        log.info("%s late %s place_market result for %s: ok=%s %s", self.symbol, ex.name, e.client_id, res.ok,
                 res.error or "")
        try:
            self._on_send_result(e, ex, res)
        finally:
            self._wake.set()

    def _on_send_result(self, e: _Inflight, ex: Exchange, res: OrderResult) -> bool:
        e.sending = False
        order = res.order
        if order is not None and order.client_id != e.client_id and self._inflight.get(e.client_id) is e:
            self._inflight.pop(e.client_id)
            e.client_id = order.client_id
            self._inflight[order.client_id] = e
        if not res.ok:
            if order is not None and order.status is OrderStatus.UNKNOWN:
                e.unknown = True  # e.g. transport error after send: may still execute
                self._restart_deadline(e)
                self._note_failure(retryable=True, err=res.error or "unknown outcome", venue=ex.name)
                return False
            if self._inflight.get(e.client_id) is e:
                self._inflight.pop(e.client_id)
            e.finish_err = None  # counted once below (the adapter's retryable flag wins)
            self._note_failure(retryable=res.retryable, err=f"{ex.name}: {res.error}", venue=ex.name)
            return False
        if self.risk is not None:
            try:
                self.risk.record_success(ex.name)
            except Exception:  # pragma: no cover
                pass
        if e.finish_err is not None:  # the order listener finished it without any fill while we were sending
            retryable, err, venue = e.finish_err
            e.finish_err = None
            self._note_failure(retryable=retryable, err=err, venue=venue)
            return True
        self._restart_deadline(e)  # the timeout counts from venue acceptance
        if order is not None:
            if order.status is OrderStatus.UNKNOWN:
                e.unknown = True
            elif order.status.is_terminal and self._inflight.get(e.client_id) is e and not e.terminal:
                self._finish(e, order)
        return True

    # ------------------------------------------------------------------ emergency
    async def _maybe_emergency(self, now: float) -> None:
        self._update_escalation(now)  # fills may have resolved the delta while a hedge was being sent
        since = self._unhedged_since
        timeout = float(self.rcfg.unhedged_timeout_s)
        if since is None or now - since <= 3 * timeout or now - self._last_emergency < timeout:
            return
        self._last_emergency = now
        delta = self.delta  # in-flight orders are expected to be resolved/expired by now
        unhedged = self.unhedged
        if abs(delta) < self.hedge_min():
            msg = (f"{self.symbol}: unhedged {unhedged} for {now - since:.0f}s is covered only by unconfirmed "
                   f"in-flight orders ({self.inflight}); waiting for reconcile")
            log.critical(msg)
            self._notify_once("emerg-inflight", msg)
            return
        if self.rcfg.emergency_flatten and self.maker_pos != 0 and _sign(delta) == _sign(self.maker_pos):
            async with self._send_lock:
                mm = self.pair.maker_market
                size = mm.round_size(min(abs(self.delta), abs(self.maker_pos)))
                if size <= 0 or size < mm.min_size:
                    self._warn("emerg-small", f"{self.symbol}: emergency flatten size {size} below maker minimum")
                    return
                side = Side.SELL if self.maker_pos > 0 else Side.BUY
                book = await self._book(self.maker, self.ms)
                worst = (worst_taker_price(side, book, size, self.pair.cfg.max_slippage_bps * 3, mm)
                         if book is not None else None)
                if worst is None:
                    self._warn("emerg-book", f"{self.symbol}: emergency flatten impossible (no maker book)")
                    return
                self.stats["emergencies"] += 1
                msg = (f"EMERGENCY {self.symbol}: unhedged {unhedged} for {now - since:.0f}s -> flattening "
                       f"{side.value} {size} on {self.maker.name} (taker IOC, reduce-only). "
                       f"Last hedge error: {self._last_error}")
                log.critical(msg)
                self._notify(msg)
                self._event("emergency_flatten", unhedged=unhedged, size=size, side=side.value)
                await self._send(MAKER, side, size, worst, reduce_only=True, role="emergency")
                self._after_emergency()
            return
        msg = (f"EMERGENCY {self.symbol}: unhedged {unhedged} for {now - since:.0f}s cannot be flattened on "
               f"{self.maker.name} (excess is on the {self.taker.name} leg or emergency_flatten is off); "
               f"halting quotes. Last hedge error: {self._last_error}")
        log.critical(msg)
        self._event("emergency_unresolved", unhedged=unhedged)
        if not self._halt_requested:
            self._halt_requested = True
            self._notify(msg)
            if self.risk is not None:
                try:
                    self.risk.halt(f"{self.symbol}: unhedged delta {unhedged} persists")
                except Exception:  # pragma: no cover
                    log.exception("risk.halt failed")

    def _after_emergency(self) -> None:
        """Latch quoting off after a maker-leg flatten (the hedge venue is failing: re-opening would just repeat
        fill -> naked -> paid flatten); halt after ``max_emergencies_per_hour``."""
        now = self.clock()
        self._emergency_block_until = now + self.emergency_cooldown_s
        self._emergency_ts.append(now)
        while self._emergency_ts and now - self._emergency_ts[0] > 3600.0:
            self._emergency_ts.popleft()
        n = len(self._emergency_ts)
        log.warning("%s: quoting paused for %.0fs after emergency flatten (%d in the last hour)", self.symbol,
                    self.emergency_cooldown_s, n)
        if 0 < self.max_emergencies_per_hour <= n and not self._halt_requested:
            self._halt_requested = True
            msg = (f"{self.symbol}: {n} emergency flattens within an hour (hedge venue keeps failing: "
                   f"{self._last_error}); halting")
            log.critical(msg)
            self._notify(msg)
            if self.risk is not None:
                try:
                    self.risk.halt(msg)
                except Exception:  # pragma: no cover
                    log.exception("risk.halt failed")

    # ------------------------------------------------------------------ flatten (close_all taker mode)
    async def flatten(self, slippage_mult: Decimal | int = 2) -> bool:
        """Send reduce-only IOCs that flatten BOTH legs at once (both registered in flight first).

        Returns True if anything was sent. Dust below a venue minimum is left alone.
        """
        async with self._send_lock:
            px = self.ref_price()
            plans = []
            for leg, ex, sym, m, pos in ((MAKER, self.maker, self.ms, self.pair.maker_market,
                                          self.maker_pos + self.maker_inflight),
                                         (TAKER, self.taker, self.ts, self.pair.taker_market,
                                          self.taker_pos + self.taker_inflight)):
                size = m.round_size(abs(pos))
                if size <= 0 or size < min_trade_size(m, px):  # dust: same rule as is_flat()
                    continue
                side = Side.SELL if pos > 0 else Side.BUY
                book = await self._book(ex, sym)
                worst = (worst_taker_price(side, book, size, self.pair.cfg.max_slippage_bps * Decimal(slippage_mult), m)
                         if book is not None else None)
                if worst is None:
                    log.warning("%s flatten: no %s book, skipping that leg", self.symbol, ex.name)
                    continue
                plans.append((leg, side, size, worst))
            if not plans:
                return False
            results = await asyncio.gather(*(self._send(leg, side, size, worst, reduce_only=True, role="close")
                                             for leg, side, size, worst in plans), return_exceptions=True)
            for r in results:
                if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                    log.error("%s flatten send failed: %r", self.symbol, r)
            return True

    async def flush(self, timeout_s: float, settle_s: float = 0.0) -> bool:
        """Wait until hedged (no in-flight, residual below minimum) or timeout. Returns True if hedged.

        ``settle_s`` > 0 additionally requires that long without a maker fill (and since the call), so fills
        still in transit (e.g. racing a quote cancel) are hedged before the caller stops the hedger."""
        start = time.monotonic()
        deadline = start + max(0.0, timeout_s)
        t0 = self.clock()
        while True:
            if self.is_hedged() and (settle_s <= 0 or (time.monotonic() - start >= settle_s
                                                        and self.clock() - max(t0, self._last_maker_fill)
                                                        >= settle_s)):
                return True
            if time.monotonic() >= deadline:
                if self.is_hedged():  # hedged, only the settle period was cut short
                    return True
                log.warning("%s hedger flush timed out: delta %s inflight %s", self.symbol, self.delta, self.inflight)
                return False
            if not self._running:  # nobody drives the loop: do it here
                try:
                    await self.step()
                except Exception:
                    log.exception("%s flush step failed", self.symbol)
            self._wake.set()
            await asyncio.sleep(min(0.05, self.tick_s))

    # ------------------------------------------------------------------ reconcile
    async def _read_positions(self) -> tuple[Decimal, Decimal] | None:
        epoch = self._fill_epoch
        try:
            mp, tp = await asyncio.wait_for(asyncio.gather(self.maker.fetch_positions(), self.taker.fetch_positions()),
                                            self.io_timeout_s)
        except Exception as exc:
            self._warn("recon", f"{self.symbol}: position read failed: {exc!r}")
            return None
        if epoch != self._fill_epoch:
            return None  # fills arrived during the read: snapshot is ambiguous
        m = mp.get(self.ms)
        t = tp.get(self.ts)
        return (m.size if m is not None else ZERO), (t.size if t is not None else ZERO)

    async def _throttled_reconcile(self, reason: str, *, drop_inflight: bool = False) -> bool:
        ok = await self.reconcile(reason, drop_inflight=drop_inflight)
        if ok:
            self._reconcile_fails = 0
            self._reconcile_retry_at = 0.0
        else:
            self._reconcile_fails += 1
            self._reconcile_retry_at = self.clock() + min(10.0, self.reconcile_retry_s * 2 ** min(
                self._reconcile_fails - 1, 6))
        return ok

    async def reconcile(self, reason: str, *, drop_inflight: bool = False) -> bool:
        """Compare local positions with venue truth; adopt a discrepancy only if two reads agree.

        Returns True when the local state was confirmed or corrected.
        """
        if self._reconciling:
            return False
        self._reconciling = True
        try:
            self.stats["reconciles"] += 1
            self._last_reconcile = self.clock()
            r1 = await self._read_positions()
            if r1 is None:
                return False
            adopted: set[str] = set()
            if r1 != (self.maker_pos, self.taker_pos):
                self._fill_evt.clear()
                try:  # give in-transit fills time to arrive; abort on a new fill
                    await asyncio.wait_for(self._fill_evt.wait(), self.reconcile_confirm_s)
                    return False
                except asyncio.TimeoutError:
                    pass
                r2 = await self._read_positions()
                if r2 is None:
                    return False
                local = (self.maker_pos, self.taker_pos)
                for name, v1, v2, loc in ((self.maker.name, r1[0], r2[0], local[0]),
                                          (self.taker.name, r1[1], r2[1], local[1])):
                    if v2 == loc:
                        continue
                    if v1 != v2:
                        log.info("%s reconcile (%s): %s position unstable (%s -> %s); retry later", self.symbol,
                                 reason, name, v1, v2)
                        return False
                for leg, name, v2, loc in ((MAKER, self.maker.name, r2[0], local[0]),
                                           (TAKER, self.taker.name, r2[1], local[1])):
                    if v2 == loc:
                        continue
                    log.warning("%s reconcile (%s): %s position local %s != venue %s -> adopting venue", self.symbol,
                                reason, name, loc, v2)
                    self.stats["adoptions"] += 1
                    self._event("position_adopted", venue=name, local=loc, venue_pos=v2, reason=reason)
                    adopted.add(leg)
                    if leg == MAKER:
                        self.maker_pos = v2
                    else:
                        self.taker_pos = v2
                self._fill_epoch += 1
            if drop_inflight:
                now = self.clock()
                for cid, e in list(self._inflight.items()):
                    if e.deadline > now or self._send_pending(e, now):
                        continue
                    if e.terminal and e.leg not in adopted and now - e.terminal_ts < self.terminal_max_s:
                        # the venue reported fills that neither arrived nor show in the position read (lagging
                        # account stream): dropping would hedge them a second time -> keep waiting (bounded)
                        e.deadline = min(now + self.timeout_s, e.terminal_ts + self.terminal_max_s)
                        log.info("%s: %s order %s reported %s filled but the venue position does not show it "
                                 "yet; waiting", self.symbol, e.leg, e.client_id, e.remaining)
                        continue
                    log.warning("%s: dropping expired in-flight %s %s %s (venue positions adopted)", self.symbol,
                                e.leg, e.side.value, e.size)
                    self._inflight.pop(cid, None)
                    if e.terminal and e.leg in adopted:  # its reported fills are in the adopted position
                        self._settled_cids[e.client_id] = None
                        while len(self._settled_cids) > 200:
                            self._settled_cids.popitem(last=False)
            self._reconcile_requested = None
            self._wake.set()
            return True
        finally:
            self._reconciling = False
        self._reconcile_fails = 0
        self._reconcile_retry_at = 0.0

    # ------------------------------------------------------------------ misc
    def _record(self, f: Fill, role: str) -> None:
        if self.store is None:
            return
        try:
            self.store.record_fill(f, role)
        except Exception as exc:  # never let persistence break hedging
            self._warn("store", f"store.record_fill failed: {exc!r}")

    def _event(self, kind: str, **data: Any) -> None:
        if self.store is None:
            return
        try:
            self.store.record_event(kind, {"symbol": self.symbol, **data})
        except Exception:
            pass

    def _notify(self, text: str) -> None:
        if self.notifier is None:
            return
        try:
            fut = asyncio.ensure_future(self.notifier.send(text))
        except Exception:  # pragma: no cover
            return
        self._bg.add(fut)
        fut.add_done_callback(lambda f: (self._bg.discard(f), f.cancelled() or f.exception()))

    def _notify_once(self, key: str, text: str, every_s: float = 600.0) -> None:
        now = time.monotonic()
        if now - self._warned.get("n:" + key, -1e9) >= every_s:
            self._warned["n:" + key] = now
            self._notify(text)

    def _warn(self, key: str, msg: str, every_s: float = 30.0) -> None:
        now = time.monotonic()
        if now - self._warned.get(key, -1e9) >= every_s:
            self._warned[key] = now
            log.warning(msg)

    def status(self) -> dict:
        now = self.clock()
        px = self.ref_price()
        return {
            "maker_pos": self.maker_pos,
            "taker_pos": self.taker_pos,
            "inflight": self.inflight,
            "inflight_orders": len(self._inflight),
            "delta": self.delta,
            "unhedged": self.unhedged,
            "unhedged_notional": abs(self.unhedged) * px if px is not None else None,
            "unhedged_age_s": None if self._unhedged_since is None else round(now - self._unhedged_since, 3),
            "escalated": self.escalated,
            "escalated_reason": self._escalated_reason,
            "fail_count": self._fail_count,
            "retry_in_s": max(0.0, round(self._retry_at - now, 3)),
            "last_error": self._last_error,
            "close_mode": self.close_mode,
            "running": self._running,
            **{k: v for k, v in self.stats.items()},
        }


__all__ = ["Hedger", "min_trade_size"]
