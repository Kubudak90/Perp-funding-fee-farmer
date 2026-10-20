"""PairTrader: per-market quoting state machine OPENING -> HOLDING -> CLOSING -> OPENING.

- OPENING: post-only maker quotes toward ``direction * target_base``.
- HOLDING: no quote; wait ``hold_seconds`` (+ the next funding settlement if ``hold_through_funding``).
- CLOSING: post-only (reduce-only) quotes toward ``direction * floor_base`` (0 for a direction flip / forced).

Every maker fill is hedged by the pair's ``Hedger`` (which keeps running when quoting is paused). The trader
never trades the taker venue itself. Quotes are amended (``modify_order``) instead of cancel/replace:
PROTECTIVE amends (resting price violates the fresh edge limit, or the size overshoots the goal) are sent
immediately; IMPROVEMENT amends need ``reprice_threshold_bps`` of movement and ``min_quote_lifetime_s`` of
age (stretched when the venue's order budget runs low); all amends share ``max_requotes_per_min``.
"""

from __future__ import annotations

import asyncio
import collections
import enum
import logging
import time
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from farmer.config import RiskConfig, StrategyConfig
from farmer.exchanges.base import Exchange
from farmer.models import BPS, ZERO, Fill, Order, OrderBook, OrderStatus, Side
from farmer.strategy.funding import MAKER_LONG, choose_direction, next_hour_ts
from farmer.strategy.hedger import Hedger, private_stream_ok
from farmer.strategy.mapping import PairSpec
from farmer.strategy.pricing import (
    BasisTracker,
    Quote,
    compute_maker_quote,
    mid_excluding_own,
    price_moved_bps,
    quote_violates_edge,
    round_pair_size,
)
from farmer.utils.decimals import fmt
from farmer.utils.ids import new_client_id

log = logging.getLogger("farmer.trader")

QUOTE_TAG = "fq"  # client-id prefix of maker quotes (hedger: fh / fe)


class Phase(str, enum.Enum):
    OPENING = "OPENING"
    HOLDING = "HOLDING"
    CLOSING = "CLOSING"


def _sign(x: Decimal) -> int:
    return 1 if x > 0 else (-1 if x < 0 else 0)


class _Dying:
    __slots__ = ("order", "first_ts", "last_ts", "attempts", "accepted")

    def __init__(self, order: Order, now: float, accepted: bool, attempts: int = 1) -> None:
        self.order = order
        self.first_ts = now
        self.last_ts = now
        self.attempts = attempts
        self.accepted = accepted


class PairTrader:
    """Quote one pair. ``run()`` until ``stop()``; ``status()`` for monitoring."""

    def __init__(self, pair: PairSpec, maker: Exchange, taker: Exchange, scfg: StrategyConfig, risk: Any,
                 funding: Any, store: Any | None = None, *, hedger: Hedger | None = None,
                 rcfg: RiskConfig | None = None, notifier: Any | None = None,
                 clock: Callable[[], float] = time.time) -> None:
        self.pair = pair
        self.maker = maker
        self.taker = taker
        self.scfg = scfg
        self.risk = risk
        self.funding = funding
        self.store = store
        self.clock = clock
        self.symbol = pair.symbol
        self.ms = pair.maker_symbol
        self.ts = pair.taker_symbol
        self.rcfg = rcfg or getattr(risk, "cfg", None) or RiskConfig()
        self._owns_hedger = hedger is None
        self.hedger = hedger or Hedger(pair, maker, taker, scfg, self.rcfg, risk, store, notifier=notifier,
                                       clock=clock)
        self.loop_s = max(0.001, float(scfg.loop_interval_s))
        self.heartbeat_s = min(1.0, max(self.loop_s * 4, 0.05))
        self.ack_timeout_s = float(scfg.order_ack_timeout_s)
        self.amend_ack_timeout_s = float(scfg.amend_ack_timeout_s)
        self.cancel_confirm_s = float(scfg.cancel_confirm_timeout_s)
        self.settle_grace_s = float(scfg.funding_settle_grace_s)

        self.basis = BasisTracker(scfg.basis_halflife_s, clock=clock)
        self.phase = Phase.OPENING
        self.direction: int | None = None
        self.pending_direction: int | None = None
        self.target_base: Decimal | None = None
        self.floor_base: Decimal | None = None
        self._close_flat = False
        self._forced_close = False
        self._phase_ts = clock()
        self._hold_settlement: float | None = None
        self._next_settle: float | None = None
        self._in_window = False
        self._forced = False
        pre = float(scfg.protect_funding_window_s)
        post = scfg.protect_funding_post_s
        # the venues snapshot positions up to ~0.5 s AFTER the hour: keep closes / flips away from it too
        self.protect_post_s = min(3.0, pre) if post is None else float(post)
        self.funding_stale_s = max(3.0 * float(scfg.funding_refresh_s), 180.0)
        self._funding_stale_warned = False
        self._taker_budget_low = False

        self._order: Order | None = None
        self._order_born = 0.0
        self._order_ts = 0.0  # last place / amend
        self._full_since: tuple[str, float] | None = None  # (client id, ts) quote fully filled, terminal pending
        self._placing_cid: str | None = None
        self._amend_target: Decimal | None = None
        self._amend_until = 0.0
        self._amend_sent = 0.0
        self._dying: dict[str, _Dying] = {}
        self._done_cids: collections.OrderedDict[str, None] = collections.OrderedDict()  # recently terminal
        self._cid_fills: collections.OrderedDict[str, Decimal] = collections.OrderedDict()
        self._actions: collections.deque[float] = collections.deque()
        self._backoff_until = 0.0
        self._fail_count = 0
        self.block_reason: str | None = "starting"

        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._running = False
        self.stats: dict[str, Any] = {
            "places": 0, "amends": 0, "protective": 0, "improve": 0, "resize": 0, "cancels": 0, "would_cross": 0,
            "rejects": 0, "place_failures": 0, "maker_fills": 0, "maker_volume": ZERO, "cycles": 0, "flips": 0,
        }

        maker.add_book_listener(self._on_maker_book)
        taker.add_book_listener(self._on_taker_book)
        maker.add_order_listener(self._on_maker_order)
        maker.add_fill_listener(self._on_maker_fill)

    # ------------------------------------------------------------------ listeners
    def _on_maker_book(self, b: OrderBook) -> None:
        if b.symbol == self.ms:
            self._update_basis(b, None)
            self._wake.set()

    def _on_taker_book(self, b: OrderBook) -> None:
        if b.symbol == self.ts:
            self._update_basis(None, b)
            self._wake.set()

    def _update_basis(self, mb: OrderBook | None, tb: OrderBook | None) -> None:
        if not self.scfg.use_basis:
            return
        try:
            mb = mb or self.maker.get_book(self.ms)
            tb = tb or self.taker.get_book(self.ts)
        except Exception:
            return
        if mb is not None and tb is not None:
            self.basis.update(self._maker_mid_ex_own(mb), tb.mid)

    def _maker_mid_ex_own(self, mb: OrderBook) -> Decimal | None:
        """Maker mid without our own resting quote (the live venue book contains it; paper books do not)."""
        o = self._order
        if (o is None or o.status not in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
                or getattr(self.maker, "is_paper", False)):
            return mb.mid
        return mid_excluding_own(mb, o.side, o.price, self._remaining(o))

    def _is_own_quote_id(self, cid: str | None) -> bool:
        return bool(cid) and cid.startswith(QUOTE_TAG)  # type: ignore[union-attr]

    def _track_orphan(self, o: Order, why: str) -> None:
        """A live quote of ours that we no longer track (e.g. a place that landed after its cancel was
        acknowledged): schedule a cancel through the dying set."""
        if (o.status.is_terminal or o.symbol != self.ms or not self._is_own_quote_id(o.client_id)
                or o.client_id == self._placing_cid or o.client_id in self._dying or o.client_id in self._done_cids):
            return
        cur = self._order
        if cur is not None and (o.client_id == cur.client_id or (o.order_id and o.order_id == cur.order_id)):
            return
        if self._remaining(o) <= 0:  # fully filled by our own fill count: nothing can rest
            return
        log.warning("%s: untracked live quote %s %s %s @ %s (%s) -> cancel", self.symbol, o.client_id,
                    o.side.value, o.size, o.price, why)
        d = _Dying(o, self.clock() - self.cancel_confirm_s, accepted=False, attempts=0)  # cancel on the next tick
        self._dying[o.client_id] = d
        self._wake.set()

    def handle_open_orders(self, orders: list[Order]) -> None:
        """Runtime sweep: cancel open quotes of ours on this market that we do not track."""
        for o in orders:
            if o.symbol == self.ms and o.is_open:
                self._track_orphan(o, "open-order sweep")

    def _on_maker_order(self, o: Order) -> None:
        if o.symbol != self.ms:
            return
        cur = self._order
        if cur is not None and (o.client_id == cur.client_id or (o.order_id and o.order_id == cur.order_id)):
            if o is not cur:
                self._order = o  # adopt the venue's latest object
            if self._amend_target is not None and o.price == self._amend_target:
                self._amend_target = None
            if o.status.is_terminal:
                self._order = None
                self._amend_target = None
                if o.status is OrderStatus.REJECTED:
                    self.stats["rejects"] += 1
                    self._backoff_until = max(self._backoff_until, self.clock() + self.loop_s)
                    log.info("%s maker order %s rejected: %s", self.symbol, o.client_id, o.error)
        if o.status.is_terminal:
            self._dying.pop(o.client_id, None)
            if o.client_id:
                self._done_cids[o.client_id] = None
                while len(self._done_cids) > 500:
                    self._done_cids.popitem(last=False)
        elif o.status is not OrderStatus.PENDING and o.status is not OrderStatus.UNKNOWN:
            self._track_orphan(o, "venue reports it live")
        self._wake.set()

    def _on_maker_fill(self, f: Fill) -> None:
        if f.symbol != self.ms or not f.client_id:
            return
        self._cid_fills[f.client_id] = self._cid_fills.get(f.client_id, ZERO) + f.size
        while len(self._cid_fills) > 500:
            self._cid_fills.popitem(last=False)
        if f.is_maker is not False:  # taker IOCs on the maker venue (emergency / close) are not quote fills
            self.stats["maker_fills"] += 1
            self.stats["maker_volume"] += f.notional
        self._wake.set()

    # ------------------------------------------------------------------ lifecycle
    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    @property
    def running(self) -> bool:
        return self._running

    def resume(self) -> None:
        """Allow ``run()`` again after ``stop()``."""
        self._stop.clear()
        if self._owns_hedger:
            self.hedger.resume()

    def force_close(self, on: bool = True) -> None:
        """Close to flat (ignoring the funding window) and do not reopen while set."""
        self._forced_close = on
        self._wake.set()

    async def run(self) -> None:
        self._running = True
        hedger_task = asyncio.ensure_future(self.hedger.run()) if self._owns_hedger else None
        errors = 0
        try:
            while not self._stop.is_set():
                t0 = time.monotonic()
                try:
                    await self.tick()
                    errors = 0
                except asyncio.CancelledError:
                    raise
                except Exception:
                    errors += 1
                    log.exception("%s trader tick failed (%d)", self.symbol, errors)
                    await self._sleep(min(10.0, self.loop_s * 2 ** min(errors, 8)))
                rest = self.loop_s - (time.monotonic() - t0)
                if rest > 0:
                    await self._sleep(rest)  # debounce
                if self._wake.is_set():
                    self._wake.clear()
                    continue
                try:
                    await asyncio.wait_for(self._wake.wait(), self.heartbeat_s)
                except TimeoutError:
                    pass
                self._wake.clear()
        finally:
            self._running = False
            if hedger_task is not None:
                self.hedger.stop()
                try:
                    await asyncio.wait_for(hedger_task, 5.0)
                except (TimeoutError, asyncio.CancelledError, Exception):
                    hedger_task.cancel()

    async def _sleep(self, s: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), s)
        except TimeoutError:
            pass

    async def stop(self, cancel_quotes: bool = True) -> bool:
        """Stop quoting. Returns False if the quote cancel could not be confirmed (a quote may still rest)."""
        self._stop.set()
        self._wake.set()
        ok = True
        if cancel_quotes:
            ok = await self.cancel_quotes()
        if self._owns_hedger:
            self.hedger.stop()
        return ok

    async def cancel_quotes(self, wait_s: float | None = None) -> bool:
        """Cancel our resting quote(s) and wait briefly for confirmation; cancel_all(symbol) as a fallback."""
        wait_s = self.cancel_confirm_s if wait_s is None else wait_s
        async with self._lock:
            now = self.clock()
            if self._order is not None:
                await self._cancel(self._order, "stop", now)
            for d in list(self._dying.values()):
                if not d.order.status.is_terminal and not d.accepted:
                    d.accepted = await self._send_cancel(d.order)
        deadline = time.monotonic() + wait_s
        while self._pending_cancels() and time.monotonic() < deadline:
            await asyncio.sleep(min(0.05, max(0.005, self.loop_s)))
        if self._pending_cancels():
            log.warning("%s: %d quote cancel(s) unconfirmed -> cancel_all(%s)", self.symbol, len(self._dying), self.ms)
            try:
                await self.maker.cancel_all(self.ms)
            except Exception as exc:
                log.error("%s cancel_all failed: %r", self.symbol, exc)
                return False
            for cid, d in list(self._dying.items()):
                if d.accepted:
                    self._dying.pop(cid, None)
        return not self._pending_cancels()

    def _pending_cancels(self) -> bool:
        for cid, d in list(self._dying.items()):
            if d.order.status.is_terminal:
                self._dying.pop(cid, None)
        return bool(self._dying)

    # ------------------------------------------------------------------ tick
    def _books(self) -> tuple[OrderBook | None, OrderBook | None]:
        try:
            mb = self.maker.get_book(self.ms)
        except Exception:
            mb = None
        try:
            tb = self.taker.get_book(self.ts)
        except Exception:
            tb = None
        return mb, tb

    def _mid(self, mb: OrderBook | None, tb: OrderBook | None) -> Decimal | None:
        if mb is not None and mb.mid is not None and mb.mid > 0:
            return mb.mid
        if tb is not None and tb.mid is not None and tb.mid > 0:
            b = self.basis.value if self.scfg.use_basis and self.basis.value is not None else ZERO
            return tb.mid * (1 + b)
        return None

    def _must_reduce(self) -> bool:
        try:
            return bool(self.risk.must_reduce()) if self.risk is not None else False
        except Exception:
            log.exception("risk.must_reduce failed")
            return False

    async def tick(self) -> None:
        """One evaluation of phase, gating and the resting quote."""
        now = self.clock()
        mb, tb = self._books()
        mpos = self.hedger.maker_pos
        self._report_exposure(mb, tb)
        snap = self.funding.latest(self.symbol) if self.funding is not None else None
        snap_next = snap.next_settlement_ts if snap is not None else None
        # a snapshot not refreshed since the hour passed still names that (past) settlement: use the clock
        nxt = snap_next if snap_next is not None and snap_next > now else next_hour_ts(now)
        self._next_settle = nxt
        prev = (int(now) // 3600) * 3600.0  # both venues settle at the top of the hour
        if snap_next is not None and snap_next <= now:
            prev = max(prev, snap_next)  # the settlement the snapshot names has just passed
        self._in_window = (0 <= nxt - now <= float(self.scfg.protect_funding_window_s)
                           or 0 <= now - prev < self.protect_post_s)
        self._forced = self._forced_close or self._must_reduce()
        chosen = choose_direction(self._direction_snapshot(snap), self.direction, self.scfg.flip_hysteresis_apr,
                                  self.scfg.direction_mode)
        mid = self._mid(mb, tb)
        self._advance(now, mpos, mid, chosen)
        quote, reduce_only, reason = self._desired(mb, tb, mpos, mid)
        self.block_reason = reason
        await self._sync(quote, reduce_only, reason, mid, now)

    def _direction_snapshot(self, snap: Any) -> Any:
        """The snapshot used for direction choice; a stale one (funding refresh failing) keeps the direction."""
        if snap is None or self.direction is None:
            return snap
        try:
            age = float(snap.age)
        except Exception:
            return snap
        if age <= self.funding_stale_s:
            self._funding_stale_warned = False
            return snap
        if not self._funding_stale_warned:
            self._funding_stale_warned = True
            log.warning("%s: funding snapshot is %.0fs old (refresh failing?) -> keeping direction %s", self.symbol,
                        age, self.direction)
        return None

    # ------------------------------------------------------------------ phases
    def _freeze(self, mid: Decimal) -> None:
        self.target_base = self.pair.cfg.target_notional / mid
        self.floor_base = self.scfg.churn_floor_frac * self.target_base

    def goal(self) -> Decimal | None:
        if self.direction is None or self.target_base is None or self.phase is Phase.HOLDING:
            return None
        if self.phase is Phase.OPENING:
            return self.direction * self.target_base
        if self._close_flat or self._forced:
            return ZERO
        return self.direction * (self.floor_base or ZERO)

    def _reached(self, mpos: Decimal, goal: Decimal, min_sz: Decimal) -> bool:
        rem = goal - mpos
        if abs(rem) < min_sz:
            return True
        d = self.direction or MAKER_LONG
        return d * rem <= 0 if self.phase is Phase.OPENING else d * rem >= 0

    def _set_phase(self, phase: Phase, now: float, mid: Decimal, reason: str, *, close_flat: bool = False) -> None:
        prev = self.phase
        self.phase = phase
        self._phase_ts = now
        if phase is Phase.OPENING:
            self._close_flat = False
            if prev is Phase.CLOSING:
                self.stats["cycles"] += 1
            self._freeze(mid)
        elif phase is Phase.HOLDING:
            self._hold_settlement = self._next_settle
        else:
            self._close_flat = close_flat
            self._freeze(mid)
        g = self.goal()
        log.info("%s %s -> %s (%s) dir=%s maker_pos=%s goal=%s", self.symbol, prev.value, phase.value, reason,
                 self.direction, self.hedger.maker_pos, fmt(g) if g is not None else None)
        if self.store is not None:
            try:
                self.store.record_event("phase", {"symbol": self.symbol, "from": prev.value, "to": phase.value,
                                                  "reason": reason, "direction": self.direction,
                                                  "maker_pos": str(self.hedger.maker_pos)})
            except Exception:
                pass

    def _hold_done(self, now: float) -> bool:
        if now - self._phase_ts < float(self.scfg.hold_seconds):
            return False
        if self.scfg.hold_through_funding:
            return self._hold_settlement is not None and now >= self._hold_settlement + self.settle_grace_s
        return True

    def _advance(self, now: float, mpos: Decimal, mid: Decimal | None, chosen: int) -> None:
        if mid is None:
            return
        min_sz = self.pair.min_size(mid)
        flat = abs(mpos) < min_sz
        win = self._in_window
        if self.direction is None:
            if not flat:  # adopt an existing position (restart)
                self.direction = 1 if mpos > 0 else -1
                self._freeze(mid)
                assert self.target_base is not None
                self.phase = Phase.HOLDING if abs(mpos) >= self.target_base - min_sz else Phase.OPENING
                self._phase_ts = now
                self._hold_settlement = self._next_settle
                log.info("%s: adopting existing maker position %s -> dir=%s phase=%s", self.symbol, mpos,
                         self.direction, self.phase.value)
            else:
                self.direction = chosen
                self._set_phase(Phase.OPENING, now, mid, "start")
        if self.target_base is None:
            self._freeze(mid)
        self.pending_direction = chosen if chosen != self.direction else None
        pend = self.pending_direction
        forced = self._forced

        if self.phase is Phase.OPENING:
            if forced:
                self._set_phase(Phase.CLOSING, now, mid, "forced close", close_flat=True)
            elif pend is not None:
                if flat and not win:
                    self._flip(pend, now, mid)
                elif not flat:
                    self._set_phase(Phase.CLOSING, now, mid, f"direction flip -> {pend}", close_flat=True)
            elif self._reached(mpos, self.goal() or ZERO, min_sz):
                self._set_phase(Phase.HOLDING, now, mid, "target reached")
        elif self.phase is Phase.HOLDING:
            if forced:
                self._set_phase(Phase.CLOSING, now, mid, "forced close", close_flat=True)
            elif pend is not None and not win:
                self._set_phase(Phase.CLOSING, now, mid, f"direction flip -> {pend}", close_flat=True)
            elif self.scfg.churn and not win and self._hold_done(now):
                self._set_phase(Phase.CLOSING, now, mid, "hold done")
        else:  # CLOSING
            if forced and not self._close_flat:
                self._close_flat = True
            if pend is not None and not self._close_flat:
                self._close_flat = True  # a flip needs a flat book
            if self._reached(mpos, self.goal() or ZERO, min_sz):
                if forced:
                    return  # stay flat while forced
                if pend is not None:
                    if flat and not win:
                        self._flip(pend, now, mid)
                        self._set_phase(Phase.OPENING, now, mid, "flat after flip")
                    return
                self._set_phase(Phase.OPENING, now, mid, "floor reached")

    def _flip(self, new_dir: int, now: float, mid: Decimal) -> None:
        log.info("%s: direction %s -> %s", self.symbol, self.direction, new_dir)
        self.stats["flips"] += 1
        self.direction = new_dir
        self.pending_direction = None
        self._freeze(mid)
        if self.store is not None:
            try:
                self.store.record_event("direction", {"symbol": self.symbol, "direction": new_dir})
            except Exception:
                pass

    # ------------------------------------------------------------------ desired quote
    def _venues_open(self) -> bool:
        if self.rcfg.skip_rwa_off_hours:
            return bool(self.maker.market_open(self.ms) and self.taker.market_open(self.ts))
        return bool(self.pair.maker_market.active and self.pair.taker_market.active)

    def _desired(self, mb: OrderBook | None, tb: OrderBook | None, mpos: Decimal,
                 mid: Decimal | None) -> tuple[Quote | None, bool, str | None]:
        if self._stop.is_set():
            return None, False, "stopped"
        if self.risk is not None and getattr(self.risk, "halted", False):
            return None, False, f"halted: {getattr(self.risk, 'halt_reason', None)}"
        ok, why = private_stream_ok(self.maker)
        if not ok:  # acks / fills of a resting quote would be invisible: pull it
            return None, False, f"maker private stream down: {why}"
        hb = self.hedger.quoting_blocked() or self._taker_budget_block()
        if hb:
            return None, False, hb
        if self.phase is Phase.HOLDING:
            return None, False, "holding"
        if mb is None or tb is None or mid is None or mb.mid is None or tb.mid is None:
            return None, False, "waiting for books"
        if self.scfg.use_basis and not self.basis.warmed_up(float(self.scfg.basis_warmup_s)):
            return None, False, "basis warm-up"
        if self.phase is Phase.OPENING and self.pending_direction is not None:
            return None, False, "direction flip pending (funding window)"
        if self.phase is Phase.CLOSING and self._in_window and not self._forced:
            return None, False, "close deferred (funding window)"
        if self.phase is Phase.OPENING and self._forced:
            return None, False, "forced close"
        goal = self.goal()
        if goal is None:
            return None, False, "no goal"
        mm, tm = self.pair.maker_market, self.pair.taker_market
        min_sz = self.pair.min_size(mid)
        diff = goal - mpos
        if abs(diff) < min_sz:
            return None, False, "target reached"
        side = Side.BUY if diff > 0 else Side.SELL
        size = round_pair_size(min(self.pair.cfg.order_notional / mid, abs(diff)), mm, tm)
        if size < min_sz:
            size = min_sz
        # an opposite position below the pair minimum is dust (same rule as _advance): quote the normal clip
        # through zero instead of a reduce-only remainder that could never be placed
        reducing = abs(mpos) >= min_sz and _sign(mpos) == -side.sign
        if reducing:
            size = min(size, round_pair_size(abs(mpos), mm, tm))
            if size < min_sz:
                return None, False, "residual below pair minimum"
        basis = self.basis.value if self.scfg.use_basis and self.basis.value is not None else ZERO
        q = compute_maker_quote(side=side, size=size, maker_market=mm, taker_market=tm, maker_book=mb, taker_book=tb,
                                min_edge_bps=self.pair.cfg.min_edge_bps, max_improve_ticks=self.scfg.max_improve_ticks,
                                basis=basis, buffer_bps=self.scfg.requote_buffer_bps)
        if q is None:
            return None, False, "no edge-compliant quote (depth/edge)"
        if self.risk is not None:
            div = (mb.mid / tb.mid - 1) / BPS
            dev = self.basis.deviation_bps() if self.scfg.use_basis else None
            ok, why = self.risk.check_quote(self.symbol, mb, tb, dev, div)
            if not ok:
                return None, False, f"risk: {why}"
        if reducing:
            if not (mm.reduce_only_ok and tm.reduce_only_ok):  # a reduce-only market can still be closed
                return None, False, "market inactive"
        else:
            if not self._venues_open():
                return None, False, "market closed (trading hours)"
            ok, why = private_stream_ok(self.taker)
            if not ok:
                return None, False, f"taker private stream down: {why}"
            if self.risk is not None:
                ok, why = self.risk.check_open(self.symbol, q.size * q.price)
                if not ok:
                    return None, False, f"risk: {why}"
        return q, reducing, None

    # ------------------------------------------------------------------ order management
    def _size_cap(self, q: Quote) -> Decimal:
        """Largest resting size still useful: what remains to the goal (and, reduce-only, the position)."""
        g = self.goal()
        if g is None:
            return q.size
        mpos = self.hedger.maker_pos
        need = abs(g - mpos)
        if self._order is not None and self._order.reduce_only:
            need = min(need, abs(mpos))
        return max(q.size, round_pair_size(need, self.pair.maker_market, self.pair.taker_market))

    def _remaining(self, o: Order) -> Decimal:
        filled = max(o.filled, self._cid_fills.get(o.client_id, ZERO))
        return max(ZERO, o.size - filled)

    def _taker_budget_block(self) -> str | None:
        """Every maker fill needs a hedge tx on the taker venue: pause quoting (resting quote included) while its
        request budget is nearly used up, instead of piling up deltas whose hedges wait for the budget."""
        lim = float(self.scfg.min_taker_budget)
        if lim <= 0:
            return None
        try:
            h = float(self.taker.budget_health())
        except Exception:
            return None
        if self._taker_budget_low:
            if h >= min(1.0, 1.5 * lim):
                self._taker_budget_low = False
                log.info("%s: %s rate budget recovered (%.0f%%) -> quoting resumes", self.symbol, self.taker.name,
                         100 * h)
        elif h < lim:
            self._taker_budget_low = True
            log.warning("%s: %s rate budget low (%.0f%%) -> pausing quotes so hedges are not delayed", self.symbol,
                        self.taker.name, 100 * h)
        return f"{self.taker.name} rate budget low" if self._taker_budget_low else None

    def _budget_ok(self, now: float) -> bool:
        while self._actions and now - self._actions[0] >= 60.0:
            self._actions.popleft()
        return len(self._actions) < int(self.scfg.max_requotes_per_min)

    def _lifetime(self) -> float:
        base = float(self.scfg.min_quote_lifetime_s)
        try:
            h = float(self.maker.budget_health())
        except Exception:
            h = 1.0
        if h >= 0.5:
            return base
        return base * min(20.0, 0.5 / max(h, 0.025))

    async def _sync(self, q: Quote | None, reduce_only: bool, reason: str | None, mid: Decimal | None,
                    now: float) -> None:
        async with self._lock:
            if self._stop.is_set():
                return
            await self._process_dying(now)
            o = self._order
            if o is not None and o.status.is_terminal:
                self._order = o = None
            if q is None:
                if o is not None:
                    await self._cancel(o, reason or "no quote", now)
                return
            if o is None:
                if self._dying or now < self._backoff_until or not self._budget_ok(now):
                    return
                await self._place(q, reduce_only, now)
                return
            if o.status in (OrderStatus.PENDING, OrderStatus.UNKNOWN):
                stale = o.price is not None and (o.side is not q.side or
                                                 quote_violates_edge(o.side, o.price, q.limit_price))
                if stale or now - self._order_born >= self.ack_timeout_s:
                    log.warning("%s quote %s unacknowledged for %.1fs%s -> cancel", self.symbol, o.client_id,
                                now - self._order_born, " and now off-edge" if stale else "")
                    await self._cancel(o, "no ack", now)
                return  # never double-place while the venue has not acknowledged
            if o.side is not q.side or bool(o.reduce_only) != reduce_only:
                await self._cancel(o, "side/flags changed", now)
                return
            if self._amend_target is not None:
                if now < self._amend_until:
                    # old AND new price both off-edge while the amend is unconfirmed -> pull the quote
                    if (o.price is not None and now - self._amend_sent >= max(2 * self.loop_s, 0.5)
                            and quote_violates_edge(o.side, o.price, q.limit_price)
                            and quote_violates_edge(o.side, self._amend_target, q.limit_price)):
                        await self._cancel(o, "off-edge while amend unconfirmed", now)
                    return
                self._amend_target = None
            if now < self._backoff_until or o.price is None:
                return
            remaining = self._remaining(o)
            if remaining <= 0:  # fully filled; terminal update pending
                fs = self._full_since
                if fs is None or fs[0] != o.client_id:
                    self._full_since = (o.client_id, now)
                elif now - fs[1] >= max(self.cancel_confirm_s, self.amend_ack_timeout_s):
                    # the terminal frame was missed (stream gap): don't freeze the pair on a dead quote
                    log.warning("%s quote %s fully filled but no terminal update after %.1fs -> dropping",
                                self.symbol, o.client_id, now - fs[1])
                    self._full_since = None
                    self._drop(o)
                return
            protective = quote_violates_edge(o.side, o.price, q.limit_price)
            # size-only amends only when the quote overshoots what is still NEEDED (goal / reduce-only position),
            # not whenever the mid-dependent clip size shrinks by a step
            oversize = remaining > self._size_cap(q)
            ref = mid if mid is not None else o.price
            moved = price_moved_bps(o.price, q.price, ref)
            if protective or oversize:
                if not self._budget_ok(now):
                    if protective:
                        await self._cancel(o, "protective (requote budget exhausted)", now)
                    return
                if protective:
                    await self._amend(o, q.price, q.size, remaining, now, "protective")
                elif now - self._order_ts >= self._lifetime():
                    price = q.price if moved >= self.scfg.reprice_threshold_bps else o.price
                    await self._amend(o, price, q.size, remaining, now, "resize")
                return
            if (moved >= self.scfg.reprice_threshold_bps and now - self._order_ts >= self._lifetime()
                    and self._budget_ok(now)):
                await self._amend(o, q.price, q.size, remaining, now, "improve")

    async def _place(self, q: Quote, reduce_only: bool, now: float) -> None:
        cid = new_client_id(QUOTE_TAG)
        self._placing_cid = cid
        self._actions.append(now)
        self.stats["places"] += 1
        try:
            res = await self.maker.place_limit(self.ms, q.side, q.size, q.price, post_only=True,
                                               reduce_only=reduce_only, client_id=cid)
        except Exception as exc:
            self._place_failed(now, f"place_limit raised {exc!r}", retryable=False)
            return
        finally:
            self._placing_cid = None
        o = res.order
        if res.ok and o is not None:
            if o.status.is_terminal:
                if o.status is OrderStatus.REJECTED:
                    self._backoff_until = now + self.loop_s
                return
            self._order = o
            self._order_born = self._order_ts = now
            self._amend_target = None
            self._fail_count = 0
            self._record_success()
            log.debug("%s quote %s %s %s @ %s (edge %.2f bps, limit %s)", self.symbol, cid, q.side.value, q.size,
                      q.price, q.expected_edge_bps, q.limit_price)
            return
        if res.would_cross:
            self.stats["would_cross"] += 1
            self._backoff_until = now + self.loop_s  # book moved: retry on the next tick
            return
        if o is not None and not o.status.is_terminal:  # unknown outcome: make sure it is gone
            self._dying[o.client_id] = _Dying(o, now, accepted=await self._send_cancel(o))
        self._place_failed(now, res.error or "place failed", retryable=res.retryable)

    def _place_failed(self, now: float, err: str, *, retryable: bool) -> None:
        self.stats["place_failures"] += 1
        self._fail_count += 1
        delay = min(5.0, self.loop_s * 2 ** min(self._fail_count, 10))
        self._backoff_until = now + delay
        log.warning("%s maker place failed (%s; retry in %.2fs): %s", self.symbol,
                    "retryable" if retryable else "permanent", delay, err)
        if not retryable and self.risk is not None:
            try:
                self.risk.record_error(self.maker.name, err)
            except Exception:
                pass

    def _record_success(self) -> None:
        if self.risk is not None:
            try:
                self.risk.record_success(self.maker.name)
            except Exception:
                pass

    async def _amend(self, o: Order, price: Decimal, size: Decimal, remaining: Decimal, now: float,
                     kind: str) -> None:
        self._actions.append(now)
        self.stats["amends"] += 1
        self.stats[kind] += 1
        new_size = size if size != remaining else None
        try:
            res = await self.maker.modify_order(o, price=price, size=new_size)
        except Exception as exc:
            log.warning("%s modify raised %r", self.symbol, exc)
            self._backoff_until = now + 2 * self.loop_s
            if kind == "protective":
                await self._cancel(o, f"protective amend raised {exc!r}", now)
            return
        if res.ok:
            new = res.order
            if new is not None and new.client_id != o.client_id:  # adapter did cancel + replace
                if not o.status.is_terminal:
                    self._dying[o.client_id] = _Dying(o, now, accepted=True)
                self._order = None if new.status.is_terminal else new
                self._order_born = now
            elif new is not None and new is not o and new.status is OrderStatus.PENDING:
                self._amend_target = price  # ack pending: don't amend again until confirmed / timeout
                self._amend_sent = now
                self._amend_until = now + self.amend_ack_timeout_s
            elif new is not None and new is not o and not new.status.is_terminal:
                self._order = new
            self._order_ts = now
            self._record_success()
            return
        err = res.error or ""
        gone = o.status.is_terminal or (res.order is not None and res.order.status.is_terminal)
        if res.would_cross:
            self.stats["would_cross"] += 1
            self._backoff_until = now + self.loop_s
            if gone:  # a crossing ALO modify kills the order on Arcus
                log.info("%s quote %s killed by a crossing modify; placing a new one", self.symbol, o.client_id)
                self._drop(o)
            return
        if gone:
            self._drop(o)
            return
        code = err.upper().replace(" ", "_")  # match venue codes (ORDER_NOT_FOUND_FOR_MODIFY) and prose alike
        if "NOT_FOUND" in code or "NOT_OPEN" in code or "SUPERSEDED_BY_CANCEL" in code:
            if res.retryable and now - self._order_born < 1.0:  # maybe not sequenced yet: retry briefly
                self._backoff_until = now + self.loop_s
                return
            await self._cancel(o, f"vanished: {err}", now)
            return
        if kind == "protective":
            # the resting price violates the edge limit NOW: retrying the modify (e.g. order pool exhausted)
            # leaves it to be picked off -> pull it (cancels use their own, larger budget)
            log.warning("%s protective amend failed (%s) -> cancel", self.symbol, err)
            if not res.retryable and self.risk is not None:
                try:
                    self.risk.record_error(self.maker.name, err)
                except Exception:
                    pass
            await self._cancel(o, f"protective amend failed: {err}", now)
            return
        if res.retryable:
            self._backoff_until = now + self.loop_s
            return
        log.warning("%s modify failed permanently: %s -> cancel", self.symbol, err)
        if self.risk is not None:
            try:
                self.risk.record_error(self.maker.name, err)
            except Exception:
                pass
        await self._cancel(o, f"modify failed: {err}", now)
        self._backoff_until = now + 4 * self.loop_s

    def _drop(self, o: Order) -> None:
        if self._order is o or (self._order is not None and self._order.client_id == o.client_id):
            self._order = None
        self._amend_target = None

    async def _send_cancel(self, o: Order) -> bool:
        try:
            return bool(await self.maker.cancel_order(self.ms, order_id=o.order_id, client_id=o.client_id))
        except Exception as exc:
            log.warning("%s cancel %s raised %r", self.symbol, o.client_id, exc)
            return False

    async def _cancel(self, o: Order, reason: str, now: float) -> None:
        self._drop(o)
        if o.status.is_terminal:
            return
        self.stats["cancels"] += 1
        log.debug("%s cancel quote %s (%s)", self.symbol, o.client_id, reason)
        d = _Dying(o, now, accepted=False)
        self._dying[o.client_id] = d
        d.accepted = await self._send_cancel(o)
        if o.status.is_terminal:
            self._dying.pop(o.client_id, None)

    async def _process_dying(self, now: float) -> None:
        for cid, d in list(self._dying.items()):
            if d.order.status.is_terminal:
                self._dying.pop(cid, None)
                continue
            if now - d.last_ts < self.cancel_confirm_s:
                continue
            if d.accepted:
                live = (d.order.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
                        or d.order.updated_ts > d.last_ts)
                if not live:  # never acknowledged by the venue, cancel accepted, no news since: assume gone
                    log.info("%s cancel of %s accepted but unconfirmed after %.1fs; assuming gone", self.symbol,
                             cid, now - d.first_ts)
                    self._dying.pop(cid, None)
                    continue
                # the venue reports the order live (or updated it) although the cancel was accepted: e.g. the
                # cancel overtook its own not-yet-sequenced place -> cancel again (escalates to cancel_all)
                log.info("%s cancel of %s accepted but the order is still %s after %.1fs -> cancel again",
                         self.symbol, cid, d.order.status.value, now - d.first_ts)
            if d.attempts >= 3:
                log.warning("%s cancel of %s failed %d times -> cancel_all(%s)", self.symbol, cid, d.attempts,
                            self.ms)
                try:
                    await self.maker.cancel_all(self.ms)
                    self._dying.pop(cid, None)
                except Exception as exc:
                    log.error("%s cancel_all failed: %r", self.symbol, exc)
                    d.last_ts = now
                continue
            d.attempts += 1
            d.last_ts = now
            d.accepted = await self._send_cancel(d.order)

    # ------------------------------------------------------------------ reporting
    def _report_exposure(self, mb: OrderBook | None, tb: OrderBook | None) -> None:
        if self.risk is None:
            return
        try:
            if mb is not None and mb.mid is not None:
                self.risk.update_exposure(self.symbol, self.maker.name, abs(self.hedger.maker_pos) * mb.mid)
            if tb is not None and tb.mid is not None:
                self.risk.update_exposure(self.symbol, self.taker.name, abs(self.hedger.taker_pos) * tb.mid)
        except Exception:
            log.exception("risk.update_exposure failed")

    def status(self) -> dict:
        now = self.clock()
        mb, tb = self._books()
        o = self._order
        snap = self.funding.latest(self.symbol) if self.funding is not None else None
        funding = None
        if snap is not None:
            funding = {
                "maker_rate_h": snap.maker.rate_hourly, "taker_rate_h": snap.taker.rate_hourly,
                "best_direction": snap.best_direction,
                "carry_apr": snap.apr(self.direction) if self.direction is not None else None,
                "next_settlement_ts": snap.next_settlement_ts,
            }
        div = None
        if mb is not None and tb is not None and mb.mid and tb.mid:
            div = (mb.mid / tb.mid - 1) / BPS
        h = self.hedger
        return {
            "symbol": self.symbol,
            "maker_symbol": self.ms,
            "taker_symbol": self.ts,
            "phase": self.phase.value,
            "direction": self.direction,
            "direction_label": {1: "maker LONG / taker SHORT", -1: "maker SHORT / taker LONG"}.get(self.direction or 0),
            "pending_direction": self.pending_direction,
            "forced_close": self._forced,
            "target_base": self.target_base,
            "floor_base": self.floor_base,
            "goal": self.goal(),
            "maker_pos": h.maker_pos,
            "taker_pos": h.taker_pos,
            "delta": h.delta,
            "inflight": h.inflight,
            "quote": None if o is None else {
                "client_id": o.client_id, "side": o.side.value, "price": o.price, "size": o.size, "filled": o.filled,
                "remaining": self._remaining(o), "status": o.status.value, "reduce_only": o.reduce_only,
                "age_s": round(now - self._order_born, 3),
            },
            "block_reason": self.block_reason,
            "funding": funding,
            "in_funding_window": self._in_window,
            "next_settlement_ts": self._next_settle,
            "basis": {
                "ema_bps": self.basis.value / BPS if self.basis.value is not None else None,
                "last_bps": self.basis.last / BPS if self.basis.last is not None else None,
                "deviation_bps": self.basis.deviation_bps(),
                "warmed_up": self.basis.warmed_up(float(self.scfg.basis_warmup_s)),
            },
            "books": {
                "maker_mid": mb.mid if mb is not None else None,
                "taker_mid": tb.mid if tb is not None else None,
                "maker_age_s": round(mb.age(), 3) if mb is not None else None,
                "taker_age_s": round(tb.age(), 3) if tb is not None else None,
                "divergence_bps": div,
            },
            "hedger": h.status(),
            "running": self._running,
            "stats": dict(self.stats),
        }


__all__ = ["PairTrader", "Phase"]
