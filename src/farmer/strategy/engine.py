"""Engine: wires venues, maps markets, starts monitors/hedgers/traders and shuts everything down safely.

Lifecycle:
- ``start()``: connect venues, resolve pairs, set leverage, cancel stale maker orders, read positions (and warn
  about positions no pair manages), subscribe books, start the funding monitor, the risk manager and one Hedger
  per pair (pre-existing delta is hedged right away). Traders are created but do not quote yet.
  ``include_reduce_only`` (close-all) also maps markets that only accept reduce-only orders as ``close_only``
  pairs: a hedger (reduce-only) but no trader; ``close_all`` flattens them with reduce-only IOCs in either mode.
- ``run()``: ``start()`` + traders + periodic status log, until ``request_stop()``; then ``shutdown()``.
- ``shutdown()``: stop traders (cancel quotes; then cancel_all on every maker market as insurance), let hedgers
  flush and settle (fills racing the cancels), optionally ``close_all("taker")`` (``risk.close_on_exit``, only if
  ``start()`` completed), stop background tasks, check every pair is hedged, close venues, deliver alerts.

Every background task is supervised: exceptions are logged and the task restarts with backoff.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any, Literal

from farmer.config import AppConfig
from farmer.exchanges.base import Exchange
from farmer.models import ZERO
from farmer.strategy.funding import FundingMonitor, describe
from farmer.strategy.hedger import Hedger
from farmer.strategy.mapping import PairSpec, resolve_pairs
from farmer.strategy.trader import PairTrader
from farmer.utils.decimals import fmt

log = logging.getLogger("farmer.engine")


class Engine:
    def __init__(self, cfg: AppConfig, venues: dict[str, Exchange], store: Any, notifier: Any, risk: Any, *,
                 funding: FundingMonitor | None = None, include_reduce_only: bool = False) -> None:
        self.cfg = cfg
        self.include_reduce_only = include_reduce_only
        self.venues = venues
        self.store = store
        self.notifier = notifier
        self.risk = risk
        self.maker = venues[cfg.strategy.maker_venue]
        self.taker = venues[cfg.strategy.taker_venue]
        self.funding = funding
        self.pairs: list[PairSpec] = []
        self.skipped: list[str] = []
        self.hedgers: dict[str, Hedger] = {}
        self.traders: dict[str, PairTrader] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._restarts: dict[str, int] = {}
        self._trader_tasks: dict[str, asyncio.Task] = {}
        self._stop_evt = asyncio.Event()
        self._start_lock = asyncio.Lock()
        self._shutdown_lock = asyncio.Lock()
        self._started = False
        self._trading = False
        self._stopping = False
        self._shutdown_done = False
        self._traders_started = False
        self._bg: set[asyncio.Future] = set()
        self.started_ts: float | None = None
        self.flush_timeout_s = float(cfg.strategy.shutdown_flush_s)
        self.close_timeout_s = float(cfg.strategy.close_on_exit_timeout_s)
        self.status_interval_s = float(cfg.monitoring.report_interval_s or 60.0)
        self.check_prices = bool(cfg.strategy.check_prices)
        self.position_retries = int(cfg.strategy.startup_position_retries)
        # shutdown: hedgers keep running this long after the last maker fill (fills racing the quote cancels)
        self.settle_s = float(cfg.strategy.shutdown_settle_s)
        # runtime sweep of untracked open quotes (e.g. a place that landed after its cancel); 0 = off
        self.orphan_sweep_s = float(cfg.strategy.orphan_sweep_s)

    # ------------------------------------------------------------------ lifecycle
    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        """Connect, map, prepare (idempotent). Raises if no pair is tradeable."""
        async with self._start_lock:
            if self._started:
                return
            await self._start()
            self._started = True
            self.started_ts = time.time()

    async def _start(self) -> None:
        scfg = self.cfg.strategy
        log.info("engine starting: maker=%s taker=%s mode=%s", self.maker.name, self.taker.name, self.cfg.mode)
        venues = list(dict.fromkeys([self.maker, self.taker, *self.venues.values()]))
        res = await asyncio.gather(*(v.connect() for v in venues), return_exceptions=True)
        for v, r in zip(venues, res, strict=True):
            if isinstance(r, BaseException):
                if v in (self.maker, self.taker):
                    raise RuntimeError(f"{v.name}: connect failed: {r!r}") from r
                log.warning("%s: connect failed (unused venue): %r", v.name, r)

        self.pairs, self.skipped = await resolve_pairs(self.maker, self.taker, self.cfg.enabled_markets(),
                                                       check_prices=self.check_prices,
                                                       allow_reduce_only=self.include_reduce_only)
        for reason in self.skipped:
            log.warning("market skipped: %s", reason)
        for p in self.pairs:
            log.info("pair: %s", p.describe())
        self._record("pairs", {"pairs": [p.describe() for p in self.pairs], "skipped": self.skipped})
        if not self.pairs:
            raise RuntimeError("no tradeable pairs: " + "; ".join(self.skipped))
        tradeable = [p for p in self.pairs if p.tradeable]

        self._trading = self.maker.has_credentials and self.taker.has_credentials
        if not self._trading:
            log.warning("credentials missing (maker=%s taker=%s): read-only engine", self.maker.has_credentials,
                        self.taker.has_credentials)
        if self._trading:
            await self._set_leverage(tradeable)
            await self._cancel_stale_orders()
        m_pos, t_pos = await self._read_positions()
        self._warn_unmanaged(m_pos, t_pos)
        if self._trading:  # a pair that starts without positions starts a fresh price-move reference
            self._reset_flat_risk([p.symbol for p in self.pairs
                                   if not m_pos.get(p.maker_symbol) and not t_pos.get(p.taker_symbol)])

        for p in self.pairs:
            h = Hedger(p, self.maker, self.taker, scfg, self.cfg.risk, self.risk, self.store, notifier=self.notifier)
            mp = m_pos.get(p.maker_symbol, ZERO)
            tp = t_pos.get(p.taker_symbol, ZERO)
            h.set_positions(mp, tp)
            if mp or tp:
                log.warning("%s: existing positions maker=%s taker=%s (delta %s) - adopting and hedging", p.symbol,
                            mp, tp, mp + tp)
            self.hedgers[p.symbol] = h

        await self._subscribe_books()

        if self.funding is None:
            self.funding = FundingMonitor(self.maker, self.taker, [p.symbol for p in tradeable],
                                          refresh_s=scfg.funding_refresh_s,
                                          maker_symbols={p.symbol: p.maker_symbol for p in tradeable},
                                          taker_symbols={p.symbol: p.taker_symbol for p in tradeable})
        try:
            await asyncio.wait_for(self.funding.refresh(), 15.0)
            for snap in self.funding.all().values():
                log.info("funding %s", describe(snap))
        except Exception as exc:
            log.warning("initial funding refresh failed: %r", exc)

        for p in tradeable:
            self.traders[p.symbol] = PairTrader(p, self.maker, self.taker, scfg, self.risk, self.funding, self.store,
                                                hedger=self.hedgers[p.symbol], rcfg=self.cfg.risk,
                                                notifier=self.notifier)

        fm = self.funding
        self._spawn("funding", fm.run, should_run=lambda: not fm.stopped)
        if self.risk is not None and hasattr(self.risk, "run"):
            self._spawn("risk", self.risk.run)
        if self._trading:
            for sym, h in self.hedgers.items():
                self._spawn(f"hedger:{sym}", h.run, should_run=lambda h=h: not h.stopped)
            await self._setup_dms()
            if self.orphan_sweep_s > 0:
                self._spawn("orphan-sweep", self._orphan_sweep_loop)
        self._spawn("status", self._status_loop)

    async def run(self) -> None:
        """start() + trade until request_stop(); always ends with shutdown()."""
        try:
            await self.start()
            if not self._trading:
                raise RuntimeError("cannot trade without credentials on both venues")
            self._start_traders()
            self._notify(f"farmer started: {len(self.pairs)} pairs ({', '.join(p.symbol for p in self.pairs)})"
                         + (f"; skipped {len(self.skipped)}" if self.skipped else ""))
            await self._stop_evt.wait()
        finally:
            await self.shutdown()

    def request_stop(self) -> None:
        """Ask ``run()`` to shut down (safe from a signal handler on the loop thread)."""
        log.info("stop requested")
        self._stop_evt.set()

    async def shutdown(self) -> None:
        async with self._shutdown_lock:
            if self._shutdown_done:
                return
            self._stopping = True
            log.info("engine shutting down")
            # 1. stop quoting (hedgers keep running)
            unconfirmed: list[str] = []
            if self.traders:
                res = await asyncio.gather(*(self._stop_trader(t) for t in self.traders.values()),
                                           return_exceptions=True)
                unconfirmed = [s for s, r in zip(self.traders, res, strict=True) if r is not True]
            quoted = self._trading and self._traders_started
            if quoted:
                await self._final_cancel_all()  # insurance: quotes whose cancel is unconfirmed / in transit
                if unconfirmed:
                    msg = f"shutdown: quote cancel unconfirmed for {', '.join(unconfirmed)}; a quote may still rest"
                    log.critical(msg)
                    self._notify(msg)
            # 2. let hedgers flush outstanding deltas (and settle: maker fills racing the cancels)
            if self.hedgers and self._trading:
                if quoted and unconfirmed:
                    await self._wait_quotes_gone(max(self._dms_seconds(), self.flush_timeout_s))
                settle = min(self.settle_s, self.flush_timeout_s) if quoted else 0.0
                ok = await asyncio.gather(*(h.flush(self.flush_timeout_s, settle_s=settle)
                                            for h in self.hedgers.values()), return_exceptions=True)
                bad = [s for s, r in zip(self.hedgers, ok, strict=True) if r is not True]
                if bad:
                    self._notify(f"shutdown: hedgers not flat for {', '.join(bad)}")
            # 3. optional flatten (never re-runs a startup that did not complete)
            if self.cfg.risk.close_on_exit and self._trading and self.hedgers:
                if self._started:
                    try:
                        await self.close_all("taker", timeout_s=self.close_timeout_s)
                    except Exception:
                        log.exception("close_on_exit failed")
                else:
                    msg = "close_on_exit skipped: engine did not finish starting; positions left as they are"
                    log.critical(msg)
                    self._notify(msg)
            # 4. stop background tasks
            for h in self.hedgers.values():
                h.stop()
            if self.funding is not None:
                self.funding.stop()
            if self.risk is not None and hasattr(self.risk, "stop"):
                try:
                    self.risk.stop()
                except Exception:
                    log.exception("risk.stop failed")
            tasks = list(self._tasks.values()) + list(self._trader_tasks.values())
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._tasks.clear()
            self._trader_tasks.clear()
            if self._trading:
                self._final_delta_check()
            # 5. close venues
            venues = list(dict.fromkeys([self.maker, self.taker, *self.venues.values()]))
            await asyncio.gather(*(self._close_venue(v) for v in venues), return_exceptions=True)
            try:
                self._log_session_stats()
            except Exception:
                log.exception("session stats failed")
            self._record("shutdown", self._summary())
            # 6. deliver queued alerts before the caller closes the notifier
            await self._drain_notifications()
            self._shutdown_done = True
            log.info("engine stopped")

    async def _final_cancel_all(self) -> None:
        for sym in dict.fromkeys(p.maker_symbol for p in self.pairs):
            try:
                await asyncio.wait_for(self.maker.cancel_all(sym), 15.0)
            except Exception as exc:
                log.error("%s: shutdown cancel_all(%s) failed: %r", self.maker.name, sym, exc)

    async def _wait_quotes_gone(self, timeout_s: float) -> bool:
        """Poll the maker's open orders on our markets until none rest (hedgers keep running meanwhile)."""
        syms = {p.maker_symbol for p in self.pairs}
        deadline = time.monotonic() + max(0.0, timeout_s)
        while True:
            try:
                orders = await asyncio.wait_for(self.maker.fetch_open_orders(), 15.0)
                left = [o for o in orders if o.symbol in syms and o.is_open]
            except Exception as exc:
                log.warning("%s: open-order check failed: %r", self.maker.name, exc)
                left = None
            if left == []:
                return True
            if time.monotonic() >= deadline:
                msg = f"shutdown: maker orders may still rest on {sorted(syms)} (venue dead man's switch pending)"
                log.critical(msg)
                self._notify(msg)
                return False
            if left:
                await self._final_cancel_all()
            await asyncio.sleep(1.0)

    def _final_delta_check(self) -> None:
        for s, h in self.hedgers.items():
            try:
                hedged = h.is_hedged()
            except Exception:
                hedged = False
            if not hedged:
                msg = (f"shutdown: {s} NOT hedged: maker={fmt(h.maker_pos)} taker={fmt(h.taker_pos)} "
                       f"delta={fmt(h.delta)} inflight={fmt(h.inflight)}")
                log.critical(msg)
                self._notify(msg)

    async def _drain_notifications(self, timeout_s: float = 10.0) -> None:
        pending = set(self._bg)
        for h in self.hedgers.values():
            pending |= set(getattr(h, "_bg", ()))
        pending = {f for f in pending if not f.done()}
        if pending:
            await asyncio.wait(pending, timeout=timeout_s)

    # ------------------------------------------------------------------ start helpers
    def _warn_unmanaged(self, m_pos: dict[str, Decimal], t_pos: dict[str, Decimal]) -> None:
        """Positions on markets no pair manages are neither hedged nor closed by the bot: say so at startup."""
        mine = {self.maker.name: {p.maker_symbol for p in self.pairs},
                self.taker.name: {p.taker_symbol for p in self.pairs}}
        extra = [f"{ex.name}:{s} {fmt(q)}" for ex, pos in ((self.maker, m_pos), (self.taker, t_pos))
                 for s, q in sorted(pos.items()) if q != 0 and s not in mine[ex.name]]
        if not extra:
            return
        msg = (f"positions not managed by any pair (neither hedged nor closed by the bot): {', '.join(extra)}. "
               "Close them on the venue, or with `farmer close-all` if they belong to a configured market")
        log.warning(msg)
        self._notify(f"WARNING: {msg}")
        self._record("unmanaged_positions", {"positions": extra, "skipped": self.skipped})

    async def _set_leverage(self, pairs: list[PairSpec]) -> None:
        """Per pair on both venues (venues in parallel, markets sequentially: Arcus setLeverage costs 125 IP
        weight, Lighter one tx of the shared 60/min budget). Failures are logged, never fatal."""
        if not self.cfg.strategy.set_leverage_on_start:
            return

        async def one_venue(ex: Exchange, items: list[tuple[str, int]]) -> None:
            for sym, lev in items:
                try:
                    await asyncio.wait_for(ex.set_leverage(sym, lev), 30.0)
                except Exception as exc:
                    log.warning("%s: set_leverage(%s, %s) failed: %r", ex.name, sym, lev, exc)

        await asyncio.gather(one_venue(self.maker, [(p.maker_symbol, int(p.cfg.leverage)) for p in pairs]),
                             one_venue(self.taker, [(p.taker_symbol, int(p.cfg.leverage)) for p in pairs]))

    async def _cancel_stale_orders(self) -> None:
        """Cancel resting maker orders on our markets (by id: cancelAll can cost far more pool units)."""
        syms = {p.maker_symbol for p in self.pairs}
        try:
            orders = await asyncio.wait_for(self.maker.fetch_open_orders(), 20.0)
        except Exception as exc:
            log.warning("%s: open-order read failed (%r) -> cancel_all per market", self.maker.name, exc)
            for s in syms:
                try:
                    await self.maker.cancel_all(s)
                except Exception as exc2:
                    log.error("%s cancel_all(%s) failed: %r", self.maker.name, s, exc2)
            return
        stale = [o for o in orders if o.symbol in syms and o.is_open]
        if not stale:
            return
        log.warning("%s: cancelling %d stale order(s) on %s", self.maker.name, len(stale), sorted(syms))
        by_sym: dict[str, list] = {}
        for o in stale:
            by_sym.setdefault(o.symbol, []).append(o)
        for s, lst in by_sym.items():
            if len(lst) > 20:
                try:
                    await self.maker.cancel_all(s)
                except Exception as exc:
                    log.error("%s cancel_all(%s) failed: %r", self.maker.name, s, exc)
                continue
            for o in lst:
                try:
                    await self.maker.cancel_order(s, order_id=o.order_id, client_id=o.client_id or None)
                except Exception as exc:
                    log.error("%s cancel %s failed: %r", self.maker.name, o.order_id, exc)

    async def _read_positions(self) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
        if not self._trading:
            return {}, {}
        last: BaseException | None = None
        for attempt in range(max(1, self.position_retries)):
            try:
                mp, tp = await asyncio.wait_for(asyncio.gather(self.maker.fetch_positions(),
                                                               self.taker.fetch_positions()), 30.0)
                return {s: p.size for s, p in mp.items()}, {s: p.size for s, p in tp.items()}
            except Exception as exc:
                last = exc
                log.warning("startup position read failed (%d): %r", attempt + 1, exc)
                await asyncio.sleep(min(5.0, 0.5 * 2 ** attempt))
        raise RuntimeError(f"cannot read positions at startup: {last!r}")

    async def _subscribe_books(self) -> None:
        for ex, syms in ((self.maker, [p.maker_symbol for p in self.pairs]),
                         (self.taker, [p.taker_symbol for p in self.pairs])):
            try:
                await ex.subscribe_books(syms)
            except Exception as exc:
                log.error("%s subscribe_books failed: %r", ex.name, exc)
                raise

    def _dms_seconds(self) -> float:
        section = getattr(self.cfg, self.cfg.strategy.maker_venue, None)
        return float(getattr(section, "dead_mans_switch_s", 0) or 0)

    async def _setup_dms(self) -> None:
        dms = self._dms_seconds()
        if dms <= 0 or self.maker.is_paper or not self.maker.has_credentials:
            return
        if callable(getattr(self.maker, "_refresh_dms", None)):
            log.info("%s refreshes its dead man's switch (%.0fs) itself", self.maker.name, dms)
            return
        try:
            await self.maker.arm_dead_mans_switch(dms)
        except Exception as exc:
            log.warning("%s: arming dead man's switch failed: %r", self.maker.name, exc)
        self._spawn("dms", lambda: self._dms_loop(dms))

    async def _dms_loop(self, dms: float) -> None:
        while not self._stopping:
            await asyncio.sleep(max(1.0, dms / 3.0))
            if self._stopping:
                return
            try:
                await self.maker.arm_dead_mans_switch(dms)
            except Exception as exc:
                log.warning("%s: dead man's switch refresh failed: %r", self.maker.name, exc)

    def _start_traders(self, only: list[str] | None = None) -> None:
        for sym, t in self.traders.items():
            if only is not None and sym not in only:
                continue
            task = self._trader_tasks.get(sym)
            if task is not None and not task.done():
                continue
            t.resume()
            self._traders_started = True
            self._trader_tasks[sym] = self._supervise(f"trader:{sym}", t.run, should_run=lambda t=t: not t.stopped)

    async def _stop_trader(self, t: PairTrader) -> bool:
        """Stop one trader; False if its quote cancel was not confirmed."""
        ok = False
        try:
            ok = await asyncio.wait_for(t.stop(cancel_quotes=True), 15.0) is not False
        except Exception as exc:
            log.error("%s: trader stop failed: %r", t.symbol, exc)
        task = self._trader_tasks.pop(t.symbol, None)
        if task is not None and not task.done():
            try:
                await asyncio.wait_for(asyncio.shield(task), 5.0)
            except (TimeoutError, asyncio.CancelledError, Exception):
                task.cancel()
        return ok

    async def _orphan_sweep_loop(self) -> None:
        """Every ``orphan_sweep_s``: hand the maker's open orders to the running traders, which cancel quotes of
        theirs they no longer track (the WS order stream is the primary source; this catches what it missed)."""
        while not self._stopping:
            await asyncio.sleep(self.orphan_sweep_s)
            if self._stopping:
                return
            live = [t for t in self.traders.values() if t.running]
            if not live:
                continue
            try:
                orders = await asyncio.wait_for(self.maker.fetch_open_orders(), 20.0)
            except Exception as exc:
                log.warning("%s: open-order sweep failed: %r", self.maker.name, exc)
                continue
            for t in live:
                try:
                    t.handle_open_orders(orders)
                except Exception:
                    log.exception("%s: open-order sweep failed", t.symbol)

    async def _close_venue(self, v: Exchange) -> None:
        try:
            await asyncio.wait_for(v.close(), 15.0)
        except Exception as exc:
            log.warning("%s close failed: %r", v.name, exc)

    # ------------------------------------------------------------------ supervision
    def _spawn(self, name: str, factory: Callable[[], Awaitable], *,
               should_run: Callable[[], bool] | None = None) -> asyncio.Task:
        task = self._supervise(name, factory, should_run=should_run)
        self._tasks[name] = task
        return task

    def _supervise(self, name: str, factory: Callable[[], Awaitable], *,
                   should_run: Callable[[], bool] | None = None) -> asyncio.Task:
        async def runner() -> None:
            delay = 1.0
            while True:
                try:
                    await factory()
                    if self._stopping or (should_run is not None and not should_run()):
                        return
                    log.warning("task %s exited unexpectedly; restarting in %.1fs", name, delay)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("task %s crashed; restarting in %.1fs", name, delay)
                if self._stopping or (should_run is not None and not should_run()):
                    return
                self._restarts[name] = self._restarts.get(name, 0) + 1
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

        return asyncio.create_task(runner(), name=f"farmer:{name}")

    async def _status_loop(self) -> None:
        while not self._stopping:
            await asyncio.sleep(self.status_interval_s)
            try:
                for line in self.status_lines():
                    log.info("%s", line)
            except Exception:
                log.exception("status log failed")

    # ------------------------------------------------------------------ close all
    async def close_all(self, mode: Literal["maker", "taker"] = "maker", timeout_s: float = 600) -> dict:
        """Flatten every pair. ``maker``: hedged post-only closing (taker hedges) until flat; traders stay in
        forced-close afterwards. ``taker``: stop quoting and IOC both legs (reduce-only) until flat. ``close_only``
        pairs (no trader) are flattened with reduce-only IOCs in both modes.

        Returns {symbol: {"maker_pos", "taker_pos", "flat"}}.
        """
        if mode not in ("maker", "taker"):
            raise ValueError(f"unknown close mode {mode!r}")
        if self._stopping and not self._started:
            raise RuntimeError("engine did not finish starting; not (re)starting it during shutdown")
        await self.start()
        if not self._trading:
            raise RuntimeError("cannot close without credentials on both venues")
        for sym, h in self.hedgers.items():  # hedgers must run (they may be stopped after shutdown began)
            task = self._tasks.get(f"hedger:{sym}")
            if task is None or task.done():
                self._spawn(f"hedger:{sym}", h.run, should_run=lambda h=h: not h.stopped)
        deadline = time.monotonic() + max(0.0, float(timeout_s))
        log.warning("close_all(%s) for %s (timeout %.0fs)", mode, ", ".join(self.hedgers), timeout_s)
        self._notify(f"close_all({mode}) started for {', '.join(self.hedgers)}")
        if mode == "maker":
            for t in self.traders.values():
                t.force_close(True)
            self._start_traders()
            close_only = [h for h in self.hedgers.values() if h.close_only]
            while time.monotonic() < deadline:
                if all(self._is_flat(h, mode) for h in self.hedgers.values()):
                    break
                await self._flatten_pending(close_only)
                await asyncio.sleep(0.05 if self.cfg.strategy.loop_interval_s < 0.05 else 0.25)
        else:
            await asyncio.gather(*(self._stop_trader(t) for t in self.traders.values()), return_exceptions=True)
            for h in self.hedgers.values():
                h.close_mode = True
            try:
                while time.monotonic() < deadline:
                    if all(h.is_flat() for h in self.hedgers.values()):
                        break
                    await self._flatten_pending(list(self.hedgers.values()))
                    await asyncio.sleep(0.05 if self.cfg.strategy.loop_interval_s < 0.05 else 0.5)
            finally:
                for h in self.hedgers.values():
                    h.close_mode = False
        result = {s: {"maker_pos": h.maker_pos, "taker_pos": h.taker_pos, "flat": self._is_flat(h, mode)}
                  for s, h in self.hedgers.items()}
        self._reset_flat_risk([s for s, r in result.items() if r["flat"]])
        not_flat = [s for s, r in result.items() if not r["flat"]]
        msg = (f"close_all({mode}) done" if not not_flat else
               f"close_all({mode}) TIMED OUT, not flat: " + ", ".join(
                   f"{s} maker={fmt(result[s]['maker_pos'])} taker={fmt(result[s]['taker_pos'])}" for s in not_flat))
        (log.info if not not_flat else log.error)(msg)
        self._notify(msg)
        self._record("close_all", {"mode": mode, "result": result})
        return result

    def _reset_flat_risk(self, symbols: list[str]) -> None:
        """Flat pairs: zero their reported exposure (taker-mode traders are stopped and report nothing) and drop
        their price-move reference so the next position takes a fresh one."""
        if not symbols or self.risk is None:
            return
        try:
            for s in symbols:
                self.risk.update_exposure(s, self.maker.name, ZERO)
                self.risk.update_exposure(s, self.taker.name, ZERO)
            reset = getattr(self.risk, "reset_move_refs", None)
            if callable(reset):
                reset(symbols)
        except Exception:
            log.exception("risk reset for flat pairs failed")

    @staticmethod
    async def _flatten_pending(hedgers: list[Hedger]) -> None:
        """One reduce-only IOC round on both legs of every hedger that is not flat and has nothing in flight."""
        for h in hedgers:
            if h.is_flat() or h.has_inflight():
                continue
            try:
                await h.flatten()
            except Exception:
                log.exception("%s flatten failed", h.symbol)

    @staticmethod
    def _is_flat(h: Hedger, mode: str) -> bool:
        """Maker mode: a maker residual below the PAIR minimum can never be quoted (the forced-close trader stops
        there), so it is dust like in the trader; taker mode (and close-only pairs, which are always flattened with
        IOCs) uses the per-venue minimums (``flatten``)."""
        if mode != "maker" or h.close_only:
            return h.is_flat()
        px = h.ref_price()
        return h.is_flat(maker_min=h.pair.min_size(px) if px is not None and px > 0 else None)

    # ------------------------------------------------------------------ status
    def _venue_status(self, v: Exchange) -> dict:
        out: dict[str, Any] = {"has_credentials": v.has_credentials, "paper": v.is_paper}
        for k, fn in (("budget_health", v.budget_health), ("can_increase_exposure", v.can_increase_exposure)):
            try:
                out[k] = fn()
            except Exception as exc:
                out[k] = f"error: {exc!r}"
        stats = getattr(v, "stats", None)
        if callable(stats):
            try:
                out["stats"] = stats()
            except Exception:
                pass
        try:
            bal = self.risk.balance(v.name) if self.risk is not None and hasattr(self.risk, "balance") else None
        except Exception:
            bal = None
        if bal is not None:
            out["balance"] = {"equity": bal.equity, "available": bal.available, "margin_ratio": bal.margin_ratio}
        return out

    def status(self) -> dict:
        markets: dict[str, Any] = {}
        for p in self.pairs:
            t = self.traders.get(p.symbol)
            h = self.hedgers.get(p.symbol)
            if t is not None:
                markets[p.symbol] = t.status()
            elif h is not None:
                markets[p.symbol] = {"symbol": p.symbol, "hedger": h.status(), "maker_pos": h.maker_pos,
                                     "taker_pos": h.taker_pos, "delta": h.delta}
        risk = None
        if self.risk is not None and hasattr(self.risk, "status"):
            try:
                risk = self.risk.status()
            except Exception as exc:
                risk = {"error": repr(exc)}
        tasks = {n: ("running" if not t.done() else "done") for n, t in {**self._tasks, **self._trader_tasks}.items()}
        return {
            "mode": self.cfg.mode,
            "started": self._started,
            "started_ts": self.started_ts,
            "trading": self._trading,
            "stopping": self._stopping,
            "maker_venue": self.maker.name,
            "taker_venue": self.taker.name,
            "markets": markets,
            "skipped": list(self.skipped),
            "venues": {n: self._venue_status(v) for n, v in self.venues.items()},
            "risk": risk,
            "tasks": tasks,
            "restarts": dict(self._restarts),
        }

    def status_lines(self) -> list[str]:
        lines = []
        for p in self.pairs:
            t = self.traders.get(p.symbol)
            h = self.hedgers.get(p.symbol)
            if h is None:
                continue
            ph = t.phase.value if t is not None else "-"
            d = t.direction if t is not None else None
            q = t.status()["quote"] if t is not None else None
            qs = f"{q['side']} {fmt(q['remaining'])}@{fmt(q['price'])}" if q else (t.block_reason if t else "-")
            lines.append(f"{p.symbol} {ph} dir={d} maker={fmt(h.maker_pos)} taker={fmt(h.taker_pos)} "
                         f"delta={fmt(h.delta)} vol_m={fmt(h.stats['maker_volume'], 2)} "
                         f"vol_t={fmt(h.stats['hedge_volume'], 2)} quote={qs}")
        return lines

    def _summary(self) -> dict:
        out: dict[str, dict] = {}
        for s, h in self.hedgers.items():
            t = self.traders.get(s)
            out[s] = {"maker_pos": h.maker_pos, "taker_pos": h.taker_pos, "maker_volume": h.stats["maker_volume"],
                      "hedge_volume": h.stats["hedge_volume"], "hedger": dict(h.stats),
                      "trader": dict(t.stats) if t is not None else None}
        return out

    def _log_session_stats(self) -> None:
        """One line per pair with the quoting / hedging counters of this session (requote rate check)."""
        up = max(1.0, time.time() - (self.started_ts or time.time()))
        for s, h in self.hedgers.items():
            t = self.traders.get(s)
            ts = t.stats if t is not None else {}
            hs = h.stats
            log.info("%s session: places=%s amends=%s (protective %s, improve %s, resize %s, %.1f/min) cancels=%s "
                     "would_cross=%s "
                     "rejects=%s maker_fills=%s hedges=%s hedge_fills=%s failures=%s reconciles=%s adoptions=%s "
                     "emergencies=%s cycles=%s flips=%s vol maker=%s hedge=%s", s, ts.get("places"), ts.get("amends"),
                     ts.get("protective"), ts.get("improve"), ts.get("resize"),
                     60.0 * float(ts.get("amends") or 0) / up,
                     ts.get("cancels"), ts.get("would_cross"), ts.get("rejects"), hs.get("maker_fills"),
                     hs.get("hedges_sent"), hs.get("hedge_fills"), hs.get("hedge_failures"), hs.get("reconciles"),
                     hs.get("adoptions"), hs.get("emergencies"), ts.get("cycles"), ts.get("flips"),
                     fmt(hs.get("maker_volume"), 2), fmt(hs.get("hedge_volume"), 2))

    # ------------------------------------------------------------------ misc
    def _record(self, kind: str, data: dict) -> None:
        if self.store is None:
            return
        try:
            self.store.record_event(kind, data)
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


__all__ = ["Engine"]
