"""Risk manager: pre-trade checks, margin monitoring, circuit breaker and daily loss limit.

The strategy asks ``check_quote`` before (re)quoting and ``check_open`` before increasing a
position; hedging is never gated here (risk-reducing). ``run`` polls balances on every venue
with credentials and enforces the daily loss limit. ``halt`` is sticky until ``resume``.

Margin: ``check_open`` refuses new exposure when a venue's maintenance/equity ratio reaches
``max_margin_ratio`` or its free collateral cannot post the clip's initial margin with a buffer
(``min_free_collateral_mult``). At ``reduce_margin_ratio`` the manager halts (no quoting, no opening)
and runs the ``reducer`` the engine owner installed (``set_reducer``: reduce-only taker flatten of every
pair), retrying while the ratio stays high. Diverging venue equities are reported so the operator can
rebalance collateral (the bot never moves funds between venues).
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Awaitable, Callable, Mapping

from farmer.config import RiskConfig
from farmer.exchanges.base import Exchange
from farmer.models import BPS, ZERO, Balance, OrderBook
from farmer.monitoring.stats import IncrementalSummary, net_base, pnl_from_summary

if TYPE_CHECKING:  # pragma: no cover
    from farmer.monitoring.notifier import Notifier
    from farmer.persistence.store import Store

log = logging.getLogger("farmer.risk")

SECONDS_PER_DAY = 86400
REDUCE_RETRY_S = 60.0  # re-run the reducer this long after the previous run ended while margin stays high


def utc_midnight(ts: float) -> float:
    """POSIX seconds of the UTC midnight at or before ``ts``."""
    return float(int(ts // SECONDS_PER_DAY) * SECONDS_PER_DAY)


def _has_credentials(ex: Exchange) -> bool:
    try:
        return bool(ex.has_credentials)
    except Exception:
        return False


class RiskManager:
    """Venue-level and portfolio-level guard rails."""

    def __init__(
        self,
        cfg: RiskConfig,
        venues: dict[str, Exchange],
        store: "Store | None" = None,
        notifier: "Notifier | None" = None,
        *,
        interval_s: float = 10.0,
        clock: Callable[[], float] = time.time,
        leverage: Mapping[str, int] | None = None,
        default_leverage: int = 1,
    ) -> None:
        """``leverage``: {pair symbol: leverage} used for the free-collateral check (``default_leverage``
        otherwise; 1 = the most conservative)."""
        self.cfg = cfg
        self.venues = venues
        self.store = store
        self.notifier = notifier
        self.interval_s = float(interval_s)
        self._clock = clock
        # balance freshness (RiskConfig); never stricter than two poll intervals
        self.balance_timeout_s = float(cfg.balance_timeout_s)
        self.balance_max_age_s = max(float(cfg.balance_max_age_s), 2 * self.interval_s)

        self._halted = False
        self._halt_reason: str | None = None
        self._halt_ts: float | None = None
        self._errors: dict[str, int] = {}
        self._last_error: dict[str, str] = {}
        self._exposure: dict[str, dict[str, Decimal]] = {}  # venue -> symbol -> abs notional
        self._balances: dict[str, Balance] = {}
        self._balance_ts: dict[str, float] = {}  # local clock time of the last successful fetch
        self._balance_errors: dict[str, int] = {}
        self._margin_state: dict[str, str] = {}  # venue -> "ok" | "high" | "reduce" (log on transitions)
        self._daily_pnl: Decimal | None = None
        self._daily_pnl_ts: float | None = None
        self._daily: IncrementalSummary | None = None
        self._stop = asyncio.Event()
        self._tasks: set[asyncio.Task] = set()
        self._leverage = {str(k).upper(): max(1, int(v)) for k, v in (leverage or {}).items()}
        self.default_leverage = max(1, int(default_leverage))
        self._reducer: Callable[[], Awaitable] | None = None
        self._reduce_task: asyncio.Task | None = None
        self._reduce_end = float("-inf")
        self.reduce_retry_s = REDUCE_RETRY_S
        self._equity_diverged = False
        self._no_reducer_warned = False

    # ================================================================== state
    @property
    def halted(self) -> bool:
        return self._halted

    @property
    def halt_reason(self) -> str | None:
        return self._halt_reason

    def balance(self, venue: str) -> Balance | None:
        return self._balances.get(venue)

    def exposure(self, venue: str) -> Decimal:
        return sum(self._exposure.get(venue, {}).values(), ZERO)

    # ================================================================== loop
    async def run(self) -> None:
        """Poll balances, margin and daily loss every ``interval_s`` until ``stop()``."""
        while not self._stop.is_set():
            try:
                await self.refresh_balances()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("balance refresh failed")
            try:
                self.check_margin()
            except Exception:
                log.exception("margin check failed")
            try:
                await self.check_daily_loss_async()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("daily loss check failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_s)
            except asyncio.TimeoutError:
                pass

    def stop(self) -> None:
        self._stop.set()
        task, self._reduce_task = self._reduce_task, None
        if task is not None and not task.done():
            task.cancel()

    def set_reducer(self, fn: Callable[[], Awaitable] | None) -> None:
        """Coroutine factory run when a venue's margin ratio reaches ``reduce_margin_ratio`` (normally
        ``lambda: engine.close_all("taker")``: reduce-only IOCs on both legs, regardless of halt or edge)."""
        self._reducer = fn

    async def refresh_balances(self) -> None:
        """Fetch ``Balance`` on every venue with credentials; failures keep the previous value."""
        names = [n for n, v in self.venues.items() if _has_credentials(v)]
        if not names:
            return
        results = await asyncio.gather(
            *(asyncio.wait_for(self.venues[n].fetch_balance(), self.balance_timeout_s) for n in names),
            return_exceptions=True,
        )
        for name, res in zip(names, results):
            if isinstance(res, BaseException):
                if isinstance(res, asyncio.CancelledError):
                    raise res
                self._balance_errors[name] = self._balance_errors.get(name, 0) + 1
                log.warning("%s fetch_balance failed (%d in a row): %r", name, self._balance_errors[name], res)
                continue
            if not isinstance(res, Balance):
                log.warning("%s fetch_balance returned %r", name, type(res).__name__)
                continue
            self._balance_errors[name] = 0
            self._balances[name] = res
            self._balance_ts[name] = self._clock()
            self._log_margin(name, res)
        self._check_equity_divergence()

    def _check_equity_divergence(self) -> None:
        """Warn (log + notify, on transitions) when venue equities drift apart: a trend drains the losing
        venue's collateral while the bot never moves funds, so the operator has to rebalance."""
        frac = Decimal(str(self.cfg.max_equity_divergence_frac or 0))
        eqs = {v: b.equity for v, b in self._balances.items()}
        if frac <= 0 or len(eqs) < 2:
            return
        hi_v = max(eqs, key=lambda v: eqs[v])
        lo_v = min(eqs, key=lambda v: eqs[v])
        hi, lo = eqs[hi_v], eqs[lo_v]
        diverged = hi > 0 and (hi - lo) / hi > frac
        if diverged == self._equity_diverged:
            return
        self._equity_diverged = diverged
        if diverged:
            msg = (f"venue equities diverge: {lo_v} {lo:.2f} vs {hi_v} {hi:.2f} (> {frac * 100:.0f}% apart); "
                   f"rebalance collateral before {lo_v} runs out of margin")
            log.warning(msg)
            self._record("equity_divergence", {"low": lo_v, "low_equity": lo, "high": hi_v, "high_equity": hi})
            self._notify(f"WARNING: {msg}")
        else:
            log.info("venue equities back within %.0f%% of each other", frac * 100)

    def _log_margin(self, venue: str, bal: Balance) -> None:
        mr = bal.margin_ratio
        state = "reduce" if mr >= self.cfg.reduce_margin_ratio else "high" if mr >= self.cfg.max_margin_ratio else "ok"
        prev = self._margin_state.get(venue, "ok")
        if state != prev:
            lvl = logging.INFO if state == "ok" else logging.WARNING
            log.log(lvl, "%s margin ratio %.4f -> %s (equity %s, maintenance %s)", venue, mr, state.upper(),
                    bal.equity, bal.maintenance_margin)
            self._margin_state[venue] = state

    # ================================================================== margin reduction
    def check_margin(self) -> bool:
        """At ``reduce_margin_ratio``: halt (stops quoting and opening) and start the reducer unless it runs
        already or ended less than ``reduce_retry_s`` ago. Returns ``must_reduce()``."""
        if not self.must_reduce():
            return False
        high = ", ".join(f"{v} {b.margin_ratio:.3f}" for v, b in self._balances.items()
                         if b.margin_ratio >= self.cfg.reduce_margin_ratio)
        self.halt(f"margin ratio >= reduce_margin_ratio {self.cfg.reduce_margin_ratio} ({high}): reducing positions")
        if self._reducer is None:
            if not self._no_reducer_warned:
                self._no_reducer_warned = True
                log.critical("margin ratio %s >= %s and no reducer installed: positions are NOT being reduced",
                             high, self.cfg.reduce_margin_ratio)
            return True
        if self._reduce_task is not None and not self._reduce_task.done():
            return True
        if time.monotonic() - self._reduce_end < self.reduce_retry_s:
            return True
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - only ever called from run()
            log.critical("margin ratio %s too high but no running event loop to reduce from", high)
            return True
        log.critical("margin ratio %s >= %s: flattening with reduce-only taker orders", high,
                     self.cfg.reduce_margin_ratio)
        self._record("margin_reduce", {"venues": high})
        self._reduce_task = loop.create_task(self._run_reducer(), name="risk:reduce")
        return True

    async def _run_reducer(self) -> None:
        assert self._reducer is not None
        try:
            await self._reducer()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("margin reduction failed")
        finally:
            self._reduce_end = time.monotonic()

    # ================================================================== daily loss
    def _venue_marks(self, symbols: set[str]) -> dict[tuple[str, str], Decimal]:
        """{(venue name, symbol): that venue's book mid} (each leg valued at its own venue's price)."""
        out: dict[tuple[str, str], Decimal] = {}
        for key, v in self.venues.items():
            name = getattr(v, "name", None) or key
            for sym in symbols:
                try:
                    book = v.get_book(sym)
                except Exception:
                    book = None
                mid = book.mid if book is not None else None
                if mid is not None and mid > 0:
                    out[(name, sym)] = mid
        return out

    def _marks(self, symbols: set[str]) -> dict[str, Decimal]:
        marks: dict[str, Decimal] = {}
        for sym in symbols:
            mids: list[Decimal] = []
            for v in self.venues.values():
                try:
                    book = v.get_book(sym)
                except Exception:
                    book = None
                mid = book.mid if book is not None else None
                if mid is not None and mid > 0:
                    mids.append(mid)
            if mids:
                marks[sym] = sum(mids, ZERO) / len(mids)
        return marks

    def _daily_summary_source(self) -> IncrementalSummary:
        if self._daily is None or self._daily.store is not self.store:
            self._daily = IncrementalSummary(self.store, clock=self._clock)  # type: ignore[arg-type]
        return self._daily

    def check_daily_loss(self) -> Decimal | None:
        """PnL since UTC midnight; halts beyond ``max_daily_loss``.

        Cash flow + fees + funding of today's fills plus today's net base of each venue valued at that venue's
        mid (a hedged pair entered across the structural basis is ~0, not a phantom loss); a leg without a venue
        mark falls back to the mean venue mid of the net base (``Store.pnl`` semantics).

        Returns the PnL, or None when it cannot be valued (no store, or a symbol with net open base
        traded today has no mark - skipping beats halting on a fake loss).
        """
        if self.store is None:
            return None
        now = self._clock()
        summary = self._daily_summary_source().get(utc_midnight(now))
        return self._evaluate_daily(summary, now)

    async def check_daily_loss_async(self) -> Decimal | None:
        """``check_daily_loss`` with the store read in a worker thread (when the store supports it)."""
        if self.store is None:
            return None
        now = self._clock()
        summary = await self._daily_summary_source().aget(utc_midnight(now))
        return self._evaluate_daily(summary, now)

    def _evaluate_daily(self, summary: dict, now: float) -> Decimal | None:
        net = net_base(summary)
        held = {sym for sym, q in net.items() if q != 0}
        for per in self._exposure.values():
            held |= {sym for sym, n in per.items() if n}
        marks = self._marks(held)
        missing = sorted(sym for sym, q in net.items() if q != 0 and sym not in marks)
        if missing:
            log.debug("daily loss check skipped: no mark for %s", ", ".join(missing))
            return None
        legs = {sym for s in summary.values() for sym, q in (s.net_base or {}).items() if q != 0}
        pnl = pnl_from_summary(summary, marks, self._venue_marks(legs))
        self._daily_pnl = pnl
        self._daily_pnl_ts = now
        limit = Decimal(self.cfg.max_daily_loss)
        if limit > 0 and pnl < -limit:
            self.halt(f"daily loss limit breached: pnl {pnl:.2f} USD < -{limit} USD")
        return pnl

    # ================================================================== halt / resume
    def halt(self, reason: str) -> None:
        """Stop quoting/opening (idempotent). Logs CRITICAL, records an event, notifies."""
        if self._halted:
            log.debug("already halted (%s); ignoring: %s", self._halt_reason, reason)
            return
        self._halted = True
        self._halt_reason = reason
        self._halt_ts = self._clock()
        log.critical("TRADING HALTED: %s", reason)
        self._record("halt", {"reason": reason})
        self._notify(f"HALT: {reason}")

    def resume(self) -> None:
        """Clear the halt and the error counters (a still-breached daily loss re-halts on the next check)."""
        if not self._halted:
            return
        prev = self._halt_reason
        self._halted = False
        self._halt_reason = None
        self._halt_ts = None
        self._errors.clear()
        log.warning("trading resumed (was halted: %s)", prev)
        self._record("resume", {"previous_reason": prev})
        self._notify(f"RESUMED (was: {prev})")

    def _record(self, kind: str, data: dict) -> None:
        if self.store is None:
            return
        try:
            self.store.record_event(kind, data)
        except Exception:
            log.exception("store.record_event(%s) failed", kind)

    def _notify(self, text: str) -> None:
        if self.notifier is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            log.warning("no running event loop; notification dropped: %s", text)
            return
        task = loop.create_task(self._send(text))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _send(self, text: str) -> None:
        assert self.notifier is not None
        try:
            await asyncio.wait_for(self.notifier.send(text), 30.0)
        except Exception as exc:
            log.warning("notification failed: %r", exc)

    async def drain_notifications(self, timeout_s: float = 10.0) -> None:
        """Wait (bounded) for queued alerts, e.g. before the caller closes the notifier."""
        pending = {t for t in self._tasks if not t.done()}
        if pending:
            await asyncio.wait(pending, timeout=timeout_s)

    # ================================================================== circuit breaker
    def record_error(self, venue: str, err: str) -> None:
        n = self._errors.get(venue, 0) + 1
        self._errors[venue] = n
        self._last_error[venue] = err
        if n >= self.cfg.max_consecutive_errors:
            self.halt(f"circuit breaker: {n} consecutive errors on {venue} (last: {err})")
        elif n >= max(1, self.cfg.max_consecutive_errors // 2):
            log.warning("%s: %d consecutive errors (last: %s)", venue, n, err)

    def record_success(self, venue: str) -> None:
        if self._errors.get(venue):
            self._errors[venue] = 0

    def _too_many_errors(self, venue: str) -> bool:
        return self._errors.get(venue, 0) >= self.cfg.max_consecutive_errors

    # ================================================================== exposure
    def update_exposure(self, symbol: str, venue: str, notional: Decimal) -> None:
        """Traders report the absolute notional of each leg."""
        self._exposure.setdefault(venue, {})[symbol] = abs(Decimal(notional))

    # ================================================================== checks
    def _book_problem(self, which: str, book: OrderBook | None, now: float) -> str | None:
        if book is None:
            return f"no {which} book"
        if not book.bids or not book.asks:
            return f"{which} book ({book.venue}) one-sided"
        age = book.age(now)
        if age > self.cfg.stale_book_s:
            return f"stale {which} book ({book.venue}) {age:.1f}s > {self.cfg.stale_book_s}s"
        return None

    def check_quote(
        self,
        symbol: str,
        maker_book: OrderBook | None,
        taker_book: OrderBook | None,
        basis_dev_bps: Decimal | None = None,
        divergence_bps: Decimal | None = None,
    ) -> tuple[bool, str]:
        """May the maker leg quote ``symbol`` now? Returns (ok, reason)."""
        if self._halted:
            return False, f"halted: {self._halt_reason}"
        now = self._clock()
        for which, book in (("maker", maker_book), ("taker", taker_book)):
            problem = self._book_problem(which, book, now)
            if problem:
                return False, f"{symbol}: {problem}"
        assert maker_book is not None and taker_book is not None
        if divergence_bps is None:
            m_mid, t_mid = maker_book.mid, taker_book.mid
            if m_mid and t_mid and t_mid > 0:
                divergence_bps = (m_mid / t_mid - 1) / BPS
        if divergence_bps is not None and abs(divergence_bps) > self.cfg.max_divergence_bps:
            return False, f"{symbol}: venue mids diverge {divergence_bps:.1f}bps > {self.cfg.max_divergence_bps}bps"
        if basis_dev_bps is not None and abs(basis_dev_bps) > self.cfg.max_basis_dev_bps:
            return False, f"{symbol}: basis deviation {basis_dev_bps:.2f}bps > {self.cfg.max_basis_dev_bps}bps"
        for venue in {maker_book.venue, taker_book.venue}:
            if self._too_many_errors(venue):
                return False, f"{venue}: {self._errors.get(venue)} consecutive errors"
        return True, ""

    def leverage_for(self, symbol: str) -> int:
        return self._leverage.get(str(symbol).upper(), self.default_leverage)

    def check_open(self, symbol: str, add_notional: Decimal, leverage: int | None = None) -> tuple[bool, str]:
        """May positions in ``symbol`` grow by ``add_notional`` (USD, per leg)? Returns (ok, reason).

        Per venue with a known balance: fresh, margin ratio below ``max_margin_ratio`` and free collateral of at
        least ``add / leverage * min_free_collateral_mult`` (the hedge leg must be able to post its initial margin,
        or a filled maker clip stays unhedged until the emergency flatten)."""
        if self._halted:
            return False, f"halted: {self._halt_reason}"
        now = self._clock()
        add = abs(Decimal(add_notional))
        lev = max(1, int(leverage)) if leverage else self.leverage_for(symbol)
        mult = Decimal(str(self.cfg.min_free_collateral_mult or 0))
        for name, ex in self.venues.items():
            bal = self._balances.get(name)
            if bal is not None:
                age = now - self._balance_ts.get(name, now)
                if age > self.balance_max_age_s:
                    return False, f"{name}: balance stale ({age:.0f}s)"
                mr = bal.margin_ratio
                if mr >= self.cfg.max_margin_ratio:
                    return False, f"{name}: margin ratio {mr:.3f} >= {self.cfg.max_margin_ratio}"
                need = add / lev * mult
                if mult > 0 and bal.available < need:
                    return False, (f"{name}: free collateral {bal.available:.2f} < {need:.2f} needed for "
                                   f"{add:.0f} USD at {lev}x (x{mult} buffer)")
            total = self.exposure(name) + add
            if total > self.cfg.max_total_notional:
                return False, f"{name}: exposure {total:.0f} would exceed max_total_notional {self.cfg.max_total_notional}"
            try:
                ok, why = ex.can_increase_exposure()
            except Exception as exc:
                ok, why = False, f"can_increase_exposure failed: {exc!r}"
            if not ok:
                return False, f"{name}: {why or 'venue refuses new exposure'}"
        return True, ""

    def must_reduce(self) -> bool:
        """True when any venue's margin ratio is at/above ``reduce_margin_ratio``."""
        return any(b.margin_ratio >= self.cfg.reduce_margin_ratio for b in self._balances.values())

    # ================================================================== status
    def status(self) -> dict:
        now = self._clock()
        return {
            "halted": self._halted,
            "halt_reason": self._halt_reason,
            "halt_ts": self._halt_ts,
            "must_reduce": self.must_reduce(),
            "reducing": self._reduce_task is not None and not self._reduce_task.done(),
            "equity_diverged": self._equity_diverged,
            "daily_pnl": self._daily_pnl,
            "daily_pnl_ts": self._daily_pnl_ts,
            "max_daily_loss": self.cfg.max_daily_loss,
            "errors": dict(self._errors),
            "last_errors": dict(self._last_error),
            "exposure": {v: {"total": sum(d.values(), ZERO), "by_symbol": dict(d)} for v, d in self._exposure.items()},
            "balances": {
                v: {
                    "equity": b.equity,
                    "available": b.available,
                    "margin_used": b.margin_used,
                    "maintenance_margin": b.maintenance_margin,
                    "margin_ratio": b.margin_ratio,
                    "age_s": now - self._balance_ts.get(v, now),
                }
                for v, b in self._balances.items()
            },
            "balance_errors": dict(self._balance_errors),
        }


__all__ = ["RiskManager", "utc_midnight"]
