"""Risk manager: pre-trade checks, margin monitoring, circuit breaker and daily loss limit.

The strategy asks ``check_quote`` before (re)quoting and ``check_open`` before increasing a
position; hedging is never gated here (risk-reducing). ``run`` polls balances on every venue
with credentials and enforces the daily loss limit. ``halt`` is sticky until ``resume``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Callable

from farmer.config import RiskConfig
from farmer.exchanges.base import Exchange
from farmer.models import BPS, ZERO, Balance, OrderBook
from farmer.monitoring.stats import IncrementalSummary, net_base, pnl_from_summary

if TYPE_CHECKING:  # pragma: no cover
    from farmer.monitoring.notifier import Notifier
    from farmer.persistence.store import Store

log = logging.getLogger("farmer.risk")

SECONDS_PER_DAY = 86400


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
    ) -> None:
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
        """Poll balances + daily loss every ``interval_s`` until ``stop()``."""
        while not self._stop.is_set():
            try:
                await self.refresh_balances()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("balance refresh failed")
            try:
                self.check_daily_loss()
            except Exception:
                log.exception("daily loss check failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_s)
            except asyncio.TimeoutError:
                pass

    def stop(self) -> None:
        self._stop.set()

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

    def _log_margin(self, venue: str, bal: Balance) -> None:
        mr = bal.margin_ratio
        state = "reduce" if mr >= self.cfg.reduce_margin_ratio else "high" if mr >= self.cfg.max_margin_ratio else "ok"
        prev = self._margin_state.get(venue, "ok")
        if state != prev:
            lvl = logging.INFO if state == "ok" else logging.WARNING
            log.log(lvl, "%s margin ratio %.4f -> %s (equity %s, maintenance %s)", venue, mr, state.upper(),
                    bal.equity, bal.maintenance_margin)
            self._margin_state[venue] = state

    # ================================================================== daily loss
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

    def check_daily_loss(self) -> Decimal | None:
        """PnL since UTC midnight (``Store.pnl`` semantics, marks = mean venue mid); halts beyond ``max_daily_loss``.

        Returns the PnL, or None when it cannot be valued (no store, or a symbol with net open base
        traded today has no mark - skipping beats halting on a fake loss).
        """
        if self.store is None:
            return None
        now = self._clock()
        since = utc_midnight(now)
        if self._daily is None or self._daily.store is not self.store:
            self._daily = IncrementalSummary(self.store, clock=self._clock)
        summary = self._daily.get(since)  # incremental: no full-day rescan every 10 s on the event loop
        net = net_base(summary)
        held = {sym for sym, q in net.items() if q != 0}
        for per in self._exposure.values():
            held |= {sym for sym, n in per.items() if n}
        marks = self._marks(held)
        missing = sorted(sym for sym, q in net.items() if q != 0 and sym not in marks)
        if missing:
            log.debug("daily loss check skipped: no mark for %s", ", ".join(missing))
            return None
        pnl = pnl_from_summary(summary, marks)  # == store.pnl(since, marks)
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

    def check_open(self, symbol: str, add_notional: Decimal) -> tuple[bool, str]:
        """May positions in ``symbol`` grow by ``add_notional`` (USD, per leg)? Returns (ok, reason)."""
        if self._halted:
            return False, f"halted: {self._halt_reason}"
        now = self._clock()
        add = abs(Decimal(add_notional))
        for name, ex in self.venues.items():
            bal = self._balances.get(name)
            if bal is not None:
                age = now - self._balance_ts.get(name, now)
                if age > self.balance_max_age_s:
                    return False, f"{name}: balance stale ({age:.0f}s)"
                mr = bal.margin_ratio
                if mr >= self.cfg.max_margin_ratio:
                    return False, f"{name}: margin ratio {mr:.3f} >= {self.cfg.max_margin_ratio}"
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
