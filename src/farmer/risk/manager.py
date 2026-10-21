"""Risk manager: pre-trade checks, margin monitoring, circuit breaker and daily loss limit.

The strategy asks ``check_quote`` before (re)quoting and ``check_open`` before increasing a
position; hedging is never gated here (risk-reducing). ``run`` polls balances on every venue
with credentials and enforces the daily loss limit.

Halts have a kind. A circuit-breaker halt (``HALT_ERROR``: ``max_consecutive_errors`` on one venue) clears
itself once no venue error was recorded for ``error_halt_cooldown_s`` (doubling with every automatic resume
within 24 h, at most ``max_auto_resumes_per_day``); every other kind (daily loss, margin reduction, hedge
failures, manual) is sticky until ``resume`` / a restart, and keeps the bot halted while it is active.

Margin: ``check_open`` refuses new exposure when a venue's maintenance/equity ratio reaches
``max_margin_ratio`` or its free collateral cannot post the clip's initial margin with a buffer
(``min_free_collateral_mult``). At ``reduce_margin_ratio`` the manager halts (no quoting, no opening)
and runs the ``reducer`` the engine owner installed (``set_reducer``: reduce-only taker flatten of every
pair), retrying while the ratio stays high. Diverging venue equities are reported so the operator can
rebalance collateral (the bot never moves funds between venues).

Price move: a hedged pair still drains the losing venue's collateral when the price trends (one leg loses
on every move, in either direction). When a market with exposure first gets a price, its reference is stored:
the mid of every venue with a fresh two-sided book (``Store`` meta, so it survives a restart with open positions;
normal churn cycles do not reset it). Books are looked up by each venue's own symbol (``set_pair_symbols``: the
pair BONK is Arcus KBONK / Lighter 1000BONK). The move is measured per venue against that venue's reference and
the largest one counts (a frozen book on one venue cannot hide the move on the other). Once it reaches
``auto_close_move_frac`` the manager halts (``HALT_MOVE``) and runs the reducer like a margin reduction, retried
every ``reduce_retry_s`` while any pair is still exposed; ``check_open`` refuses new exposure in a market already
past the threshold (a restart before the first check). It warns once at ``MOVE_WARN_AT`` of the threshold. The
engine resets a market's reference when a close-all leaves it flat and when it starts with no position in it.
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

from farmer.config import RiskConfig
from farmer.exchanges.base import Exchange
from farmer.models import BPS, ZERO, Balance, OrderBook
from farmer.monitoring.stats import IncrementalSummary, net_base, pnl_from_summary
from farmer.utils.decimals import fmt

if TYPE_CHECKING:  # pragma: no cover
    from farmer.monitoring.notifier import Notifier
    from farmer.persistence.store import Store

log = logging.getLogger("farmer.risk")

SECONDS_PER_DAY = 86400
REDUCE_RETRY_S = 60.0  # re-run the reducer this long after the previous run ended while margin stays high
MAX_ERROR_COOLDOWN_S = 4 * 3600.0  # cap of the doubling circuit-breaker cooldown

# halt kinds; only HALT_ERROR clears itself (see the module docstring)
HALT_ERROR = "error"
HALT_LOSS = "daily_loss"
HALT_MARGIN = "margin"
HALT_MOVE = "price_move"
HALT_MANUAL = "manual"

MOVE_REFS_META = "risk.move_refs"  # Store meta: {pair symbol: {"price": str, "ts": float, "venues": {venue: str}}}
MOVE_WARN_AT = Decimal("0.75")  # warn once at this fraction of auto_close_move_frac ...
MOVE_WARN_RESET = Decimal("0.6")  # ... and re-arm the warning below this fraction
MOVE_NOMARK_WARN = 6  # warn once an exposed market had no fresh price on any venue for this many checks


@dataclass
class MoveRef:
    """Price-move reference of one pair: the mean venue mid and each venue's own mid when it was taken."""

    price: Decimal
    ts: float
    venues: dict[str, Decimal] = field(default_factory=dict)

    def base(self, venue: str) -> Decimal:
        return self.venues.get(venue) or self.price


def _positive(x: object) -> Decimal | None:
    """``x`` as a finite positive Decimal, else None (NaN / Infinity / garbage from a hand-edited database)."""
    try:
        d = Decimal(str(x))
    except Exception:
        return None
    return d if d.is_finite() and d > 0 else None


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
        store: Store | None = None,
        notifier: Notifier | None = None,
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

        self._halts: dict[str, tuple[str, float]] = {}  # kind -> (reason, ts); insertion order = halt order
        self._errors: dict[str, int] = {}
        self._last_error: dict[str, str] = {}
        self._last_error_ts: float | None = None
        self._auto_resumes: collections.deque[float] = collections.deque()  # ts of automatic resumes (24 h)
        self._auto_resume_exhausted = False
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
        self._move_refs: dict[str, MoveRef] = self._load_move_refs()  # pair symbol -> reference
        self._moves: dict[str, Decimal] = {}  # pair symbol -> latest largest venue move (mid / ref - 1)
        self._move_warned: set[str] = set()
        self._move_nomark: dict[str, int] = {}  # pair symbol -> consecutive checks without a fresh price
        self._pair_syms: dict[str, dict[str, str]] = {}  # pair symbol -> {venue name: venue symbol}

    # ================================================================== state
    @property
    def halted(self) -> bool:
        return bool(self._halts)

    @property
    def halt_reason(self) -> str | None:
        """Reason of the oldest active halt."""
        return next(iter(self._halts.values()))[0] if self._halts else None

    @property
    def _halt_ts(self) -> float | None:
        return next(iter(self._halts.values()))[1] if self._halts else None

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
                self.check_price_move()
            except Exception:
                log.exception("price move check failed")
            try:
                await self.check_daily_loss_async()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("daily loss check failed")
            try:
                self.maybe_auto_resume()
            except Exception:
                log.exception("automatic resume check failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_s)
            except TimeoutError:
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
        for name, res in zip(names, results, strict=True):
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
        self.halt(f"margin ratio >= reduce_margin_ratio {self.cfg.reduce_margin_ratio} ({high}): reducing positions",
                  kind=HALT_MARGIN)
        self._start_reducer(f"margin ratio {high} >= {self.cfg.reduce_margin_ratio}", "margin_reduce",
                            {"venues": high})
        return True

    def _start_reducer(self, why: str, event: str, data: dict) -> None:
        """Run the reducer unless it runs already or ended less than ``reduce_retry_s`` ago."""
        if self._reducer is None:
            if not self._no_reducer_warned:
                self._no_reducer_warned = True
                log.critical("%s and no reducer installed: positions are NOT being reduced", why)
            return
        if self._reduce_task is not None and not self._reduce_task.done():
            return
        if time.monotonic() - self._reduce_end < self.reduce_retry_s:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - only ever called from run()
            log.critical("%s but no running event loop to reduce from", why)
            return
        log.critical("%s: flattening with reduce-only taker orders", why)
        self._record(event, data)
        self._reduce_task = loop.create_task(self._run_reducer(), name="risk:reduce")

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

    # ================================================================== price move auto close
    def set_pair_symbols(self, pairs: Mapping[str, Mapping[str, str]]) -> None:
        """{pair symbol: {venue name: venue symbol}} (the engine, after mapping): the price-move guard reads each
        venue's book under the venue's own symbol (pair BONK = Arcus KBONK / Lighter 1000BONK)."""
        self._pair_syms = {str(s): {str(v): str(x) for v, x in m.items()} for s, m in pairs.items()}

    def _move_frac(self) -> Decimal:
        return abs(Decimal(str(self.cfg.auto_close_move_frac or 0)))

    def _load_move_refs(self) -> dict[str, MoveRef]:
        get = getattr(self.store, "get_meta", None)
        if not callable(get):
            return {}
        try:
            raw = json.loads(get(MOVE_REFS_META) or "{}")
        except Exception:
            log.exception("cannot read stored price-move references; starting without")
            return {}
        if not isinstance(raw, dict):
            log.warning("ignoring stored price-move references: %r is not a mapping", type(raw).__name__)
            return {}
        out: dict[str, MoveRef] = {}
        for sym, v in raw.items():  # one bad entry must not discard the others
            price = _positive(v.get("price")) if isinstance(v, dict) else None
            if price is None:
                log.warning("ignoring stored price-move reference %s=%r", sym, v)
                continue
            try:
                ts = float(v.get("ts") or 0)
            except (TypeError, ValueError):
                ts = 0.0
            venues = v.get("venues") if isinstance(v.get("venues"), dict) else {}
            out[str(sym)] = MoveRef(price, ts, {str(k): p for k, x in venues.items() if (p := _positive(x))})
        return out

    def _save_move_refs(self) -> None:
        put = getattr(self.store, "set_meta", None)
        if not callable(put):
            return
        try:
            put(MOVE_REFS_META, json.dumps({s: {"price": str(r.price), "ts": r.ts,
                                                "venues": {v: str(p) for v, p in r.venues.items()}}
                                            for s, r in self._move_refs.items()}))
        except Exception:
            log.exception("cannot store price-move references")

    def move_reference(self, symbol: str) -> Decimal | None:
        ref = self._move_refs.get(symbol)
        return ref.price if ref else None

    def reset_move_refs(self, symbols: list[str] | None = None) -> None:
        """Forget the reference price of ``symbols`` (all when None): the next exposure takes a fresh one.
        The engine calls this when a close-all leaves a pair flat and at startup for pairs without positions."""
        syms = list(self._move_refs) if symbols is None else [s for s in symbols if s in self._move_refs]
        if not syms:
            return
        for s in syms:
            self._move_refs.pop(s, None)
            self._moves.pop(s, None)
            self._move_warned.discard(s)
        log.info("price-move reference reset for %s", ", ".join(sorted(syms)))
        self._save_move_refs()

    def _move_mids(self, symbol: str) -> dict[str, Decimal]:
        """{venue name: mid} of every venue whose book of ``symbol`` (under the venue's own symbol) is two-sided and
        fresh (``stale_book_s``, never stricter than two check intervals): a frozen book is no price."""
        now = self._clock()
        max_age = max(float(self.cfg.stale_book_s), 2 * self.interval_s)
        syms = self._pair_syms.get(symbol)
        out: dict[str, Decimal] = {}
        for key, v in self.venues.items():
            name = getattr(v, "name", None) or key
            vsym = syms.get(name) if syms else symbol
            if not vsym:
                continue
            try:
                book = v.get_book(vsym)
                if book is None or not book.bids or not book.asks or book.age(now) > max_age:
                    continue
                mid = book.mid
            except Exception:
                continue
            if mid is not None and mid.is_finite() and mid > 0:
                out[name] = mid
        return out

    def _current_move(self, symbol: str, mids: dict[str, Decimal] | None = None
                      ) -> tuple[Decimal, str, Decimal, Decimal] | None:
        """Largest venue move of ``symbol`` from its reference: (move, venue, that venue's reference, its mid);
        None without a reference or a fresh price."""
        ref = self._move_refs.get(symbol)
        if ref is None:
            return None
        best: tuple[Decimal, str, Decimal, Decimal] | None = None
        for venue, mid in (self._move_mids(symbol) if mids is None else mids).items():
            base = ref.base(venue)
            move = mid / base - 1
            if best is None or abs(move) > abs(best[0]):
                best = (move, venue, base, mid)
        return best

    def _no_move_mark(self, symbol: str) -> None:
        n = self._move_nomark[symbol] = self._move_nomark.get(symbol, 0) + 1
        if n == MOVE_NOMARK_WARN:
            log.warning("%s: no fresh two-sided book on any venue for %d checks: the price-move guard cannot see its "
                        "price (auto close-all at ±%s%% is blind until it can)", symbol, n,
                        fmt(self._move_frac() * 100, 2))
            self._record("price_move_blind", {"symbol": symbol, "checks": n})

    def check_price_move(self) -> bool:
        """Halt + flatten everything once an exposed market moved ``auto_close_move_frac`` (either direction)
        from its reference price. Takes a reference for exposed markets that have none, warns once at
        ``MOVE_WARN_AT`` of the threshold. While a price-move halt is active and any pair is still exposed (the
        close-all timed out, or the price came back meanwhile) the reducer is retried every ``reduce_retry_s``.
        Returns True when it (re)started or keeps the auto close-all."""
        frac = self._move_frac()
        if frac <= 0:
            return False
        exposed = {sym for per in self._exposure.values() for sym, n in per.items() if n > 0}
        for sym in [s for s in self._move_nomark if s not in exposed]:
            del self._move_nomark[sym]
        if not exposed:
            return False
        added: list[str] = []
        hit: list[tuple[str, Decimal, Decimal, Decimal, str]] = []
        for sym in sorted(exposed):
            mids = self._move_mids(sym)
            if not mids:
                self._no_move_mark(sym)
                continue
            if self._move_nomark.pop(sym, 0) >= MOVE_NOMARK_WARN:
                log.info("%s: the price-move guard sees prices again", sym)
            if sym not in self._move_refs:
                mark = sum(mids.values(), ZERO) / len(mids)
                self._move_refs[sym] = MoveRef(mark, self._clock(), dict(mids))
                self._moves[sym] = ZERO
                added.append(sym)
                log.info("%s price-move reference %s: auto close-all at %s / %s (±%s%%)", sym, fmt(mark),
                         fmt(mark * (1 - frac), 6), fmt(mark * (1 + frac), 6), fmt(frac * 100, 2))
                continue
            cur = self._current_move(sym, mids)
            if cur is None:  # pragma: no cover - a reference and a price exist here
                continue
            move, venue, base, mark = cur
            self._moves[sym] = move
            if abs(move) >= frac:
                hit.append((sym, base, mark, move, venue))
            elif abs(move) >= frac * MOVE_WARN_AT:
                if sym not in self._move_warned:
                    self._move_warned.add(sym)
                    msg = (f"{sym} moved {move * 100:+.2f}% from its reference {fmt(base)} on {venue} (now "
                           f"{fmt(mark)}); the bot closes everything at ±{fmt(frac * 100, 2)}% - consider "
                           "rebalancing collateral")
                    log.warning(msg)
                    self._record("price_move_warning", {"symbol": sym, "venue": venue, "ref": base, "mark": mark,
                                                        "move": move})
                    self._notify(f"WARNING: {msg}")
            elif abs(move) < frac * MOVE_WARN_RESET:
                self._move_warned.discard(sym)
        if added:
            self._save_move_refs()
        if hit:
            desc = "; ".join(f"{s} {m * 100:+.2f}% on {v} ({fmt(r)} -> {fmt(mk)})" for s, r, mk, m, v in hit)
            eq = ", ".join(f"{v} equity {b.equity:.2f}" for v, b in self._balances.items())
            self.halt(f"price moved >= ±{fmt(frac * 100, 2)}% from the reference: {desc}"
                      + (f" [{eq}]" if eq else "") + ": closing every pair", kind=HALT_MOVE)
            self._start_reducer(f"price move {desc}", "auto_close_move",
                                {"moves": {s: {"venue": v, "ref": r, "mark": mk, "move": m} for s, r, mk, m, v in hit}})
            return True
        if HALT_MOVE in self._halts:  # the close-all left positions open: keep closing until every pair is flat
            still = sorted(exposed)
            self._start_reducer(f"price-move close-all incomplete, still exposed: {', '.join(still)}",
                                "auto_close_move_retry", {"symbols": still})
            return True
        return False

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
            self.halt(f"daily loss limit breached: pnl {pnl:.2f} USD < -{limit} USD", kind=HALT_LOSS)
        return pnl

    # ================================================================== halt / resume
    def halt(self, reason: str, *, kind: str = HALT_MANUAL) -> None:
        """Stop quoting/opening; one halt per ``kind`` (repeats are ignored). Logs CRITICAL, records an event,
        notifies. Only a ``HALT_ERROR`` halt clears itself (``maybe_auto_resume``)."""
        if kind in self._halts:
            log.debug("already halted (%s); ignoring: %s", self._halts[kind][0], reason)
            return
        also = self.halted
        self._halts[kind] = (reason, self._clock())
        log.critical("TRADING HALTED%s: %s", " (again)" if also else "", reason)
        self._record("halt", {"reason": reason, "kind": kind})
        self._notify(f"HALT: {reason}")

    def resume(self) -> None:
        """Clear every halt and the error counters (a still-breached daily loss re-halts on the next check)."""
        if not self._halts:
            return
        prev = "; ".join(r for r, _ in self._halts.values())
        self._halts.clear()
        self._errors.clear()
        self._auto_resume_exhausted = False
        log.warning("trading resumed (was halted: %s)", prev)
        self._record("resume", {"previous_reason": prev})
        self._notify(f"RESUMED (was: {prev})")

    def maybe_auto_resume(self) -> bool:
        """Clear a circuit-breaker halt once no venue error was recorded for the cooldown (``error_halt_cooldown_s``
        doubled per automatic resume within 24 h). Other halt kinds stay. Returns True when it cleared one."""
        h = self._halts.get(HALT_ERROR)
        base = float(self.cfg.error_halt_cooldown_s or 0)
        if h is None or base <= 0:
            return False
        now = self._clock()
        while self._auto_resumes and now - self._auto_resumes[0] >= SECONDS_PER_DAY:
            self._auto_resumes.popleft()
        n = len(self._auto_resumes)
        if n >= int(self.cfg.max_auto_resumes_per_day):
            if not self._auto_resume_exhausted:
                self._auto_resume_exhausted = True
                msg = (f"circuit breaker: {n} automatic resume(s) within 24 h already; staying halted until restart "
                       f"({h[0]})")
                log.critical(msg)
                self._notify(msg)
            return False
        cooldown = min(base * 2 ** n, MAX_ERROR_COOLDOWN_S)
        quiet = now - max(h[1], self._last_error_ts or h[1])
        if quiet < cooldown:
            return False
        del self._halts[HALT_ERROR]
        self._errors.clear()
        self._auto_resumes.append(now)
        self._auto_resume_exhausted = False
        if self._halts:
            log.warning("circuit breaker cleared after %.0fs without venue errors, still halted: %s", quiet,
                        self.halt_reason)
        else:
            log.warning("trading resumed automatically: no venue error for %.0fs (was halted: %s)", quiet, h[0])
        self._record("resume", {"previous_reason": h[0], "auto": True, "quiet_s": round(quiet, 1)})
        self._notify(f"RESUMED automatically after {quiet:.0f}s without venue errors (was: {h[0]})"
                     + (f"; still halted: {self.halt_reason}" if self._halts else ""))
        return True

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
        self._last_error_ts = self._clock()
        if n >= self.cfg.max_consecutive_errors:
            self.halt(f"circuit breaker: {n} consecutive errors on {venue} (last: {err})", kind=HALT_ERROR)
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
        if self.halted:
            return False, f"halted: {self.halt_reason}"
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
        if self.halted:
            return False, f"halted: {self.halt_reason}"
        frac = self._move_frac()
        if frac > 0 and symbol in self._move_refs:  # e.g. a restart: traders tick before the first price-move check
            cur = self._current_move(symbol)
            if cur is not None and abs(cur[0]) >= frac:
                return False, (f"{symbol}: price moved {cur[0] * 100:+.2f}% on {cur[1]} from the reference "
                               f"{fmt(cur[2])} (auto close-all at ±{fmt(frac * 100, 2)}%)")
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
            "halted": self.halted,
            "halt_reason": self.halt_reason,
            "halt_ts": self._halt_ts,
            "halts": {k: r for k, (r, _) in self._halts.items()},
            "must_reduce": self.must_reduce(),
            "reducing": self._reduce_task is not None and not self._reduce_task.done(),
            "equity_diverged": self._equity_diverged,
            "daily_pnl": self._daily_pnl,
            "daily_pnl_ts": self._daily_pnl_ts,
            "max_daily_loss": self.cfg.max_daily_loss,
            "max_margin_ratio": self.cfg.max_margin_ratio,
            "reduce_margin_ratio": self.cfg.reduce_margin_ratio,
            "auto_close_move_frac": self.cfg.auto_close_move_frac,
            "price_moves": {s: {"ref": r.price, "ref_ts": r.ts, "move": self._moves.get(s)}
                            for s, r in self._move_refs.items()},
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


__all__ = ["HALT_ERROR", "HALT_LOSS", "HALT_MANUAL", "HALT_MARGIN", "HALT_MOVE", "MoveRef", "RiskManager",
           "utc_midnight"]
