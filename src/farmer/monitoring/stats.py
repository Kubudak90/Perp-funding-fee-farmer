"""Periodic statistics report (log + notifier) and realized-funding collection.

``status_fn`` comes from the strategy engine; every key is read defensively (missing keys,
``None`` values and str/float/Decimal numbers are all tolerated).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable

from farmer.exchanges.base import Exchange
from farmer.models import ZERO
from farmer.persistence.store import VenueSummary
from farmer.utils.decimals import D_or_none

if TYPE_CHECKING:  # pragma: no cover
    from farmer.monitoring.notifier import Notifier
    from farmer.persistence.store import Store

log = logging.getLogger("farmer.stats")

FUNDING_COLLECT_S = 300.0


# --------------------------------------------------------------------------- aggregation helpers
def merge_summaries(*parts: dict[str, VenueSummary]) -> dict[str, VenueSummary]:
    """Sum per-venue ``VenueSummary`` maps of disjoint time windows."""
    out: dict[str, VenueSummary] = {}
    for part in parts:
        for venue, s in part.items():
            t = out.setdefault(venue, VenueSummary(net_base={}))
            t.volume += s.volume
            t.maker_volume += s.maker_volume
            t.taker_volume += s.taker_volume
            t.fees += s.fees
            t.funding += s.funding
            t.fills += s.fills
            t.cash_flow += s.cash_flow
            assert t.net_base is not None
            for sym, q in (s.net_base or {}).items():
                t.net_base[sym] = t.net_base.get(sym, ZERO) + q
    return out


def net_base(summary: dict[str, VenueSummary]) -> dict[str, Decimal]:
    """Net signed base traded per symbol, both venues combined."""
    net: dict[str, Decimal] = {}
    for s in summary.values():
        for sym, q in (s.net_base or {}).items():
            net[sym] = net.get(sym, ZERO) + q
    return net


def pnl_from_summary(summary: dict[str, VenueSummary], marks: dict[str, Decimal]) -> Decimal:
    """Exactly ``Store.pnl`` applied to an already computed summary (cash flow + funding + marked net base)."""
    total = sum((s.cash_flow + s.funding for s in summary.values()), ZERO)
    for sym, q in net_base(summary).items():
        if q != 0 and sym in marks:
            total += q * marks[sym]
    return total


class IncrementalSummary:
    """``store.summary(since)`` for a FIXED ``since`` without rescanning every row on each call.

    SQLite reads are synchronous (they run on the event loop): rows older than ``tail_s`` are
    aggregated once per ``rebase_s`` and only the recent tail is rescanned per call. A row inserted
    late with a timestamp older than the tail is picked up at the next rebase.
    """

    def __init__(self, store: "Store", *, tail_s: float = 7200.0, rebase_s: float = 600.0,
                 clock: Callable[[], float] = time.time) -> None:
        self.store = store
        self.tail_s = float(tail_s)
        self.rebase_s = float(rebase_s)
        self._clock = clock
        self._since: float | None = None
        self._cp = 0.0
        self._base: dict[str, VenueSummary] = {}
        self._base_ts: float | None = None

    def get(self, since: float) -> dict[str, VenueSummary]:
        now = self._clock()
        if self._since != since or self._base_ts is None or now - self._base_ts >= self.rebase_s:
            self._since = since
            self._cp = max(since, now - self.tail_s)
            self._base = self.store.summary(since, self._cp) if self._cp > since else {}
            self._base_ts = now
        return merge_summaries(self._base, self.store.summary(self._cp))


# --------------------------------------------------------------------------- formatting helpers
def _num(x: Any) -> Decimal | None:
    if x is None or isinstance(x, bool):
        return None
    return D_or_none(x)


def _first(d: dict, *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _coalesce(*vals: Any) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None


def _usd(x: Decimal | None, places: int = 0) -> str:
    if x is None:
        return "-"
    return f"${x:,.{places}f}" if x >= 0 else f"-${-x:,.{places}f}"


def _signed_usd(x: Decimal | None, places: int = 2) -> str:
    if x is None:
        return "-"
    return f"+${x:,.{places}f}" if x >= 0 else f"-${-x:,.{places}f}"


def _qty(x: Decimal | None) -> str:
    if x is None:
        return "-"
    s = f"{x:+f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return "0" if s in ("+0", "-0") else s


def _uptime(seconds: float) -> str:
    s = max(0, int(seconds))
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if d:
        return f"{d}d{h:02d}h{m:02d}m"
    if h:
        return f"{h}h{m:02d}m"
    return f"{m}m{s:02d}s"


def _direction(x: Any) -> str:
    if isinstance(x, str):
        u = x.strip().upper()
        if u in ("+1", "1", "LONG", "MAKER_LONG", "LONG_MAKER"):
            return "maker LONG"
        if u in ("-1", "SHORT", "MAKER_SHORT", "SHORT_MAKER"):
            return "maker SHORT"
        return x or "-"
    n = _num(x)
    if n is None:
        return "-"
    return "maker LONG" if n > 0 else "maker SHORT" if n < 0 else "-"


def _quote(q: Any) -> str:
    if q is None or q == {} or q == []:
        return "none"
    if isinstance(q, str):
        return q
    if isinstance(q, dict):
        side = _first(q, "side")
        side = getattr(side, "value", side)
        size = _num(_first(q, "remaining", "size", "qty"))
        px = _num(_first(q, "price", "px"))
        if side is None and size is None and px is None:
            return "none"
        txt = f"{side or '?'} {_qty(size).lstrip('+') if size is not None else '?'} @ {px if px is not None else '?'}"
        edge = _num(_first(q, "edge_bps", "expected_edge_bps"))
        if edge is not None:
            txt += f" (edge {edge:.2f}bps)"
        return txt
    side = getattr(q, "side", None)  # Order / Quote objects
    price = getattr(q, "price", None)
    size = getattr(q, "remaining", None) or getattr(q, "size", None)
    if side is not None or price is not None:
        return f"{getattr(side, 'value', side) or '?'} {size if size is not None else '?'} @ {price if price is not None else '?'}"
    return str(q)


def _apr(m: dict) -> Decimal | None:
    v = _num(_first(m, "funding_apr", "apr", "carry_apr"))
    if v is None and isinstance(m.get("funding"), dict):
        v = _num(_first(m["funding"], "apr", "funding_apr", "carry_apr"))
    return v  # fraction per year (0.1095 = 10.95 %)


def _basis_bps(m: dict) -> Decimal | None:
    v = _num(_first(m, "basis_bps"))
    if v is not None:
        return v
    b = m.get("basis")
    if isinstance(b, dict):  # PairTrader: {"ema_bps", "last_bps", "deviation_bps", ...}
        return _num(_first(b, "ema_bps", "bps", "last_bps"))
    frac = _num(b)
    return None if frac is None else frac * 10000


def _mark_of(m: dict) -> Decimal | None:
    px = _num(_first(m, "mid", "mark", "mark_price", "price", "taker_mid", "maker_mid"))
    books = m.get("books")
    if px is None and isinstance(books, dict):  # PairTrader: {"maker_mid", "taker_mid", ...}
        px = _num(_first(books, "taker_mid", "maker_mid", "mid"))
    return px if px is not None and px > 0 else None


def _markets(status: dict) -> list[tuple[str, dict]]:
    raw = _first(status, "markets", "pairs", "traders")
    out: list[tuple[str, dict]] = []
    if isinstance(raw, dict):
        for sym, m in raw.items():
            if isinstance(m, dict):
                out.append((str(_first(m, "symbol") or sym), m))
    elif isinstance(raw, (list, tuple)):
        for m in raw:
            if isinstance(m, dict):
                out.append((str(_first(m, "symbol", "market") or "?"), m))
    return out


class StatsReporter:
    """Logs a compact report every ``interval_s``; sends every ``notify_every``-th one."""

    def __init__(
        self,
        store: "Store",
        notifier: "Notifier",
        status_fn: Callable[[], dict],
        venues: dict[str, Exchange],
        interval_s: float = 60,
        notify_every: int = 60,
        *,
        funding_interval_s: float = FUNDING_COLLECT_S,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.store = store
        self.notifier = notifier
        self.status_fn = status_fn
        self.venues = venues
        self.interval_s = float(interval_s)
        self.notify_every = max(1, int(notify_every))
        self.funding_interval_s = float(funding_interval_s)
        self._clock = clock
        self.start_ts = clock()
        self.reports = 0
        self._stop = asyncio.Event()
        self._all_time = IncrementalSummary(store, clock=clock)  # avoids a full-table scan per report

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------ loop
    async def run(self) -> None:
        self.start_ts = self._clock()
        next_funding = self._clock()
        while not self._stop.is_set():
            if self._clock() >= next_funding:
                try:
                    await self.collect_funding()
                except Exception:
                    log.exception("funding collection failed")
                next_funding = self._clock() + self.funding_interval_s
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_s)
                break
            except asyncio.TimeoutError:
                pass
            await self.report_once()

    async def report_once(self) -> str:
        try:
            text = self.render_report()
        except Exception as exc:  # never kill the loop over a formatting bug
            log.exception("render_report failed")
            text = f"stats report failed: {exc!r}"
        log.info("\n%s", text)
        self.reports += 1
        if self.reports % self.notify_every == 0:
            try:
                await asyncio.wait_for(self.notifier.send(text), 30.0)
            except Exception as exc:
                log.warning("stats notification failed: %r", exc)
        return text

    async def collect_funding(self) -> int:
        """Pull realized funding from every venue with credentials into the store (store dedupes)."""
        n = 0
        for key, ex in self.venues.items():
            try:
                if not ex.has_credentials:
                    continue
            except Exception:
                continue
            venue = getattr(ex, "name", None) or key
            since = self.store.last_funding_ts(venue)
            if since is None:
                since = self.start_ts
            try:
                pays = await asyncio.wait_for(ex.fetch_funding_payments(since_ts=since), 30.0)
            except Exception as exc:
                log.warning("%s fetch_funding_payments failed: %r", venue, exc)
                continue
            for p in pays or []:
                if p.ts < since:
                    continue
                try:
                    self.store.record_funding(p)
                    n += 1
                except Exception:
                    log.exception("record_funding failed for %s", p)
        if n:
            log.debug("collected %d funding payment(s)", n)
        return n

    # ------------------------------------------------------------------ report
    def _status(self) -> dict:
        try:
            st = self.status_fn()
        except Exception as exc:
            log.warning("status_fn failed: %r", exc)
            return {}
        if inspect.isawaitable(st):  # tolerate an async status_fn by ignoring it here
            close = getattr(st, "close", None)
            if close is not None:
                close()
            return {}
        return st if isinstance(st, dict) else {}

    def _marks(self, status: dict, symbols: set[str]) -> dict[str, Decimal]:
        marks: dict[str, Decimal] = {}
        for sym, m in _markets(status):
            px = _mark_of(m)
            if px is not None:
                marks[sym] = px
            books = m.get("books") if isinstance(m.get("books"), dict) else {}
            for key, mid_key in (("maker_symbol", "maker_mid"), ("taker_symbol", "taker_mid")):
                vsym = m.get(key)  # venue keys may differ from the pair symbol (Arcus KBONK / Lighter 1000BONK)
                vpx = _num(books.get(mid_key)) or px
                if isinstance(vsym, str) and vsym and vsym not in marks and vpx is not None and vpx > 0:
                    marks[vsym] = vpx
        for sym in symbols:
            if sym in marks:
                continue
            mids: list[Decimal] = []
            for ex in self.venues.values():
                try:
                    book = ex.get_book(sym)
                except Exception:
                    book = None
                mid = book.mid if book is not None else None
                if mid is not None and mid > 0:
                    mids.append(mid)
            if mids:
                marks[sym] = sum(mids, ZERO) / len(mids)
        return marks

    def _pnl(self, status: dict, summary: dict[str, VenueSummary]) -> tuple[Decimal, list[str]]:
        """``Store.pnl`` of the window ``summary`` covers (computed from it, no extra scan)."""
        held = {sym for sym, q in net_base(summary).items() if q != 0}
        marks = self._marks(status, held)
        missing = sorted(sym for sym in held if sym not in marks)
        return pnl_from_summary(summary, marks), missing

    def render_report(self) -> str:
        now = self._clock()
        status = self._status()
        lines: list[str] = []

        mode = status.get("mode")
        head = f"farmer{' ' + str(mode) if mode else ''} | up {_uptime(now - self.start_ts)} | " + time.strftime(
            "%Y-%m-%d %H:%M UTC", time.gmtime(now))
        risk = status.get("risk") if isinstance(status.get("risk"), dict) else {}
        halted = bool(_first(status, "halted") or risk.get("halted"))
        if halted:
            reason = _first(status, "halt_reason") or risk.get("halt_reason") or "?"
            head += f" | HALTED: {reason}"
        lines.append(head)

        s1h = self.store.summary(now - 3600)
        s24 = self.store.summary(now - 86400)
        sall = self._all_time.get(0.0)
        venue_names = list(dict.fromkeys([getattr(v, "name", k) for k, v in self.venues.items()] + list(sall)))
        width = max([len(v) for v in venue_names] + [5])
        for v in venue_names:
            a, b, c = s1h.get(v), s24.get(v), sall.get(v)
            if a is None and b is None and c is None:
                lines.append(f"{v:<{width}} no fills yet")
                continue

            def vol(s) -> str:
                if s is None:
                    return "$0"
                return f"{_usd(s.volume)} (M {_usd(s.maker_volume)}/T {_usd(s.taker_volume)})"

            lines.append(
                f"{v:<{width}} vol 1h {vol(a)} | 24h {vol(b)} | all {_usd(c.volume if c else ZERO)}"
                f" | fills {c.fills if c else 0} | fees 24h {_usd(b.fees if b else ZERO, 2)} all {_usd(c.fees if c else ZERO, 2)}"
                f" | funding 24h {_signed_usd(b.funding if b else ZERO)} all {_signed_usd(c.funding if c else ZERO)}"
            )

        pnl24, miss24 = self._pnl(status, s24)
        pnl_all, miss_all = self._pnl(status, sall)
        tot_vol = sum((s.volume for s in sall.values()), ZERO)
        tot24 = sum((s.volume for s in s24.values()), ZERO)
        fund = sum((s.funding for s in sall.values()), ZERO)
        fees = sum((s.fees for s in sall.values()), ZERO)
        pnl_line = (f"total vol 24h {_usd(tot24)} all {_usd(tot_vol)} | fees {_usd(fees, 2)} | funding {_signed_usd(fund)}"
                    f" | PnL est 24h {_signed_usd(pnl24)} all {_signed_usd(pnl_all)}")
        missing = sorted(set(miss24) | set(miss_all))
        if missing:
            pnl_line += f" (no mark: {', '.join(missing)})"
        lines.append(pnl_line)

        bals = risk.get("balances") if isinstance(risk.get("balances"), dict) else {}
        if bals:
            parts = []
            for v, b in bals.items():
                if not isinstance(b, dict):
                    continue
                eq, mr = _num(b.get("equity")), _num(b.get("margin_ratio"))
                parts.append(f"{v} eq {_usd(eq, 2)} mr {f'{mr:.3f}' if mr is not None else '-'}")
            if parts:
                dp = _num(risk.get("daily_pnl"))
                lines.append("risk: " + " | ".join(parts) + (f" | day pnl {_signed_usd(dp)}" if dp is not None else ""))

        for sym, m in _markets(status):
            phase = _first(m, "phase", "state") or "-"
            phase = getattr(phase, "value", phase)
            h = m.get("hedger") if isinstance(m.get("hedger"), dict) else {}
            mk = _num(_coalesce(_first(m, "maker_pos", "maker_position", "maker_size"), _first(h, "maker_pos")))
            tk = _num(_coalesce(_first(m, "taker_pos", "taker_position", "taker_size"), _first(h, "taker_pos")))
            delta = _num(_coalesce(_first(m, "delta", "unhedged", "net"), _first(h, "delta")))
            if delta is None and mk is not None and tk is not None:
                delta = mk + tk
            apr = _apr(m)
            basis = _basis_bps(m)
            parts = [
                f"{sym:<6}{str(phase):<9} {_direction(_first(m, 'direction', 'dir'))}",
                f"mk {_qty(mk)} tk {_qty(tk)} d {_qty(delta)}",
                f"quote {_quote(_first(m, 'quote', 'maker_order', 'order'))}",
                f"APR {apr * 100:.2f}%" if apr is not None else "APR -",
                f"basis {basis:.1f}bps" if basis is not None else "basis -",
            ]
            infl = _num(_first(m, "inflight"))
            if infl:
                parts[1] += f" (inflight {_qty(infl)})"
            note = _first(m, "block_reason", "pause_reason", "paused", "reason")
            if isinstance(note, str) and note:
                parts.append(f"paused: {note}")
            lines.append(" | ".join(parts))
        return "\n".join(lines)


__all__ = ["StatsReporter"]
