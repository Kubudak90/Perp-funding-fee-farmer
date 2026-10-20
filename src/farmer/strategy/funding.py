"""Funding monitor: polls both venues and decides the profitable hedge direction."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal

from farmer.exchanges.base import Exchange
from farmer.models import ZERO, FundingInfo

log = logging.getLogger("farmer.funding")

HOURS_PER_YEAR = Decimal(24 * 365)

# direction: +1 = maker LONG / taker SHORT, -1 = maker SHORT / taker LONG
MAKER_LONG = 1
MAKER_SHORT = -1


@dataclass
class FundingSnapshot:
    symbol: str
    maker: FundingInfo
    taker: FundingInfo

    def hourly_carry(self, direction: int) -> Decimal:
        """Funding earned per hour per 1 USD of notional on EACH leg for ``direction``.

        A long pays ``rate`` (receives when negative); a short receives ``rate``.
        maker LONG / taker SHORT  -> -r_m + r_t ;  maker SHORT / taker LONG -> r_m - r_t
        """
        diff = self.maker.rate_hourly - self.taker.rate_hourly
        return diff if direction == MAKER_SHORT else -diff

    def apr(self, direction: int) -> Decimal:
        return self.hourly_carry(direction) * HOURS_PER_YEAR

    @property
    def best_direction(self) -> int:
        return MAKER_SHORT if self.maker.rate_hourly >= self.taker.rate_hourly else MAKER_LONG

    @property
    def next_settlement_ts(self) -> float | None:
        ts = [t for t in (self.maker.next_funding_ts, self.taker.next_funding_ts) if t]
        return min(ts) if ts else None

    @property
    def age(self) -> float:
        return time.time() - min(self.maker.ts, self.taker.ts)


def choose_direction(snapshot: FundingSnapshot | None, current: int | None, hysteresis_apr: Decimal,
                     mode: str = "auto") -> int:
    """Direction with hysteresis. ``mode`` = auto | long_maker | short_maker."""
    if mode == "long_maker":
        return MAKER_LONG
    if mode == "short_maker":
        return MAKER_SHORT
    if snapshot is None:
        return current if current is not None else MAKER_SHORT
    best = snapshot.best_direction
    if current is None or current == best:
        return best
    # only flip if the improvement beats the hysteresis band
    if snapshot.apr(best) - snapshot.apr(current) > hysteresis_apr:
        return best
    return current


def next_hour_ts(now: float | None = None) -> float:
    now = time.time() if now is None else now
    return (int(now) // 3600 + 1) * 3600.0


class FundingMonitor:
    """Polls ``fetch_funding`` on both venues every ``refresh_s`` seconds."""

    def __init__(self, maker: Exchange, taker: Exchange, symbols: list[str], refresh_s: float = 60.0, *,
                 maker_symbols: dict[str, str] | None = None, taker_symbols: dict[str, str] | None = None) -> None:
        """``symbols`` are the snapshot keys (pair names); the optional maps translate them to each venue's own
        market key (e.g. pair ``KBONK`` -> taker ``1000BONK``). Unmapped symbols are used as-is."""
        self.maker = maker
        self.taker = taker
        self.symbols = list(symbols)
        self.refresh_s = refresh_s
        self.maker_symbols = dict(maker_symbols or {})
        self.taker_symbols = dict(taker_symbols or {})
        self._snap: dict[str, FundingSnapshot] = {}
        self._stop = asyncio.Event()
        self.updated = asyncio.Event()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    def latest(self, symbol: str) -> FundingSnapshot | None:
        return self._snap.get(symbol)

    def all(self) -> dict[str, FundingSnapshot]:
        return dict(self._snap)

    async def refresh(self) -> None:
        m_syms = [self.maker_symbols.get(s, s) for s in self.symbols]
        t_syms = [self.taker_symbols.get(s, s) for s in self.symbols]
        m_res, t_res = await asyncio.gather(
            self.maker.fetch_funding(m_syms), self.taker.fetch_funding(t_syms), return_exceptions=True
        )
        if isinstance(m_res, BaseException):
            log.warning("maker funding fetch failed: %s", m_res)
            m_res = {}
        if isinstance(t_res, BaseException):
            log.warning("taker funding fetch failed: %s", t_res)
            t_res = {}
        for s in self.symbols:
            m, t = m_res.get(self.maker_symbols.get(s, s)), t_res.get(self.taker_symbols.get(s, s))
            if m is None or t is None:
                continue
            if m.next_funding_ts is None:
                m.next_funding_ts = next_hour_ts()
            if t.next_funding_ts is None:
                t.next_funding_ts = next_hour_ts()
            self._snap[s] = FundingSnapshot(symbol=s, maker=m, taker=t)
        self.updated.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.refresh()
            except Exception:
                log.exception("funding refresh failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.refresh_s)
            except TimeoutError:
                pass

    def stop(self) -> None:
        self._stop.set()


def describe(snap: FundingSnapshot) -> str:
    d = snap.best_direction
    return (
        f"{snap.symbol}: maker {snap.maker.rate_hourly * 100:.5f}%/h taker {snap.taker.rate_hourly * 100:.5f}%/h "
        f"-> {'maker SHORT/taker LONG' if d == MAKER_SHORT else 'maker LONG/taker SHORT'} "
        f"carry {snap.apr(d) * 100:.2f}% APR"
    )


__all__ = [
    "FundingMonitor",
    "FundingSnapshot",
    "MAKER_LONG",
    "MAKER_SHORT",
    "ZERO",
    "choose_direction",
    "describe",
    "next_hour_ts",
]
