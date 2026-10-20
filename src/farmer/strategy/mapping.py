"""Cross-venue market mapping: config markets -> (maker market, taker market) pairs.

Rules (research: specs/overlap-feasibility.md section 3):
1. An explicit ``arcus_symbol`` / ``lighter_symbol`` in the market config wins (canonical key such as
   ``"BTC"`` or a venue ticker such as ``"BTC-USD"``).
2. Otherwise the same base symbol on both venues.
3. Otherwise the K <-> 1000 prefix rule (Arcus ``KBONK`` == Lighter ``1000BONK``, same contract scale).
4. Both markets must be active (Lighter ``force_reduce_only`` / Arcus ``OFFLINE`` markets are inactive). With
   ``allow_reduce_only`` (close-all) a market that still accepts reduce-only orders is mapped as a ``close_only``
   pair: flattened with reduce-only orders, never quoted.
5. ALWAYS verify the price ratio ``maker_mid / taker_mid`` lies within ``1 +/- 2 %``: it catches different
   instruments that share a ticker (live: Arcus QNT = 0.18 x Lighter QNT). Commodity ETFs are never mapped to
   spot proxies (GLD != XAU): there is no proxy table here on purpose.

Venue market keys are each adapter's canonical symbol (Arcus: ``baseAsset``; Lighter: its ``symbol``).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal

from farmer.config import ResolvedMarket
from farmer.exchanges.base import Exchange
from farmer.models import MarketInfo
from farmer.strategy.pricing import pair_min_size, pair_step

log = logging.getLogger("farmer.mapping")

DEFAULT_MAX_RATIO_DEV = Decimal("0.02")
PRICE_TIMEOUT_S = 10.0


@dataclass
class PairSpec:
    """One hedged market: maker leg on the maker venue, hedge leg on the taker venue."""

    symbol: str  # canonical pair name (the config symbol), used for funding/risk/store keys
    maker_symbol: str  # maker venue's canonical key (Arcus baseAsset, e.g. "KBONK")
    taker_symbol: str  # taker venue's canonical key (Lighter symbol, e.g. "1000BONK")
    maker_market: MarketInfo  # live object (adapters refresh it in place)
    taker_market: MarketInfo
    cfg: ResolvedMarket
    price_ratio: Decimal | None = None  # maker/taker price at mapping time (None if not checked)
    close_only: bool = False  # a leg's market only accepts reduce-only orders: flatten it, never quote

    @property
    def tradeable(self) -> bool:
        return not self.close_only

    @property
    def step(self) -> Decimal:
        """Size increment valid on both venues."""
        return pair_step(self.maker_market, self.taker_market)

    def min_size(self, price: Decimal) -> Decimal:
        """Smallest size tradeable on both venues at ``price``."""
        return pair_min_size(self.maker_market, self.taker_market, price)

    def describe(self) -> str:
        r = f" ratio={self.price_ratio:.4f}" if self.price_ratio is not None else ""
        return (f"{self.symbol}: {self.maker_market.venue}:{self.maker_symbol} <-> "
                f"{self.taker_market.venue}:{self.taker_symbol}{r}{' (close only)' if self.close_only else ''}")


def candidate_symbols(symbol: str) -> list[str]:
    """Venue keys to try for a canonical config symbol, most specific first."""
    s = symbol.strip().upper()
    out = [s]
    if s.startswith("1000") and len(s) > 4:
        out.append("K" + s[4:])
    elif s.startswith("K") and len(s) > 1:
        out.append("1000" + s[1:])
    else:  # a bare base such as "BONK" may be listed in thousands on either venue
        out += ["K" + s, "1000" + s]
    seen: set[str] = set()
    return [c for c in out if not (c in seen or seen.add(c))]


def find_market(ex: Exchange, symbol: str, override: str | None = None) -> tuple[str | None, str]:
    """Venue key for ``symbol`` on ``ex`` (or the explicit ``override``). Returns (key, how)."""
    markets = ex.markets
    if override:
        o = override.strip().upper()
        if o in markets:
            return o, "override"
        for key, m in markets.items():
            if str(m.venue_symbol).upper() == o:
                return key, "override"
        return None, f"override {override!r} not listed on {ex.name}"
    for i, c in enumerate(candidate_symbols(symbol)):
        if c in markets:
            return c, "same symbol" if i == 0 else f"prefix rule ({c})"
    return None, f"{symbol} not listed on {ex.name}"


def _override_for(cfg: ResolvedMarket, ex: Exchange) -> str | None:
    return getattr(cfg, f"{ex.name}_symbol", None)


async def venue_prices(ex: Exchange, symbols: list[str]) -> dict[str, Decimal]:
    """Reference price per venue key: live book mid, else funding mark/index (one call), else REST book.

    Never raises; symbols without any price are simply missing from the result.
    """
    out: dict[str, Decimal] = {}
    for s in symbols:
        try:
            b = ex.get_book(s)
        except Exception:
            b = None
        mid = b.mid if b is not None else None
        if mid is not None and mid > 0:
            out[s] = mid
    missing = [s for s in symbols if s not in out]
    if missing:
        try:
            fund = await asyncio.wait_for(ex.fetch_funding(missing), PRICE_TIMEOUT_S)
        except Exception as exc:
            log.debug("%s: funding/mark fetch for mapping failed: %r", ex.name, exc)
            fund = {}
        for s in missing:
            f = fund.get(s)
            if f is None:
                continue
            px = f.mark_price if f.mark_price and f.mark_price > 0 else f.index_price
            if px is not None and px > 0:
                out[s] = px
    for s in [s for s in symbols if s not in out]:
        try:
            b = await asyncio.wait_for(ex.fetch_book(s, depth=5), PRICE_TIMEOUT_S)
        except Exception as exc:
            log.debug("%s: REST book for %s failed: %r", ex.name, s, exc)
            continue
        if b is not None and b.mid is not None and b.mid > 0:
            out[s] = b.mid
    return out


def _inactive_reason(m: MarketInfo) -> str:
    if m.reduce_only_ok or m.extra.get("force_reduce_only"):
        return "reduce-only"
    return m.extra.get("status") or "not active"


async def resolve_pairs(maker: Exchange, taker: Exchange, markets: list[ResolvedMarket], *,
                        check_prices: bool = True, allow_reduce_only: bool = False,
                        max_ratio_dev: Decimal = DEFAULT_MAX_RATIO_DEV) -> tuple[list[PairSpec], list[str]]:
    """Map configured markets onto both venues. Returns (pairs, human-readable skip reasons).

    ``allow_reduce_only``: an inactive market that still accepts reduce-only orders yields a ``close_only`` pair
    instead of a skip (close-all). Both venues must have their markets loaded (``connect()``/``load_markets()``).
    """
    skipped: list[str] = []
    staged: list[tuple[ResolvedMarket, str, str]] = []

    def usable(m: MarketInfo) -> bool:
        return m.active or (allow_reduce_only and m.reduce_only_ok)

    used_maker: dict[str, str] = {}
    used_taker: dict[str, str] = {}
    for cfg in markets:
        sym = cfg.symbol.upper()
        mk, how_m = find_market(maker, sym, _override_for(cfg, maker))
        if mk is None:
            skipped.append(f"{sym}: {how_m}")
            continue
        m_info = maker.markets[mk]
        if not usable(m_info):
            skipped.append(f"{sym}: {maker.name}:{mk} inactive ({_inactive_reason(m_info)})")
            continue
        tk, how_t = find_market(taker, sym, _override_for(cfg, taker))
        if tk is None and not _override_for(cfg, taker) and mk != sym:
            tk, how_t = find_market(taker, mk)  # e.g. config "BONK" -> maker KBONK -> taker 1000BONK
        if tk is None:
            skipped.append(f"{sym}: {how_t}")
            continue
        if mk in used_maker or tk in used_taker:
            skipped.append(f"{sym}: duplicate of {used_maker.get(mk) or used_taker.get(tk)} "
                           f"({maker.name}:{mk} / {taker.name}:{tk})")
            continue
        t_info = taker.markets[tk]
        if not usable(t_info):
            skipped.append(f"{sym}: {taker.name}:{tk} inactive ({_inactive_reason(t_info)})")
            continue
        used_maker[mk] = sym
        used_taker[tk] = sym
        staged.append((cfg, mk, tk))
        if how_m != "same symbol" or how_t != "same symbol":
            log.info("%s mapped via %s / %s: %s:%s <-> %s:%s", sym, how_m, how_t, maker.name, mk, taker.name, tk)

    m_px: dict[str, Decimal] = {}
    t_px: dict[str, Decimal] = {}
    if check_prices and staged:
        m_px, t_px = await asyncio.gather(venue_prices(maker, [mk for _, mk, _ in staged]),
                                          venue_prices(taker, [tk for _, _, tk in staged]))

    lo, hi = Decimal(1) - max_ratio_dev, Decimal(1) + max_ratio_dev
    pairs: list[PairSpec] = []
    for cfg, mk, tk in staged:
        sym = cfg.symbol.upper()
        ratio: Decimal | None = None
        if check_prices:
            pm, pt = m_px.get(mk), t_px.get(tk)
            if pm is None or pt is None:
                missing = ", ".join(v for v, p in ((f"{maker.name}:{mk}", pm), (f"{taker.name}:{tk}", pt)) if p is None)
                skipped.append(f"{sym}: no reference price for {missing} (price check required)")
                continue
            ratio = pm / pt
            if not (lo <= ratio <= hi):
                skipped.append(f"{sym}: price ratio {maker.name}/{taker.name} = {ratio:.4f} outside "
                               f"[{lo}, {hi}] ({pm} vs {pt}): different instruments?")
                continue
        mm, tm = maker.markets[mk], taker.markets[tk]
        pairs.append(PairSpec(symbol=sym, maker_symbol=mk, taker_symbol=tk, maker_market=mm, taker_market=tm, cfg=cfg,
                              price_ratio=ratio, close_only=not (mm.active and tm.active)))
    return pairs, skipped


__all__ = ["PairSpec", "candidate_symbols", "find_market", "resolve_pairs", "venue_prices"]
