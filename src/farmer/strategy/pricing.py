"""Pure pricing/sizing functions for the hedged maker quote.

Everything here is side-effect free so it can be unit-tested exhaustively.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal

from farmer.models import BPS, ZERO, MarketInfo, OrderBook, Side
from farmer.utils.decimals import common_step


@dataclass(frozen=True)
class Quote:
    side: Side  # maker-venue side
    price: Decimal  # rounded, post-only-safe maker price
    size: Decimal  # base size, multiple of the common step
    hedge_price: Decimal  # taker VWAP used to derive the limit
    limit_price: Decimal  # edge-limit (worst acceptable maker price before tick rounding)
    expected_edge_bps: Decimal  # edge vs the hedge at the quoted price (after fees)


def pair_step(maker: MarketInfo, taker: MarketInfo) -> Decimal:
    """Size increment valid on both venues."""
    return common_step(maker.step_size, taker.step_size)


def pair_min_size(maker: MarketInfo, taker: MarketInfo, price: Decimal) -> Decimal:
    """Smallest base size (multiple of the pair step) tradeable on BOTH venues at ``price``."""
    step = pair_step(maker, taker)
    need = max(maker.min_size, taker.min_size, step)
    if price > 0:
        need = max(need, maker.min_notional / price, taker.min_notional / price)
    # round UP to the step
    n = (need / step).to_integral_value(rounding="ROUND_CEILING")
    return n * step


def round_pair_size(size: Decimal, maker: MarketInfo, taker: MarketInfo) -> Decimal:
    if size <= 0:
        return ZERO
    step = pair_step(maker, taker)
    return (size / step).to_integral_value(rounding=ROUND_DOWN) * step


def edge_limit_price(
    side: Side,
    hedge_price: Decimal,
    mid: Decimal,
    min_edge_bps: Decimal,
    maker_fee: Decimal,
    taker_fee: Decimal,
) -> Decimal:
    """Worst maker price that still earns ``min_edge_bps`` after fees when hedged at ``hedge_price``.

    maker BUY  -> hedge SELL: edge = hedge*(1-ft) - px*(1+fm)  >= e*mid  -> px <= (hedge*(1-ft) - e*mid)/(1+fm)
    maker SELL -> hedge BUY : edge = px*(1-fm) - hedge*(1+ft)  >= e*mid  -> px >= (hedge*(1+ft) + e*mid)/(1-fm)
    """
    e = min_edge_bps * BPS * mid
    if side is Side.BUY:
        return (hedge_price * (1 - taker_fee) - e) / (1 + maker_fee)
    return (hedge_price * (1 + taker_fee) + e) / (1 - maker_fee)


def expected_edge_bps(side: Side, maker_px: Decimal, hedge_price: Decimal, mid: Decimal, maker_fee: Decimal,
                      taker_fee: Decimal) -> Decimal:
    if mid <= 0:
        return ZERO
    if side is Side.BUY:
        edge = hedge_price * (1 - taker_fee) - maker_px * (1 + maker_fee)
    else:
        edge = maker_px * (1 - maker_fee) - hedge_price * (1 + taker_fee)
    return edge / mid / BPS


def compute_maker_quote(
    *,
    side: Side,
    size: Decimal,
    maker_market: MarketInfo,
    taker_market: MarketInfo,
    maker_book: OrderBook,
    taker_book: OrderBook,
    min_edge_bps: Decimal,
    max_improve_ticks: int = 1_000_000,
    maker_fee: Decimal | None = None,
    taker_fee: Decimal | None = None,
) -> Quote | None:
    """Best (most aggressive) post-only maker price that keeps the hedge edge >= ``min_edge_bps``.

    Returns None if the quote cannot be built (empty books, insufficient taker depth, size too
    small, or the edge-limited price would be non-positive).
    """
    if size <= 0:
        return None
    fm = maker_market.maker_fee if maker_fee is None else maker_fee
    ft = taker_market.taker_fee if taker_fee is None else taker_fee

    hedge_side = side.opposite
    hedge_vwap, _ = taker_book.vwap(hedge_side, size)
    if hedge_vwap is None:
        return None
    mid = taker_book.mid
    if mid is None or mid <= 0:
        return None

    limit = edge_limit_price(side, hedge_vwap, mid, min_edge_bps, fm, ft)
    if limit <= 0:
        return None

    m_bid, m_ask = maker_book.best_bid, maker_book.best_ask
    if side is Side.BUY:
        px = limit
        if m_ask is not None:
            px = min(px, m_ask - maker_market.tick_for(m_ask))  # stay passive (post-only)
        if m_bid is not None and max_improve_ticks >= 0:
            px = min(px, m_bid + maker_market.tick_for(m_bid) * max_improve_ticks)
        px = maker_market.round_price(px, Side.BUY, passive=True)  # floor
    else:
        px = limit
        if m_bid is not None:
            px = max(px, m_bid + maker_market.tick_for(m_bid))
        if m_ask is not None and max_improve_ticks >= 0:
            px = max(px, m_ask - maker_market.tick_for(m_ask) * max_improve_ticks)
        px = maker_market.round_price(px, Side.SELL, passive=True)  # ceil

    if px <= 0:
        return None
    # rounding of tiered ticks could in theory land on the touch: re-check passivity
    if side is Side.BUY and m_ask is not None and px >= m_ask:
        return None
    if side is Side.SELL and m_bid is not None and px <= m_bid:
        return None

    return Quote(
        side=side,
        price=px,
        size=size,
        hedge_price=hedge_vwap,
        limit_price=limit,
        expected_edge_bps=expected_edge_bps(side, px, hedge_vwap, mid, fm, ft),
    )


def quote_violates_edge(side: Side, resting_price: Decimal, fresh_limit: Decimal) -> bool:
    """True if a resting maker order is now worse than the fresh edge limit (must be pulled/repriced)."""
    if side is Side.BUY:
        return resting_price > fresh_limit
    return resting_price < fresh_limit


def price_moved_bps(a: Decimal, b: Decimal, mid: Decimal) -> Decimal:
    if mid <= 0:
        return ZERO
    return abs(a - b) / mid / BPS


def worst_taker_price(side: Side, book: OrderBook, size: Decimal, max_slippage_bps: Decimal,
                      market: MarketInfo) -> Decimal | None:
    """Protective limit for a taker IOC: beyond the VWAP worst level by ``max_slippage_bps``."""
    touch = book.best_ask if side is Side.BUY else book.best_bid
    if touch is None:
        return None
    _, worst_level = book.vwap(side, size)
    ref = worst_level if worst_level and worst_level > 0 else touch
    if side is Side.BUY:
        ref = max(ref, touch)
        px = ref * (1 + max_slippage_bps * BPS)
        return market.round_price(px, Side.BUY, passive=False)  # ceil -> more permissive
    ref = min(ref, touch)
    px = ref * (1 - max_slippage_bps * BPS)
    px = market.round_price(px, Side.SELL, passive=False)  # floor
    return px if px > 0 else None
