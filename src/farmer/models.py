"""Venue-agnostic domain models shared by every module.

Conventions (MUST be respected by every exchange adapter):
- Prices and sizes are ``Decimal``. Never ``float`` for anything that is sent to a venue.
- Sizes are in BASE asset units (e.g. BTC), positions are SIGNED (long > 0, short < 0).
- Funding rates are FRACTIONS PER HOUR (0.0001 == 0.01 % per hour). Positive => longs pay shorts.
- Timestamps are POSIX seconds as ``float`` (``time.time()``).
- ``symbol`` is the canonical cross-venue symbol (upper-case base asset, e.g. "BTC", "ETH", "NVDA").
"""

from __future__ import annotations

import enum
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, Decimal

ZERO = Decimal(0)
ONE = Decimal(1)
BPS = Decimal("0.0001")


class Side(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def sign(self) -> int:
        return 1 if self is Side.BUY else -1

    @property
    def opposite(self) -> Side:
        return Side.SELL if self is Side.BUY else Side.BUY

    @staticmethod
    def from_sign(x: Decimal | int) -> Side:
        if x == 0:
            raise ValueError("cannot derive side from zero quantity")
        return Side.BUY if x > 0 else Side.SELL


class OrderStatus(str, enum.Enum):
    PENDING = "PENDING"  # sent, no ack yet
    OPEN = "OPEN"  # resting on the book
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"

    @property
    def is_terminal(self) -> bool:
        return self in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED)


class MarketCategory(str, enum.Enum):
    CRYPTO = "CRYPTO"
    RWA = "RWA"  # equities / ETFs / commodities with regular trading hours


@dataclass(frozen=True)
class TickTier:
    """Tick size ``tick`` applies for prices strictly below ``up_to_price`` (None = no upper bound)."""

    up_to_price: Decimal | None
    tick: Decimal


@dataclass
class MarketInfo:
    """Static-ish market metadata for one venue, keyed by canonical ``symbol``."""

    venue: str
    symbol: str  # canonical, e.g. "BTC"
    venue_symbol: str  # e.g. "BTC-USD" on Arcus, "BTC" on Lighter
    market_id: int  # Arcus marketId / Lighter market_index
    tick_size: Decimal  # default / smallest tick
    step_size: Decimal  # base size increment
    min_size: Decimal = ZERO  # min base size per order
    min_notional: Decimal = ZERO  # min quote notional per order
    max_size: Decimal | None = None
    maker_fee: Decimal = ZERO  # fraction of notional (0.0002 = 2 bps); negative = rebate
    taker_fee: Decimal = ZERO
    category: MarketCategory = MarketCategory.CRYPTO
    max_leverage: Decimal | None = None
    # maintenance margin per USD of position notional (fraction, e.g. 0.0167); None = unknown
    maintenance_margin_fraction: Decimal | None = None
    tick_tiers: tuple[TickTier, ...] = ()
    active: bool = True
    # Venue-specific integer scaling (Lighter): price_int = price * 10**price_decimals
    price_decimals: int | None = None
    size_decimals: int | None = None
    extra: dict = field(default_factory=dict)

    @property
    def reduce_only_ok(self) -> bool:
        """Reduce-only orders are accepted: active, or a venue-forced reduce-only market (the adapter sets
        ``extra["reduce_only_allowed"]``, e.g. Lighter ``force_reduce_only``)."""
        return self.active or bool(self.extra.get("reduce_only_allowed"))

    def tick_for(self, price: Decimal) -> Decimal:
        """Tick size applicable at ``price`` (handles Arcus tick tiers)."""
        for tier in self.tick_tiers:
            if tier.up_to_price is None or price < tier.up_to_price:
                return tier.tick
        return self.tick_size

    def round_price(self, price: Decimal, side: Side, *, passive: bool = True) -> Decimal:
        """Round to a valid tick. ``passive`` rounds away from the touch (BUY down, SELL up)."""
        tick = self.tick_for(price)
        if passive:
            rounding = ROUND_FLOOR if side is Side.BUY else ROUND_CEILING
        else:
            rounding = ROUND_CEILING if side is Side.BUY else ROUND_FLOOR
        return (price / tick).to_integral_value(rounding=rounding) * tick

    def round_size(self, size: Decimal) -> Decimal:
        """Round a non-negative base size DOWN to the step size."""
        if size <= 0:
            return ZERO
        return (size / self.step_size).to_integral_value(rounding=ROUND_DOWN) * self.step_size

    def is_tradeable_size(self, size: Decimal, price: Decimal) -> bool:
        size = abs(size)
        return size > 0 and size >= self.min_size and size * price >= self.min_notional


@dataclass
class BookLevel:
    price: Decimal
    size: Decimal


@dataclass
class OrderBook:
    """Top-N order book snapshot. bids sorted desc, asks sorted asc."""

    venue: str
    symbol: str
    bids: list[BookLevel]
    asks: list[BookLevel]
    ts: float = field(default_factory=time.time)  # local receive time
    seq: int | None = None

    @property
    def best_bid(self) -> Decimal | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.asks[0].price if self.asks else None

    @property
    def mid(self) -> Decimal | None:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0].price + self.asks[0].price) / 2

    @property
    def spread_bps(self) -> Decimal | None:
        m = self.mid
        if m is None or m == 0:
            return None
        return (self.asks[0].price - self.bids[0].price) / m / BPS

    def age(self, now: float | None = None) -> float:
        return (now if now is not None else time.time()) - self.ts

    def vwap(self, side: Side, size: Decimal) -> tuple[Decimal | None, Decimal]:
        """Average price for a TAKER order of ``side`` and base ``size``.

        BUY consumes asks, SELL consumes bids. Returns (vwap, worst_level_price);
        vwap is None if the visible book cannot fill ``size``.
        """
        levels = self.asks if side is Side.BUY else self.bids
        remaining = size
        cost = ZERO
        worst = ZERO
        for lvl in levels:
            take = min(remaining, lvl.size)
            cost += take * lvl.price
            remaining -= take
            worst = lvl.price
            if remaining <= 0:
                return cost / size, worst
        return None, worst


@dataclass
class FundingInfo:
    venue: str
    symbol: str
    rate_hourly: Decimal  # fraction per hour, + => longs pay shorts
    next_funding_ts: float | None = None  # POSIX seconds of the next settlement
    is_predicted: bool = True  # True if this is the rate expected for the NEXT settlement
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    ts: float = field(default_factory=time.time)

    @property
    def apr(self) -> Decimal:
        return self.rate_hourly * 24 * 365


@dataclass
class Position:
    venue: str
    symbol: str
    size: Decimal  # signed base units
    entry_price: Decimal | None = None
    mark_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    liquidation_price: Decimal | None = None
    leverage: Decimal | None = None
    ts: float = field(default_factory=time.time)

    @property
    def side(self) -> Side | None:
        if self.size == 0:
            return None
        return Side.BUY if self.size > 0 else Side.SELL

    def notional(self, price: Decimal | None = None) -> Decimal:
        px = price or self.mark_price or self.entry_price or ZERO
        return abs(self.size) * px


@dataclass
class Balance:
    venue: str
    equity: Decimal  # total account value incl. unrealized pnl (quote)
    available: Decimal  # free collateral usable for new orders
    margin_used: Decimal = ZERO  # initial margin in use
    maintenance_margin: Decimal = ZERO
    ts: float = field(default_factory=time.time)

    @property
    def margin_ratio(self) -> Decimal:
        """maintenance_margin / equity (0 = no risk, >= 1 = liquidatable)."""
        if self.equity <= 0:
            return Decimal(1) if self.maintenance_margin > 0 else ZERO
        return self.maintenance_margin / self.equity


@dataclass
class Order:
    venue: str
    symbol: str
    side: Side
    price: Decimal | None  # None for market orders
    size: Decimal
    client_id: str
    order_id: str | None = None
    filled: Decimal = ZERO
    avg_fill_price: Decimal | None = None
    status: OrderStatus = OrderStatus.PENDING
    post_only: bool = False
    reduce_only: bool = False
    ioc: bool = False
    created_ts: float = field(default_factory=time.time)
    updated_ts: float = field(default_factory=time.time)
    error: str | None = None

    @property
    def remaining(self) -> Decimal:
        return max(ZERO, self.size - self.filled)

    @property
    def is_open(self) -> bool:
        return not self.status.is_terminal


@dataclass
class Fill:
    venue: str
    symbol: str
    side: Side
    price: Decimal
    size: Decimal  # positive base units
    fee: Decimal = ZERO  # quote currency, positive = paid, negative = rebate received
    is_maker: bool | None = None
    order_id: str | None = None
    client_id: str | None = None
    trade_id: str | None = None
    ts: float = field(default_factory=time.time)
    # replayed by the adapter from a snapshot after a private-stream reconnect (it may already be part of a
    # position the strategy adopted from REST meanwhile -> reconcile instead of adding it blindly)
    recovered: bool = False

    @property
    def signed_size(self) -> Decimal:
        return self.size * self.side.sign

    @property
    def notional(self) -> Decimal:
        return self.price * self.size


@dataclass
class FundingPayment:
    venue: str
    symbol: str
    amount: Decimal  # quote; positive = received, negative = paid
    rate: Decimal | None = None
    position_size: Decimal | None = None
    ts: float = field(default_factory=time.time)


@dataclass
class OrderResult:
    """Outcome of a place/modify call.

    For venues that settle asynchronously (Lighter) ``filled_size`` may be ZERO at return
    time even if the order later fills; fills are always delivered through fill listeners.
    """

    ok: bool
    order: Order | None = None
    error: str | None = None
    retryable: bool = False  # transient (rate limit, network) vs permanent (bad params, would-cross)
    would_cross: bool = False  # post-only rejected because it would take liquidity


FillListener = Callable[[Fill], None]
OrderListener = Callable[[Order], None]
BookListener = Callable[[OrderBook], None]
