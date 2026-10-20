"""Pure converters from Arcus JSON payloads to farmer domain models (no I/O)."""

from __future__ import annotations

import time
from datetime import datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from farmer.models import (
    ZERO,
    Fill,
    FundingInfo,
    FundingPayment,
    MarketCategory,
    MarketInfo,
    Order,
    OrderStatus,
    Position,
    Side,
    TickTier,
)
from farmer.utils.decimals import D_or_none

VENUE = "arcus"
PPM = Decimal(1_000_000)

# Failed cancel/modify frames ("placeholder" REJECTED frames: marketId 0, "MARKET-0", zero sizes,
# orderId = the TARGET order which is NOT affected).
ACTION_REJECTS = frozenset({"ORDER_NOT_FOUND", "ORDER_NOT_FOUND_FOR_MODIFY", "MODIFY_SUPERSEDED_BY_CANCEL",
                            "MODIFY_CHANGED_IMMUTABLE_FIELD", "MODIFY_ZERO_SIZE", "MODIFY_TPSL_NOT_SUPPORTED",
                            "MODIFY_SIZE_ALREADY_FILLED"})
WOULD_CROSS = "POST_ONLY_WOULD_CROSS"


def dec(x: Any, default: Decimal | None = None) -> Decimal | None:
    """Decimal from an API value (decimal string / int); ``default`` for null / empty / invalid."""
    if x is None or x == "":
        return default
    v = D_or_none(x)
    return default if v is None else v


def us_to_s(x: Any) -> float | None:
    try:
        return int(x) / 1e6 if x is not None else None
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- markets
def canonical_symbol(raw: dict) -> str:
    """Canonical cross-venue symbol: upper-case base asset (``BTC-USD`` -> ``BTC``, ``KBONK-USD`` -> ``KBONK``)."""
    base = raw.get("baseAsset")
    if not base:
        base = str(raw.get("marketDisplayName") or "").split("-")[0]
    return str(base).upper()


def _charged_ppm(rate: int, discount_ppm: int) -> int:
    """Discount applies to positive rates only, truncated toward zero; rebates are never reduced."""
    if rate <= 0 or not discount_ppm:
        return rate
    return int((Decimal(rate) * (PPM - Decimal(discount_ppm)) / PPM).to_integral_value(rounding=ROUND_DOWN))


def market_fees(raw: dict, fee_level: int | None = None, discount_ppm: int = 0) -> tuple[Decimal, Decimal]:
    """(maker, taker) fee fractions for this account's tier on this market."""
    maker_ppm = taker_ppm = None
    if fee_level is not None:
        for row in raw.get("feeTiers") or []:
            if row.get("level") == fee_level:
                maker_ppm, taker_ppm = int(row.get("makerFeePpm", 0)), int(row.get("takerFeePpm", 0))
                break
    if maker_ppm is None:
        maker_ppm = int(raw.get("effectiveMakerFeePpm", raw.get("baseMakerFeePpm", 0)) or 0)
        taker_ppm = int(raw.get("effectiveTakerFeePpm", raw.get("baseTakerFeePpm", 0)) or 0)
    return (Decimal(_charged_ppm(maker_ppm, discount_ppm)) / PPM,
            Decimal(_charged_ppm(int(taker_ppm or 0), discount_ppm)) / PPM)


def tick_tiers(raw: dict) -> tuple[TickTier, ...]:
    tiers = []
    for t in raw.get("tickTiers") or []:
        tick = dec(t.get("tick"))
        if tick is None or tick <= 0:
            continue
        tiers.append(TickTier(up_to_price=dec(t.get("upToPrice")), tick=tick))
    # ascending by bound, unbounded last
    tiers.sort(key=lambda x: (x.up_to_price is None, x.up_to_price or ZERO))
    return tuple(tiers)


def _max_leverage(imf: Decimal | None) -> Decimal | None:
    """1 / initialMarginFraction as a plain Decimal (40, 25, 3.33)."""
    if not imf or imf <= 0:
        return None
    lev = Decimal(1) / imf
    if lev == lev.to_integral_value():
        return Decimal(int(lev))
    return lev.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def market_fields(raw: dict, fee_level: int | None = None, discount_ppm: int = 0) -> dict:
    """All MarketInfo constructor fields for one ``/v1/markets`` row (also used for in-place refresh)."""
    imf = dec(raw.get("initialMarginFraction"))
    maker, taker = market_fees(raw, fee_level, discount_ppm)
    category = MarketCategory.CRYPTO if str(raw.get("category") or "CRYPTO").upper() == "CRYPTO" else MarketCategory.RWA
    tick = dec(raw.get("tickSize"))
    if tick is None or tick <= 0:
        raise ValueError(f"market {raw.get('marketDisplayName')}: invalid tickSize")
    step = dec(raw.get("stepSize"))
    if step is None or step <= 0:
        raise ValueError(f"market {raw.get('marketDisplayName')}: invalid stepSize")
    extra = {
        "status": raw.get("status"),
        "is_outside_rth": raw.get("isOutsideRth"),
        "regular_trading_hours": raw.get("regularTradingHours"),
        "raw_category": raw.get("category"),
        "imf": imf,
        "mmf": dec(raw.get("maintenanceMarginFraction")),
        "off_hours_imf": dec(raw.get("offHoursInitialMarginFraction")),
        "oi_cap_notional": dec(raw.get("openInterestCapNotional")),
        "open_interest": dec(raw.get("openInterest")),
        "next_funding_at": raw.get("nextFundingAt"),
        "funding_rate": dec(raw.get("fundingRate")),
        "next_funding_rate": dec(raw.get("nextFundingRate")),
        "mark_price": dec(raw.get("markPrice")),
        "oracle_price": dec(raw.get("oraclePrice")),
        "liquidation_fee_ppm": raw.get("liquidationFeePpm"),
        "trading_bounds": {k: raw.get(k) for k in ("upperTradingBound", "lowerTradingBound", "currentSettlementPrice")},
        "updated_ts": time.time(),
        "raw": raw,
    }
    return dict(
        venue=VENUE,
        symbol=canonical_symbol(raw),
        venue_symbol=str(raw.get("marketDisplayName")),
        market_id=int(raw["marketId"]),
        tick_size=tick,
        step_size=step,
        min_size=dec(raw.get("minOrderSize"), ZERO),
        min_notional=dec(raw.get("minOrderNotional"), ZERO),
        max_size=dec(raw.get("maxOrderSize")),
        maker_fee=maker,
        taker_fee=taker,
        category=category,
        max_leverage=_max_leverage(imf),
        tick_tiers=tick_tiers(raw),
        active=str(raw.get("status") or "").upper() == "ONLINE",
        extra=extra,
    )


def parse_market(raw: dict, fee_level: int | None = None, discount_ppm: int = 0) -> MarketInfo:
    return MarketInfo(**market_fields(raw, fee_level, discount_ppm))


def update_market(m: MarketInfo, raw: dict, fee_level: int | None = None, discount_ppm: int = 0) -> MarketInfo:
    """Refresh ``m`` IN PLACE (risk params, ticks, fees change intraday)."""
    for k, v in market_fields(raw, fee_level, discount_ppm).items():
        if k in ("venue", "symbol"):
            continue
        setattr(m, k, v)
    return m


def is_market_open(m: MarketInfo | None, *, close_buffer_s: float = 0.0, now: float | None = None) -> bool:
    """Active, and either 24/7 crypto or an RWA market inside regular trading hours.

    ``isOutsideRth`` stays authoritative (holidays, early closes); ``close_buffer_s`` additionally reports
    an RWA market closed that long before the scheduled ``regularTradingHours`` end (no new opens into
    the off-hours regime: higher IM, price bands, SOFR funding).
    """
    if m is None or not m.active:
        return False
    if m.category is MarketCategory.CRYPTO:
        return True
    if m.extra.get("is_outside_rth") is not False:
        return False
    if close_buffer_s > 0:
        left = rth_seconds_left(m.extra.get("regular_trading_hours"), now)
        if left is not None and left <= close_buffer_s:
            return False
    return True


def rth_seconds_left(rth: Any, now: float | None = None) -> float | None:
    """Seconds until the scheduled end of the regular-trading-hours window that contains ``now``.

    ``rth`` = ``{"startSecondsOfDay", "endSecondsOfDay", "timezone", "isOvernight"}`` (half-open window,
    local time). None when unknown / not inside the window by schedule.
    """
    if not isinstance(rth, dict):
        return None
    try:
        start, end = int(rth["startSecondsOfDay"]), int(rth["endSecondsOfDay"])
        tz = ZoneInfo(str(rth.get("timezone") or "America/New_York"))
    except (KeyError, TypeError, ValueError, ZoneInfoNotFoundError):
        return None
    local = datetime.fromtimestamp(time.time() if now is None else now, tz)
    sod = local.hour * 3600 + local.minute * 60 + local.second + local.microsecond / 1e6
    if start < end:
        return end - sod if start <= sod < end else None
    if sod >= start:  # window wraps midnight (isOvernight)
        return end + 86400 - sod
    return end - sod if sod < end else None


def funding_info(raw: dict, *, mark: Decimal | None = None, oracle: Decimal | None = None) -> FundingInfo:
    """Predicted next hourly rate (``nextFundingRate``, fraction/hour) + mark/oracle."""
    rate = dec(raw.get("nextFundingRate"))
    predicted = rate is not None
    if rate is None:
        rate = dec(raw.get("fundingRate"), ZERO)
    mk = mark if mark is not None else dec(raw.get("markPrice"))
    orc = oracle if oracle is not None else dec(raw.get("oraclePrice"))
    nfa = raw.get("nextFundingAt")
    return FundingInfo(
        venue=VENUE,
        symbol=canonical_symbol(raw),
        rate_hourly=rate,
        next_funding_ts=float(nfa) if nfa else None,
        is_predicted=predicted,
        mark_price=mk if mk and mk > 0 else None,  # "0" = no mark yet
        index_price=orc if orc and orc > 0 else None,
    )


# --------------------------------------------------------------------------- orders
def classify_order_frame(c: dict, *, in_modify: bool = False) -> str:
    """Classify one ``orders`` channel object.

    - ``action_reject``: a cancel/modify failed; ``c["orderId"]`` is the TARGET, its state is unchanged
    - ``modify_leg``: CANCELED + MODIFY_CANCELED, not terminal (replacement follows under the same id)
    - ``update``: a real order state update (live or terminal)

    ``in_modify`` = a MODIFY_CANCELED leg was seen for this order: a full REJECTED frame is then the
    replacement outcome (e.g. POST_ONLY_WOULD_CROSS kills the order), not an action reject.
    """
    status, reason = c.get("status"), c.get("rejectionReason")
    placeholder = c.get("marketId") in (0, "0") or c.get("marketDisplayName") == "MARKET-0"
    if status == "REJECTED":
        if placeholder:
            return "action_reject"
        if reason in ACTION_REJECTS and not in_modify:
            return "action_reject"
    if status == "CANCELED" and c.get("cancelReason") == "MODIFY_CANCELED":
        return "modify_leg"
    return "update"


def map_order_status(c: dict, tif: str | None = None) -> tuple[OrderStatus, str | None]:
    """(status, error) from an order object; drives off ``state`` (``status`` never says PARTIALLY_FILLED)."""
    tif = (tif or c.get("timeInForce") or "").upper()
    ioc = tif in ("IOC", "FOK")
    state = (c.get("state") or c.get("orderState") or c.get("status") or "").upper()
    status = (c.get("status") or "").upper()
    reason = c.get("rejectionReason")
    if state == "FILLED" or status == "FILLED":
        return OrderStatus.FILLED, None
    if state == "PARTIALLY_FILLED":
        if ioc or status in ("CANCELED", "MARGIN_CANCELED", "REJECTED"):
            # PARTIALLY_FILLED is terminal for IOC orders (remainder canceled)
            return OrderStatus.CANCELED, reason or c.get("cancelReason") or "IOC remainder canceled"
        return OrderStatus.PARTIALLY_FILLED, None
    if state == "OPEN":
        return OrderStatus.OPEN, None
    if state == "CANCELED" or status in ("CANCELED", "MARGIN_CANCELED"):
        err = c.get("cancelReason") or ("MARGIN_CANCELED" if status == "MARGIN_CANCELED" else None)
        return OrderStatus.CANCELED, err
    if state == "REJECTED" or status == "REJECTED":
        if reason in ("IOC_CANCELED", "FOK_FAILED"):
            return OrderStatus.CANCELED, reason  # taker order that found nothing to take
        if reason == "EXPIRED":
            return OrderStatus.EXPIRED, reason
        if reason == WOULD_CROSS:
            return OrderStatus.REJECTED, f"{WOULD_CROSS}: post-only would cross"
        return OrderStatus.REJECTED, reason or "REJECTED"
    if state in ("PENDING", "ACK"):
        return OrderStatus.PENDING, None
    return OrderStatus.UNKNOWN, None


def order_from_row(c: dict, symbol: str, *, reduce_only: bool | None = None) -> Order:
    """Order from a REST openOrders row / orders-channel object (fresh object)."""
    tif = (c.get("timeInForce") or "").upper()
    status, err = map_order_status(c)
    created = us_to_s(c.get("createdAt")) or time.time()
    ro = reduce_only if reduce_only is not None else bool(c.get("reduceOnly", False))
    return Order(
        venue=VENUE,
        symbol=symbol,
        side=Side(str(c.get("side") or "BUY").upper()),
        price=dec(c.get("price")),
        size=dec(c.get("originalSize"), ZERO),
        client_id=str(c.get("clientId") or ""),
        order_id=str(c.get("orderId")) if c.get("orderId") else None,
        filled=dec(c.get("filledSize"), ZERO),
        avg_fill_price=dec(c.get("avgFillPrice")),
        status=status,
        post_only=tif == "ALO",
        reduce_only=ro,
        ioc=tif in ("IOC", "FOK"),
        created_ts=created,
        updated_ts=us_to_s(c.get("updatedAt")) or created,
        error=err,
    )


def apply_order_row(o: Order, c: dict, tif: str | None = None) -> None:
    """Update a live Order object from a channel frame (keeps identity for listeners)."""
    status, err = map_order_status(c, tif)
    if c.get("orderId"):
        o.order_id = str(c["orderId"])
    if c.get("clientId") and not o.client_id:
        o.client_id = str(c["clientId"])
    px = dec(c.get("price"))
    if px is not None and px > 0:
        o.price = px
    size = dec(c.get("originalSize"))
    if size is not None and size > 0:
        o.size = size
    filled = dec(c.get("filledSize"))
    if filled is not None and filled >= o.filled:
        o.filled = filled
    avg = dec(c.get("avgFillPrice"))
    if avg is not None and avg > 0:
        o.avg_fill_price = avg
    o.status = status
    o.error = err
    o.updated_ts = us_to_s(c.get("updatedAt")) or time.time()


# --------------------------------------------------------------------------- fills / positions / funding
def fill_from_row(f: dict, symbol: str) -> Fill:
    role = str(f.get("role") or "").upper()
    return Fill(
        venue=VENUE,
        symbol=symbol,
        side=Side(str(f.get("side")).upper()),
        price=dec(f.get("price"), ZERO),
        size=dec(f.get("size"), ZERO),
        fee=dec(f.get("fee"), ZERO),  # quote, negative = rebate (same sign convention as Fill)
        is_maker=True if role == "MAKER" else (False if role in ("TAKER", "ALP") else None),
        order_id=str(f.get("orderId")) if f.get("orderId") else None,
        client_id=str(f.get("clientId")) if f.get("clientId") else None,
        trade_id=str(f.get("tradeId")) if f.get("tradeId") is not None else None,
        ts=us_to_s(f.get("createdAt")) or time.time(),
    )


def fill_key(f: dict) -> tuple[str, str]:
    return str(f.get("tradeId")), str(f.get("orderId"))


def position_from_row(p: dict, symbol: str) -> Position | None:
    size = dec(p.get("size"), ZERO)
    if size == 0 or str(p.get("side") or "").upper() == "FLAT":
        return None
    return Position(
        venue=VENUE,
        symbol=symbol,
        size=size,
        entry_price=dec(p.get("averageEntryPrice")),
        mark_price=dec(p.get("markPx")) or None,
        unrealized_pnl=dec(p.get("unrealizedPnl")),
        liquidation_price=None,  # not published by Arcus
        leverage=dec(p.get("leverage")),
    )


def funding_payment_from_row(r: dict, symbol: str) -> FundingPayment:
    return FundingPayment(
        venue=VENUE,
        symbol=symbol,
        amount=dec(r.get("payment"), ZERO),  # positive = received
        rate=dec(r.get("fundingRate")),
        position_size=dec(r.get("size")),
        ts=us_to_s(r.get("time")) or 0.0,
    )


__all__ = [
    "ACTION_REJECTS",
    "WOULD_CROSS",
    "apply_order_row",
    "canonical_symbol",
    "classify_order_frame",
    "dec",
    "fill_from_row",
    "fill_key",
    "funding_info",
    "funding_payment_from_row",
    "is_market_open",
    "map_order_status",
    "market_fees",
    "market_fields",
    "order_from_row",
    "parse_market",
    "position_from_row",
    "rth_seconds_left",
    "tick_tiers",
    "update_market",
    "us_to_s",
]
