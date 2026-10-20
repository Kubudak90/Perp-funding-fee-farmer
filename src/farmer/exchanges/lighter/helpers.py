"""Pure Lighter helpers: market parsing, integer encoding, payload parsing, error codes.

Everything here is side-effect free (no I/O) so it can be unit-tested against captured payloads.

Units (see specs/lighter-data.md §2):
- book/trade/order prices and sizes are decimal STRINGS already scaled to human units;
- signed txs use integers: ``base_amount = size * 10**size_decimals``, ``price = price * 10**price_decimals``;
- WS ``market_stats.current_funding_rate`` is PERCENT per hour; ``/api/v1/funding-rates`` is an 8 h FRACTION;
- margin fractions in metadata are 1/10_000, but ``Position.initial_margin_fraction`` is a percent string;
- Trade ``taker_fee``/``maker_fee`` are fee RATES in ppm (omitted when 0, e.g. Standard accounts).
"""

from __future__ import annotations

import json
import re
import time
from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any, Callable

from farmer.exchanges.base import ExchangeError
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
)

VENUE = "lighter"

MAX_CLIENT_ORDER_INDEX = 2**48 - 1
MAX_BASE_AMOUNT = 2**48 - 1
# The native signer takes ``price`` as a C int (ctypes does NOT range-check: 2**32 + 5 silently signs
# as 5). Stay within the signed 32-bit range, which is conservative and far above any live price.
MAX_PRICE_INT = 2**31 - 1
HOUR_MS = 3_600_000
FEE_PPM = Decimal(1_000_000)
TEN_THOUSAND = Decimal(10_000)

# --------------------------------------------------------------------------- error codes
CODE_OK = 200
ERR_RESTRICTED = 20558
ERR_INVALID_AUTH = 20013
ERR_INVALID_NONCE = 21104
ERR_TX_NOT_FOUND = 21500
ERR_STANDARD_LIMIT = 52002
ERR_TOO_MANY_REQUESTS = 23000

# Stop trading: geo-block, unregistered/invalid key or signature, outdated SDK, blacklisted API use.
FATAL_CODES = frozenset({20558, 21108, 21109, 21110, 21120, 21136, 52001})
# Transient: rate limits, pending-queue full, internal errors / timeouts, order book full, WS budget.
RETRYABLE_CODES = frozenset({23000, 21506, 21601, 29500, 29501, 30009, 30010, 30011, 20011})
# Cancel/modify of a non-active order: harmless no-ops (nonce consumed).
NOOP_CANCEL_CODES = frozenset({21600, 21709, 21715})
# sendTx answers that do NOT prove the tx was dropped (internal error / process timeout): the outcome is unknown
# and must be resolved by hash (lighter-trading.md §12). They stay in RETRYABLE_CODES for REST reads.
AMBIGUOUS_TX_CODES = frozenset({29500, 29501})
# Rejections that depend only on the signed payload / key / IP: an identical copy is rejected the same way, so a
# possibly-delivered earlier copy of the same payload cannot have executed either.
DETERMINISTIC_TX_CODES = FATAL_CODES | frozenset(
    {21501, 21518, 21700, 21701, 21702, 21703, 21704, 21705, 21706, 21711, 21713, 30007}
)

RESTRICTED_MESSAGE = (
    "Lighter rejected the request from this IP: restricted jurisdiction (code 20558). Trading (sendTx and the "
    "non-readonly websocket) is impossible from this region; run the bot from a location permitted by "
    "https://lighter.xyz/terms (Lighter suggests AWS ap-northeast-1). Read-only market data still works."
)
STANDARD_LIMIT_MESSAGE = (
    "Lighter Standard trading limit reached (code 52002): the account may only close or reduce positions"
)

# Equities/ETFs on Lighter whose funding multiplier is 1 (not the usual 1/2 of RWA markets).
_EXTRA_RWA_SYMBOLS = frozenset({"SOXX", "DIA", "MAGS", "HYUNDAI", "HANMI", "SAMSUNG", "KRCOMP", "SKHYNIX", "STONK"})


class TxNotSent(Exception):
    """The transaction certainly never left this process (no budget, socket not connected...)."""


class TxRejected(ExchangeError):
    """API-level rejection of a signed tx: the nonce was NOT consumed."""

    def __init__(self, code: int, message: str, *, retryable: bool | None = None):
        if retryable is None:
            retryable = is_retryable(code)
        super().__init__(f"lighter tx rejected ({code}): {message}", retryable=retryable, code=code)
        self.tx_code = code
        self.tx_message = message

    @property
    def fatal(self) -> bool:
        return self.tx_code in FATAL_CODES


class TxUnknown(ExchangeError):
    """The outcome of a sent tx could not be established. Never re-sign the same intent; reconcile first."""

    def __init__(self, tx_hash: str, reason: str = ""):
        super().__init__(f"lighter tx outcome unknown ({tx_hash}){': ' + reason if reason else ''}", retryable=False)
        self.tx_hash = tx_hash


def is_retryable(code: int | None) -> bool:
    if code is None:
        return True
    if code in RETRYABLE_CODES:
        return True
    return code < 0 or 500 <= code < 600  # synthetic HTTP status codes (-status) / 5xx


def is_ambiguous_tx_code(code: int | None) -> bool:
    """A sendTx answer that leaves the outcome unknown (resolve by hash; never treat as 'nonce not consumed')."""
    if code is None:
        return True
    return code in AMBIGUOUS_TX_CODES or code == -1 or 500 <= code < 600  # -1: malformed / code-less ack


_CODE_RE = re.compile(r"code[\"'=:\s]+(\d{3,6})")


def parse_error_code(text: str | None) -> int | None:
    """Extract a numeric Lighter code from SDK error strings / bodies ("code=21104 message=...")."""
    if not text:
        return None
    m = _CODE_RE.search(text)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(2\d{4}|3\d{4}|5\d{4}|6\d{4})\b", text)
    return int(m.group(1)) if m else None


def parse_ack(resp: Any) -> tuple[bool, int, str]:
    """Normalize a sendTx response (REST JSON or WS frame) to (accepted, code, message)."""
    if not isinstance(resp, dict):
        return False, -1, f"unexpected response {resp!r}"[:300]
    if "error" in resp:
        err = resp["error"]
        if isinstance(err, dict):
            try:
                code = int(err.get("code", -1))
            except (TypeError, ValueError):
                code = -1
            return False, code, str(err.get("message", ""))
        return False, parse_error_code(str(err)) or -1, str(err)[:300]
    try:
        code = int(resp.get("code", CODE_OK))
    except (TypeError, ValueError):
        code = -1
    return code == CODE_OK, code, str(resp.get("message") or "")


# --------------------------------------------------------------------------- decimals
def dec(x: Any, default: Decimal | None = None) -> Decimal | None:
    """Decimal from a wire value (string/int/float); floats go through repr to avoid binary noise."""
    if x is None or x == "":
        return default
    try:
        if isinstance(x, Decimal):
            return x
        if isinstance(x, float):
            return Decimal(repr(x))
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return default


def unit(decimals: int) -> Decimal:
    """10**-decimals as an exact Decimal (tick / step)."""
    return Decimal(1).scaleb(-int(decimals))


def size_to_int(size: Decimal, size_decimals: int, *, exact: bool = True) -> int:
    """Base size -> signer integer. ``exact`` rejects sizes that are not a multiple of the step."""
    if not isinstance(size, Decimal):
        raise ValueError("size must be a Decimal")
    scaled = size.scaleb(size_decimals)
    integral = scaled.to_integral_value(rounding=ROUND_DOWN)
    if exact and scaled != integral:
        raise ValueError(f"size {size} is not a multiple of the step {unit(size_decimals)}")
    v = int(integral)
    if not 1 <= v <= MAX_BASE_AMOUNT:
        raise ValueError(f"size {size} out of range (base_amount {v})")
    return v


def price_to_int(price: Decimal, price_decimals: int, *, rounding: str | None = None) -> int:
    """Price -> signer integer. Without ``rounding`` the price must already be on the tick."""
    if not isinstance(price, Decimal):
        raise ValueError("price must be a Decimal")
    scaled = price.scaleb(price_decimals)
    if rounding is None:
        integral = scaled.to_integral_value(rounding=ROUND_HALF_EVEN)
        if scaled != integral:
            raise ValueError(f"price {price} is not a multiple of the tick {unit(price_decimals)}")
    else:
        integral = scaled.to_integral_value(rounding=rounding)
    v = int(integral)
    if not 1 <= v <= MAX_PRICE_INT:
        raise ValueError(f"price {price} out of signer range (int {v})")
    return v


def worst_price_to_int(price: Decimal, side: Side, price_decimals: int) -> int:
    """Taker worst price rounded CONSERVATIVELY: BUY never above ``price``, SELL never below it."""
    return price_to_int(price, price_decimals, rounding=ROUND_FLOOR if side is Side.BUY else ROUND_CEILING)


def int_to_size(n: int, size_decimals: int) -> Decimal:
    return Decimal(int(n)).scaleb(-int(size_decimals))


def int_to_price(n: int, price_decimals: int) -> Decimal:
    return Decimal(int(n)).scaleb(-int(price_decimals))


# --------------------------------------------------------------------------- markets
def market_category(d: dict) -> MarketCategory:
    """RWA if Lighter flags trading hours, uses the RWA funding multiplier (50 = 1/2) or is a known equity."""
    cfg = d.get("market_config") or {}
    if (cfg.get("trading_hours") or "").strip():
        return MarketCategory.RWA
    try:
        if int(d.get("funding_premium_multiplier") or 0) == 50:
            return MarketCategory.RWA
    except (TypeError, ValueError):
        pass
    if str(d.get("symbol", "")).upper() in _EXTRA_RWA_SYMBOLS:
        return MarketCategory.RWA
    return MarketCategory.CRYPTO


def parse_market(d: dict) -> MarketInfo | None:
    """One ``orderBookDetails.order_book_details`` entry -> MarketInfo (None for non-perp / malformed)."""
    try:
        if str(d.get("market_type", "perp")) != "perp":
            return None
        symbol = str(d["symbol"])
        mid = int(d["market_id"])
        sd = int(d["size_decimals"])
        pd = int(d["price_decimals"])
    except (KeyError, TypeError, ValueError):
        return None
    cfg = d.get("market_config") or {}
    force_ro = bool(cfg.get("force_reduce_only"))
    frozen = bool(d.get("is_frozen"))
    status = str(d.get("status", ""))
    min_imf = int(d.get("min_initial_margin_fraction") or 0)
    max_lev = (TEN_THOUSAND / Decimal(min_imf)) if min_imf > 0 else None
    extra = {
        "status": status,
        "force_reduce_only": force_ro,
        "isolated_only": cfg.get("market_margin_mode") == 1,
        "min_imf": min_imf,
        "default_imf": int(d.get("default_initial_margin_fraction") or 0),
        "mmf": int(d.get("maintenance_margin_fraction") or 0),
        "order_quote_limit": dec(d.get("order_quote_limit")),
        "funding_premium_multiplier": d.get("funding_premium_multiplier"),
        "base_interest_rate": d.get("base_interest_rate"),
        "quote_multiplier": d.get("quote_multiplier"),
        "raw": d,
    }
    return MarketInfo(
        venue=VENUE,
        symbol=symbol,
        venue_symbol=symbol,
        market_id=mid,
        tick_size=unit(pd),
        step_size=unit(sd),
        min_size=dec(d.get("min_base_amount"), ZERO),
        min_notional=dec(d.get("min_quote_amount"), ZERO),
        maker_fee=ZERO,  # Standard account: 0 % maker / 0 % taker (metadata fee fields are not per-account)
        taker_fee=ZERO,
        category=market_category(d),
        max_leverage=max_lev,
        active=status == "active" and not force_ro and not frozen,
        price_decimals=pd,
        size_decimals=sd,
        extra=extra,
    )


def parse_markets(payload: dict) -> dict[str, MarketInfo]:
    """Full ``orderBookDetails`` response -> {symbol: MarketInfo}. Duplicate symbols keep the active one."""
    out: dict[str, MarketInfo] = {}
    for d in payload.get("order_book_details") or []:
        m = parse_market(d)
        if m is None:
            continue
        prev = out.get(m.symbol)
        if prev is not None and prev.active and not m.active:
            continue
        out[m.symbol] = m
    return out


def check_market_invariants(m: MarketInfo) -> list[str]:
    """Sanity checks the integer encoding relies on (all hold live on every perp)."""
    problems = []
    if m.size_decimals is None or m.price_decimals is None:
        problems.append("missing decimals")
    elif m.size_decimals + m.price_decimals != 6:
        problems.append(f"size_decimals+price_decimals={m.size_decimals + m.price_decimals} != 6")
    if m.extra.get("quote_multiplier") not in (None, 1):
        problems.append(f"quote_multiplier={m.extra.get('quote_multiplier')}")
    return problems


# --------------------------------------------------------------------------- funding
def next_funding_ts(funding_timestamp_ms: int | None, now: float | None = None) -> float:
    """Next hourly settlement (POSIX s) from the LAST funding timestamp (ms), never in the past."""
    now = time.time() if now is None else now
    nxt_now = (int(now) // 3600 + 1) * 3600.0
    if not funding_timestamp_ms:
        return nxt_now
    nxt = float((int(funding_timestamp_ms) // HOUR_MS + 1) * 3600)
    return nxt if nxt > now else nxt_now


def funding_from_market_stats(ms: dict, symbol: str, *, ts: float | None = None, now: float | None = None) -> FundingInfo | None:
    """WS ``market_stats`` object -> FundingInfo (``current_funding_rate`` is PERCENT per hour)."""
    rate_pct = dec(ms.get("current_funding_rate"))
    if rate_pct is None:
        return None
    return FundingInfo(
        venue=VENUE,
        symbol=symbol,
        rate_hourly=rate_pct / 100,
        next_funding_ts=next_funding_ts(ms.get("funding_timestamp"), now),
        is_predicted=True,
        mark_price=dec(ms.get("mark_price")),
        index_price=dec(ms.get("index_price")),
        ts=ts if ts is not None else time.time(),
    )


_RATE_Q = Decimal("1e-10")


def hourly_from_funding_rates(rate_8h: Any) -> Decimal | None:
    """``/api/v1/funding-rates`` rate (8 h-equivalent FRACTION, float) -> fraction per hour."""
    r = dec(rate_8h)
    if r is None:
        return None
    return (r / 8).quantize(_RATE_Q, rounding=ROUND_HALF_EVEN)


def parse_funding_payment(f: dict, symbol: str) -> FundingPayment | None:
    """WS ``funding_histories`` / REST ``positionFunding`` row (signed fraction rate, signed USDC change)."""
    amount = dec(f.get("change"))
    if amount is None:
        return None
    size = dec(f.get("position_size"))
    if size is not None and str(f.get("position_side", "")).lower() == "short":
        size = -size
    try:
        ts = float(f.get("timestamp") or 0)
    except (TypeError, ValueError):
        ts = 0.0
    if ts > 1e11:  # defensive: ms
        ts /= 1000.0
    return FundingPayment(venue=VENUE, symbol=symbol, amount=amount, rate=dec(f.get("rate")), position_size=size, ts=ts)


# --------------------------------------------------------------------------- positions / balance
def signed_position(p: dict) -> Decimal:
    """``position`` is UNSIGNED; direction is ``sign`` (1 long, -1 short; 1 when flat)."""
    size = dec(p.get("position"), ZERO) or ZERO
    try:
        sign = int(p.get("sign", 1))
    except (TypeError, ValueError):
        sign = 1
    return -size if sign < 0 else size


def parse_position(p: dict, symbol: str, *, ts: float | None = None) -> Position:
    size = signed_position(p)
    entry = dec(p.get("avg_entry_price"))
    value = dec(p.get("position_value"))
    mark = (abs(value) / abs(size)) if (value is not None and size != 0) else None
    liq = dec(p.get("liquidation_price"))
    imf_pct = dec(p.get("initial_margin_fraction"))
    lev = (Decimal(100) / imf_pct) if imf_pct else None
    return Position(
        venue=VENUE,
        symbol=symbol,
        size=size,
        entry_price=entry if entry else None,
        mark_price=mark,
        unrealized_pnl=dec(p.get("unrealized_pnl")),
        liquidation_price=liq if liq else None,
        leverage=lev,
        ts=ts if ts is not None else time.time(),
    )


def position_imf(p: dict) -> int | None:
    """Current initial margin fraction in 1/10_000 from a Position (percent string, "5.00" -> 500)."""
    v = dec(p.get("initial_margin_fraction"))
    return int((v * 100).to_integral_value()) if v is not None else None


def leverage_to_fraction(leverage: int | Decimal, min_imf: int = 0) -> int:
    """Leverage -> UpdateLeverage ``fraction`` (1/10_000), never below the market minimum IMF."""
    lev = Decimal(leverage)
    if lev <= 0:
        raise ValueError("leverage must be positive")
    frac = int((TEN_THOUSAND / lev).to_integral_value(rounding=ROUND_DOWN))
    frac = max(frac, int(min_imf or 0), 1)
    return min(frac, 10_000)


# --------------------------------------------------------------------------- trades / orders
def trade_id(t: dict) -> str:
    return str(t.get("trade_id_str") or t.get("trade_id"))


def trade_side_for(t: dict, account_index: int) -> tuple[Side, int, int] | None:
    """(our side, our order_index, our client_order_index) if ``account_index`` is a party, else None."""
    try:
        if int(t.get("bid_account_id", -1)) == account_index:
            return Side.BUY, int(t.get("bid_id") or 0), int(t.get("bid_client_id") or 0)
        if int(t.get("ask_account_id", -1)) == account_index:
            return Side.SELL, int(t.get("ask_id") or 0), int(t.get("ask_client_id") or 0)
    except (TypeError, ValueError):
        return None
    return None


def trade_to_fill(
    t: dict,
    account_index: int,
    symbol: str,
    client_id_for: Callable[[int], str | None] | None = None,
) -> Fill | None:
    """Trade JSON -> Fill from OUR perspective (None if we are not a party).

    Matching is by account + order/client index, never by ``tx_hash`` (multi-maker sweeps put the 2nd+ fills
    in type-21 child txs with synthetic hashes).
    """
    who = trade_side_for(t, account_index)
    if who is None:
        return None
    side, oidx, coi = who
    price = dec(t.get("price"))
    size = dec(t.get("size"))
    if price is None or size is None or size <= 0:
        return None
    is_maker_ask = bool(t.get("is_maker_ask"))
    is_maker = is_maker_ask if side is Side.SELL else not is_maker_ask
    fee_ppm = dec(t.get("maker_fee" if is_maker else "taker_fee"), ZERO) or ZERO
    notional = dec(t.get("usd_amount")) or price * size
    fee = notional * fee_ppm / FEE_PPM if fee_ppm else ZERO
    cid = client_id_for(coi) if (client_id_for is not None and coi) else None
    if cid is None and coi:
        cid = str(coi)
    ts_ms = t.get("timestamp")
    try:
        ts = float(ts_ms) / 1000.0 if ts_ms else time.time()
    except (TypeError, ValueError):
        ts = time.time()
    return Fill(
        venue=VENUE,
        symbol=symbol,
        side=side,
        price=price,
        size=size,
        fee=fee,
        is_maker=is_maker,
        order_id=str(oidx) if oidx else None,
        client_id=cid,
        trade_id=trade_id(t),
        ts=ts,
    )


ACTIVE_ORDER_STATUSES = frozenset({"open", "pending", "in-progress"})


def map_order_status(status: str, filled: Decimal = ZERO) -> OrderStatus:
    """Lighter Order ``status`` string -> OrderStatus."""
    s = (status or "").lower()
    if s == "open" or s == "in-progress":
        return OrderStatus.PARTIALLY_FILLED if filled > 0 else (OrderStatus.OPEN if s == "open" else OrderStatus.PENDING)
    if s == "pending":
        return OrderStatus.PENDING
    if s == "filled":
        return OrderStatus.FILLED
    if s == "canceled-expired":
        return OrderStatus.EXPIRED
    if s.startswith("canceled"):
        return OrderStatus.CANCELED
    return OrderStatus.UNKNOWN


# tx event_info order status codes (``st``) -> Order status strings
EVENT_STATUS = {
    0: "in-progress",
    1: "pending",
    2: "open",
    3: "filled",
    4: "canceled",
    5: "canceled-post-only",
    6: "canceled-reduce-only",
    7: "canceled-position-not-allowed",
    8: "canceled-margin-not-allowed",
    9: "canceled-too-much-slippage",
    10: "canceled-not-enough-liquidity",
    11: "canceled-self-trade",
    12: "canceled-expired",
    13: "canceled-oco",
    14: "canceled-child",
    15: "canceled-liquidation",
    16: "canceled-invalid-balance",
}


def parse_event_info(tx: dict) -> dict:
    ev = tx.get("event_info")
    if isinstance(ev, dict):
        return ev
    if isinstance(ev, str) and ev:
        try:
            v = json.loads(ev)
            return v if isinstance(v, dict) else {}
        except ValueError:
            return {}
    return {}


def order_json_side(o: dict) -> Side:
    return Side.SELL if bool(o.get("is_ask")) else Side.BUY


def order_from_json(o: dict, symbol: str, client_id: str | None = None) -> Order:
    """Order JSON (WS account_all_orders / REST accountActiveOrders) -> Order."""
    filled = dec(o.get("filled_base_amount"), ZERO) or ZERO
    fq = dec(o.get("filled_quote_amount"), ZERO) or ZERO
    coi = int(o.get("client_order_index") or 0)
    tif = str(o.get("time_in_force", ""))
    status_s = str(o.get("status", ""))
    ts = o.get("timestamp") or o.get("updated_at")
    order = Order(
        venue=VENUE,
        symbol=symbol,
        side=order_json_side(o),
        price=dec(o.get("price")),
        size=dec(o.get("initial_base_amount"), ZERO) or ZERO,
        client_id=client_id or (str(coi) if coi else str(o.get("order_index"))),
        order_id=str(o.get("order_index")) if o.get("order_index") is not None else None,
        filled=filled,
        avg_fill_price=(fq / filled) if filled > 0 else None,
        status=map_order_status(status_s, filled),
        post_only=tif == "post-only",
        reduce_only=bool(o.get("reduce_only")),
        ioc=tif == "immediate-or-cancel",
        error=status_s if status_s.startswith("canceled") and status_s != "canceled" else None,
    )
    if ts:
        try:
            order.updated_ts = float(ts)
        except (TypeError, ValueError):
            pass
    return order


def readonly_url(url: str) -> str:
    if "readonly=" in url:
        return url
    return url + ("&" if "?" in url else "?") + "readonly=true"

