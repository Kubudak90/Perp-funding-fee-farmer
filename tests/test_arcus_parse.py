"""Pure Arcus payload converters, driven by REAL captured payloads."""

from decimal import Decimal as D

from farmer.exchanges.arcus.parse import (
    classify_order_frame,
    fill_from_row,
    funding_info,
    funding_payment_from_row,
    is_market_open,
    map_order_status,
    market_fees,
    order_from_row,
    parse_market,
    position_from_row,
    update_market,
)
from farmer.models import MarketCategory, OrderStatus, Side
from tests.test_arcus_support import load


def rows() -> dict[str, dict]:
    return {m["marketDisplayName"]: m for m in load("markets.json")["markets"]}


def test_parse_btc_market():
    m = parse_market(rows()["BTC-USD"])
    assert (m.symbol, m.venue_symbol, m.market_id) == ("BTC", "BTC-USD", 1)
    assert m.tick_size == D("0.1") and m.step_size == D("0.00000001")
    assert m.min_size == D("0.0001") and m.min_notional == D("5") and m.max_size == D("10000")
    assert m.maker_fee == 0 and m.taker_fee == D("0.000225")
    assert m.category is MarketCategory.CRYPTO and m.active and is_market_open(m)
    assert m.max_leverage == D("40") and str(m.max_leverage) == "40"
    assert m.extra["imf"] == D("0.025") and m.extra["mmf"] == D("0.016667")
    assert m.extra["is_outside_rth"] is False and m.extra["next_funding_at"] > 1_700_000_000
    assert m.extra["raw"]["marketDisplayName"] == "BTC-USD"
    # tiers: upToPrice is EXCLUSIVE
    assert m.tick_for(D("499999.9")) == D("0.1")
    assert m.tick_for(D("500000")) == D("0.2")
    assert m.tick_for(D("20000000")) == D("5")


def test_symbols_categories_and_hours():
    r = rows()
    kbonk = parse_market(r["KBONK-USD"])
    assert kbonk.symbol == "KBONK" and kbonk.venue_symbol == "KBONK-USD"
    aapl = parse_market(r["AAPL-USD"])
    assert aapl.category is MarketCategory.RWA and aapl.extra["regular_trading_hours"]
    raw = dict(r["AAPL-USD"])
    raw["isOutsideRth"] = True
    assert not is_market_open(parse_market(raw))
    raw["isOutsideRth"] = False
    assert is_market_open(parse_market(raw))
    raw["isOutsideRth"] = None  # briefly null after deploys -> treat as closed
    assert not is_market_open(parse_market(raw))
    f = parse_market(r["F-USD"])
    assert f.extra["status"] == "OFFLINE" and not f.active and not is_market_open(f)
    assert parse_market(r["SPY-USD"]).max_leverage == D("50")


def test_fee_tiers_and_discount():
    btc = rows()["BTC-USD"]
    assert market_fees(btc) == (D(0), D("0.000225"))
    assert market_fees(btc, fee_level=5, discount_ppm=50000) == (D("-0.00002"), D("0.000095"))
    assert market_fees(btc, fee_level=0, discount_ppm=50000) == (D(0), D("0.000213"))  # 213.75 truncated


def test_update_market_in_place():
    raw = dict(rows()["SOL-USD"])
    m = parse_market(raw)
    ident = id(m)
    raw["initialMarginFraction"] = "0.1"
    raw["minOrderSize"] = "0.1"
    raw["status"] = "OFFLINE"
    update_market(m, raw)
    assert id(m) == ident and m.max_leverage == D("10") and m.min_size == D("0.1") and not m.active


def test_funding_info_uses_predicted_rate():
    raw = dict(rows()["BTC-USD"])
    fi = funding_info(raw)
    assert fi.symbol == "BTC" and fi.rate_hourly == D(raw["nextFundingRate"]) and fi.is_predicted
    assert fi.next_funding_ts == float(raw["nextFundingAt"])
    assert fi.mark_price == D(raw["markPrice"]) and fi.index_price == D(raw["oraclePrice"])
    raw["markPrice"] = "0"
    assert funding_info(raw).mark_price is None  # "0" = no mark yet, never fall back to oracle
    assert funding_info(raw, mark=D("1"), oracle=D("2")).index_price == D("2")


def test_classify_real_order_frames():
    fr = {k: v["contents"] for k, v in load("orders_frames.json").items()}
    assert classify_order_frame(fr["open"]) == "update"
    assert classify_order_frame(fr["mod_leg"]) == "modify_leg"
    assert classify_order_frame(fr["mod_open"], in_modify=True) == "update"
    for k in ("reject_not_found", "reject_not_found_modify", "reject_superseded"):
        assert classify_order_frame(fr[k]) == "action_reject", k
        assert classify_order_frame(fr[k], in_modify=True) == "action_reject", k  # placeholder frames
    assert classify_order_frame(fr["xmod_leg"]) == "modify_leg"
    assert classify_order_frame(fr["xmod_reject"], in_modify=True) == "update"
    assert classify_order_frame(fr["would_cross"]) == "update"
    assert classify_order_frame(fr["ioc_canceled"]) == "update"


def test_map_status_real_frames():
    fr = {k: v["contents"] for k, v in load("orders_frames.json").items()}
    assert map_order_status(fr["open"]) == (OrderStatus.OPEN, None)
    assert map_order_status(fr["partial"])[0] is OrderStatus.PARTIALLY_FILLED  # status OPEN, state PARTIALLY_FILLED
    assert map_order_status(fr["filled"])[0] is OrderStatus.FILLED
    assert map_order_status(fr["canceled"])[0] is OrderStatus.CANCELED
    st, err = map_order_status(fr["would_cross"])
    assert st is OrderStatus.REJECTED and "would cross" in err
    st, err = map_order_status(fr["ioc_canceled"])
    assert st is OrderStatus.CANCELED and err == "IOC_CANCELED"  # IOC with zero fills
    ioc_partial = dict(fr["ioc_canceled"], state="PARTIALLY_FILLED", status="CANCELED", filledSize="0.1")
    assert map_order_status(ioc_partial)[0] is OrderStatus.CANCELED  # terminal for IOC
    expired = dict(fr["open"], status="REJECTED", state="REJECTED", rejectionReason="EXPIRED")
    assert map_order_status(expired)[0] is OrderStatus.EXPIRED


def test_order_from_row_real():
    c = load("orders_frames.json")["partial"]["contents"]
    o = order_from_row(c, "CPER")
    assert o.post_only and not o.ioc and o.side is Side(c["side"])
    assert o.filled == D(c["filledSize"]) and o.size == D(c["originalSize"]) and o.remaining == D(c["remainingSize"])
    assert o.avg_fill_price == D(c["avgFillPrice"]) and o.order_id == c["orderId"] and o.client_id == c["clientId"]
    ioc = order_from_row(load("orders_frames.json")["ioc_canceled"]["contents"], "BTC")
    assert ioc.ioc and ioc.reduce_only and ioc.status is OrderStatus.CANCELED


def test_fill_from_row_real():
    fills = load("userfills_frames.json")
    f = fills["updates"][0]["contents"]["fills"][0]
    fill = fill_from_row(f, "X")
    assert fill.size == D(f["size"]) and fill.price == D(f["price"]) and fill.fee == D(f["fee"])
    assert fill.is_maker is (f["role"] == "MAKER") and fill.trade_id == f["tradeId"] and fill.order_id == f["orderId"]
    assert abs(fill.ts - f["createdAt"] / 1e6) < 1e-6
    taker = fill_from_row({**f, "role": "TAKER", "fee": "0.5"}, "X")
    assert taker.is_maker is False and taker.fee == D("0.5")


def test_positions_and_funding_rows_real():
    pos = load("rest_positions.json")["positions"]
    for row in pos.values():
        p = position_from_row(row, row["marketDisplayName"].split("-")[0])
        assert p is not None and p.size == D(row["size"])
        assert (p.size > 0) == (row["side"] == "LONG")
        assert p.unrealized_pnl == D(row["unrealizedPnl"])
    flat = dict(next(iter(pos.values())), side="FLAT", size="0")
    assert position_from_row(flat, "X") is None
    for r in load("rest_funding.json")["fundingPayments"]:
        fp = funding_payment_from_row(r, "X")
        assert fp.amount == D(r["payment"]) and fp.ts == r["time"] / 1e6
        # sign: long pays positive rate, short receives it
        if D(r["fundingRate"]) > 0:
            assert (fp.amount < 0) == (D(r["size"]) > 0)
