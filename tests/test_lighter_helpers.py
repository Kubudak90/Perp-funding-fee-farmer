"""Lighter pure helpers, validated against captured live payloads (tests/data/lighter)."""

import json
from decimal import Decimal as D
from pathlib import Path

import pytest

from farmer.exchanges.lighter import helpers as h
from farmer.models import MarketCategory, OrderStatus, Side

DATA = Path(__file__).parent / "data" / "lighter"


def load(name):
    return json.loads((DATA / name).read_text())


@pytest.fixture(scope="module")
def markets():
    return h.parse_markets(load("order_book_details.json"))


# --------------------------------------------------------------------------- markets
def test_parse_markets_core_fields(markets):
    eth, btc = markets["ETH"], markets["BTC"]
    assert (eth.market_id, eth.size_decimals, eth.price_decimals) == (0, 4, 2)
    assert eth.tick_size == D("0.01") and eth.step_size == D("0.0001")
    assert eth.min_size == D("0.0020") and eth.min_notional == D("10")
    assert eth.max_leverage == D(50)  # min IMF 200 / 10000
    assert eth.maker_fee == 0 and eth.taker_fee == 0
    assert eth.venue == "lighter" and eth.venue_symbol == "ETH" and eth.active
    assert (btc.market_id, btc.tick_size, btc.step_size, btc.min_size) == (1, D("0.1"), D("0.00001"), D("0.00007"))
    assert eth.extra["mmf"] == 120 and eth.extra["min_imf"] == 200 and eth.extra["raw"]["symbol"] == "ETH"


def test_parse_markets_symbols_and_flags(markets):
    bonk = markets["1000BONK"]
    assert bonk.step_size == D("1") and bonk.tick_size == D("0.000001")
    assert markets["GME"].active is False  # force_reduce_only
    assert markets["GME"].extra["force_reduce_only"] is True
    inactive = [m for m in markets.values() if m.extra["status"] != "active"]
    assert inactive and all(not m.active for m in inactive)
    assert markets["MKR"].min_notional == 0


def test_market_category(markets):
    for s in ("NVDA", "SPY", "GME", "USDJPY", "HYUNDAI"):
        assert markets[s].category is MarketCategory.RWA, s
    for s in ("ETH", "BTC", "SOL", "DOGE", "1000BONK", "OPENAI"):
        assert markets[s].category is MarketCategory.CRYPTO, s


def test_encoding_invariant_holds_on_capture(markets):
    for m in markets.values():
        assert h.check_market_invariants(m) == [], m.symbol


def test_non_perp_and_malformed_rows_ignored():
    assert h.parse_market({"market_type": "spot", "symbol": "ETH", "market_id": 2048, "size_decimals": 4, "price_decimals": 2}) is None
    assert h.parse_market({"symbol": "X"}) is None


# --------------------------------------------------------------------------- integer encoding
def test_size_and_price_to_int():
    assert h.size_to_int(D("0.05"), 4) == 500
    assert h.size_to_int(D("0.0500"), 4) == 500
    assert h.price_to_int(D("2400.37"), 2) == 240037
    assert h.price_to_int(D("82015.3"), 1) == 820153
    with pytest.raises(ValueError):
        h.size_to_int(D("0.00005"), 4)  # not on step
    with pytest.raises(ValueError):
        h.price_to_int(D("2400.375"), 2)  # not on tick
    with pytest.raises(ValueError):
        h.size_to_int(D("0"), 4)
    with pytest.raises(ValueError):
        h.size_to_int(D(2**48), 0)  # above uint48
    with pytest.raises(ValueError):
        h.price_to_int(D(2**31), 0)  # would wrap through the C int
    with pytest.raises(ValueError):
        h.price_to_int(2400.37, 2)  # floats are refused


def test_worst_price_rounding_is_conservative():
    # BUY never above the worst price, SELL never below
    assert h.worst_price_to_int(D("2400.379"), Side.BUY, 2) == 240037
    assert h.worst_price_to_int(D("2400.371"), Side.SELL, 2) == 240038
    assert h.worst_price_to_int(D("2400.37"), Side.SELL, 2) == 240037
    assert h.int_to_price(240037, 2) == D("2400.37")
    assert h.int_to_size(500, 4) == D("0.05")


def test_leverage_fraction():
    assert h.leverage_to_fraction(5) == 2000
    assert h.leverage_to_fraction(3) == 3333
    assert h.leverage_to_fraction(100, min_imf=200) == 200  # capped at market max (50x)
    assert h.leverage_to_fraction(1) == 10_000
    with pytest.raises(ValueError):
        h.leverage_to_fraction(0)


# --------------------------------------------------------------------------- errors
def test_parse_error_code_and_ack():
    assert h.parse_error_code("HTTP response body: code=21104 message='invalid nonce' additional_properties={}") == 21104
    assert h.parse_error_code('{"code":20558,"message":"restricted"}') == 20558
    assert h.parse_error_code("nothing here") is None
    assert h.parse_ack({"code": 200, "tx_hash": "ab"}) == (True, 200, "")
    assert h.parse_ack({"code": 21104, "message": "invalid nonce"}) == (False, 21104, "invalid nonce")
    assert h.parse_ack({"error": {"code": 21701, "message": "invalid base amount"}, "id": "x"}) == (False, 21701, "invalid base amount")
    assert h.parse_ack("garbage")[0] is False
    assert h.parse_ack({"tx_hash": "ab"})[0] is True  # WS success frame without code (shape unverified)


def test_error_classification():
    assert h.is_retryable(23000) and h.is_retryable(29500) and h.is_retryable(-502)
    assert not h.is_retryable(21701) and not h.is_retryable(20558)
    e = h.TxRejected(20558, "restricted")
    assert e.fatal and not e.retryable and e.code == 20558
    assert h.TxRejected(23000, "rate").retryable


# --------------------------------------------------------------------------- funding
def test_funding_from_market_stats_capture():
    snap, _ = load("ws_market_stats_all.json")
    eth = snap["market_stats"]["0"]
    fi = h.funding_from_market_stats(eth, "ETH", now=eth["funding_timestamp"] / 1000 + 10)
    assert fi.rate_hourly == D(eth["current_funding_rate"]) / 100  # percent per hour -> fraction
    assert fi.rate_hourly == D("0.000012")
    assert fi.next_funding_ts == (eth["funding_timestamp"] // 3_600_000 + 1) * 3600
    assert fi.mark_price == D(eth["mark_price"]) and fi.index_price == D(eth["index_price"])
    assert fi.is_predicted and fi.venue == "lighter"
    h100 = snap["market_stats"]["182"]
    assert h.funding_from_market_stats(h100, "H100").rate_hourly == D(h100["current_funding_rate"]) / 100


def test_next_funding_ts_never_in_past():
    assert h.next_funding_ts(1792429200001, now=1792429300) == 1792432800
    assert h.next_funding_ts(1792429200001, now=1792440000) == 1792443600  # stale timestamp -> next hour from now
    assert h.next_funding_ts(None, now=7200.5) == 10800


def test_funding_rates_rest_is_8h_fraction():
    rows = {(r["exchange"], r["symbol"]): r["rate"] for r in load("rest_funding_rates.json")["funding_rates"]}
    assert h.hourly_from_funding_rates(rows[("lighter", "BTC")]) == D("0.000012")
    assert h.hourly_from_funding_rates(9.599999999999999e-05) == D("0.000012")
    assert h.hourly_from_funding_rates(rows[("lighter", "NVDA")]) == D("0.000004")


def test_funding_payment_rows():
    frames = load("ws_account_all_funding.json")
    mine = next(f for f in frames if f["channel"] == "account_all:313004")
    row = mine["funding_histories"]["0"][0]
    fp = h.parse_funding_payment(row, "ETH")
    assert fp.amount == D("-0.218616") and fp.rate == D("0.000012") and fp.position_size == D("7.5965")
    assert fp.ts == 1792432800
    short = h.parse_funding_payment({"change": "10.802313", "rate": "0.000012", "position_size": "375.3587",
                                     "position_side": "short", "timestamp": 1792432800}, "ETH")
    assert short.position_size == D("-375.3587") and short.amount > 0


# --------------------------------------------------------------------------- positions / fills / orders
def test_positions_signed():
    acct = load("rest_account_313004.json")["accounts"][0]
    eth = next(p for p in acct["positions"] if p["symbol"] == "ETH")
    pos = h.parse_position(eth, "ETH")
    assert pos.size == -D(eth["position"])  # sign -1
    assert pos.entry_price == D(eth["avg_entry_price"]) and pos.leverage == D(20)
    assert pos.mark_price == D(eth["position_value"]) / D(eth["position"])
    flat = next(p for p in acct["positions"] if p["symbol"] == "BTC")
    assert h.parse_position(flat, "BTC").size == 0 and h.parse_position(flat, "BTC").liquidation_price is None
    assert h.position_imf(eth) == 500


def test_trade_to_fill_taker_buy_and_sell():
    snap = load("ws_account_all_snapshot_313004.json")
    trades = snap["trades"]["0"]
    buy = h.trade_to_fill(trades[0], 313004, "ETH")
    assert buy.side is Side.BUY and buy.is_maker is False  # is_maker_ask True -> we (bid) are the taker
    assert buy.size == D("0.0044") and buy.price == D("2403.25") and buy.fee == 0  # Standard: fee keys omitted
    assert buy.client_id == "40132174441712" and buy.order_id == "562946773250577"
    assert buy.trade_id == "35439675593" and buy.ts == pytest.approx(1792430288.798)
    sell = h.trade_to_fill(trades[3], 313004, "ETH")
    assert sell.side is Side.SELL and sell.is_maker is False and sell.client_id == "40132174441700"
    assert h.trade_to_fill(trades[0], 999, "ETH") is None


def test_trade_fee_ppm_and_maker_side():
    t = load("ws_account_all_trades_llp.json")[1]["trades"]["129"][0]  # LLP is the maker ask, maker_fee 40 ppm
    f = h.trade_to_fill(t, 281474976710654, "QQQ")
    assert f.side is Side.SELL and f.is_maker is True
    assert f.fee == D(t["usd_amount"]) * 40 / 1_000_000
    t2 = load("ws_account_all_trades_llp.json")[0]["trades"]["98"][0]  # is_maker_ask False: LLP bid is the maker
    f2 = h.trade_to_fill(t2, 281474976710654, "USDJPY")
    assert f2.side is Side.BUY and f2.is_maker is True and f2.fee == D(t2["usd_amount"]) * 40 / 1_000_000
    f3 = h.trade_to_fill(t2, 2373, "USDJPY")  # the counterparty: ask taker paying 280 ppm
    assert f3.side is Side.SELL and f3.is_maker is False and f3.fee == D(t2["usd_amount"]) * 280 / 1_000_000


def test_sweep_trades_all_attributed_by_client_index():
    """Live multi-maker sweep: 1 fill in the parent tx + 5 in type-21 child txs, all for one order."""
    trades = load("sweep_trades.json")
    fills = [h.trade_to_fill(t, 110400, "ETH") for t in trades]
    assert all(f is not None and f.client_id == "152116289068" for f in fills)
    assert len({t["tx_hash"] for t in trades}) == 6  # different hashes, same order
    assert sum(f.size for f in fills) == D("19.8176")  # == BaseAmount 198176 at size_decimals 4


def test_order_status_mapping():
    m = h.map_order_status
    assert m("open") is OrderStatus.OPEN and m("open", D("0.1")) is OrderStatus.PARTIALLY_FILLED
    assert m("in-progress") is OrderStatus.PENDING and m("pending") is OrderStatus.PENDING
    assert m("filled") is OrderStatus.FILLED
    assert m("canceled") is OrderStatus.CANCELED and m("canceled-post-only") is OrderStatus.CANCELED
    assert m("canceled-too-much-slippage") is OrderStatus.CANCELED
    assert m("canceled-expired") is OrderStatus.EXPIRED
    assert m("???") is OrderStatus.UNKNOWN


def test_order_from_json_capture():
    frames = load("ws_account_all_orders_llp.json")
    partial = frames[2]["orders"]["178"][0]
    o = h.order_from_json(partial, "LITE")
    assert o.status is OrderStatus.PARTIALLY_FILLED and o.filled == D("0.1997") and o.post_only
    assert o.avg_fill_price == D("170.700719") / D("0.1997") and o.side is Side.BUY
    assert o.order_id == "50665495736785060" and o.client_id == "179241692415231"
    po = frames[3]["orders"]
    o2 = h.order_from_json(next(iter(po.values()))[0], "X")
    assert o2.status is OrderStatus.CANCELED and o2.error == "canceled-post-only"


def test_event_info_parsing():
    tx = load("rest_tx_limit_ioc.json")
    ev = h.parse_event_info(tx)
    assert ev["to"]["u"] == 40132174441500 and ev["to"]["st"] == 3 and ev["ae"] == ""
    assert h.EVENT_STATUS[ev["to"]["st"]] == "filled"
    parent = h.parse_event_info(load("rest_tx_sweep_parent.json"))
    assert parent["to"]["st"] == 0  # sweep still in progress after the first maker
    child = load("rest_tx_sweep_child.json")
    assert child["type"] == 21 and child["parent_hash"] == load("rest_tx_sweep_parent.json")["hash"]


def test_readonly_url():
    assert h.readonly_url("wss://x/stream") == "wss://x/stream?readonly=true"
    assert h.readonly_url("wss://x/stream?a=1") == "wss://x/stream?a=1&readonly=true"
    assert h.readonly_url("wss://x/stream?readonly=true") == "wss://x/stream?readonly=true"
