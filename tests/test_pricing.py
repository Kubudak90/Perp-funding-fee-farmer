from decimal import Decimal as D

from farmer.models import BookLevel, OrderBook, Side
from farmer.strategy.pricing import (
    compute_maker_quote,
    edge_limit_price,
    expected_edge_bps,
    pair_min_size,
    pair_step,
    quote_violates_edge,
    round_pair_size,
    worst_taker_price,
)
from tests.fakes import make_market

AM = make_market("arcus", "BTC", tick="0.1", step="0.0001", min_size="0.0001", min_notional="5")
LM = make_market("lighter", "BTC", tick="0.1", step="0.00001", min_size="0.0002", min_notional="10")


def book(venue, bids, asks):
    return OrderBook(venue, "BTC", bids=[BookLevel(D(str(p)), D(str(s))) for p, s in bids],
                     asks=[BookLevel(D(str(p)), D(str(s))) for p, s in asks])


def test_pair_sizes():
    assert pair_step(AM, LM) == D("0.0001")
    # min notional 10 at 100000 -> 0.0001, min size lighter 0.0002 -> 0.0002
    assert pair_min_size(AM, LM, D("100000")) == D("0.0002")
    assert pair_min_size(AM, LM, D("20000")) == D("0.0005")
    assert round_pair_size(D("0.012345"), AM, LM) == D("0.0123")


def test_edge_limit_zero_fee():
    # maker buy, hedge sells at 100000, 1bps edge -> 99990
    assert edge_limit_price(Side.BUY, D(100000), D(100000), D(1), D(0), D(0)) == D(99990)
    assert edge_limit_price(Side.SELL, D(100000), D(100000), D(1), D(0), D(0)) == D(100010)


def test_quote_buy_capped_by_edge():
    mb = book("arcus", [(99980, 1)], [(100020, 1)])
    tb = book("lighter", [(100000, 1)], [(100001, 1)])
    q = compute_maker_quote(side=Side.BUY, size=D("0.01"), maker_market=AM, taker_market=LM, maker_book=mb,
                            taker_book=tb, min_edge_bps=D("0.5"))
    # limit = 100000 - 0.5bps*100000.5 = 99994.99997.. -> floor tick 99994.9
    assert q.price == D("99994.9")
    assert q.price < mb.best_ask
    assert q.expected_edge_bps >= D("0.5")


def test_quote_buy_capped_by_post_only():
    # maker venue is far below taker venue -> edge allows crossing, post-only caps at ask - tick
    mb = book("arcus", [(99900, 1)], [(99910, 1)])
    tb = book("lighter", [(100000, 1)], [(100001, 1)])
    q = compute_maker_quote(side=Side.BUY, size=D("0.01"), maker_market=AM, taker_market=LM, maker_book=mb,
                            taker_book=tb, min_edge_bps=D("0.5"))
    assert q.price == D("99909.9")


def test_quote_sell_symmetric_and_improve_cap():
    mb = book("arcus", [(99990, 1)], [(100030, 1)])
    tb = book("lighter", [(99999, 1)], [(100000, 1)])
    q = compute_maker_quote(side=Side.SELL, size=D("0.01"), maker_market=AM, taker_market=LM, maker_book=mb,
                            taker_book=tb, min_edge_bps=D("0"), max_improve_ticks=2)
    # edge limit 100000 -> but improve cap: ask - 2 ticks = 100029.8
    assert q.price == D("100029.8")
    assert q.expected_edge_bps > 0


def test_quote_none_on_thin_taker_book():
    mb = book("arcus", [(99990, 1)], [(100030, 1)])
    tb = book("lighter", [(99999, D("0.001"))], [(100000, D("0.001"))])
    assert compute_maker_quote(side=Side.BUY, size=D("0.01"), maker_market=AM, taker_market=LM, maker_book=mb,
                               taker_book=tb, min_edge_bps=D(0)) is None


def test_fees_reduce_limit():
    lim0 = edge_limit_price(Side.BUY, D(100000), D(100000), D(0), D(0), D(0))
    lim1 = edge_limit_price(Side.BUY, D(100000), D(100000), D(0), D(0), D("0.000225"))
    assert lim1 < lim0
    assert expected_edge_bps(Side.BUY, lim1, D(100000), D(100000), D(0), D("0.000225")).quantize(D("0.0001")) == 0


def test_violation_and_worst_price():
    assert quote_violates_edge(Side.BUY, D(100), D("99.9"))
    assert not quote_violates_edge(Side.SELL, D(101), D("100.9"))
    tb = book("lighter", [(100000, D("0.005")), (99990, 1)], [(100001, 1)])
    w = worst_taker_price(Side.SELL, tb, D("0.01"), D("10"), LM)
    assert w == D("99890.0") or w == D("99890")
    w2 = worst_taker_price(Side.BUY, tb, D("0.01"), D("10"), LM)
    assert w2 == D("100101.1")


def test_basis_shifts_limit_both_sides():
    # Arcus trades 10 bps below Lighter structurally
    mb = book("arcus", [(99890, 1)], [(99910, 1)])
    tb = book("lighter", [(99999, 1)], [(100001, 1)])
    basis = D("99900") / D("100000") - 1
    sell = compute_maker_quote(side=Side.SELL, size=D("0.01"), maker_market=AM, taker_market=LM, maker_book=mb,
                               taker_book=tb, min_edge_bps=D("0.5"), basis=basis)
    buy = compute_maker_quote(side=Side.BUY, size=D("0.01"), maker_market=AM, taker_market=LM, maker_book=mb,
                              taker_book=tb, min_edge_bps=D("0.5"), basis=basis)
    # without basis a maker SELL would need >= 100001 (unfillable at 99910); with basis it's near 99906
    assert sell.price < D("99911") and buy.price > D("99889")
    assert sell.price > buy.price


def test_buffer_moves_quote_behind_limit():
    mb = book("arcus", [(99980, 1)], [(100020, 1)])
    tb = book("lighter", [(100000, 1)], [(100001, 1)])
    q0 = compute_maker_quote(side=Side.BUY, size=D("0.01"), maker_market=AM, taker_market=LM, maker_book=mb,
                             taker_book=tb, min_edge_bps=D("0.5"))
    q1 = compute_maker_quote(side=Side.BUY, size=D("0.01"), maker_market=AM, taker_market=LM, maker_book=mb,
                             taker_book=tb, min_edge_bps=D("0.5"), buffer_bps=D("1"))
    assert q1.price < q0.price and q1.limit_price == q0.limit_price


def test_basis_tracker():
    from farmer.strategy.pricing import BasisTracker
    t = [0.0]
    bt = BasisTracker(halflife_s=10, clock=lambda: t[0])
    bt.update(D("99900"), D("100000"))
    assert bt.value == D("-0.001")
    assert not bt.warmed_up(5)
    t[0] = 10
    bt.update(D("100000"), D("100000"))
    assert abs(bt.value - D("-0.0005")) < D("0.000001")
    assert bt.warmed_up(5)
    assert bt.deviation_bps() > 0
