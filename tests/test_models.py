from decimal import Decimal as D

from farmer.models import BookLevel, MarketInfo, OrderBook, Side, TickTier
from farmer.utils.decimals import common_step, to_plain
from farmer.utils.ratelimit import SlidingWindowLimiter
from tests.fakes import FakeExchange, make_market


def test_tick_tiers_and_rounding():
    m = MarketInfo(venue="arcus", symbol="BTC", venue_symbol="BTC-USD", market_id=1, tick_size=D("0.1"),
                   step_size=D("0.0001"), tick_tiers=(TickTier(D("500000"), D("0.1")), TickTier(None, D("0.2"))))
    assert m.tick_for(D("100000")) == D("0.1")
    assert m.tick_for(D("600000")) == D("0.2")
    assert m.round_price(D("100000.17"), Side.BUY) == D("100000.1")
    assert m.round_price(D("100000.11"), Side.SELL) == D("100000.2")
    assert m.round_size(D("0.12345")) == D("0.1234")


def test_vwap():
    b = OrderBook("x", "BTC", bids=[BookLevel(D(100), D(1)), BookLevel(D(99), D(1))], asks=[BookLevel(D(101), D(1))])
    assert b.vwap(Side.SELL, D("1.5")) == (D("99.66666666666666666666666667"), D(99))
    assert b.vwap(Side.BUY, D(2))[0] is None
    assert b.mid == D("100.5")


def test_common_step():
    assert common_step(D("0.0001"), D("0.00001")) == D("0.0001")
    assert common_step(D("0.003"), D("0.002")) == D("0.006")
    assert to_plain(D("1E-8")) == "0.00000001"


async def test_limiter():
    t = [0.0]
    lim = SlidingWindowLimiter(3, 60, clock=lambda: t[0])
    assert lim.try_acquire() and lim.try_acquire() and lim.try_acquire()
    assert not lim.try_acquire()
    t[0] = 61
    assert lim.try_acquire()


async def test_fake_exchange_roundtrip():
    ex = FakeExchange("arcus", [make_market("arcus", "BTC")])
    ex.set_book("BTC", [(100, 1)], [(101, 1)])
    r = await ex.place_limit("BTC", Side.BUY, D("0.01"), D("101"))
    assert not r.ok and r.would_cross
    r = await ex.place_limit("BTC", Side.BUY, D("0.01"), D("100.5"))
    assert r.ok
    fills = []
    ex.add_fill_listener(fills.append)
    ex.fill_resting(r.order.client_id)
    assert ex.positions["BTC"] == D("0.01") and fills[0].is_maker
