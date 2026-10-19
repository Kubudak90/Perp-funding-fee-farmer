from decimal import Decimal as D

from farmer.models import FundingInfo
from farmer.strategy.funding import (
    MAKER_LONG,
    MAKER_SHORT,
    FundingMonitor,
    FundingSnapshot,
    choose_direction,
    next_hour_ts,
)
from tests.fakes import FakeExchange


def snap(rm, rt):
    return FundingSnapshot("BTC", FundingInfo("arcus", "BTC", D(rm)), FundingInfo("lighter", "BTC", D(rt)))


def test_carry_sign():
    s = snap("0.0001", "0.00002")  # maker venue pays longs more -> short maker
    assert s.best_direction == MAKER_SHORT
    assert s.hourly_carry(MAKER_SHORT) == D("0.00008")
    assert s.hourly_carry(MAKER_LONG) == D("-0.00008")
    s2 = snap("-0.0001", "0.0001")
    assert s2.best_direction == MAKER_LONG and s2.hourly_carry(MAKER_LONG) == D("0.0002")


def test_hysteresis():
    s = snap("0.000010", "0.000008")  # tiny edge for SHORT maker: 2e-6/h = 1.75% APR
    assert choose_direction(s, MAKER_LONG, D("0.05")) == MAKER_LONG  # not enough to flip (needs 5%)
    assert choose_direction(s, MAKER_LONG, D("0.01")) == MAKER_SHORT
    assert choose_direction(s, None, D("0.05")) == MAKER_SHORT
    assert choose_direction(None, None, D("0.05")) == MAKER_SHORT
    assert choose_direction(s, None, D("0"), mode="long_maker") == MAKER_LONG


def test_next_hour():
    assert next_hour_ts(7200.5) == 10800.0
    assert next_hour_ts(7199.0) == 7200.0


async def test_monitor_refresh():
    m, t = FakeExchange("arcus"), FakeExchange("lighter")
    m.funding["BTC"] = FundingInfo("arcus", "BTC", D("0.0001"))
    t.funding["BTC"] = FundingInfo("lighter", "BTC", D("0.0000"))
    mon = FundingMonitor(m, t, ["BTC", "ETH"], refresh_s=1)
    await mon.refresh()
    s = mon.latest("BTC")
    assert s is not None and s.best_direction == MAKER_SHORT and s.next_settlement_ts is not None
    assert mon.latest("ETH") is None
