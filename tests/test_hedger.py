import asyncio
from decimal import Decimal as D

import pytest

from farmer.config import MarketConfig
from farmer.models import Fill, Order, OrderStatus, Side
from farmer.persistence.store import Store
from farmer.strategy.hedger import Hedger, min_trade_size
from farmer.strategy.mapping import PairSpec
from tests.fakes import FakeExchange, FakeNotifier, FakeRisk, fast_risk, fast_strategy, make_market, wait_until


def setup(*, scfg=None, rcfg=None, taker_step="0.0001", taker_min="0.002", store=None, **mkw):
    am = make_market("arcus", "BTC", tick="0.1", step="0.001", min_size="0.001", min_notional="5")
    lm = make_market("lighter", "BTC", tick="0.1", step=taker_step, min_size=taker_min, min_notional="10")
    maker = FakeExchange("arcus", [am])
    taker = FakeExchange("lighter", [lm])
    maker.set_book("BTC", [(99990, 5)], [(100010, 5)])
    taker.set_book("BTC", [(100000, 5)], [(100002, 5)])
    scfg = scfg or fast_strategy(**mkw)
    rcfg = rcfg or fast_risk()
    pair = PairSpec("BTC", "BTC", "BTC", am, lm, scfg.for_market(MarketConfig(symbol="BTC")))
    risk = FakeRisk(rcfg)
    notifier = FakeNotifier()
    h = Hedger(pair, maker, taker, scfg, rcfg, risk, store, notifier=notifier)
    return h, maker, taker, risk, notifier


_tid = iter(range(1, 10**9))


def maker_fill(maker: FakeExchange, side: Side, size: str, price="100000", cid=None) -> None:
    """A maker fill on the maker venue (position + fill listener), as the venue WS would deliver it."""
    q = D(size)
    maker.positions["BTC"] = maker.positions.get("BTC", D(0)) + q * side.sign
    maker._emit_fill(Fill("arcus", "BTC", side, D(price), q, is_maker=True, client_id=cid or "q1",
                          trade_id=f"m{next(_tid)}"))


def markets(taker: FakeExchange):
    return [c for c in taker.calls if c[0] == "place_market"]


class running:
    def __init__(self, h: Hedger):
        self.h = h

    async def __aenter__(self):
        self.task = asyncio.create_task(self.h.run())
        return self.h

    async def __aexit__(self, *exc):
        self.h.stop()
        await asyncio.wait_for(self.task, 2)


async def test_basic_hedge_and_store_roles(tmp_path):
    st = Store(tmp_path / "f.db")
    h, maker, taker, risk, _ = setup(store=st)
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.taker_pos == D("-0.01"))
        await wait_until(lambda: h.is_hedged())
    [call] = markets(taker)
    _, sym, side, size, worst, reduce_only = call
    assert (sym, side, size, reduce_only) == ("BTC", Side.SELL, D("0.0100"), False)
    assert worst == D("99920.0")  # bid 100000 * (1 - 8 bps), floored to tick
    assert h.delta == 0 and h.maker_pos == D("0.01")
    assert "lighter" in risk.successes
    roles = dict(st._db.execute("SELECT venue, role FROM fills").fetchall())
    assert roles == {"arcus": "maker", "lighter": "hedge"}


async def test_aggregates_partial_fills_into_one_hedge():
    h, maker, taker, *_ = setup(hedge_aggregate_ms=80)
    async with running(h):
        for _ in range(3):
            maker_fill(maker, Side.SELL, "0.004")
            await asyncio.sleep(0.01)
        await wait_until(lambda: h.is_hedged() and h.taker_pos != 0)
    [call] = markets(taker)
    assert call[2] is Side.BUY and call[3] == D("0.012")
    assert h.taker_pos == D("0.012")


async def test_residual_below_taker_minimum_waits_for_more_fills():
    h, maker, taker, *_ = setup()
    async with running(h):
        maker_fill(maker, Side.BUY, "0.001")  # taker min 0.002 -> not tradeable yet
        await asyncio.sleep(0.08)
        assert markets(taker) == [] and h.delta == D("0.001") and h.is_hedged()
        assert h.quoting_blocked() is None  # dust is not an unhedged alarm
        maker_fill(maker, Side.BUY, "0.001")
        await wait_until(lambda: h.taker_pos == D("-0.002"))
    assert [c[3] for c in markets(taker)] == [D("0.002")]


async def test_rounding_toward_zero_on_coarser_taker_step():
    h, maker, taker, *_ = setup(taker_step="0.01", taker_min="0.01")
    async with running(h):
        maker_fill(maker, Side.BUY, "0.015")
        await wait_until(lambda: h.taker_pos == D("-0.01"))
        await asyncio.sleep(0.05)
    assert [c[3] for c in markets(taker)] == [D("0.01")]
    assert h.delta == D("0.005") and h.is_hedged()  # residual below the taker step stays


async def test_retryable_failure_backs_off_and_retries():
    h, maker, taker, risk, _ = setup()
    taker.fail_next(2, retryable=True, kinds={"market"})
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.taker_pos == D("-0.01"))
    sent = markets(taker)
    assert len(sent) == 3 and all(c[3] == D("0.0100") for c in sent)
    assert h.stats["hedge_failures"] == 2 and risk.errors == []
    assert h._fail_count == 0  # reset by the fill


async def test_permanent_failure_reports_to_risk():
    h, maker, taker, risk, _ = setup()
    taker.fail_next(1, retryable=False, kinds={"market"})
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.taker_pos == D("-0.01"))
    assert risk.errors and risk.errors[0][0] == "lighter"


async def test_inflight_expiry_reconcile_adopts_executed_hedge():
    """Hedge executed on the venue but no fill/terminal update ever arrived: adopt venue truth, no double hedge."""
    h, maker, taker, *_ = setup(hedge_timeout_s=0.1)
    taker.taker_auto_fill = False
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: len(markets(taker)) == 1)
        assert h.inflight == D("-0.01") and h.delta == 0
        taker.positions["BTC"] = D("-0.01")  # venue state only
        await wait_until(lambda: not h.has_inflight(), timeout=3)
        await asyncio.sleep(0.05)
    assert h.taker_pos == D("-0.01") and h.delta == 0
    assert len(markets(taker)) == 1 and h.stats["adoptions"] == 1


async def test_inflight_expiry_reconcile_rehedges_when_not_executed():
    h, maker, taker, *_ = setup(hedge_timeout_s=0.1)
    taker.taker_auto_fill = False
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: len(markets(taker)) == 1)
        await wait_until(lambda: len(markets(taker)) == 2, timeout=3)  # expired, venue flat -> hedge again
        taker.complete_taker(list(taker.orders)[-1], D("0.01"), D("100000"))
        await wait_until(lambda: h.taker_pos == D("-0.01"))
    assert h.stats["adoptions"] == 0 and h.stats["expired"] >= 1


async def test_reduce_only_only_when_strictly_reducing_taker():
    h, maker, taker, *_ = setup()
    maker.positions["BTC"], taker.positions["BTC"] = D("0.05"), D("-0.05")
    h.set_positions(D("0.05"), D("-0.05"))
    async with running(h):
        maker_fill(maker, Side.SELL, "0.02")  # hedge BUY 0.02 reduces the taker short
        await wait_until(lambda: h.taker_pos == D("-0.03"))
        maker_fill(maker, Side.BUY, "0.02")  # hedge SELL grows the taker short
        await wait_until(lambda: h.taker_pos == D("-0.05"))
        maker_fill(maker, Side.SELL, "0.08")  # hedge BUY 0.08 > |taker 0.05|: crosses zero -> not reduce-only
        await wait_until(lambda: h.taker_pos == D("0.03"))
    assert [(c[2], c[3], c[5]) for c in markets(taker)] == [
        (Side.BUY, D("0.0200"), True), (Side.SELL, D("0.0200"), False), (Side.BUY, D("0.0800"), False)]


async def test_partial_ioc_fill_rehedges_remainder():
    h, maker, taker, *_ = setup()
    taker.taker_fill_ratio = D("0.5")
    async with running(h):
        maker_fill(maker, Side.BUY, "0.016")
        await wait_until(lambda: len(markets(taker)) >= 2)
        taker.taker_fill_ratio = D(1)
        await wait_until(lambda: h.is_hedged() and abs(h.delta) < D("0.002"))
    sizes = [c[3] for c in markets(taker)]
    assert sizes[0] == D("0.0160") and sizes[1] == D("0.0080")


async def test_escalation_pauses_quotes_and_doubles_slippage():
    h, maker, taker, *_ = setup(rcfg=fast_risk(max_unhedged_notional=D("150"), unhedged_grace_s=0.0))
    taker.fail_next(1, retryable=True, kinds={"market"})
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")  # $1000 unhedged > $150
        await wait_until(lambda: h.quoting_blocked() is not None)
        await wait_until(lambda: h.taker_pos == D("-0.01"))
        await wait_until(lambda: h.quoting_blocked() is None)
    worst = [c[4] for c in markets(taker)]
    assert len(worst) == 2 and all(w == D("99840.0") for w in worst)  # 2 x 8 bps below the 100000 bid


async def test_emergency_flatten_on_maker_venue(tmp_path):
    st = Store(tmp_path / "f.db")
    rcfg = fast_risk(unhedged_timeout_s=0.05, emergency_flatten=True)
    h, maker, taker, risk, notifier = setup(rcfg=rcfg, store=st, hedge_retry_max_s=0.05)
    taker.fail_next(10_000, retryable=False, kinds={"market"})  # hedge venue rejects everything
    maker.positions["BTC"] = D(0)
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.maker_pos == 0, timeout=3)
        await wait_until(lambda: notifier.messages)
    em = [c for c in maker.calls if c[0] == "place_market"]
    assert len(em) == 1 and em[0][2] is Side.SELL and em[0][3] == D("0.010") and em[0][5] is True
    assert "EMERGENCY" in notifier.messages[0] and h.stats["emergencies"] == 1
    assert h.delta == 0 and not risk.halted
    roles = sorted(r for (r,) in st._db.execute("SELECT role FROM fills").fetchall())
    assert roles == ["emergency", "maker"]


async def test_taker_side_excess_halts():
    rcfg = fast_risk(unhedged_timeout_s=0.05)
    h, maker, taker, risk, notifier = setup(rcfg=rcfg, hedge_retry_max_s=0.05)
    taker.fail_next(10_000, retryable=False, kinds={"market"})
    h.set_positions(D(0), D("-0.01"))  # excess on the taker leg: nothing to reduce on the maker venue
    async with running(h):
        await wait_until(lambda: risk.halted, timeout=3)
    assert "unhedged" in risk.halt_reason and notifier.messages
    assert not [c for c in maker.calls if c[0] == "place_market"]


async def test_periodic_reconcile_adopts_external_change():
    h, maker, taker, *_ = setup(reconcile_interval_s=0.05)
    async with running(h):
        maker.positions["BTC"] = D("0.02")  # e.g. a fill missed during a reconnect
        await wait_until(lambda: h.maker_pos == D("0.02"))
        await wait_until(lambda: h.taker_pos == D("-0.02"))
    assert h.stats["adoptions"] == 1 and h.delta == 0


async def test_reconcile_aborts_on_fill_during_confirm():
    h, maker, taker, *_ = setup(reconcile_confirm_s=0.2)
    maker.positions["BTC"] = D("0.01")  # venue shows a fill that is still in transit on the WS
    task = asyncio.create_task(h.reconcile("test"))
    await asyncio.sleep(0.05)
    maker._emit_fill(Fill("arcus", "BTC", Side.BUY, D("100000"), D("0.01"), is_maker=True, client_id="q", trade_id="x"))
    assert await task is False
    assert h.maker_pos == D("0.01") and h.stats["adoptions"] == 0  # counted once, not twice
    assert await h.reconcile("again") is True and h.maker_pos == D("0.01")


async def test_terminal_update_without_fills_stays_inflight_then_reconciles():
    """Venue says FILLED but the fill frames never come: no second hedge; positions adopted after expiry."""
    h, maker, taker, *_ = setup()
    taker.taker_auto_fill = False
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.has_inflight())
        cid = next(iter(h._inflight))
        o = taker.orders[cid]
        o.filled, o.status = D("0.01"), OrderStatus.FILLED
        taker.positions["BTC"] = D("-0.01")
        taker._emit_order(o)
        assert h.has_inflight() and h.delta == 0  # unseen fills still count as in flight
        await wait_until(lambda: h.taker_pos == D("-0.01") and not h.has_inflight(), timeout=3)
    assert h.stats["adoptions"] == 1 and len(markets(taker)) == 1


async def test_terminal_update_overtaking_its_fills_is_not_double_hedged():
    h, maker, taker, *_ = setup(hedge_fill_wait_s=5.0)
    taker.taker_auto_fill = False
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.has_inflight())
        cid = next(iter(h._inflight))
        o = taker.orders[cid]
        o.filled, o.status = D("0.01"), OrderStatus.FILLED
        taker._emit_order(o)  # terminal frame first ...
        maker_fill(maker, Side.BUY, "0.005")  # ... meanwhile more maker fills -> a second hedge goes out
        await wait_until(lambda: len(markets(taker)) == 2)
        assert markets(taker)[1][3] == D("0.0050")  # only the new fill is hedged
        taker.positions["BTC"] = D("-0.01")
        taker._emit_fill(Fill("lighter", "BTC", Side.SELL, D("100000"), D("0.01"), is_maker=False, client_id=cid,
                              trade_id="late"))  # ... then the late fill
        assert cid not in h._inflight
        taker.complete_taker(list(taker.orders)[-1], D("0.005"), D("100000"))
        await wait_until(lambda: h.is_hedged())
    assert h.taker_pos == D("-0.015") and h.delta == 0 and h.stats["adoptions"] == 0


async def test_flatten_both_legs_and_flush():
    h, maker, taker, *_ = setup()
    maker.positions["BTC"], taker.positions["BTC"] = D("0.03"), D("-0.03")
    h.set_positions(D("0.03"), D("-0.03"))
    h.close_mode = True
    async with running(h):
        assert await h.flatten() is True
        await wait_until(lambda: h.is_flat())
        assert await h.flush(1.0)
    mk = [c for c in maker.calls if c[0] == "place_market"]
    tk = markets(taker)
    assert [(c[2], c[3], c[5]) for c in mk] == [(Side.SELL, D("0.030"), True)]
    assert [(c[2], c[3], c[5]) for c in tk] == [(Side.BUY, D("0.0300"), True)]
    assert await h.flatten() is False  # nothing left


async def test_close_mode_never_opens_taker():
    h, maker, taker, *_ = setup()
    h.close_mode = True
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")  # hedge would open a taker short
        await asyncio.sleep(0.05)
    assert markets(taker) == []


async def test_unknown_outcome_stays_inflight():
    h, maker, taker, *_ = setup(hedge_timeout_s=5)
    taker.taker_auto_fill = False

    async def boom(*a, **k):
        raise ConnectionError("socket closed mid-send")

    taker.place_market = boom
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.has_inflight())
        await asyncio.sleep(0.1)
        assert h.delta == 0 and len(h._inflight) == 1  # never re-sent blindly


def test_min_trade_size():
    m = make_market("lighter", "BTC", step="0.00001", min_size="0.00007", min_notional="10")
    assert min_trade_size(m, D("80000")) == D("0.00013")
    assert min_trade_size(m, None) == D("0.00007")


def test_order_listener_ignores_other_symbols():
    h, maker, taker, *_ = setup()
    h._on_taker_order(Order("lighter", "ETH", Side.BUY, D(1), D(1), "x", status=OrderStatus.FILLED))
    h._on_taker_fill(Fill("lighter", "ETH", Side.BUY, D(1), D(1)))
    assert h.taker_pos == 0


@pytest.mark.parametrize("status", [OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED])
async def test_terminal_without_fill_clears_inflight_and_backs_off(status):
    h, maker, taker, risk, _ = setup()
    taker.taker_auto_fill = False
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.has_inflight())
        cid = next(iter(h._inflight))
        taker.orders[cid].status = status
        taker._emit_order(taker.orders[cid])
        assert not h.has_inflight() and h.delta == D("0.01")
        await wait_until(lambda: len(markets(taker)) == 2)
    assert bool(risk.errors) == (status is OrderStatus.REJECTED)
