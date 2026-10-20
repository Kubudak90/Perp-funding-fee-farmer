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


# --------------------------------------------------------------------------- review regressions
def _recovered(side: Side, size: str, *, venue="arcus", cid: str = "q1") -> Fill:
    """A fill replayed from the venue's fill snapshot after a private-stream reconnect (``Fill.recovered``)."""
    import dataclasses

    f = Fill(venue, "BTC", side, D("100000"), D(size), is_maker=venue == "arcus", client_id=cid,
             trade_id=f"r{next(_tid)}")
    if any(fl.name == "recovered" for fl in dataclasses.fields(f)):
        return dataclasses.replace(f, recovered=True)
    f.recovered = True  # models without the field yet: the adapters set the same attribute
    return f


async def test_slow_send_timeout_counts_from_acceptance_single_hedge():
    """money-2: the place call queues longer than hedge_timeout_s; the hedge must not expire (and be re-sent)
    right after the venue accepted it."""
    h, maker, taker, risk, _ = setup(rcfg=fast_risk(unhedged_timeout_s=30, max_unhedged_notional=D("1e9")))
    taker.taker_auto_fill = False
    orig = taker.place_market

    async def slow_place(symbol, side, size, **kw):
        await asyncio.sleep(0.6)  # > hedge_timeout_s (0.5): rate budget / executor queue
        res = await orig(symbol, side, size, **kw)
        cid = res.order.client_id
        asyncio.get_running_loop().call_later(0.15, lambda: taker.complete_taker(cid, size, D("100000")))
        return res

    taker.place_market = slow_place
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.taker_pos == D("-0.01"), timeout=3)
        await asyncio.sleep(0.3)
    assert len(markets(taker)) == 1 and taker.positions["BTC"] == D("-0.01") and h.delta == 0


async def test_send_outliving_io_timeout_stays_inflight_until_result():
    """The shielded place call outlives hedge_io_timeout_s and hedge_timeout_s: never expired/re-sent meanwhile."""
    h, maker, taker, risk, _ = setup(hedge_timeout_s=0.05, hedge_io_timeout_s=0.1,
                                     rcfg=fast_risk(unhedged_timeout_s=30, max_unhedged_notional=D("1e9")))
    orig = taker.place_market

    async def very_slow(symbol, side, size, **kw):
        await asyncio.sleep(0.4)
        return await orig(symbol, side, size, **kw)  # auto-fills on acceptance

    taker.place_market = very_slow
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.has_inflight())
        await asyncio.sleep(0.25)  # io timeout passed, hedge deadline long passed, call still pending
        assert h.has_inflight() and h.delta == 0 and h.stats["hedges_sent"] == 1
        await wait_until(lambda: h.taker_pos == D("-0.01") and h.is_hedged(), timeout=3)
        await asyncio.sleep(0.1)
    assert len(markets(taker)) == 1 and h.stats["adoptions"] == 0


async def test_terminal_fill_not_yet_in_position_read_is_not_dropped():
    """money-2 (variant): the venue reported FILLED, the fill frame and the position stream lag; an expiry
    reconcile that sees no position change must keep waiting instead of hedging a second time."""
    h, maker, taker, *_ = setup(hedge_timeout_s=0.1, hedge_fill_wait_s=0.05)
    taker.taker_auto_fill = False
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.has_inflight())
        cid = next(iter(h._inflight))
        o = taker.orders[cid]
        o.filled, o.status = D("0.01"), OrderStatus.FILLED
        taker._emit_order(o)  # positions (taker.positions) do NOT show it yet
        await asyncio.sleep(0.2)  # several expiry reconciles
        assert len(markets(taker)) == 1 and h.has_inflight() and h.delta == 0
        taker.positions["BTC"] = D("-0.01")
        taker._emit_fill(Fill("lighter", "BTC", Side.SELL, D("100000"), D("0.01"), is_maker=False, client_id=cid,
                              trade_id="late"))
        await wait_until(lambda: h.is_hedged())
    assert len(markets(taker)) == 1 and h.taker_pos == D("-0.01") and h.stats["adoptions"] == 0


async def test_recovered_fill_after_adoption_is_not_double_counted():
    """money-3 / async-late-fill-after-adoption: the fill frame was lost, the reconcile adopted the venue position
    and hedged it; the same fill replayed on reconnect must not be added (and hedged) again."""
    h, maker, taker, *_ = setup()
    async with running(h):
        maker.positions["BTC"] = D("-0.02")
        assert await h.reconcile("periodic") is True
        await wait_until(lambda: h.taker_pos == D("0.02") and h.is_hedged())
        maker._emit_fill(_recovered(Side.SELL, "0.02"))
        await wait_until(lambda: h._reconcile_requested is None)
        await asyncio.sleep(0.05)
    assert h.maker_pos == D("-0.02") and h.taker_pos == D("0.02") and len(markets(taker)) == 1
    assert h.stats["adoptions"] == 1 and h.stats["maker_fills"] == 1


async def test_recovered_taker_fill_after_adoption_is_not_double_counted():
    h, maker, taker, *_ = setup()
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.taker_pos == D("-0.01") and h.is_hedged())
        h.taker_pos = D(0)  # e.g. the hedge fill frame was lost while the account stream was down ...
        h.maker_pos = D(0)
        await h.reconcile("expired")  # ... and an expiry reconcile adopted both legs from REST
        assert (h.maker_pos, h.taker_pos) == (D("0.01"), D("-0.01"))
        n = len(markets(taker))
        taker._emit_fill(_recovered(Side.SELL, "0.01", venue="lighter", cid="fh-old"))  # account snapshot replay
        await wait_until(lambda: h._reconcile_requested is None)
        await asyncio.sleep(0.05)
    assert h.taker_pos == D("-0.01") and len(markets(taker)) == n


async def test_recovered_fill_without_prior_adoption_is_adopted_via_reconcile():
    h, maker, taker, *_ = setup()
    async with running(h):
        maker.positions["BTC"] = D("-0.02")
        maker._emit_fill(_recovered(Side.SELL, "0.02"))
        await wait_until(lambda: h.maker_pos == D("-0.02"))
        await wait_until(lambda: h.taker_pos == D("0.02") and h.is_hedged())
    assert h.stats["adoptions"] == 1 and len(markets(taker)) == 1


async def test_private_stream_resync_triggers_reconcile():
    h, maker, taker, *_ = setup()  # periodic reconcile effectively off (3600 s)
    async with running(h):
        await asyncio.sleep(0.03)
        maker.stream_ok = False
        await asyncio.sleep(0.03)
        maker.positions["BTC"] = D("0.01")  # filled while the stream was down; nothing replayed
        maker.stream_ok = True
        await wait_until(lambda: h.maker_pos == D("0.01") and h.taker_pos == D("-0.01"))
    assert h.stats["adoptions"] == 1


async def test_sync_rejected_retryable_refusal_counted_once_not_as_risk_error():
    """async-retryable-refusal-trips-breaker: an adapter that emits REJECTED synchronously for a refusal where
    nothing was sent (budget / 429 cooldown) and returns ok=False retryable=True."""
    from farmer.models import OrderResult

    h, maker, taker, risk, _ = setup()
    n = {"x": 0}

    async def refuse(symbol, side, size, *, worst_price, reduce_only=False, client_id=None):
        n["x"] += 1
        taker.calls.append(("place_market", symbol, side, size, worst_price, reduce_only))
        o = Order("lighter", symbol, side, worst_price, size, client_id, status=OrderStatus.REJECTED,
                  error="lighter rate budget exhausted", ioc=True)
        taker._emit_order(o)
        return OrderResult(ok=False, order=o, error=o.error, retryable=True)

    taker.place_market = refuse
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: n["x"] >= 3)
    assert risk.errors == []
    assert h._fail_count == n["x"] and h.stats["hedge_failures"] == n["x"]  # once per attempt


async def test_sync_rejected_permanent_refusal_recorded_once_per_attempt():
    from farmer.models import OrderResult

    h, maker, taker, risk, _ = setup()
    n = {"x": 0}

    async def reject(symbol, side, size, *, worst_price, reduce_only=False, client_id=None):
        n["x"] += 1
        o = Order("lighter", symbol, side, worst_price, size, client_id, status=OrderStatus.REJECTED,
                  error="invalid size", ioc=True)
        taker._emit_order(o)
        return OrderResult(ok=False, order=o, error=o.error, retryable=False)

    taker.place_market = reject
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: n["x"] >= 2)
        h.stop()
    assert len(risk.errors) == h._fail_count == n["x"] >= 2  # recorded once per attempt, not twice


async def test_emergency_check_ignores_delta_resolved_during_the_hedge_send(caplog):
    """The unhedged clock is re-evaluated before the emergency check: fills that arrived while the hedge was
    being sent may have resolved the delta (no bogus 'covered only by in-flight orders' alert)."""
    h, maker, taker, risk, notifier = setup()
    h._unhedged_since = h.clock() - 100.0  # stale: set before a (slow) hedge send
    h.maker_pos, h.taker_pos = D("0.01"), D("-0.01")
    with caplog.at_level("CRITICAL", logger="farmer.hedger"):
        await h._maybe_emergency(h.clock())
    assert h._unhedged_since is None and "covered only" not in caplog.text and h.stats["emergencies"] == 0


async def test_margin_cancel_counts_toward_circuit_breaker():
    """An IOC the venue cancels for lack of margin repeats on every retry: it is a risk error (breaker), while a
    plain unfilled IOC cancel (no liquidity at the limit) is not."""
    from farmer.models import OrderResult

    for reason, permanent in (("canceled-margin-not-allowed", True), ("canceled-too-much-slippage", False)):
        h, maker, taker, risk, _ = setup()
        n = {"x": 0}

        async def cancel(symbol, side, size, *, worst_price, reduce_only=False, client_id=None, reason=reason):
            n["x"] += 1
            o = Order("lighter", symbol, side, worst_price, size, client_id, status=OrderStatus.PENDING, ioc=True)
            res = OrderResult(ok=True, order=o)

            def done():
                taker._emit_order(Order("lighter", symbol, side, worst_price, size, client_id,
                                        status=OrderStatus.CANCELED, error=reason, ioc=True))
            asyncio.get_running_loop().call_soon(done)
            return res

        taker.place_market = cancel
        async with running(h):
            maker_fill(maker, Side.BUY, "0.01")
            await wait_until(lambda: n["x"] >= 2)
            h.stop()
        assert (len(risk.errors) >= 1) is permanent, reason


async def test_emergency_latches_quoting_and_repeated_emergencies_halt():
    """money-8: after a maker-leg emergency flatten quoting stays blocked; repeated emergencies halt."""
    rcfg = fast_risk(unhedged_timeout_s=0.05, emergency_flatten=True, emergency_cooldown_s=60.0,
                     max_emergencies_per_hour=2)
    h, maker, taker, risk, notifier = setup(rcfg=rcfg, hedge_retry_max_s=0.05)
    taker.fail_next(10_000, retryable=True, kinds={"market"})  # hedge venue keeps failing (retryable)
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.stats["emergencies"] == 1 and h.maker_pos == 0, timeout=3)
        await wait_until(lambda: not h.escalated)
        assert "emergency" in (h.quoting_blocked() or "") and not risk.halted
        maker_fill(maker, Side.BUY, "0.01")  # e.g. a quote that was still resting
        await wait_until(lambda: h.stats["emergencies"] == 2, timeout=3)
        await wait_until(lambda: risk.halted)
    assert "emergency" in risk.halt_reason and risk.errors == []


async def test_sell_hedge_min_notional_checked_at_worst_price():
    """money-12: the adapter checks min notional at the worst price; a SELL just above it at mid is bumped by
    a few steps (dust over-hedge) instead of being rejected locally on every retry."""
    from farmer.models import OrderResult

    am = make_market("arcus", "BTC", tick="0.1", step="0.0001", min_size="0.0001", min_notional="5")
    lm = make_market("lighter", "BTC", tick="0.1", step="0.000001", min_size="0.000001", min_notional="10")
    maker, taker = FakeExchange("arcus", [am]), FakeExchange("lighter", [lm])
    maker.set_book("BTC", [(99990, 5)], [(100010, 5)])
    taker.set_book("BTC", [(100000, 5)], [(100002, 5)])
    scfg, rcfg = fast_strategy(), fast_risk(unhedged_timeout_s=30, max_unhedged_notional=D("1e9"))
    pair = PairSpec("BTC", "BTC", "BTC", am, lm, scfg.for_market(MarketConfig(symbol="BTC")))
    risk = FakeRisk(rcfg)
    h = Hedger(pair, maker, taker, scfg, rcfg, risk)
    orig = taker.place_market

    async def validating(symbol, side, size, *, worst_price, reduce_only=False, client_id=None):
        if not reduce_only and size * worst_price < lm.min_notional:  # LighterExchange._validate_order
            taker.calls.append(("place_market", symbol, side, size, worst_price, reduce_only))
            return OrderResult(ok=False, error="notional below minimum")
        return await orig(symbol, side, size, worst_price=worst_price, reduce_only=reduce_only, client_id=client_id)

    taker.place_market = validating
    async with running(h):
        maker_fill(maker, Side.BUY, "0.0001")  # $10.0001 at the taker mid, $9.992 at the 8 bps worst price
        await wait_until(lambda: h.taker_pos != 0)
        await asyncio.sleep(0.05)
    [call] = markets(taker)
    assert call[2] is Side.SELL and call[3] == D("0.000101") and call[3] * call[4] >= 10
    assert risk.errors == [] and h.is_hedged() and h.delta == D("-0.000001")


def test_is_flat_with_pair_minimum_for_maker_leg():
    h, *_ = setup()
    h.set_positions(D("0.001"), D("-0.001"))  # maker min 0.001 (not dust), taker min 0.002
    assert not h.is_flat()
    assert h.is_flat(maker_min=D("0.002"))


async def test_late_fill_after_terminal_adopted_is_not_double_counted():
    """money-2 (variant): the venue reported FILLED and the position read shows it; the expiry reconcile adopts
    and drops the entry. The fill frame arriving afterwards must not be added a second time."""
    h, maker, taker, *_ = setup(hedge_timeout_s=0.1, hedge_fill_wait_s=0.05)
    taker.taker_auto_fill = False
    async with running(h):
        maker_fill(maker, Side.BUY, "0.01")
        await wait_until(lambda: h.has_inflight())
        cid = next(iter(h._inflight))
        o = taker.orders[cid]
        o.filled, o.status = D("0.01"), OrderStatus.FILLED
        taker.positions["BTC"] = D("-0.01")
        taker._emit_order(o)
        await wait_until(lambda: not h.has_inflight() and h.taker_pos == D("-0.01"), timeout=3)
        taker._emit_fill(Fill("lighter", "BTC", Side.SELL, D("100000"), D("0.01"), is_maker=False, client_id=cid,
                              trade_id="late"))
        await wait_until(lambda: h._reconcile_requested is None)
        await asyncio.sleep(0.05)
    assert h.taker_pos == D("-0.01") and h.delta == 0 and len(markets(taker)) == 1
