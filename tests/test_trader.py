import asyncio
import time
from decimal import Decimal as D

from farmer.config import MarketConfig
from farmer.models import Fill, FundingInfo, OrderResult, OrderStatus, Side
from farmer.strategy.funding import MAKER_LONG, MAKER_SHORT, FundingSnapshot
from farmer.strategy.mapping import PairSpec
from farmer.strategy.trader import PairTrader, Phase
from tests.fakes import FakeExchange, FakeRisk, fast_risk, fast_strategy, make_market, wait_until


class FundingStub:
    def __init__(self, rm="0", rt="0", next_ts=None):
        self.snap = None
        self.set(rm, rt, next_ts)

    def set(self, rm, rt, next_ts=None):
        nts = next_ts if next_ts is not None else time.time() + 3000
        self.snap = FundingSnapshot("BTC", FundingInfo("arcus", "BTC", D(rm), next_funding_ts=nts),
                                    FundingInfo("lighter", "BTC", D(rt), next_funding_ts=nts))

    def latest(self, symbol):
        return self.snap


def setup(*, funding=None, rcfg=None, **kw):
    am = make_market("arcus", "BTC", tick="0.1", step="0.001", min_size="0.001", min_notional="5")
    lm = make_market("lighter", "BTC", tick="0.1", step="0.0001", min_size="0.002", min_notional="10")
    maker, taker = FakeExchange("arcus", [am]), FakeExchange("lighter", [lm])
    maker.set_book("BTC", [(99990, 5)], [(100010, 5)])
    taker.set_book("BTC", [(100000, 5)], [(100002, 5)])
    kw.setdefault("direction_mode", "long_maker")
    kw.setdefault("target_notional", D("2000"))
    kw.setdefault("order_notional", D("500"))
    scfg = fast_strategy(**kw)
    rcfg = rcfg or fast_risk()
    pair = PairSpec("BTC", "BTC", "BTC", am, lm, scfg.for_market(MarketConfig(symbol="BTC")))
    risk = FakeRisk(rcfg)
    t = PairTrader(pair, maker, taker, scfg, risk, funding, None, rcfg=rcfg)
    return t, maker, taker, risk


class running:
    def __init__(self, t: PairTrader):
        self.t = t

    async def __aenter__(self):
        self.task = asyncio.create_task(self.t.run())
        return self.t

    async def __aexit__(self, *exc):
        await self.t.stop(cancel_quotes=True)
        await asyncio.wait_for(self.task, 3)


def resting(maker):
    return maker.resting("BTC")


async def fill_next(t: PairTrader, maker: FakeExchange, size=None):
    await wait_until(lambda: resting(maker) and resting(maker)[0].status is OrderStatus.OPEN)
    o = resting(maker)[0]
    maker.fill_resting(o.client_id, size)
    await wait_until(lambda: t.hedger.is_hedged() and abs(t.hedger.delta) < D("0.002"))
    return o


def calls(ex, kind):
    return [c for c in ex.calls if c[0] == kind]


async def test_full_cycle_open_hold_close_open():
    t, maker, taker, risk = setup()
    h = t.hedger
    phases = []
    async with running(t):
        for _ in range(4):  # 4 x 0.005 = target 0.02
            o = await fill_next(t, maker)
            assert o.side is Side.BUY and not o.reduce_only and o.price == D("99991.9")
            phases.append(t.phase)
            assert h.maker_pos + h.taker_pos == 0  # delta-neutral after every hedge
        await wait_until(lambda: t.phase is Phase.CLOSING)
        for _ in range(4):
            o = await fill_next(t, maker)
            assert o.side is Side.SELL and o.reduce_only and o.price == D("100010.1")
            assert h.maker_pos + h.taker_pos == 0
        await wait_until(lambda: t.phase is Phase.OPENING and t.stats["cycles"] == 1)
        await wait_until(lambda: resting(maker) and resting(maker)[0].side is Side.BUY)
    assert h.maker_pos == 0 and h.taker_pos == 0
    # volume on both venues: 2 x (target - floor) per cycle
    assert h.stats["maker_volume"] == D("0.02") * D("99991.9") + D("0.02") * D("100010.1")
    assert h.stats["hedge_volume"] == D("0.02") * D("100000") + D("0.02") * D("100002")
    hedges = calls(taker, "place_market")
    assert [c[5] for c in hedges] == [False] * 4 + [True] * 4  # closing hedges reduce the taker leg
    assert all(o.post_only for o in maker.orders.values())
    assert not resting(maker)  # stop() canceled the quote
    assert ("BTC", "arcus") in risk.exposure and risk.open_checks


async def test_direction_flip_only_when_flat():
    fund = FundingStub(rm="-0.0001", rt="0")  # maker venue pays shorts -> maker LONG
    t, maker, taker, risk = setup(funding=fund, direction_mode="auto", flip_hysteresis_apr=D("0.02"))
    flips = []
    orig = t._flip

    def spy(new_dir, now, mid):
        flips.append((new_dir, t.hedger.maker_pos))
        orig(new_dir, now, mid)

    t._flip = spy
    async with running(t):
        await fill_next(t, maker)
        assert t.direction == MAKER_LONG and t.hedger.maker_pos == D("0.005")
        fund.set(rm="0.0001", rt="0")  # now maker SHORT earns 87.6 % APR more
        await wait_until(lambda: t.phase is Phase.CLOSING)
        assert t.direction == MAKER_LONG  # not flipped while holding a position
        o = await fill_next(t, maker)
        assert o.side is Side.SELL and o.reduce_only
        await wait_until(lambda: t.direction == MAKER_SHORT and t.phase is Phase.OPENING)
        o = await fill_next(t, maker)
        assert o.side is Side.SELL and not o.reduce_only
    assert flips == [(MAKER_SHORT, D(0))]
    assert t.hedger.maker_pos == D("-0.005") and t.hedger.taker_pos == D("0.005")


async def test_flip_not_applied_inside_funding_window():
    fund = FundingStub(rm="0.0001", rt="0", next_ts=time.time() + 30)
    t, maker, taker, risk = setup(funding=fund, direction_mode="auto", protect_funding_window_s=60)
    t.direction = MAKER_LONG  # e.g. previous choice; snapshot now prefers SHORT
    async with running(t):
        await asyncio.sleep(0.1)
        assert t.direction == MAKER_LONG and not resting(maker)
        assert "funding window" in t.block_reason
        fund.set(rm="0.0001", rt="0", next_ts=time.time() + 3000)
        await wait_until(lambda: t.direction == MAKER_SHORT and resting(maker))


async def test_close_deferred_in_funding_window_unless_forced():
    fund = FundingStub(rm="-0.0001", rt="0", next_ts=time.time() + 30)
    t, maker, taker, risk = setup(funding=fund, direction_mode="auto", protect_funding_window_s=60,
                                  target_notional=D("500"))
    async with running(t):
        await fill_next(t, maker)  # 0.005 = target
        await wait_until(lambda: t.phase is Phase.HOLDING)
        await asyncio.sleep(0.1)
        assert t.phase is Phase.HOLDING and not resting(maker)  # hold_seconds=0 but inside the window
        risk.reduce = True  # forced: ignores the window
        await wait_until(lambda: t.phase is Phase.CLOSING and resting(maker))
        assert resting(maker)[0].reduce_only


async def test_protective_requote_when_taker_moves_against_quote():
    t, maker, taker, risk = setup(min_quote_lifetime_s=100.0)
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        assert o.price == D("99991.9")
        taker.set_book("BTC", [(99980, 5)], [(99982, 5)])  # hedge now sells 20 lower -> quote too high
        await wait_until(lambda: calls(maker, "modify_order"))
    [m] = calls(maker, "modify_order")
    assert m[1] == o.client_id and m[2] == D("99972.0")
    assert o.price == D("99972.0") and t.stats["protective"] == 1


async def test_improvement_requote_throttled_by_lifetime_threshold_and_budget():
    t, maker, taker, risk = setup(min_quote_lifetime_s=0.15, reprice_threshold_bps=D("0.5"), max_requotes_per_min=3)
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        t0 = time.monotonic()
        taker.set_book("BTC", [(100001, 5)], [(100003, 5)])  # +1 -> moved 0.1 bps < threshold
        await asyncio.sleep(0.05)
        assert not calls(maker, "modify_order")
        taker.set_book("BTC", [(100006, 5)], [(100008, 5)])  # +6 -> moved 0.6 bps, but quote too young
        await wait_until(lambda: calls(maker, "modify_order"))
        assert time.monotonic() - t0 >= 0.15 - 0.03
        assert o.price == D("99997.9") and t.stats["improve"] == 1
        taker.set_book("BTC", [(100012, 5)], [(100014, 5)])
        await wait_until(lambda: len(calls(maker, "modify_order")) == 2, timeout=2)
        taker.set_book("BTC", [(100018, 5)], [(100020, 5)])
        await asyncio.sleep(0.3)
        assert len(calls(maker, "modify_order")) == 2  # 1 place + 2 amends = budget of 3 per minute


async def test_low_budget_health_stretches_lifetime():
    t, maker, taker, risk = setup(min_quote_lifetime_s=0.05)
    maker.health = 0.05
    assert abs(t._lifetime() - 0.5) < 1e-9
    maker.health = 0.9
    assert t._lifetime() == 0.05


async def test_low_taker_budget_pauses_quoting_with_hysteresis():
    """Every maker fill needs a hedge tx: with the hedge venue's budget nearly used up the quote is pulled
    (hedges would otherwise wait for the budget while deltas pile up), and quoting resumes at 1.5x the limit."""
    t, maker, taker, risk = setup(min_taker_budget=0.25)
    async with running(t):
        await wait_until(lambda: len(resting(maker)) == 1)
        taker.health = 0.1
        await wait_until(lambda: len(resting(maker)) == 0)
        assert "rate budget low" in (t.block_reason or "")
        taker.health = 0.3  # above the limit but below the resume level
        await asyncio.sleep(0.1)
        assert resting(maker) == []
        taker.health = 0.5
        await wait_until(lambda: len(resting(maker)) == 1)


async def test_would_cross_modify_keeps_order_and_retries_next_tick():
    t, maker, taker, risk = setup()
    real = maker.modify_order
    n = {"x": 0}

    async def flaky(order, *, price, size=None):
        n["x"] += 1
        if n["x"] == 1:
            maker.calls.append(("modify_order", order.client_id, price, size))
            return OrderResult(ok=False, order=order, error="post-only would cross (local check)", would_cross=True)
        return await real(order, price=price, size=size)

    maker.modify_order = flaky
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        taker.set_book("BTC", [(99980, 5)], [(99982, 5)])
        await wait_until(lambda: len(calls(maker, "modify_order")) == 2)
        assert o.price == D("99972.0") and t.stats["would_cross"] == 1
    assert len(calls(maker, "place_limit")) == 1


async def test_crossing_modify_kills_order_then_new_one_placed():
    t, maker, taker, risk = setup()
    maker.modify_cross_kills = True
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        # venue book moved: maker ask now right at our fresh price -> the venue rejects and kills the order
        orig = maker.modify_order

        async def crossing(order, *, price, size=None):
            maker.books["BTC"].asks[0].price = price
            return await orig(order, price=price, size=size)

        maker.modify_order = crossing
        taker.set_book("BTC", [(99980, 5)], [(99982, 5)])
        await wait_until(lambda: o.status is OrderStatus.REJECTED)
        maker.modify_order = orig
        maker.set_book("BTC", [(99960, 5)], [(99990, 5)])
        await wait_until(lambda: len(calls(maker, "place_limit")) == 2 and resting(maker))
    assert t.stats["would_cross"] >= 1


async def test_would_cross_on_place_backs_off_then_places():
    t, maker, taker, risk = setup()
    real = maker.place_limit
    n = {"x": 0}

    async def flaky(*a, **k):
        n["x"] += 1
        if n["x"] == 1:
            return OrderResult(ok=False, error="post-only would cross", would_cross=True)
        return await real(*a, **k)

    maker.place_limit = flaky
    async with running(t):
        await wait_until(lambda: resting(maker))
    assert n["x"] == 2 and t.stats["would_cross"] == 1


async def test_pending_order_not_double_placed_then_cancelled_after_ack_timeout():
    t, maker, taker, risk = setup(order_ack_timeout_s=0.2)
    maker.ack_pending = True
    async with running(t):
        await wait_until(lambda: calls(maker, "place_limit"))
        await asyncio.sleep(0.1)
        assert len(calls(maker, "place_limit")) == 1  # waiting for the ack
        await wait_until(lambda: calls(maker, "cancel_order"))
        o = next(iter(maker.orders.values()))
        assert o.status is OrderStatus.CANCELED
        maker.ack_pending = False
        await wait_until(lambda: len(calls(maker, "place_limit")) == 2 and resting(maker))


async def test_pending_ack_then_normal_quoting():
    t, maker, taker, risk = setup()
    maker.ack_pending = True
    async with running(t):
        await wait_until(lambda: calls(maker, "place_limit"))
        cid = next(iter(maker.orders))
        maker.ack_pending = False
        maker.ack(cid)
        await fill_next(t, maker)
        assert calls(maker, "place_limit")[0][3] == D("0.005")
    assert len(calls(maker, "place_limit")) >= 2


async def test_partial_fill_resizes_oversized_quote():
    t, maker, taker, risk = setup()
    async with running(t):
        for _ in range(2):
            await fill_next(t, maker)  # maker_pos 0.010
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        assert o.size == D("0.005")
        # an unrelated maker-venue fill (e.g. manual) moves us to 0.017 -> remaining need 0.003 < 0.005
        maker._emit_fill(Fill("arcus", "BTC", Side.BUY, D("99990"), D("0.007"), is_maker=True, client_id="manual"))
        await wait_until(lambda: calls(maker, "modify_order"))
    m = calls(maker, "modify_order")[0]
    assert m[1] == o.client_id and m[3] == D("0.003") and m[2] == o.price
    assert t.stats["resize"] == 1 and t.stats["protective"] == 0  # size-only: not an edge violation


async def test_rwa_closed_gates_opening_but_not_closing():
    t, maker, taker, risk = setup(target_notional=D("500"))
    maker.closed.add("BTC")
    async with running(t):
        await asyncio.sleep(0.1)
        assert not resting(maker) and "market closed" in t.block_reason
        maker.closed.clear()
        await fill_next(t, maker)
        maker.closed.add("BTC")
        await wait_until(lambda: t.phase is Phase.CLOSING and resting(maker))  # reducing quotes still allowed
        assert resting(maker)[0].reduce_only


async def test_hedge_market_turning_reduce_only_still_closes_but_never_opens():
    """Lighter switches the hedge market to force_reduce_only at runtime: the position is still closed with
    reduce-only maker quotes (hedged with reduce-only IOCs), no new opening quote is placed."""
    t, maker, taker, risk = setup(target_notional=D("500"))
    async with running(t):
        await fill_next(t, maker)
        lm = t.pair.taker_market
        lm.active, lm.extra["reduce_only_allowed"] = False, True
        await wait_until(lambda: t.phase is Phase.CLOSING and resting(maker))
        assert resting(maker)[0].reduce_only
        await fill_next(t, maker)
        assert t.hedger.maker_pos == 0 and t.hedger.taker_pos == 0
        assert all(c[5] for c in calls(taker, "place_market")[1:])  # the closing hedge was reduce-only
        await wait_until(lambda: t.phase is Phase.OPENING)
        await asyncio.sleep(0.1)
        assert not resting(maker) and "market closed" in t.block_reason
        lm.extra["reduce_only_allowed"] = False  # frozen: not even reducing quotes
        t.hedger.maker_pos, t.hedger.taker_pos = D("-0.005"), D("0.005")  # a BUY toward the goal would reduce
        await wait_until(lambda: t.block_reason == "market inactive")
        assert not resting(maker)


async def test_halted_cancels_quotes_but_hedging_continues():
    t, maker, taker, risk = setup()
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        maker.fill_resting(o.client_id, D("0.002"))  # partial fill
        risk.halt("daily loss")
        await wait_until(lambda: not resting(maker))
        assert o.status is OrderStatus.CANCELED and "halted" in t.block_reason
        await wait_until(lambda: t.hedger.taker_pos == D("-0.002"))
        maker._emit_fill(Fill("arcus", "BTC", Side.BUY, D("99990"), D("0.003"), is_maker=True, client_id=o.client_id))
        await wait_until(lambda: t.hedger.taker_pos == D("-0.005"))
        await asyncio.sleep(0.05)
        assert not resting(maker)
        risk.resume()
        await wait_until(lambda: resting(maker))


async def test_hedger_escalation_pauses_quotes():
    t, maker, taker, risk = setup()
    async with running(t):
        await wait_until(lambda: resting(maker))
        t.hedger.escalated = True  # e.g. unhedged delta too old (the hedger re-evaluates each step)
        t.hedger._escalated_reason = "test"
        t.hedger.quoting_blocked = lambda: "hedger escalated: test"
        await wait_until(lambda: not resting(maker))
        assert "escalated" in t.block_reason


async def test_must_reduce_forces_close_and_blocks_reopen():
    t, maker, taker, risk = setup()
    async with running(t):
        await fill_next(t, maker)
        await fill_next(t, maker)
        risk.reduce = True
        await wait_until(lambda: t.phase is Phase.CLOSING and t.goal() == 0)
        await fill_next(t, maker)
        await fill_next(t, maker)
        assert t.hedger.maker_pos == 0 and t.hedger.taker_pos == 0
        await asyncio.sleep(0.1)
        assert t.phase is Phase.CLOSING and not resting(maker)
        risk.reduce = False
        await wait_until(lambda: t.phase is Phase.OPENING and resting(maker))


async def test_check_open_blocks_only_exposure_increasing_quotes():
    t, maker, taker, risk = setup(target_notional=D("500"))
    async with running(t):
        await fill_next(t, maker)
        risk.open_ok = False
        await wait_until(lambda: t.phase is Phase.CLOSING and resting(maker))
        assert resting(maker)[0].reduce_only
        await fill_next(t, maker)
        await asyncio.sleep(0.1)
        assert not resting(maker) and "risk" in t.block_reason


async def test_basis_warmup_blocks_quotes():
    t, maker, taker, risk = setup(use_basis=True, basis_warmup_s=0.3)
    async with running(t):
        maker.set_book("BTC", [(99990, 5)], [(100010, 5)])
        await asyncio.sleep(0.1)
        assert not resting(maker) and t.block_reason == "basis warm-up"
        await wait_until(lambda: resting(maker), timeout=2)
        taker.set_book("BTC", [(100000, 5)], [(100002, 5)])
    assert risk.quote_checks and risk.quote_checks[-1][1] is not None


async def test_status_and_existing_position_adoption():
    t, maker, taker, risk = setup()
    t.hedger.set_positions(D("0.02"), D("-0.02"))
    async with running(t):
        await wait_until(lambda: t.phase is Phase.CLOSING)
        s = t.status()
    assert t.direction == MAKER_LONG
    assert s["symbol"] == "BTC" and s["maker_pos"] == D("0.02") and s["delta"] == 0
    for k in ("phase", "direction", "quote", "funding", "basis", "books", "hedger", "stats", "block_reason"):
        assert k in s


async def test_stop_cancels_resting_quote():
    t, maker, taker, risk = setup()
    task = asyncio.create_task(t.run())
    await wait_until(lambda: resting(maker))
    await t.stop(cancel_quotes=True)
    await asyncio.wait_for(task, 2)
    assert not resting(maker) and calls(maker, "cancel_order")


async def test_unacked_quote_pulled_when_off_edge():
    t, maker, taker, risk = setup(order_ack_timeout_s=5.0)
    maker.ack_pending = True
    async with running(t):
        await wait_until(lambda: calls(maker, "place_limit"))
        o = next(iter(maker.orders.values()))
        taker.set_book("BTC", [(99980, 5)], [(99982, 5)])  # quote now violates the edge limit
        await wait_until(lambda: calls(maker, "cancel_order"), timeout=1)
    assert o.status is OrderStatus.CANCELED


async def test_quote_pulled_when_amend_ack_pending_and_off_edge():
    import dataclasses

    t, maker, taker, risk = setup(amend_ack_timeout_s=5.0)

    async def pending_modify(order, *, price, size=None):  # Arcus: 202 accepted, no WS frame yet
        maker.calls.append(("modify_order", order.client_id, price, size))
        return OrderResult(ok=True, order=dataclasses.replace(order, status=OrderStatus.PENDING))

    maker.modify_order = pending_modify
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        taker.set_book("BTC", [(99980, 5)], [(99982, 5)])
        await wait_until(lambda: calls(maker, "modify_order"))
        await asyncio.sleep(0.1)
        assert len(calls(maker, "modify_order")) == 1 and o.status is OrderStatus.OPEN  # waiting for the ack
        taker.set_book("BTC", [(99960, 5)], [(99962, 5)])  # the amended price is now off-edge too
        await wait_until(lambda: o.status is OrderStatus.CANCELED, timeout=2)


async def test_adapter_cancel_replace_modify_adopts_new_order():
    from farmer.exchanges.base import Exchange

    t, maker, taker, risk = setup()

    async def base_modify(order, *, price, size=None):  # default Exchange.modify_order: cancel + place
        return await Exchange.modify_order(maker, order, price=price, size=size)

    maker.modify_order = base_modify
    async with running(t):
        await wait_until(lambda: resting(maker))
        old = resting(maker)[0]
        taker.set_book("BTC", [(99980, 5)], [(99982, 5)])
        await wait_until(lambda: old.status is OrderStatus.CANCELED and resting(maker))
        new = resting(maker)[0]
        await asyncio.sleep(0.05)
        assert len(resting(maker)) == 1 and new.price == D("99972.0") and t.status()["quote"]["client_id"] == new.client_id


# --------------------------------------------------------------------------- review regressions
async def test_startup_with_opposite_dust_quotes_through_zero():
    """money-1: a leftover position below the pair minimum, opposite to the direction, must not block quoting."""
    t, maker, taker, risk = setup(direction_mode="short_maker")
    t.hedger.set_positions(D("0.001"), D("-0.001"))  # pair minimum 0.002
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        assert o.side is Side.SELL and not o.reduce_only and o.size == D("0.005")
        assert t.phase is Phase.OPENING and t.direction == MAKER_SHORT


async def test_flip_after_close_with_dust_quotes_new_direction():
    """money-1: a hedged close leaves a dust residual of the OLD direction; after the funding flip the pair must
    keep quoting in the new direction."""
    fund = FundingStub(rm="0", rt="0.0001")  # maker LONG
    t, maker, taker, risk = setup(funding=fund, direction_mode="auto", churn=False)
    async with running(t):
        await wait_until(lambda: t.direction == MAKER_LONG)
        for _ in range(3):
            await fill_next(t, maker)
        await fill_next(t, maker, D("0.0047"))  # partial: 0.0197 ~ target 0.02
        await wait_until(lambda: t.phase is Phase.HOLDING)
        fund.set(rm="0.0002", rt="0")  # maker SHORT now earns more
        while t.hedger.maker_pos >= D("0.002"):
            o = await fill_next(t, maker)
            assert o.side is Side.SELL and o.reduce_only
        assert D(0) < t.hedger.maker_pos < D("0.002")  # dust of the old direction
        await wait_until(lambda: t.direction == MAKER_SHORT and t.phase is Phase.OPENING)
        await wait_until(lambda: resting(maker) and resting(maker)[0].status is OrderStatus.OPEN)
        o = resting(maker)[0]
        assert o.side is Side.SELL and not o.reduce_only and o.size == D("0.005")
        assert t.block_reason is None


async def test_protective_amend_retryable_failure_cancels_quote():
    """money-4: an edge-violating quote whose amend fails retryably (order pool exhausted) is pulled, not kept."""
    t, maker, taker, risk = setup(min_quote_lifetime_s=100.0)
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        maker.fail_next(10_000, retryable=True, kinds={"modify", "limit"})
        taker.set_book("BTC", [(99900, 5)], [(99902, 5)])  # resting BUY now far above the edge limit
        await wait_until(lambda: o.status is OrderStatus.CANCELED, timeout=1)
    assert len(calls(maker, "modify_order")) == 1 and t.stats["protective"] == 1


async def test_dead_quote_order_not_found_for_modify_is_retired():
    """async-dead-quote-never-retired: ORDER_NOT_FOUND_FOR_MODIFY (underscored venue code) retires the quote."""
    t, maker, taker, risk = setup(min_quote_lifetime_s=0.0, reprice_threshold_bps=D("0.5"))
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        t._order_born -= 5.0  # not a just-placed (maybe unsequenced) order

        async def dead(order, *, price, size=None):
            maker.calls.append(("modify_order", order.client_id, price, size))
            return OrderResult(ok=False, error="ORDER_NOT_FOUND_FOR_MODIFY", retryable=True)

        maker.modify_order = dead
        taker.set_book("BTC", [(100010, 5)], [(100012, 5)])  # improvement amend
        await wait_until(lambda: len(calls(maker, "place_limit")) == 2)
    assert len(calls(maker, "modify_order")) == 1 and calls(maker, "cancel_order")[0][3] == o.client_id


async def test_fully_filled_quote_without_terminal_update_is_dropped():
    t, maker, taker, risk = setup(amend_ack_timeout_s=0.1, cancel_confirm_timeout_s=0.1)
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        # the fill arrives, the FILLED order frame is lost (stream gap): the order object stays OPEN
        maker._emit_fill(Fill("arcus", "BTC", Side.BUY, o.price, o.size, is_maker=True, client_id=o.client_id))
        await wait_until(lambda: len(calls(maker, "place_limit")) == 2, timeout=2)


async def test_stale_past_settlement_does_not_skip_hold_through_funding():
    """money-6: right after the hour the snapshot still names the past settlement; HOLDING must wait for the
    next one instead of ending at once."""
    fund = FundingStub(rm="0", rt="0.0001", next_ts=time.time() - 20)
    t, maker, taker, risk = setup(funding=fund, hold_through_funding=True, funding_settle_grace_s=0.0,
                                  target_notional=D("500"))
    async with running(t):
        await fill_next(t, maker)
        await wait_until(lambda: t.phase is Phase.HOLDING)
        await asyncio.sleep(0.2)
        assert t.phase is Phase.HOLDING and not resting(maker)
    assert t._hold_settlement is not None and t._hold_settlement > time.time()
    assert t._next_settle > time.time()


async def test_stale_funding_snapshot_keeps_direction():
    fund = FundingStub(rm="-0.0001", rt="0")  # maker LONG
    t, maker, taker, risk = setup(funding=fund, direction_mode="auto")
    async with running(t):
        await wait_until(lambda: t.direction == MAKER_LONG and resting(maker))
        fund.set(rm="0.0001", rt="0")  # would prefer SHORT ...
        fund.snap.maker.ts = fund.snap.taker.ts = time.time() - 3600  # ... but the data is an hour old
        await asyncio.sleep(0.1)
        assert t.direction == MAKER_LONG and t.pending_direction is None and t.phase is Phase.OPENING
        fund.set(rm="0.0001", rt="0")  # fresh again
        await wait_until(lambda: t.pending_direction == MAKER_SHORT or t.direction == MAKER_SHORT)


async def test_funding_window_covers_seconds_after_settlement():
    """money-5: the venues snapshot positions up to ~0.5 s after the hour; protection must extend past it."""
    hour = (int(time.time()) // 3600) * 3600.0
    fund = FundingStub(rm="0", rt="0.0001", next_ts=hour)  # not refreshed since the hour
    t, maker, taker, risk = setup(funding=fund, protect_funding_window_s=120)
    now = {"t": hour + 0.2}
    t.clock = lambda: now["t"]
    await t.tick()
    assert t._in_window and t._next_settle == hour + 3600
    now["t"] = hour + 3.5
    await t.tick()
    assert not t._in_window
    now["t"] = hour + 3600 - 60
    await t.tick()
    assert t._in_window


async def test_mid_upticks_do_not_cause_size_only_amends():
    """money-7: the mid-dependent clip shrinking by a step is not an oversize quote."""
    from farmer.config import MarketConfig as MC

    am = make_market("arcus", "KBONK", tick="0.0000001", step="1", min_size="1", min_notional="5")
    lm = make_market("lighter", "KBONK", tick="0.000001", step="1", min_size="1", min_notional="10")
    maker, taker = FakeExchange("arcus", [am]), FakeExchange("lighter", [lm])
    maker.set_book("KBONK", [("0.0029380", 1e7)], [("0.0029420", 1e7)])
    taker.set_book("KBONK", [("0.002940", 1e7)], [("0.002941", 1e7)])
    scfg = fast_strategy(direction_mode="long_maker", target_notional=D("1000"), order_notional=D("150"),
                         min_quote_lifetime_s=5.0, reprice_threshold_bps=D("5"), min_edge_bps=D("-3"))
    pair = PairSpec("KBONK", "KBONK", "KBONK", am, lm, scfg.for_market(MC(symbol="KBONK")))
    t = PairTrader(pair, maker, taker, scfg, FakeRisk(fast_risk()), None, None, rcfg=fast_risk())
    async with running(t):
        await wait_until(lambda: maker.resting("KBONK"))
        for i in range(1, 11):  # maker ask ticks up: mid +0.17 bps each, our bid price unchanged
            maker.set_book("KBONK", [("0.0029380", 1e7)], [(D("0.0029420") + D("0.0000001") * i, 1e7)])
            await asyncio.sleep(0.02)
    assert calls(maker, "modify_order") == [] and t.stats["protective"] == 0 and t.stats["resize"] == 0


async def test_maker_private_stream_down_pulls_quote_and_blocks_new_ones():
    """money-9 / async-quoting-without-fill-stream."""
    t, maker, taker, risk = setup()
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        maker.stream_ok = False
        await wait_until(lambda: o.status is OrderStatus.CANCELED)
        await asyncio.sleep(0.05)
        assert not resting(maker) and len(calls(maker, "place_limit")) == 1
        assert "maker private stream down" in t.block_reason
        maker.stream_ok = True
        await wait_until(lambda: resting(maker))


async def test_taker_private_stream_down_blocks_opening_only():
    t, maker, taker, risk = setup(target_notional=D("500"))
    taker.stream_ok = False
    async with running(t):
        await asyncio.sleep(0.1)
        assert not resting(maker) and "taker private stream down" in t.block_reason
        taker.stream_ok = True
        await fill_next(t, maker)
        taker.stream_ok = False
        await wait_until(lambda: t.phase is Phase.CLOSING and resting(maker))  # reducing quotes still allowed
        assert resting(maker)[0].reduce_only


async def test_cancel_overtaking_its_place_leaves_no_orphan():
    """async-dying-assume-gone-orphan: the place times out (UNKNOWN), its cancel comes back ORDER_NOT_FOUND
    (accepted), then the place is sequenced and goes OPEN: the trader must cancel it, not ignore it."""
    from farmer.models import Order

    t, maker, taker, risk = setup(cancel_confirm_timeout_s=0.05)
    real_place, real_cancel = maker.place_limit, maker.cancel_order
    state = {"late": None}

    async def place(symbol, side, size, price, *, post_only=True, reduce_only=False, client_id=None):
        if state["late"] is None:
            maker.calls.append(("place_limit", symbol, side, size, price, post_only, reduce_only))
            o = Order("arcus", symbol, side, price, size, client_id, order_id="late-1", status=OrderStatus.UNKNOWN,
                      post_only=post_only, reduce_only=reduce_only)
            state["late"] = o
            return OrderResult(ok=False, order=o, error="timeout (outcome unknown)", retryable=True)
        return await real_place(symbol, side, size, price, post_only=post_only, reduce_only=reduce_only,
                                client_id=client_id)

    async def cancel(symbol, *, order_id=None, client_id=None):
        late = state["late"]
        if late is not None and client_id == late.client_id and late.status is OrderStatus.UNKNOWN:
            maker.calls.append(("cancel_order", symbol, order_id, client_id))
            return True  # ORDER_NOT_FOUND: the place is not sequenced yet
        if late is not None and client_id == late.client_id:
            maker.calls.append(("cancel_order", symbol, order_id, client_id))
            late.status = OrderStatus.CANCELED
            maker._emit_order(late)
            return True
        return await real_cancel(symbol, order_id=order_id, client_id=client_id)

    maker.place_limit, maker.cancel_order = place, cancel
    async with running(t):
        await wait_until(lambda: state["late"] is not None)
        await asyncio.sleep(0.2)  # cancel 'accepted', entry assumed gone
        late = state["late"]
        late.status = OrderStatus.OPEN  # ... and now the place is sequenced
        maker._emit_order(late)
        await wait_until(lambda: late.status is OrderStatus.CANCELED, timeout=2)
        await wait_until(lambda: resting(maker))
    assert t.stats["cancels"] >= 1


async def test_cancel_accepted_but_order_still_open_is_retried_then_cancel_all():
    t, maker, taker, risk = setup(cancel_confirm_timeout_s=0.05)
    real_cancel = maker.cancel_order
    lost = {"on": False}

    async def cancel(symbol, *, order_id=None, client_id=None):
        if lost["on"]:
            maker.calls.append(("cancel_order", symbol, order_id, client_id))
            return True  # accepted, but the venue never ends the order
        return await real_cancel(symbol, order_id=order_id, client_id=client_id)

    maker.cancel_order = cancel
    async with running(t):
        await wait_until(lambda: resting(maker))
        o = resting(maker)[0]
        lost["on"] = True
        risk.halt("test")  # -> cancel the quote
        await wait_until(lambda: calls(maker, "cancel_all"), timeout=2)
        assert o.status is OrderStatus.CANCELED and len(calls(maker, "cancel_order")) == 3
        lost["on"] = False


async def test_open_order_sweep_cancels_untracked_quote():
    from farmer.models import Order

    t, maker, taker, risk = setup()
    async with running(t):
        await wait_until(lambda: resting(maker))
        cur = resting(maker)[0]
        stray = (await maker.place_limit("BTC", Side.BUY, D("0.005"), D("99000"), client_id="fqdeadbeef1")).order
        foreign = (await maker.place_limit("BTC", Side.BUY, D("0.005"), D("98000"), client_id="manual-1")).order
        snapshot = [Order(o.venue, o.symbol, o.side, o.price, o.size, o.client_id, order_id=o.order_id,
                          status=OrderStatus.OPEN) for o in (cur, stray, foreign)]  # REST rows (copies)
        t.handle_open_orders(snapshot)
        await wait_until(lambda: stray.status is OrderStatus.CANCELED)
        await asyncio.sleep(0.05)
        assert cur.status is OrderStatus.OPEN and foreign.status is OrderStatus.OPEN
        t.handle_open_orders(snapshot)  # a stale REST row of an order already seen terminal: ignored
        assert stray.client_id not in t._dying


async def test_basis_ignores_own_quote_at_the_touch():
    """money-14: our own resting quote improving the maker touch must not move the basis estimate."""
    t, maker, taker, risk = setup(use_basis=True, basis_warmup_s=0)
    async with running(t):
        maker.set_book("BTC", [(99990, 5)], [(100010, 5)])  # first basis sample
        await wait_until(lambda: resting(maker) and resting(maker)[0].status is OrderStatus.OPEN)
        o = resting(maker)[0]
        tb = taker.books["BTC"]
        mb = maker.set_book("BTC", [(o.price, o.size), (99990, 5)], [(100010, 5)])  # venue book shows our bid
        t._update_basis(mb, tb)
        assert t.basis.last == ((D("99990") + D("100010")) / 2) / tb.mid - 1


async def test_stop_reports_unconfirmed_cancel():
    t, maker, taker, risk = setup(cancel_confirm_timeout_s=0.05)
    task = asyncio.create_task(t.run())
    await wait_until(lambda: resting(maker))

    async def no_cancel(symbol, *, order_id=None, client_id=None):
        return False

    async def boom(symbol=None):
        raise ConnectionError("down")

    maker.cancel_order, maker.cancel_all = no_cancel, boom
    assert await t.stop(cancel_quotes=True) is False
    await asyncio.wait_for(task, 2)


async def test_deferred_close_stays_deferred_just_after_settlement():
    """money-5: the snapshot's settlement passed 0.2 s ago: the deferred close must wait the post window."""
    fund = FundingStub(rm="0", rt="0.0001", next_ts=time.time() + 60)
    t, maker, taker, risk = setup(funding=fund, protect_funding_window_s=120, protect_funding_post_s=0.4,
                                  target_notional=D("500"))
    async with running(t):
        await fill_next(t, maker)
        await wait_until(lambda: t.phase is Phase.HOLDING)
        fund.set("0", "0.0001", next_ts=time.time() - 0.2)  # not refreshed since the settlement
        await asyncio.sleep(0.1)
        assert t._in_window and not resting(maker)
        await wait_until(lambda: t.phase is Phase.CLOSING and resting(maker), timeout=2)
        assert resting(maker)[0].reduce_only
