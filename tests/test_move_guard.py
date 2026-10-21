"""Price-move auto close-all: reference prices, warning, trigger, persistence and the engine round trip."""

from __future__ import annotations

import asyncio
from decimal import Decimal as D

import pytest

from farmer.config import AppConfig, MarketConfig, MonitoringConfig, RiskConfig
from farmer.persistence.store import Store
from farmer.risk.manager import HALT_MOVE, MOVE_REFS_META, RiskManager
from farmer.strategy.engine import Engine
from tests.fakes import FakeExchange, FakeNotifier, fast_risk, fast_strategy, make_market, wait_until


def venues(px="100"):
    v = {"arcus": FakeExchange("arcus", [make_market("arcus", "ETH")]),
         "lighter": FakeExchange("lighter", [make_market("lighter", "ETH")])}
    set_px(v, px)
    return v


def set_px(v, px):
    p = D(str(px))
    for ex in v.values():
        ex.set_book("ETH", [(p - D("0.1"), 10)], [(p + D("0.1"), 10)])


def setup(store=None, **cfg):
    v = venues()
    store = store or Store(":memory:")
    notifier = FakeNotifier()
    rm = RiskManager(RiskConfig(**cfg), v, store, notifier)
    return rm, v, store, notifier


async def _drain() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def test_reference_taken_on_first_exposure_and_persisted():
    rm, v, store, _ = setup()
    assert rm.check_price_move() is False and rm.move_reference("ETH") is None  # no exposure: nothing to watch
    rm.update_exposure("ETH", "arcus", D("150"))
    assert rm.check_price_move() is False
    assert rm.move_reference("ETH") == D("100")
    assert '"ETH"' in (store.get_meta(MOVE_REFS_META) or "")
    # a restart (new manager on the same database) keeps the reference: a restart must not reset the guard
    rm2 = RiskManager(RiskConfig(), v, store, FakeNotifier())
    assert rm2.move_reference("ETH") == D("100")
    # normal churn (position back to 0 and reopened) keeps it too
    rm.update_exposure("ETH", "arcus", D("0"))
    set_px(v, "110")
    rm.check_price_move()
    rm.update_exposure("ETH", "arcus", D("150"))
    rm.check_price_move()
    assert rm.move_reference("ETH") == D("100")


@pytest.mark.parametrize("px", ["119.1", "80.9"])  # either direction drains one venue
async def test_trigger_halts_and_runs_the_reducer(px):
    rm, v, store, notifier = setup(auto_close_move_frac=D("0.19"))
    calls = []

    async def reducer():
        calls.append(1)

    rm.set_reducer(reducer)
    rm.update_exposure("ETH", "lighter", D("150"))
    rm.check_price_move()
    set_px(v, "118")  # 18 %: below the threshold
    assert rm.check_price_move() is False and not rm.halted
    set_px(v, px)
    assert rm.check_price_move() is True
    await _drain()
    assert rm.halted and HALT_MOVE in rm.status()["halts"] and "ETH" in rm.halt_reason
    assert calls == [1]
    assert any(k == "auto_close_move" for _, k, _ in store.recent_events(20))
    assert any(m.startswith("HALT:") for m in notifier.messages)
    ok, why = rm.check_open("ETH", D("50"))
    assert not ok and "halted" in why
    # still exposed after the run: retried only after the cooldown
    assert rm.check_price_move() is True
    await _drain()
    assert calls == [1]
    rm.reduce_retry_s = 0.0
    rm.check_price_move()
    await _drain()
    assert calls == [1, 1]


async def test_warning_once_at_75_percent_and_rearmed():
    rm, v, _, notifier = setup(auto_close_move_frac=D("0.20"))
    rm.update_exposure("ETH", "arcus", D("150"))
    rm.check_price_move()
    set_px(v, "115.5")  # 15.5 % >= 0.75 x 20 %
    rm.check_price_move()
    rm.check_price_move()
    await _drain()
    warns = [m for m in notifier.messages if m.startswith("WARNING") and "ETH" in m]
    assert len(warns) == 1 and not rm.halted
    set_px(v, "105")  # back below 0.6 x threshold: re-armed
    rm.check_price_move()
    set_px(v, "84.5")
    rm.check_price_move()
    await _drain()
    assert len([m for m in notifier.messages if m.startswith("WARNING") and "ETH" in m]) == 2


async def test_disabled_and_reset():
    rm, v, store, _ = setup(auto_close_move_frac=D("0"))
    rm.update_exposure("ETH", "arcus", D("150"))
    set_px(v, "200")
    assert rm.check_price_move() is False and rm.move_reference("ETH") is None and not rm.halted
    rm.cfg.auto_close_move_frac = D("0.19")
    rm.check_price_move()
    assert rm.move_reference("ETH") == D("200")
    rm.reset_move_refs(["ETH"])
    assert rm.move_reference("ETH") is None and "ETH" not in (store.get_meta(MOVE_REFS_META) or "")
    assert rm.status()["price_moves"] == {}


async def test_status_reports_the_move():
    rm, v, *_ = setup()
    rm.update_exposure("ETH", "arcus", D("150"))
    rm.check_price_move()
    set_px(v, "105")
    rm.check_price_move()
    pm = rm.status()["price_moves"]["ETH"]
    assert pm["ref"] == D("100") and pm["move"] == D("0.05")
    assert rm.status()["auto_close_move_frac"] == D("0.19")


# ------------------------------------------------------------------ engine round trip
def build_engine(tmp_path, **risk_kw):
    maker = FakeExchange("arcus", [make_market("arcus", "ETH", tick="0.01", step="0.001", min_size="0.001",
                                               min_notional="5", market_id=2)])
    taker = FakeExchange("lighter", [make_market("lighter", "ETH", tick="0.01", step="0.001", min_size="0.002",
                                                 min_notional="10", market_id=0)])
    maker.set_book("ETH", [(2399.9, 50)], [(2400.1, 50)])
    taker.set_book("ETH", [(2400.0, 50)], [(2400.2, 50)])
    scfg = fast_strategy(markets=[MarketConfig(symbol="ETH")], target_notional=D("150"), order_notional=D("50"),
                         leverage=3, direction_mode="short_maker")
    cfg = AppConfig(strategy=scfg, risk=fast_risk(auto_close_move_frac=D("0.19"), **risk_kw),
                    monitoring=MonitoringConfig(report_interval_s=0.05))
    store = Store(tmp_path / "farmer.db")
    notifier = FakeNotifier()
    risk = RiskManager(cfg.risk, {"arcus": maker, "lighter": taker}, store, notifier, interval_s=0.02)
    eng = Engine(cfg, {"arcus": maker, "lighter": taker}, store, notifier, risk)
    risk.set_reducer(lambda: eng.close_all("taker", timeout_s=5))
    return eng, maker, taker, risk, store


async def test_engine_auto_close_all_on_price_move(tmp_path):
    eng, maker, taker, risk, store = build_engine(tmp_path)
    maker.positions["ETH"], taker.positions["ETH"] = D("-0.06"), D("0.06")  # an open hedged position
    run = asyncio.create_task(eng.run())  # runs the traders (they report exposure) and risk.run
    try:
        await wait_until(lambda: eng.started, timeout=5)
        await wait_until(lambda: risk.move_reference("ETH") is not None, timeout=5)
        ref = risk.move_reference("ETH")
        assert D("2399") < ref < D("2401")
        # ETH +20 %: the maker (Arcus) short loses on its venue -> close everything
        maker.set_book("ETH", [(2879.9, 50)], [(2880.1, 50)])
        taker.set_book("ETH", [(2880.0, 50)], [(2880.2, 50)])
        await wait_until(lambda: risk.halted and maker.positions.get("ETH") == 0 and taker.positions.get("ETH") == 0,
                         timeout=5)
        await wait_until(lambda: risk.move_reference("ETH") is None, timeout=5)  # flat -> reference dropped
        assert HALT_MOVE in risk.status()["halts"]
        assert risk.exposure("arcus") == 0 and risk.exposure("lighter") == 0
        mk = [c for c in maker.calls if c[0] == "place_market"]
        tk = [c for c in taker.calls if c[0] == "place_market"]
        assert [(c[2].value, c[5]) for c in mk] == [("BUY", True)]  # reduce-only buy back the short
        assert [(c[2].value, c[5]) for c in tk] == [("SELL", True)]
        await asyncio.sleep(0.1)
        assert not maker.resting("ETH")  # halted: no new quotes
    finally:
        eng.request_stop()
        await asyncio.wait_for(run, 10)


async def test_engine_start_flat_resets_a_stale_reference(tmp_path):
    eng, maker, taker, risk, store = build_engine(tmp_path)
    store.set_meta(MOVE_REFS_META, '{"ETH": {"price": "1000", "ts": 1}}')
    risk._move_refs = risk._load_move_refs()
    assert risk.move_reference("ETH") == D("1000")
    await eng.start()  # no positions on either venue: the old reference must not close a fresh start
    try:
        assert risk.move_reference("ETH") is None
    finally:
        await eng.shutdown()


# ------------------------------------------------------------------ review fixes
def mapped_venues(px="0.02"):
    """Pair BONK = Arcus KBONK / Lighter 1000BONK: the venues know only their own symbols."""
    v = {"arcus": FakeExchange("arcus", [make_market("arcus", "KBONK")]),
         "lighter": FakeExchange("lighter", [make_market("lighter", "1000BONK")])}
    set_mapped_px(v, px)
    return v


def set_mapped_px(v, px, venue=None):
    p = D(str(px))
    for name, sym in (("arcus", "KBONK"), ("lighter", "1000BONK")):
        if venue is None or venue == name:
            v[name].set_book(sym, [(p * D("0.999"), 10**6)], [(p * D("1.001"), 10**6)])


async def test_guard_reads_books_under_the_venue_symbols():
    v = mapped_venues()
    rm = RiskManager(RiskConfig(), v, Store(":memory:"), FakeNotifier())
    calls = []

    async def reducer():
        calls.append(1)

    rm.set_reducer(reducer)
    rm.set_pair_symbols({"BONK": {"arcus": "KBONK", "lighter": "1000BONK"}})
    rm.update_exposure("BONK", "arcus", D("150"))
    rm.check_price_move()
    assert rm.move_reference("BONK") == D("0.02")
    set_mapped_px(v, "0.03")
    assert rm.check_price_move() is True and HALT_MOVE in rm.status()["halts"]
    await _drain()
    assert calls == [1]


async def test_engine_guard_on_a_mapped_pair(tmp_path):
    maker = FakeExchange("arcus", [make_market("arcus", "KBONK", tick="0.0001", step="1", min_size="1",
                                               min_notional="5", market_id=2)])
    taker = FakeExchange("lighter", [make_market("lighter", "1000BONK", tick="0.0001", step="1", min_size="1",
                                                 min_notional="5", market_id=0)])
    maker.set_book("KBONK", [(0.0199, 10**6)], [(0.0201, 10**6)])
    taker.set_book("1000BONK", [(0.0199, 10**6)], [(0.0201, 10**6)])
    scfg = fast_strategy(markets=[MarketConfig(symbol="BONK")], target_notional=D("150"), order_notional=D("50"),
                         leverage=3, direction_mode="short_maker")
    cfg = AppConfig(strategy=scfg, risk=fast_risk(auto_close_move_frac=D("0.19")),
                    monitoring=MonitoringConfig(report_interval_s=0.05))
    store = Store(tmp_path / "farmer.db")
    risk = RiskManager(cfg.risk, {"arcus": maker, "lighter": taker}, store, FakeNotifier(), interval_s=0.02)
    eng = Engine(cfg, {"arcus": maker, "lighter": taker}, store, FakeNotifier(), risk)
    risk.set_reducer(lambda: eng.close_all("taker", timeout_s=5))
    maker.positions["KBONK"], taker.positions["1000BONK"] = D("-7500"), D("7500")
    run = asyncio.create_task(eng.run())
    try:
        await wait_until(lambda: risk.move_reference("BONK") is not None, timeout=5)
        maker.set_book("KBONK", [(0.0299, 10**6)], [(0.0301, 10**6)])
        taker.set_book("1000BONK", [(0.0299, 10**6)], [(0.0301, 10**6)])
        await wait_until(lambda: risk.halted and maker.positions.get("KBONK") == 0
                         and taker.positions.get("1000BONK") == 0, timeout=5)
        assert HALT_MOVE in risk.status()["halts"]
    finally:
        eng.request_stop()
        await asyncio.wait_for(run, 10)


async def test_no_price_for_an_exposed_market_is_reported(caplog):
    rm, v, store, _ = setup()
    rm.update_exposure("XRP", "arcus", D("150"))  # no venue has an XRP book
    for _ in range(10):
        rm.check_price_move()
    assert len([r for r in caplog.records if "price-move guard cannot see" in r.getMessage()]) == 1
    assert len([k for _, k, _ in store.recent_events(50) if k == "price_move_blind"]) == 1


async def test_frozen_book_on_one_venue_does_not_hide_the_move():
    rm, v, _, _ = setup()
    rm.update_exposure("ETH", "arcus", D("150"))
    rm.check_price_move()
    assert rm.move_reference("ETH") == D("100")
    v["lighter"].books["ETH"].ts -= 3600  # frozen at 100 (disconnected / off-hours): no price any more
    v["arcus"].set_book("ETH", [(D("119.4"), 10)], [(D("119.6"), 10)])  # +19.5 % on the live venue
    assert rm.check_price_move() is True and rm.halted  # the mean of both mids (+9.75 %) would not trigger
    assert "arcus" in rm.halt_reason


async def test_move_is_measured_against_each_venues_own_reference():
    rm, v, store, _ = setup()
    v["arcus"].set_book("ETH", [(D("101.9"), 10)], [(D("102.1"), 10)])  # a 2 % structural basis
    rm.update_exposure("ETH", "arcus", D("150"))
    rm.check_price_move()
    assert rm.move_reference("ETH") == D("101")
    for name, ex in v.items():  # both venues +18.5 %: below the threshold on each
        base = D("102") if name == "arcus" else D("100")
        mid = base * D("1.185")
        ex.set_book("ETH", [(mid - D("0.01"), 10)], [(mid + D("0.01"), 10)])
    assert rm.check_price_move() is False and not rm.halted
    rm2 = RiskManager(RiskConfig(), v, store, FakeNotifier())  # per-venue references survive a restart
    assert rm2._move_refs["ETH"].venues == {"arcus": D("102"), "lighter": D("100")}


async def test_incomplete_close_is_retried_while_exposed_after_the_price_came_back():
    rm, v, _, _ = setup()
    calls = []

    async def reducer():  # the close-all times out: positions stay open
        calls.append(1)

    rm.set_reducer(reducer)
    rm.reduce_retry_s = 0.0
    rm.update_exposure("ETH", "arcus", D("150"))
    rm.check_price_move()
    set_px(v, "120")
    assert rm.check_price_move() is True
    await _drain()
    set_px(v, "110")  # back below the threshold, still exposed: keep closing
    assert rm.check_price_move() is True
    await _drain()
    assert calls == [1, 1] and rm.halted
    rm.update_exposure("ETH", "arcus", D("0"))  # flat (close_all zeroes the exposure): done
    assert rm.check_price_move() is False
    await _drain()
    assert calls == [1, 1]


async def test_engine_retries_the_auto_close_after_a_failed_close(tmp_path):
    eng, maker, taker, risk, store = build_engine(tmp_path)
    risk.set_reducer(lambda: eng.close_all("taker", timeout_s=0.5))
    risk.reduce_retry_s = 0.1
    maker.positions["ETH"], taker.positions["ETH"] = D("-0.06"), D("0.06")
    run = asyncio.create_task(eng.run())
    try:
        await wait_until(lambda: risk.move_reference("ETH") is not None, timeout=5)
        for ex in (maker, taker):  # the venues reject market orders during the first close
            ex.fail_next(10**6, retryable=True, kinds={"market"})
        maker.set_book("ETH", [(2879.9, 50)], [(2880.1, 50)])
        taker.set_book("ETH", [(2880.0, 50)], [(2880.2, 50)])
        await wait_until(lambda: risk.halted and risk._reduce_task is not None, timeout=5)
        maker.set_book("ETH", [(2639.9, 50)], [(2640.1, 50)])  # back to +10 %
        taker.set_book("ETH", [(2640.0, 50)], [(2640.2, 50)])
        await wait_until(lambda: risk._reduce_task.done(), timeout=5)
        assert maker.positions["ETH"] == D("-0.06")  # the first close failed
        maker._fail_next = taker._fail_next = 0
        await wait_until(lambda: maker.positions.get("ETH") == 0 and taker.positions.get("ETH") == 0, timeout=5)
        assert HALT_MOVE in risk.status()["halts"]
    finally:
        eng.request_stop()
        await asyncio.wait_for(run, 10)


async def test_check_open_refuses_a_market_already_past_the_threshold():
    store = Store(":memory:")
    store.set_meta(MOVE_REFS_META, '{"ETH": {"price": "80", "ts": 1}}')  # stored by the previous session
    rm = RiskManager(RiskConfig(), venues("100"), store, FakeNotifier())
    ok, why = rm.check_open("ETH", D("50"))
    assert not ok and "price moved +25.00%" in why
    ok, _ = rm.check_open("SOL", D("50"))  # no reference: not affected
    assert ok
    rm.cfg.auto_close_move_frac = D("0")
    assert rm.check_open("ETH", D("50"))[0]


async def test_restart_past_the_threshold_opens_nothing(tmp_path):
    eng, maker, taker, risk, store = build_engine(tmp_path)
    risk.interval_s = 2.0  # the first trader ticks come before the next guard pass
    store.set_meta(MOVE_REFS_META, '{"ETH": {"price": "2000", "ts": 1}}')
    risk._move_refs = risk._load_move_refs()
    maker.positions["ETH"], taker.positions["ETH"] = D("-0.03"), D("0.03")  # half of the 150 USD target
    run = asyncio.create_task(eng.run())
    try:
        await wait_until(lambda: risk.halted, timeout=10)
        await wait_until(lambda: maker.positions.get("ETH") == 0 and taker.positions.get("ETH") == 0, timeout=5)
        opening = [c for c in maker.calls if c[0] == "place_limit" and not c[6]]
        assert opening == []
    finally:
        eng.request_stop()
        await asyncio.wait_for(run, 10)


async def test_maker_close_on_a_halted_engine_closes_with_iocs(tmp_path):
    eng, maker, taker, risk, store = build_engine(tmp_path)
    maker.positions["ETH"], taker.positions["ETH"] = D("-0.06"), D("0.06")
    run = asyncio.create_task(eng.run())
    try:
        await wait_until(lambda: eng.started, timeout=5)
        risk.halt("simulated: price move, reducer timed out", kind=HALT_MOVE)
        res = await asyncio.wait_for(eng.close_all("maker", timeout_s=30), 10)
        assert res["ETH"]["flat"] and maker.positions["ETH"] == 0 and taker.positions["ETH"] == 0
        assert not eng._trader_tasks  # the stopped traders were not restarted
    finally:
        eng.request_stop()
        await asyncio.wait_for(run, 10)


async def test_concurrent_close_alls_run_one_after_the_other(tmp_path):
    eng, maker, taker, risk, store = build_engine(tmp_path)
    maker.positions["ETH"], taker.positions["ETH"] = D("-0.06"), D("0.06")
    await eng.start()
    try:
        for ex in (maker, taker):
            ex.fail_next(10**6, retryable=True, kinds={"market"})
        first = asyncio.create_task(eng.close_all("taker", timeout_s=0.4))
        await asyncio.sleep(0.05)
        second = asyncio.create_task(eng.close_all("taker", timeout_s=1.0))
        assert not (await first)["ETH"]["flat"]
        await asyncio.sleep(0.05)
        # the second close owns the hedgers now: the first one's end must not leave reduce-only mode under it
        assert not second.done() and all(h.close_mode for h in eng.hedgers.values())
        maker._fail_next = taker._fail_next = 0
        assert (await second)["ETH"]["flat"]
        assert not any(h.close_mode for h in eng.hedgers.values())
    finally:
        await eng.shutdown()


def test_move_frac_must_be_a_fraction():
    assert RiskConfig(auto_close_move_frac=D("0.19")).auto_close_move_frac == D("0.19")
    assert RiskConfig(auto_close_move_frac=0).auto_close_move_frac == 0
    for bad in ("19", "1", "-0.1"):  # "19" meant 19 %, but would need a +1900 % move: silently off
        with pytest.raises(ValueError, match="auto_close_move_frac"):
            RiskConfig(auto_close_move_frac=D(bad))


def test_bad_stored_references_are_dropped_one_by_one():
    store = Store(":memory:")
    store.set_meta(MOVE_REFS_META, '{"ETH": {"price": "100", "ts": 5, "venues": {"arcus": "NaN", "lighter": "99"}},'
                                   ' "SOL": {"price": "NaN"}, "BTC": {"ts": 1}, "XRP": {"price": "Infinity"},'
                                   ' "DOGE": 5, "ADA": {"price": "-1"}, "LTC": {"price": "50", "ts": "x"}}')
    rm = RiskManager(RiskConfig(), venues(), store, FakeNotifier())
    assert set(rm._move_refs) == {"ETH", "LTC"}
    assert rm._move_refs["ETH"].venues == {"lighter": D("99")} and rm._move_refs["LTC"].ts == 0
    store.set_meta(MOVE_REFS_META, "[1, 2]")
    assert RiskManager(RiskConfig(), venues(), store, FakeNotifier())._move_refs == {}
