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
