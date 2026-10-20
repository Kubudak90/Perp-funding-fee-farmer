import asyncio
from decimal import Decimal as D

import pytest

from farmer.config import AppConfig, MarketConfig, MonitoringConfig
from farmer.models import FundingInfo, OrderStatus, Side
from farmer.persistence.store import Store
from farmer.strategy.engine import Engine
from farmer.strategy.trader import Phase
from tests.fakes import FakeExchange, FakeNotifier, FakeRisk, fast_risk, fast_strategy, make_market, wait_until


def build(tmp_path, *, markets=("BTC", "ETH"), risk_kw=None, **skw):
    maker = FakeExchange("arcus", [make_market("arcus", "BTC", tick="0.1", step="0.001", min_size="0.001",
                                               min_notional="5", market_id=1),
                                   make_market("arcus", "ETH", tick="0.01", step="0.01", min_size="0.01",
                                               min_notional="5", market_id=2)])
    taker = FakeExchange("lighter", [make_market("lighter", "BTC", tick="0.1", step="0.0001", min_size="0.002",
                                                 min_notional="10", market_id=1)])
    maker.set_book("BTC", [(99990, 5)], [(100010, 5)])
    taker.set_book("BTC", [(100000, 5)], [(100002, 5)])
    maker.set_book("ETH", [(2399, 5)], [(2401, 5)])
    maker.funding["BTC"] = FundingInfo("arcus", "BTC", D("0.0000125"))
    taker.funding["BTC"] = FundingInfo("lighter", "BTC", D("0.000012"))
    skw.setdefault("direction_mode", "long_maker")
    scfg = fast_strategy(markets=[MarketConfig(symbol=s) for s in markets], target_notional=D("1000"),
                         order_notional=D("500"), **skw)
    cfg = AppConfig(strategy=scfg, risk=fast_risk(**(risk_kw or {})),
                    monitoring=MonitoringConfig(report_interval_s=0.05))
    store = Store(tmp_path / "farmer.db")
    risk = FakeRisk(cfg.risk)
    notifier = FakeNotifier()
    eng = Engine(cfg, {"arcus": maker, "lighter": taker}, store, notifier, risk)
    return eng, maker, taker, risk, notifier, store


def resting(ex, sym="BTC"):
    return ex.resting(sym)


async def test_start_prepares_everything(tmp_path):
    eng, maker, taker, risk, notifier, store = build(tmp_path)
    stale = (await maker.place_limit("BTC", Side.BUY, D("0.001"), D("99000"), client_id="old1")).order
    maker.positions["BTC"] = D("0.01")  # pre-existing unhedged maker position
    armed = []

    async def arm(timeout_s):
        armed.append(timeout_s)

    maker.arm_dead_mans_switch = arm
    await eng.start()
    try:
        assert maker.connected and taker.connected
        assert [p.symbol for p in eng.pairs] == ["BTC"]
        assert any("ETH" in s and "not listed on lighter" in s for s in eng.skipped)
        assert maker.leverage == {"BTC": 5} and taker.leverage == {"BTC": 5}
        assert stale.status is OrderStatus.CANCELED
        assert ("subscribe_books", ("BTC",)) in maker.calls and ("subscribe_books", ("BTC",)) in taker.calls
        assert eng.funding.latest("BTC") is not None
        h = eng.hedgers["BTC"]
        await wait_until(lambda: h.taker_pos == D("-0.01"))  # startup delta hedged before any quoting
        assert not resting(maker)  # traders are not quoting after start() alone
        await wait_until(lambda: risk.running)
        assert armed == [eng.cfg.arcus.dead_mans_switch_s]
        st = eng.status()
        assert st["markets"]["BTC"]["maker_pos"] == D("0.01") and st["risk"]["halted"] is False
        assert set(st["venues"]) == {"arcus", "lighter"} and st["venues"]["arcus"]["budget_health"] == 1.0
        assert "hedger:BTC" in st["tasks"]
    finally:
        await eng.shutdown()
    assert not maker.connected and not taker.connected
    kinds = [k for _, k, _ in store.recent_events(50)]
    assert "pairs" in kinds and "shutdown" in kinds


async def test_run_cycle_and_shutdown_cancels_quotes(tmp_path):
    eng, maker, taker, risk, notifier, store = build(tmp_path)
    task = asyncio.create_task(eng.run())
    await wait_until(lambda: eng.started and resting(maker))
    t, h = eng.traders["BTC"], eng.hedgers["BTC"]
    for _ in range(2):  # 2 x 0.005 = target 0.01
        await wait_until(lambda: resting(maker) and resting(maker)[0].side is Side.BUY)
        maker.fill_resting(resting(maker)[0].client_id)
        await wait_until(lambda: h.is_hedged() and h.delta == 0)
    await wait_until(lambda: t.phase is Phase.CLOSING and resting(maker))
    maker.fill_resting(resting(maker)[0].client_id)
    await wait_until(lambda: h.is_hedged() and h.maker_pos == D("0.005"))
    await wait_until(lambda: resting(maker))
    q = resting(maker)[0]
    eng.request_stop()
    await asyncio.wait_for(task, 5)
    assert q.status is OrderStatus.CANCELED and not resting(maker)
    assert h.maker_pos + h.taker_pos == 0 and h.maker_pos == D("0.005")  # close_on_exit=False keeps the hedge
    assert not maker.connected and not taker.connected
    fills = store.summary(0)
    assert fills["arcus"].fills == 3 and fills["lighter"].fills == 3
    assert any("started" in m for m in notifier.messages)


async def test_close_all_taker_flattens_both_legs(tmp_path):
    eng, maker, taker, risk, notifier, store = build(tmp_path)
    maker.positions["BTC"], taker.positions["BTC"] = D("0.02"), D("-0.02")
    await eng.start()
    try:
        res = await eng.close_all("taker", timeout_s=3)
    finally:
        await eng.shutdown()
    assert res == {"BTC": {"maker_pos": D(0), "taker_pos": D(0), "flat": True}}
    mk = [c for c in maker.calls if c[0] == "place_market"]
    tk = [c for c in taker.calls if c[0] == "place_market"]
    assert [(c[2], c[3], c[5]) for c in mk] == [(Side.SELL, D("0.020"), True)]
    assert [(c[2], c[3], c[5]) for c in tk] == [(Side.BUY, D("0.0200"), True)]
    roles = sorted(r for (r,) in store._db.execute("SELECT role FROM fills").fetchall())
    assert roles == ["close", "close"]


async def test_close_all_maker_mode_post_only_until_flat(tmp_path):
    eng, maker, taker, risk, notifier, store = build(tmp_path)
    maker.positions["BTC"], taker.positions["BTC"] = D("0.01"), D("-0.01")
    await eng.start()

    async def filler():
        while True:
            for o in resting(maker):
                if o.status is OrderStatus.OPEN:
                    assert o.post_only and o.reduce_only and o.side is Side.SELL
                    maker.fill_resting(o.client_id)
            await asyncio.sleep(0.01)

    f = asyncio.create_task(filler())
    try:
        res = await eng.close_all("maker", timeout_s=3)
    finally:
        f.cancel()
        await eng.shutdown()
    assert res["BTC"]["flat"] and res["BTC"]["maker_pos"] == 0 and res["BTC"]["taker_pos"] == 0
    assert not [c for c in maker.calls if c[0] == "place_market"]  # no maker-venue taker orders
    assert eng.traders["BTC"].stats["maker_fills"] == 2


async def test_close_on_exit_flattens(tmp_path):
    eng, maker, taker, risk, notifier, store = build(tmp_path, risk_kw={"close_on_exit": True})
    maker.positions["BTC"], taker.positions["BTC"] = D("0.01"), D("-0.01")
    await eng.start()
    await eng.shutdown()
    assert maker.positions["BTC"] == 0 and taker.positions["BTC"] == 0
    assert any("close_all(taker) done" in m for m in notifier.messages)


async def test_no_pairs_raises(tmp_path):
    eng, maker, taker, *_ = build(tmp_path, markets=("ETH",))
    with pytest.raises(RuntimeError, match="no tradeable pairs"):
        await eng.start()
    assert eng.skipped and "ETH" in eng.skipped[0]
    await eng.shutdown()


async def test_supervisor_restarts_crashed_task(tmp_path, monkeypatch):
    import farmer.strategy.engine as engmod

    eng, maker, taker, risk, *_ = build(tmp_path)
    n = {"x": 0}
    real_run = risk.run

    async def flaky_run():
        n["x"] += 1
        if n["x"] == 1:
            raise RuntimeError("boom")
        await real_run()

    risk.run = flaky_run
    real_sleep = asyncio.sleep
    monkeypatch.setattr(engmod.asyncio, "sleep", lambda s, *a, **k: real_sleep(min(s, 0.01), *a, **k))
    await eng.start()
    try:
        await wait_until(lambda: n["x"] == 2 and risk.running)
        assert eng.status()["restarts"]["risk"] == 1
    finally:
        await eng.shutdown()


async def test_status_lines(tmp_path):
    eng, maker, taker, *_ = build(tmp_path)
    await eng.start()
    try:
        lines = eng.status_lines()
        assert lines and lines[0].startswith("BTC OPENING")
    finally:
        await eng.shutdown()


async def test_funding_monitor_maps_venue_symbols(tmp_path):
    from farmer.strategy.funding import FundingMonitor

    m = FakeExchange("arcus", [make_market("arcus", "KBONK")])
    t = FakeExchange("lighter", [make_market("lighter", "1000BONK")])
    m.funding["KBONK"] = FundingInfo("arcus", "KBONK", D("0.0000125"))
    t.funding["1000BONK"] = FundingInfo("lighter", "1000BONK", D("0.000012"))
    mon = FundingMonitor(m, t, ["KBONK"], refresh_s=1, taker_symbols={"KBONK": "1000BONK"})
    await mon.refresh()
    snap = mon.latest("KBONK")
    assert snap is not None and snap.taker.symbol == "1000BONK" and snap.best_direction == -1
    assert not mon.stopped
