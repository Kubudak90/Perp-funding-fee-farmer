"""RiskManager: pre-trade checks, circuit breaker, margin, daily loss (no network)."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal as D

from farmer.config import RiskConfig
from farmer.models import Balance, BookLevel, Fill, FundingPayment, OrderBook, Side
from farmer.persistence.store import Store
from farmer.risk.manager import RiskManager, utc_midnight
from tests.fakes import FakeExchange, make_market


class FakeNotifier:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, text: str) -> None:
        self.sent.append(text)


class NoCredsExchange(FakeExchange):
    @property
    def has_credentials(self) -> bool:
        return False


def book(venue: str, bid, ask, *, age: float = 0.0, symbol: str = "BTC") -> OrderBook:
    return OrderBook(venue=venue, symbol=symbol, bids=[BookLevel(D(str(bid)), D("1"))] if bid is not None else [],
                     asks=[BookLevel(D(str(ask)), D("1"))] if ask is not None else [], ts=time.time() - age)


def setup(**cfg):
    venues = {
        "arcus": FakeExchange("arcus", [make_market("arcus", "BTC")]),
        "lighter": FakeExchange("lighter", [make_market("lighter", "BTC")]),
    }
    store = Store(":memory:")
    notifier = FakeNotifier()
    rm = RiskManager(RiskConfig(**cfg), venues, store, notifier)
    return rm, venues, store, notifier


async def _drain() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


# ------------------------------------------------------------------ check_quote
async def test_check_quote_ok():
    rm, *_ = setup()
    ok, why = rm.check_quote("BTC", book("arcus", 100, 100.1), book("lighter", 100, 100.1), D("1"), D("5"))
    assert ok and why == ""


async def test_check_quote_missing_and_one_sided_books():
    rm, *_ = setup()
    ok, why = rm.check_quote("BTC", None, book("lighter", 100, 101))
    assert not ok and "no maker book" in why
    ok, why = rm.check_quote("BTC", book("arcus", 100, 101), None)
    assert not ok and "no taker book" in why
    ok, why = rm.check_quote("BTC", book("arcus", 100, None), book("lighter", 100, 101))
    assert not ok and "one-sided" in why


async def test_check_quote_stale_books():
    rm, *_ = setup(stale_book_s=5.0)
    ok, why = rm.check_quote("BTC", book("arcus", 100, 101, age=6), book("lighter", 100, 101))
    assert not ok and "stale maker" in why
    ok, why = rm.check_quote("BTC", book("arcus", 100, 101), book("lighter", 100, 101, age=7))
    assert not ok and "stale taker" in why
    ok, _ = rm.check_quote("BTC", book("arcus", 100, 101, age=4), book("lighter", 100, 101, age=4))
    assert ok


async def test_check_quote_divergence_explicit_and_computed():
    rm, *_ = setup(max_divergence_bps=D("60"))
    m, t = book("arcus", 100, 100.1), book("lighter", 100, 100.1)
    assert not rm.check_quote("BTC", m, t, divergence_bps=D("61"))[0]
    ok, why = rm.check_quote("BTC", m, t, divergence_bps=D("-61"))
    assert not ok and "diverge" in why
    assert rm.check_quote("BTC", m, t, divergence_bps=D("60"))[0]
    # None -> computed from the book mids: 100.05 vs 99.25 ~ 80 bps
    ok, why = rm.check_quote("BTC", m, book("lighter", 99.2, 99.3))
    assert not ok and "diverge" in why
    assert rm.check_quote("BTC", m, book("lighter", 99.9, 100.0))[0]  # ~10 bps


async def test_check_quote_basis_deviation():
    rm, *_ = setup(max_basis_dev_bps=D("5"))
    m, t = book("arcus", 100, 100.1), book("lighter", 100, 100.1)
    ok, why = rm.check_quote("BTC", m, t, basis_dev_bps=D("5.01"))
    assert not ok and "basis" in why
    assert rm.check_quote("BTC", m, t, basis_dev_bps=D("4.99"))[0]


async def test_check_quote_halted_and_error_count():
    rm, *_ = setup(max_consecutive_errors=3)
    m, t = book("arcus", 100, 100.1), book("lighter", 100, 100.1)
    rm._errors["lighter"] = 3  # counter at the limit without the halt (e.g. halt already cleared elsewhere)
    ok, why = rm.check_quote("BTC", m, t)
    assert not ok and "consecutive errors" in why
    rm._errors.clear()
    rm.halt("manual")
    ok, why = rm.check_quote("BTC", m, t)
    assert not ok and "halted" in why and "manual" in why


# ------------------------------------------------------------------ check_open / margin
async def test_check_open_ok_and_exposure_limit():
    rm, venues, *_ = setup(max_total_notional=D("1000"))
    assert rm.check_open("BTC", D("1000")) == (True, "")
    rm.update_exposure("BTC", "arcus", D("-600"))  # abs() applied
    rm.update_exposure("ETH", "arcus", D("300"))
    assert rm.exposure("arcus") == D("900")
    assert rm.check_open("SOL", D("100"))[0]
    ok, why = rm.check_open("SOL", D("100.01"))
    assert not ok and "arcus" in why and "max_total_notional" in why
    rm.update_exposure("BTC", "arcus", D("0"))
    assert rm.check_open("SOL", D("700"))[0]


async def test_check_open_margin_ratio_and_must_reduce():
    rm, venues, *_ = setup(max_margin_ratio=D("0.6"), reduce_margin_ratio=D("0.8"))
    venues["lighter"].balance = Balance("lighter", equity=D("1000"), available=D("100"), maintenance_margin=D("599"))
    await rm.refresh_balances()
    assert rm.balance("lighter").maintenance_margin == D("599")
    assert rm.check_open("BTC", D("10"))[0] and not rm.must_reduce()
    venues["lighter"].balance = Balance("lighter", equity=D("1000"), available=D("0"), maintenance_margin=D("600"))
    await rm.refresh_balances()
    ok, why = rm.check_open("BTC", D("10"))
    assert not ok and "lighter" in why and "margin ratio" in why
    assert not rm.must_reduce()
    venues["lighter"].balance = Balance("lighter", equity=D("1000"), available=D("0"), maintenance_margin=D("800"))
    await rm.refresh_balances()
    assert rm.must_reduce()
    # equity <= 0 with margin in use -> ratio 1
    venues["lighter"].balance = Balance("lighter", equity=D("-5"), available=D("0"), maintenance_margin=D("1"))
    await rm.refresh_balances()
    assert rm.must_reduce() and not rm.check_open("BTC", D("1"))[0]


async def test_check_open_venue_refuses_and_halted():
    rm, venues, *_ = setup()
    venues["lighter"].can_increase_exposure = lambda: (False, "standard account limit 52002")  # type: ignore
    ok, why = rm.check_open("BTC", D("10"))
    assert not ok and "52002" in why
    venues["lighter"].can_increase_exposure = lambda: (_ for _ in ()).throw(RuntimeError("x"))  # type: ignore
    assert not rm.check_open("BTC", D("10"))[0]
    del venues["lighter"].can_increase_exposure
    assert rm.check_open("BTC", D("10"))[0]
    rm.halt("x")
    ok, why = rm.check_open("BTC", D("10"))
    assert not ok and "halted" in why


async def test_check_open_blocks_on_stale_balance():
    now = [1_000_000.0]
    venues = {"arcus": FakeExchange("arcus")}
    rm = RiskManager(RiskConfig(), venues, clock=lambda: now[0], interval_s=10)
    assert rm.check_open("BTC", D("1"))[0]  # unknown balance does not block
    await rm.refresh_balances()
    assert rm.check_open("BTC", D("1"))[0]
    now[0] += rm.balance_max_age_s + 1
    ok, why = rm.check_open("BTC", D("1"))
    assert not ok and "stale" in why


async def test_refresh_balances_errors_and_creds():
    venues = {"arcus": FakeExchange("arcus"), "lighter": NoCredsExchange("lighter"), "bad": FakeExchange("bad")}

    async def boom():
        raise RuntimeError("HTTP 500")

    venues["bad"].fetch_balance = boom  # type: ignore[method-assign]
    rm = RiskManager(RiskConfig(), venues)
    await rm.refresh_balances()
    assert rm.balance("arcus") is not None
    assert rm.balance("lighter") is None  # no credentials -> never polled
    assert rm.balance("bad") is None and rm.status()["balance_errors"]["bad"] == 1
    assert not rm.halted


# ------------------------------------------------------------------ circuit breaker / halt
async def test_circuit_breaker_and_success_reset():
    rm, _, store, notifier = setup(max_consecutive_errors=3)
    rm.record_error("lighter", "e1")
    rm.record_error("lighter", "e2")
    rm.record_success("lighter")
    rm.record_error("lighter", "e3")
    rm.record_error("lighter", "e4")
    assert not rm.halted
    rm.record_error("arcus", "a1")  # per venue
    rm.record_error("lighter", "boom")
    assert rm.halted and "circuit breaker" in rm.halt_reason and "lighter" in rm.halt_reason and "boom" in rm.halt_reason
    await _drain()
    assert len(notifier.sent) == 1 and notifier.sent[0].startswith("HALT")
    assert store.recent_events(1)[0][1] == "halt"


async def test_halt_idempotent_and_resume():
    rm, _, store, notifier = setup(max_consecutive_errors=2)
    rm.halt("first")
    rm.halt("second")
    assert rm.halt_reason == "first"
    await _drain()
    assert notifier.sent == ["HALT: first"]
    assert [k for _, k, _ in store.recent_events()] == ["halt"]
    rm.record_error("arcus", "x")
    rm.resume()
    assert not rm.halted and rm.halt_reason is None and rm.status()["errors"] == {}
    assert store.recent_events(1)[0][1] == "resume"
    await _drain()
    assert notifier.sent[-1].startswith("RESUMED")
    rm.resume()  # no-op when not halted
    assert len(notifier.sent) == 2


def test_halt_without_running_loop_does_not_raise():
    rm = RiskManager(RiskConfig(), {}, None, FakeNotifier())
    rm.halt("sync context")
    assert rm.halted


async def test_halt_survives_store_and_notifier_failures():
    class BadStore:
        def record_event(self, *a, **k):
            raise RuntimeError("disk full")

    class BadNotifier:
        async def send(self, text):
            raise RuntimeError("telegram down")

    rm = RiskManager(RiskConfig(), {}, BadStore(), BadNotifier())  # type: ignore[arg-type]
    rm.halt("x")
    await _drain()
    assert rm.halted


# ------------------------------------------------------------------ daily loss
def _fill(venue, side, price, size, ts, tid, fee="0", maker=True):
    return Fill(venue, "BTC", side, D(str(price)), D(str(size)), fee=D(fee), is_maker=maker, trade_id=tid, ts=ts)


async def test_daily_loss_halts_with_marks_from_books():
    rm, venues, store, notifier = setup(max_daily_loss=D("100"))
    today = time.time()
    # yesterday's big loss is ignored
    yday = utc_midnight(today) - 3600
    store.record_fill(_fill("arcus", Side.BUY, 200, 10, yday, "y1"), "maker")
    store.record_fill(_fill("lighter", Side.SELL, 100, 10, yday, "y2", maker=False), "hedge")
    # today: hedged pair losing 50 + fees 10
    store.record_fill(_fill("arcus", Side.BUY, 1005, 1, today, "a1", fee="5"), "maker")
    store.record_fill(_fill("lighter", Side.SELL, 1000, 1, today, "l1", fee="5", maker=False), "hedge")
    venues["arcus"].set_book("BTC", [(990, 1)], [(991, 1)])
    venues["lighter"].set_book("BTC", [(989, 1)], [(990, 1)])
    # each leg at its own venue's mid: -5 entry spread - 10 fees + (+1 x 990.5 - 1 x 989.5) = -14
    assert rm.check_daily_loss() == D("-14")
    assert not rm.halted
    store.record_fill(_fill("arcus", Side.BUY, 1100, 1, today, "a2"), "maker")  # unhedged leg
    pnl = rm.check_daily_loss()  # cash -1115 + 2 x 990.5 (arcus) - 1 x 989.5 (lighter)
    assert pnl == D("-1115") + 2 * D("990.5") - D("989.5")
    assert rm.halted and "daily loss" in rm.halt_reason
    await _drain()
    assert notifier.sent and "daily loss" in notifier.sent[0]
    assert rm.status()["daily_pnl"] == pnl


async def test_daily_loss_includes_funding_and_skips_without_mark():
    rm, venues, store, _ = setup(max_daily_loss=D("10"))
    today = time.time()
    store.record_fill(_fill("arcus", Side.BUY, 100, 1, today, "a1"), "maker")
    assert rm.check_daily_loss() is None  # net +1 BTC and no book -> cannot value -> no fake halt
    assert not rm.halted
    venues["lighter"].set_book("BTC", [(99, 1)], [(101, 1)])
    assert rm.check_daily_loss() == D("0")
    store.record_funding(FundingPayment("lighter", "BTC", D("-10.5"), ts=today))
    assert rm.check_daily_loss() == D("-10.5") and rm.halted


async def test_daily_loss_disabled_without_store():
    rm = RiskManager(RiskConfig(), {"arcus": FakeExchange("arcus")})
    assert rm.check_daily_loss() is None


# ------------------------------------------------------------------ run loop / status
async def test_run_loop_polls_balances_and_stops():
    rm, venues, store, _ = setup()
    rm.interval_s = 0.01
    calls = {"n": 0}
    orig = venues["arcus"].fetch_balance

    async def counting():
        calls["n"] += 1
        if calls["n"] == 2:
            raise ConnectionError("blip")
        return await orig()

    venues["arcus"].fetch_balance = counting  # type: ignore[method-assign]
    task = asyncio.create_task(rm.run())
    t0 = time.monotonic()
    while calls["n"] < 4 and time.monotonic() - t0 < 2:
        await asyncio.sleep(0.005)
    rm.stop()
    await asyncio.wait_for(task, 1.0)
    assert calls["n"] >= 4 and rm.balance("arcus") is not None and rm.balance("lighter") is not None


async def test_status_shape():
    rm, venues, *_ = setup()
    rm.update_exposure("BTC", "arcus", D("500"))
    await rm.refresh_balances()
    rm.record_error("lighter", "oops")
    st = rm.status()
    assert st["halted"] is False and st["halt_reason"] is None
    assert st["exposure"]["arcus"]["total"] == D("500") and st["exposure"]["arcus"]["by_symbol"] == {"BTC": D("500")}
    assert st["balances"]["arcus"]["equity"] == D("10000") and st["balances"]["arcus"]["margin_ratio"] == 0
    assert st["errors"] == {"lighter": 1} and st["last_errors"] == {"lighter": "oops"}
    assert st["must_reduce"] is False


def test_utc_midnight():
    assert utc_midnight(86400 * 3 + 5) == 86400 * 3
    assert utc_midnight(86400 * 3) == 86400 * 3


async def test_daily_loss_matches_store_pnl_and_resets_at_midnight():
    now = [utc_midnight(time.time()) - 60.0]  # 23:59 yesterday
    venues = {"arcus": FakeExchange("arcus", [make_market("arcus", "BTC")])}
    store = Store(":memory:")
    rm = RiskManager(RiskConfig(max_daily_loss=D("50")), venues, store, clock=lambda: now[0])
    venues["arcus"].set_book("BTC", [(99, 1)], [(101, 1)])
    store.record_fill(_fill("arcus", Side.BUY, 160, 1, now[0] - 30, "a1"), "maker")  # -60 at mark 100
    assert rm.check_daily_loss() == store.pnl(utc_midnight(now[0]), {"BTC": D("100")}) == D("-60")
    assert rm.halted
    rm.resume()
    now[0] += 120  # new UTC day: yesterday's loss no longer counts
    assert rm.check_daily_loss() == D("0") and not rm.halted


# ------------------------------------------------------------------ free collateral / margin reduction / equity drift
async def test_check_open_requires_free_collateral_for_the_clip():
    """The hedge venue must be able to post the clip's initial margin (with a buffer) BEFORE the maker leg opens;
    the margin ratio alone stays far below its limit while free collateral runs out."""
    venues = {"arcus": FakeExchange("arcus"), "lighter": FakeExchange("lighter")}
    rm = RiskManager(RiskConfig(min_free_collateral_mult=D("1.5")), venues, leverage={"BTC": 5}, default_leverage=2)
    venues["lighter"].balance = Balance("lighter", equity=D("1000"), available=D("149"), maintenance_margin=D("60"))
    await rm.refresh_balances()
    assert rm.balance("lighter").margin_ratio < D("0.1")
    ok, why = rm.check_open("BTC", D("500"))  # 500 / 5x * 1.5 = 150 > 149
    assert not ok and "lighter" in why and "free collateral" in why
    assert rm.check_open("BTC", D("490"))[0]  # 147 <= 149
    assert not rm.check_open("ETH", D("200"))[0]  # unknown symbol -> default 2x: 150 > 149
    assert rm.check_open("ETH", D("200"), leverage=5)[0]  # explicit leverage wins
    rm.cfg = RiskConfig(min_free_collateral_mult=D("0"))  # 0 disables the check
    assert rm.check_open("BTC", D("5000"))[0]


def test_default_margin_limits_leave_real_headroom_before_liquidation():
    cfg = RiskConfig()
    # liquidation at 1.0; reducing must start while equity is still >= 2.5 x maintenance
    assert cfg.reduce_margin_ratio <= D("0.4") and cfg.max_margin_ratio < cfg.reduce_margin_ratio


async def test_margin_reduce_halts_and_runs_the_reducer_once_until_cooldown():
    rm, venues, store, notifier = setup(reduce_margin_ratio=D("0.4"))
    calls = []
    gate = asyncio.Event()

    async def reducer():
        calls.append(1)
        await gate.wait()

    rm.set_reducer(reducer)
    await rm.refresh_balances()
    assert rm.check_margin() is False and calls == [] and not rm.halted
    venues["lighter"].balance = Balance("lighter", equity=D("1000"), available=D("0"), maintenance_margin=D("450"))
    await rm.refresh_balances()
    assert rm.check_margin() is True
    await _drain()
    assert rm.halted and "reduce_margin_ratio" in rm.halt_reason and "lighter 0.450" in rm.halt_reason
    assert calls == [1] and rm.status()["reducing"]
    assert rm.check_margin() and calls == [1]  # still running: not started twice
    gate.set()
    await _drain()
    assert not rm.status()["reducing"]
    assert rm.check_margin() and calls == [1]  # cooldown after a run
    rm.reduce_retry_s = 0.0
    assert rm.check_margin()
    await _drain()
    assert calls == [1, 1]  # margin still high after the cooldown: reduce again
    gate.clear()
    rm.reduce_retry_s = 0.0
    rm.check_margin()
    await _drain()
    task = rm._reduce_task
    rm.stop()  # shutdown cancels a running reduction
    await _drain()
    assert task is not None and task.cancelled()
    assert any(k == "margin_reduce" for _, k, _ in store.recent_events(20))


async def test_run_loop_triggers_margin_reduction():
    rm, venues, *_ = setup()
    rm.interval_s = 0.01
    done = asyncio.Event()

    async def reducer():
        done.set()

    rm.set_reducer(reducer)
    venues["arcus"].balance = Balance("arcus", equity=D("100"), available=D("0"), maintenance_margin=D("90"))
    task = asyncio.create_task(rm.run())
    await asyncio.wait_for(done.wait(), 1.0)
    rm.stop()
    await asyncio.wait_for(task, 1.0)
    assert rm.halted


async def test_equity_divergence_alert_on_transitions():
    rm, venues, store, notifier = setup(max_equity_divergence_frac=D("0.3"))
    await rm.refresh_balances()
    assert not rm.status()["equity_diverged"]
    venues["lighter"].balance = Balance("lighter", equity=D("6000"), available=D("6000"))
    await rm.refresh_balances()
    await rm.refresh_balances()  # no repeat while still diverged
    await _drain()
    assert rm.status()["equity_diverged"]
    assert len([m for m in notifier.sent if "equities diverge" in m]) == 1 and "lighter 6000.00" in notifier.sent[0]
    venues["lighter"].balance = Balance("lighter", equity=D("9000"), available=D("9000"))
    await rm.refresh_balances()
    assert not rm.status()["equity_diverged"]
    assert not rm.halted  # a warning, not a halt


async def test_daily_loss_values_each_leg_at_its_own_venue_mid():
    """A hedged pair entered across the structural basis (Arcus ~10 bps below Lighter) is not a loss."""
    rm, venues, store, _ = setup(max_daily_loss=D("5"))
    now = time.time()
    store.record_fill(_fill("arcus", Side.SELL, "99.9", 100, now, "a"), "maker")  # maker SHORT at the Arcus mid
    store.record_fill(_fill("lighter", Side.BUY, "100.0", 100, now, "l", maker=False), "hedge")
    venues["arcus"].set_book("BTC", [("99.89", 1)], [("99.91", 1)])
    venues["lighter"].set_book("BTC", [("99.99", 1)], [("100.01", 1)])
    assert rm.check_daily_loss() == 0 and not rm.halted  # common mark: -10 USD of phantom basis "loss" -> halt


async def test_daily_loss_async_reads_in_a_worker_thread(tmp_path):
    import threading

    store = Store(tmp_path / "r.db")
    venues = {"arcus": FakeExchange("arcus", [make_market("arcus", "BTC")])}
    rm = RiskManager(RiskConfig(max_daily_loss=D("50")), venues, store)
    venues["arcus"].set_book("BTC", [(99, 1)], [(101, 1)])
    store.record_fill(_fill("arcus", Side.BUY, 160, 1, time.time(), "a1"), "maker")
    seen = []
    orig = store.summary

    def spy(*a, **k):
        seen.append(threading.current_thread() is threading.main_thread())
        return orig(*a, **k)

    store.summary = spy  # type: ignore[method-assign]
    assert await rm.check_daily_loss_async() == D("-60") and rm.halted
    assert seen and not any(seen)  # never on the event loop thread
    assert rm.check_daily_loss() == D("-60")
