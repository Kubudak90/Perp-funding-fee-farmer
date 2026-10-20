"""StatsReporter: report rendering (defensive status parsing) and funding collection (no network)."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal as D

from farmer.config import PaperConfig
from farmer.exchanges.paper.client import PaperExchange
from farmer.models import Fill, FundingInfo, FundingPayment, Side
from farmer.monitoring.stats import IncrementalSummary, StatsReporter, merge_summaries, pnl_from_summary
from farmer.persistence.store import Store
from tests.fakes import FakeExchange, make_market


class FakeNotifier:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, text: str) -> None:
        self.sent.append(text)


class FundingVenue(FakeExchange):
    """FakeExchange with a realized-funding history."""

    def __init__(self, name: str, creds: bool = True) -> None:
        super().__init__(name, [make_market(name, "BTC")])
        self.payments: list[FundingPayment] = []
        self.since_calls: list[float | None] = []
        self._creds = creds

    @property
    def has_credentials(self) -> bool:
        return self._creds

    async def fetch_funding_payments(self, since_ts=None):
        self.since_calls.append(since_ts)
        return [p for p in self.payments if since_ts is None or p.ts >= since_ts]


STATUS = {
    "mode": "paper",
    "markets": {
        "BTC": {
            "phase": "OPENING",
            "direction": -1,
            "maker_pos": D("-0.0120"),
            "taker_pos": "0.012",
            "quote": {"side": Side.SELL, "price": D("82000.1"), "size": D("0.006"), "edge_bps": 0.5},
            "funding_apr": D("0.1095"),
            "basis_bps": 9.8,
            "mid": "82000",
        },
        "ETH": {"phase": "HOLDING", "direction": 1, "maker_pos": 0.5, "taker_pos": -0.4},
    },
    "risk": {"halted": False, "balances": {"arcus": {"equity": "10000", "margin_ratio": "0.05"}}, "daily_pnl": "1.5"},
}


def _populate(store: Store, now: float) -> None:
    f = [
        Fill("arcus", "BTC", Side.SELL, D("82000"), D("0.01"), fee=D("0"), is_maker=True, trade_id="a1", ts=now - 60),
        Fill("lighter", "BTC", Side.BUY, D("81990"), D("0.01"), fee=D("0"), is_maker=False, trade_id="l1", ts=now - 59),
        Fill("arcus", "BTC", Side.BUY, D("81000"), D("0.01"), fee=D("0.1"), is_maker=True, trade_id="a0",
             ts=now - 7200),
        Fill("lighter", "BTC", Side.SELL, D("81010"), D("0.01"), fee=D("0"), is_maker=False, trade_id="l0",
             ts=now - 7199),
    ]
    for x in f:
        store.record_fill(x, "maker" if x.venue == "arcus" else "hedge")
    store.record_funding(FundingPayment("arcus", "BTC", D("0.25"), ts=now - 1800))
    store.record_funding(FundingPayment("lighter", "BTC", D("-0.05"), ts=now - 1800))


def make_reporter(status=None, venues=None, **kw):
    store = Store(":memory:")
    notifier = FakeNotifier()
    venues = venues if venues is not None else {"arcus": FundingVenue("arcus"), "lighter": FundingVenue("lighter")}
    status_fn = status if callable(status) else (lambda: status if status is not None else STATUS)
    rep = StatsReporter(store, notifier, status_fn, venues, **kw)
    return rep, store, notifier, venues


def test_render_report_full():
    now = time.time()
    rep, store, _, _ = make_reporter()
    rep.start_ts = now - 3 * 3600 - 125
    _populate(store, now)
    text = rep.render_report()
    lines = text.splitlines()
    assert lines[0].startswith("farmer paper | up 3h02m")
    arcus = next(line for line in lines if line.startswith("arcus"))
    assert "vol 1h $820 (M $820/T $0)" in arcus
    assert "24h $1,630 (M $1,630/T $0)" in arcus
    assert "fills 2" in arcus and "fees 24h $0.10" in arcus and "funding 24h +$0.25" in arcus
    lighter = next(line for line in lines if line.startswith("lighter"))
    assert "(M $0/T $820)" in lighter and "funding 24h -$0.05" in lighter
    # hedged: net base 0 -> PnL = spreads (+0.1 + 0.1) - fee 0.1 + funding 0.2
    total = next(line for line in lines if line.startswith("total"))
    assert "PnL est 24h +$0.30 all +$0.30" in total
    assert "risk: arcus eq $10,000.00 mr 0.050 | day pnl +$1.50" in text
    btc = next(line for line in lines if line.startswith("BTC"))
    assert "OPENING" in btc and "maker SHORT" in btc
    assert "mk -0.012 tk +0.012 d 0" in btc
    assert "quote SELL 0.006 @ 82000.1 (edge 0.50bps)" in btc
    assert "APR 10.95%" in btc and "basis 9.8bps" in btc
    eth = next(line for line in lines if line.startswith("ETH"))
    assert "maker LONG" in eth and "d +0.1" in eth and "quote none" in eth and "APR -" in eth


def test_render_report_robust_to_missing_and_weird_status():
    for status in ({}, {"markets": None}, {"markets": [{"symbol": "SOL"}, "junk", 5]}, {"markets": {"X": "bad"}},
                   {"risk": "nope", "halted": None}):
        rep, store, _, _ = make_reporter(status=lambda s=status: s)
        text = rep.render_report()
        assert text.startswith("farmer")
        assert "arcus" in text and "no fills yet" in text
    rep, *_ = make_reporter(status={"markets": [{"symbol": "SOL", "direction": "garbage", "quote": "BUY 1@2",
                                                 "funding": {"apr": "0.05"}, "basis": "0.0012",
                                                 "state": "CLOSING"}]})
    sol = [line for line in rep.render_report().splitlines() if line.startswith("SOL")][0]
    assert "CLOSING" in sol and "quote BUY 1@2" in sol and "APR 5.00%" in sol and "basis 12.0bps" in sol


def test_render_report_status_fn_raises_and_halted():
    def boom():
        raise RuntimeError("engine not ready")

    rep, *_ = make_reporter(status=boom)
    assert rep.render_report().startswith("farmer | up")
    rep, *_ = make_reporter(status={"risk": {"halted": True, "halt_reason": "daily loss"}})
    assert "HALTED: daily loss" in rep.render_report().splitlines()[0]
    rep, *_ = make_reporter(status={"halted": True, "halt_reason": "manual"})
    assert "HALTED: manual" in rep.render_report()


def test_pnl_uses_book_marks_and_flags_missing():
    now = time.time()
    venues = {"arcus": FundingVenue("arcus"), "lighter": FundingVenue("lighter")}
    rep, store, _, _ = make_reporter(status={}, venues=venues)
    store.record_fill(Fill("arcus", "BTC", Side.BUY, D("100"), D("1"), is_maker=True, trade_id="x", ts=now - 5),
                      "maker")
    text = rep.render_report()
    assert "no mark: BTC" in text
    venues["lighter"].set_book("BTC", [(109, 1)], [(111, 1)])
    text = rep.render_report()
    assert "PnL est 24h +$10.00" in text and "no mark" not in text


async def test_collect_funding_since_last_and_dedupe():
    now = time.time()
    a = FundingVenue("arcus")
    no_creds = FundingVenue("lighter", creds=False)
    rep, store, _, _ = make_reporter(venues={"arcus": a, "lighter": no_creds})
    rep.start_ts = now - 7200
    a.payments = [FundingPayment("arcus", "BTC", D("0.1"), ts=now - 10000),  # before start -> ignored
                  FundingPayment("arcus", "BTC", D("0.2"), ts=now - 3600),
                  FundingPayment("arcus", "ETH", D("-0.05"), ts=now - 3600)]
    assert await rep.collect_funding() == 2
    assert a.since_calls == [rep.start_ts]
    assert no_creds.since_calls == []
    assert store.summary(0)["arcus"].funding == D("0.15")
    # next round starts at the last stored ts; re-delivered rows are deduped by the store
    a.payments.append(FundingPayment("arcus", "BTC", D("0.3"), ts=now))
    await rep.collect_funding()
    assert a.since_calls[-1] == now - 3600
    assert store.summary(0)["arcus"].funding == D("0.45")


async def test_collect_funding_survives_venue_errors():
    a = FundingVenue("arcus")

    async def boom(since_ts=None):
        raise ConnectionError("down")

    a.fetch_funding_payments = boom  # type: ignore[method-assign]
    b = FundingVenue("lighter")
    b.payments = [FundingPayment("lighter", "BTC", D("1"), ts=time.time())]
    rep, store, _, _ = make_reporter(venues={"arcus": a, "lighter": b})
    rep.start_ts = time.time() - 60
    assert await rep.collect_funding() == 1


async def test_run_logs_notifies_every_n_and_stops():
    rep, store, notifier, venues = make_reporter(interval_s=0.01, notify_every=2, funding_interval_s=0.0)
    venues["arcus"].payments = [FundingPayment("arcus", "BTC", D("0.5"), ts=time.time() + 0.5)]
    task = asyncio.create_task(rep.run())
    t0 = time.monotonic()
    while rep.reports < 5 and time.monotonic() - t0 < 2:
        await asyncio.sleep(0.005)
    rep.stop()
    await asyncio.wait_for(task, 1.0)
    assert rep.reports >= 5
    assert len(notifier.sent) == rep.reports // 2
    assert notifier.sent[0].startswith("farmer paper")
    assert store.summary(0)["arcus"].funding == D("0.5")


async def test_run_survives_notifier_and_render_errors():
    class BadNotifier:
        async def send(self, text):
            raise RuntimeError("telegram down")

    rep = StatsReporter(Store(":memory:"), BadNotifier(), lambda: STATUS, {}, interval_s=0.005,  # type: ignore[arg-type]
                        notify_every=1)
    rep.render_report = lambda: (_ for _ in ()).throw(ValueError("bug"))  # type: ignore[method-assign]
    task = asyncio.create_task(rep.run())
    t0 = time.monotonic()
    while rep.reports < 3 and time.monotonic() - t0 < 2:
        await asyncio.sleep(0.005)
    rep.stop()
    await asyncio.wait_for(task, 1.0)
    assert rep.reports >= 3


async def test_collects_paper_venue_funding_into_paper_store():
    inner = FakeExchange("lighter", [make_market("lighter", "BTC")])
    px = PaperExchange(inner, PaperConfig(maker_latency_ms={}, taker_latency_ms={}))
    inner.set_book("BTC", [(99, 10)], [(101, 10)])
    await px.place_market("BTC", Side.SELL, D("2"), worst_price=D("90"))
    inner.funding["BTC"] = FundingInfo("lighter", "BTC", D("0.0001"), index_price=D("100"))
    await px.fetch_funding(["BTC"])
    px.settle_funding(time.time())
    store = Store(":memory:", paper=True)
    rep = StatsReporter(store, FakeNotifier(), dict, {"lighter": px})
    rep.start_ts = time.time() - 10
    assert await rep.collect_funding() == 1
    assert store.summary(0)["lighter"].funding == D("2") * D("100") * D("0.0001")


def test_render_pair_trader_status_shape():
    """Shape of strategy.trader.PairTrader.status() (nested funding/basis/books/hedger dicts)."""
    trader_status = {
        "symbol": "BTC", "phase": "CLOSING", "direction": 1, "direction_label": "maker LONG / taker SHORT",
        "maker_pos": D("0"), "taker_pos": D("-0.004"), "delta": D("-0.004"), "inflight": D("0.004"),
        "quote": {"client_id": "ff1", "side": "SELL", "price": D("81950.5"), "size": D("0.01"),
                  "filled": D("0.004"), "remaining": D("0.006"), "status": "PARTIALLY_FILLED"},
        "block_reason": None,
        "funding": {"maker_rate_h": D("0.0000125"), "taker_rate_h": D("0.00002"), "carry_apr": D("0.0657")},
        "basis": {"ema_bps": D("-8.25"), "last_bps": D("-8.1"), "deviation_bps": D("0.15"), "warmed_up": True},
        "books": {"maker_mid": D("81950"), "taker_mid": D("82017"), "divergence_bps": D("-8.2")},
        "hedger": {"maker_pos": D("0"), "taker_pos": D("-0.004"), "delta": D("-0.004")},
    }
    now = time.time()
    rep, store, _, _ = make_reporter(status={"mode": "live", "markets": [trader_status]})
    store.record_fill(Fill("arcus", "BTC", Side.SELL, D("81950"), D("0.004"), is_maker=True, trade_id="z", ts=now),
                      "maker")
    text = rep.render_report()
    btc = [line for line in text.splitlines() if line.startswith("BTC")][0]
    assert "CLOSING" in btc and "maker LONG" in btc
    assert "mk 0 tk -0.004 d -0.004 (inflight +0.004)" in btc
    assert "quote SELL 0.006 @ 81950.5" in btc
    assert "APR 6.57%" in btc and "basis -8.2bps" in btc
    # net -0.004 BTC valued at the trader's taker mid 82017: 327.8 - 328.068
    assert "PnL est 24h -$0.27" in text and "no mark" not in text
    trader_status["block_reason"] = "stale taker book"
    assert "paused: stale taker book" in rep.render_report()


def test_incremental_summary_equals_full_scan_and_picks_up_late_rows_on_rebase():
    now = [time.time()]
    store = Store(":memory:")
    for i in range(50):
        store.record_fill(Fill("arcus" if i % 2 else "lighter", "BTC", Side.BUY if i % 3 else Side.SELL, D("100") + i,
                               D("0.5"), fee=D("0.01"), is_maker=bool(i % 2), trade_id=f"t{i}", ts=now[0] - i * 600),
                          "maker")
    store.record_funding(FundingPayment("arcus", "BTC", D("0.7"), ts=now[0] - 20000))
    inc = IncrementalSummary(store, tail_s=3600, rebase_s=600, clock=lambda: now[0])

    def same(a, b):
        assert a.keys() == b.keys()
        for v in a:
            assert vars(a[v]) == vars(b[v]), v

    same(inc.get(0.0), store.summary(0.0))
    store.record_fill(Fill("arcus", "BTC", Side.BUY, D("90"), D("1"), trade_id="new", ts=now[0]), "maker")
    same(inc.get(0.0), store.summary(0.0))  # tail rows are always current
    store.record_fill(Fill("arcus", "BTC", Side.BUY, D("90"), D("1"), trade_id="late", ts=now[0] - 9000), "maker")
    assert inc.get(0.0)["arcus"].fills == store.summary(0.0)["arcus"].fills - 1  # late row: until the rebase
    now[0] += 601
    same(inc.get(0.0), store.summary(0.0))
    marks = {"BTC": D("123.4")}
    assert pnl_from_summary(inc.get(0.0), marks) == store.pnl(0.0, marks)
    since = now[0] - 7200
    assert pnl_from_summary(inc.get(since), marks) == store.pnl(since, marks)
    a = store.summary(0.0, now[0] - 5000)
    b = store.summary(now[0] - 5000)
    same(merge_summaries(a, b), store.summary(0.0))


def test_marks_cover_venue_symbols_of_mapped_pairs():
    status = {"markets": {"BONK": {"symbol": "BONK", "maker_symbol": "KBONK", "taker_symbol": "1000BONK",
                                   "books": {"maker_mid": D("0.02921"), "taker_mid": D("0.02925")}}}}
    now = time.time()
    rep, store, _, _ = make_reporter(status=status)
    store.record_fill(Fill("arcus", "KBONK", Side.BUY, D("0.02900"), D("1000"), is_maker=True, trade_id="k",
                           ts=now), "maker")
    store.record_fill(Fill("lighter", "1000BONK", Side.SELL, D("0.02930"), D("1000"), is_maker=False, trade_id="b",
                           ts=now), "hedge")
    text = rep.render_report()
    assert "no mark" not in text
    # -29.00 + 0.02921*1000 + 29.30 - 0.02925*1000 = 0.26
    assert "PnL est 24h +$0.26" in text
