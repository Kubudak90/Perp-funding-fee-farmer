import random
import sqlite3
import threading
from decimal import Decimal as D

from farmer.models import Fill, FundingPayment, Side
from farmer.persistence import store as store_mod
from farmer.persistence.store import Store


def test_store_summary_and_pnl(tmp_path):
    st = Store(tmp_path / "f.db")
    st.record_fill(Fill("arcus", "BTC", Side.BUY, D("100"), D("1"), fee=D("0"), is_maker=True, trade_id="a1", ts=10), "maker")
    st.record_fill(Fill("arcus", "BTC", Side.BUY, D("100"), D("1"), fee=D("0"), is_maker=True, trade_id="a1", ts=10), "maker")  # dup
    st.record_fill(Fill("lighter", "BTC", Side.SELL, D("100.1"), D("1"), fee=D("0"), is_maker=False, trade_id="l1", ts=11), "hedge")
    st.record_funding(FundingPayment("lighter", "BTC", D("0.5"), ts=12))
    s = st.summary(0)
    assert s["arcus"].volume == D("100") and s["arcus"].fills == 1
    assert s["lighter"].taker_volume == D("100.1")
    assert st.total_volume(0) == D("200.1")
    # hedged: net base 0 -> pnl = 0.1 spread + 0.5 funding
    assert st.pnl(0, {"BTC": D("105")}) == D("0.6")
    st.record_event("start", {"x": 1})
    assert st.recent_events(1)[0][1] == "start"


def _random_rows(st: Store, now: float, n: int = 400, seed: int = 7) -> None:
    rnd = random.Random(seed)
    for i in range(n):
        ts = now - rnd.uniform(0, 72 * 3600)
        st.record_fill(Fill(rnd.choice(["arcus", "lighter"]), rnd.choice(["BTC", "ETH"]),
                            rnd.choice([Side.BUY, Side.SELL]), D(rnd.randint(9000, 11000)) / 100,
                            D(rnd.randint(1, 500)) / 1000, fee=D(rnd.randint(0, 30)) / 1000,
                            is_maker=rnd.choice([True, False, None]), trade_id=f"t{i}", ts=ts), "maker")
    for h in range(72):
        st.record_funding(FundingPayment("lighter", "BTC", D(rnd.randint(-50, 50)) / 100, ts=now - h * 3600 - 17))


def _raw_summary(st: Store, since: float, until: float) -> dict:
    out: dict = {}
    st._scan_raw(st._db, out, since, until)
    return out


def _same(a: dict, b: dict) -> None:
    assert a.keys() == b.keys()
    for v in a:
        assert vars(a[v]) == vars(b[v]), v


def test_hourly_aggregates_match_a_raw_scan_for_any_window(tmp_path):
    now = 1_790_000_000.0 + 1234.5
    st = Store(tmp_path / "agg.db")
    _random_rows(st, now)
    for since, until in [(0.0, now + 1), (now - 86400, now + 1), (now - 3600, now + 1), (now - 5 * 3600 - 7, now - 3),
                         (now - 7200.0, now - 3600.0), (now - 100, now - 50)]:
        _same(st.summary(since, until), _raw_summary(st, since, until))
    # a late row inside an already aggregated hour, and duplicates, keep them equal
    assert st.record_fill(Fill("arcus", "BTC", Side.BUY, D("100"), D("1"), trade_id="late", ts=now - 50000), "hedge")
    assert not st.record_fill(Fill("arcus", "BTC", Side.BUY, D("100"), D("1"), trade_id="late", ts=now - 50000), "x")
    assert not st.record_funding(FundingPayment("lighter", "BTC", D("9"), ts=now - 17))
    _same(st.summary(0.0, now + 1), _raw_summary(st, 0.0, now + 1))
    st.close()


def test_all_time_summary_reads_raw_rows_only_for_partial_hours(tmp_path, monkeypatch):
    now = 1_790_000_000.0 + 1800
    st = Store(tmp_path / "agg.db")
    _random_rows(st, now, n=300)
    ranges = []
    orig = Store._scan_raw

    def spy(self, db, out, since, until):
        ranges.append((since, until))
        return orig(self, db, out, since, until)

    monkeypatch.setattr(Store, "_scan_raw", spy)
    st.summary(0.0, now + 1)
    st.summary(now - 86400, now + 1)
    assert ranges and all(b - a <= 3600 for a, b in ranges if b > a)


def test_aggregates_are_built_for_a_database_written_before_they_existed(tmp_path):
    now = 1_790_000_000.0
    path = tmp_path / "legacy.db"
    st = Store(path)
    _random_rows(st, now, n=120)
    expected = st.summary(0.0, now + 1)
    st.close()
    db = sqlite3.connect(path)
    db.execute("DROP TABLE agg_hourly")
    db.execute("DELETE FROM meta")
    db.commit()
    db.close()
    st = Store(path)  # rebuilds agg_hourly from the raw rows
    _same(st.summary(0.0, now + 1), expected)
    assert st.get_meta("agg_hourly") == store_mod.AGG_VERSION
    st.close()


def test_summary_from_a_worker_thread_while_the_loop_thread_writes(tmp_path):
    now = 1_790_000_000.0
    st = Store(tmp_path / "c.db")
    assert st.concurrent_reads and not Store(":memory:").concurrent_reads
    errors: list[BaseException] = []
    stop = threading.Event()

    def reader():
        try:
            while not stop.is_set():
                st.summary(0.0, now + 1)
        except BaseException as exc:  # pragma: no cover - the assertion below reports it
            errors.append(exc)

    t = threading.Thread(target=reader)
    t.start()
    for i in range(300):
        st.record_fill(Fill("arcus", "BTC", Side.BUY, D("100"), D("1"), trade_id=f"w{i}", ts=now - i), "maker")
    stop.set()
    t.join(5)
    assert not errors
    assert st.summary(0.0, now + 1)["arcus"].fills == 300
    st.close()
