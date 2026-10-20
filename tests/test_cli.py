"""CLI tests (no network): argument handling, safety gates, venue wiring, scan math, run/stats end to end."""

import io
import os
from decimal import Decimal as D

import pytest

from farmer import cli
from farmer.config import AppConfig, MarketConfig, MonitoringConfig
from farmer.exchanges.arcus import ArcusExchange
from farmer.exchanges.lighter import LighterExchange
from farmer.exchanges.paper.client import PaperExchange
from farmer.models import BookLevel, Fill, FundingInfo, FundingPayment, OrderBook, Side
from farmer.persistence.store import Store
from tests.fakes import FakeExchange, fast_risk, fast_strategy, make_market

SECRET_ENV = ("ARCUS_ADDRESS", "ARCUS_API_PRIVATE_KEY", "ARCUS_ACCOUNT_INDEX", "LIGHTER_API_PRIVATE_KEY",
              "LIGHTER_ACCOUNT_INDEX", "LIGHTER_API_KEY_INDEX", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "FARMER_MODE",
              "ARCUS_WALLET_PRIVATE_KEY", "LIGHTER_ETH_PRIVATE_KEY")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch, tmp_path):
    for k in SECRET_ENV:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.chdir(tmp_path)  # no stray config.yaml / .env / data dir


def creds_env(monkeypatch):
    monkeypatch.setenv("ARCUS_ADDRESS", "0x" + "ab" * 20)
    monkeypatch.setenv("ARCUS_API_PRIVATE_KEY", "11" * 32)
    monkeypatch.setenv("LIGHTER_API_PRIVATE_KEY", "0x" + "22" * 40)
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "7")
    monkeypatch.setenv("LIGHTER_API_KEY_INDEX", "5")


def book(venue, sym, bids, asks):
    return OrderBook(venue=venue, symbol=sym, bids=[BookLevel(D(str(p)), D(str(s))) for p, s in bids],
                     asks=[BookLevel(D(str(p)), D(str(s))) for p, s in asks])


# --------------------------------------------------------------------------- parsing / config
def test_parser_subcommands():
    p = cli.build_parser()
    a = p.parse_args(["run", "--paper", "--duration", "5", "--config", "x.yaml"])
    assert a.command == "run" and a.paper and a.duration == 5 and a.config == "x.yaml"
    a = p.parse_args(["close-all", "--taker", "--yes"])
    assert a.taker and a.yes
    a = p.parse_args(["keygen", "lighter", "--api-key-index", "9", "--generate-only"])
    assert a.venue == "lighter" and a.api_key_index == 9
    with pytest.raises(SystemExit):
        p.parse_args(["run", "--paper", "--live"])


def test_default_config_may_be_missing_but_explicit_may_not(tmp_path):
    cfg = cli._load(cli.build_parser().parse_args(["stats"]))
    assert cfg.mode == "paper" and [m.symbol for m in cfg.strategy.markets] == ["BTC", "ETH", "SOL"]
    assert cli.main(["stats", "--config", str(tmp_path / "nope.yaml")]) == cli.EXIT_USAGE


def test_example_config_matches_defaults():
    """config.example.yaml documents every section with the built-in defaults."""
    from pathlib import Path

    from farmer.config import load_config

    root = Path(__file__).resolve().parents[1]
    loaded = load_config(root / "config.example.yaml", env_file=None).model_dump()
    assert loaded == AppConfig().model_dump()


def test_public_configs_strip_credentials(monkeypatch):
    creds_env(monkeypatch)
    cfg = cli._load(cli.build_parser().parse_args(["scan"]))
    assert cfg.arcus.has_credentials and cfg.lighter.has_credentials
    a, lt = cli.public_venue_configs(cfg)
    assert not a.has_credentials and a.address is None and a.dead_mans_switch_s == 0
    assert not lt.has_credentials and lt.account_index is None and lt.verify_api_key is False
    venues = cli.build_venues(cfg, paper=True)
    assert all(isinstance(v, PaperExchange) for v in venues.values())
    assert isinstance(venues["arcus"].inner, ArcusExchange) and not venues["arcus"].inner.has_credentials
    assert isinstance(venues["lighter"].inner, LighterExchange) and not venues["lighter"].inner.has_credentials
    live = cli.build_venues(cfg, paper=False)
    assert live["arcus"].has_credentials and live["lighter"].has_credentials


def test_live_run_needs_credentials_and_confirmation(monkeypatch, capsys):
    assert cli.main(["run", "--live", "--yes"]) == cli.EXIT_USAGE
    assert "needs credentials" in capsys.readouterr().err
    creds_env(monkeypatch)
    assert cli.main(["run", "--live"]) == cli.EXIT_USAGE
    assert "--yes" in capsys.readouterr().err


def test_close_all_and_status_need_credentials(capsys):
    assert cli.main(["close-all", "--yes"]) == cli.EXIT_USAGE
    assert cli.main(["status"]) == cli.EXIT_USAGE
    err = capsys.readouterr().err
    assert "close-all needs credentials" in err and "no credentials configured" in err


def test_error_hints():
    assert "jurisdiction" in cli._hint_for("lighter error 20558: restricted")
    assert "whitelist" in cli._hint_for("HTTP 403 address not on access whitelist").lower()
    assert cli._hint_for("something else") is None


# --------------------------------------------------------------------------- scan helpers
def test_scan_math_and_table():
    mb = book("arcus", "BTC", [(99990, 1)], [(99991, 1)])
    tb = book("lighter", "BTC", [(100000, "0.001"), (99999, 1)], [(100002, "0.002"), (100004, 1)])
    assert cli.tob_depth_usd(tb) == D("100.000")
    cost = cli.round_trip_cost_bps(mb, tb, D("500"))
    assert cost is not None and D("0.1") < cost < D("1")  # walks past the thin top level on both sides
    assert cli.round_trip_cost_bps(mb, None, D("500")) is None
    rows = [cli.ScanRow("BTC", "BTC", "BTC", True, D("0.0000125"), D("0.000012"), D("0.00438"), -1, D("0.1"),
                        D("0.2"), D("100"), D("-9.3"), cost),
            cli.ScanRow("BONK", "KBONK", "1000BONK", False, kind="RWA", open=False)]
    out = cli.format_scan_table(rows, clip=D("500"))
    assert "*BTC" in out and "short A/long L" in out and "0.00125" in out and "0.44" in out
    assert "KBONK/1000BONK" in out and "RWA CLOSED" in out


# --------------------------------------------------------------------------- run (offline, fake venues)
def fake_venues():
    maker = FakeExchange("arcus", [make_market("arcus", "BTC", tick="0.1", step="0.001", min_size="0.001",
                                               min_notional="5")])
    taker = FakeExchange("lighter", [make_market("lighter", "BTC", tick="0.1", step="0.0001", min_size="0.002",
                                                 min_notional="10")])
    maker.set_book("BTC", [(99990, 5)], [(100010, 5)])
    taker.set_book("BTC", [(100000, 5)], [(100002, 5)])
    maker.funding["BTC"] = FundingInfo("arcus", "BTC", D("0.0000125"))
    taker.funding["BTC"] = FundingInfo("lighter", "BTC", D("0.000012"))
    return {"arcus": maker, "lighter": taker}


async def test_run_engine_offline_duration(tmp_path):
    cfg = AppConfig(data_dir=tmp_path / "d", strategy=fast_strategy(markets=[MarketConfig(symbol="BTC")],
                                                                    direction_mode="long_maker"),
                    risk=fast_risk(), monitoring=MonitoringConfig(report_interval_s=0.2))
    venues = fake_venues()
    rc = await cli.run_engine(cfg, paper=True, duration=1.0, venues=venues)
    assert rc == cli.EXIT_OK
    assert not venues["arcus"].connected and not venues["lighter"].connected
    db = tmp_path / "d" / "farmer-paper.db"
    assert db.exists()
    kinds = {k for _, k, _ in Store(db, paper=True).recent_events(100)}
    assert {"pairs", "shutdown"} <= kinds
    lock = cli.RunLock(db.with_suffix(".lock"))  # released after the run
    assert lock.acquire()
    lock.release()


async def test_run_engine_refuses_second_instance(tmp_path, capsys):
    cfg = AppConfig(data_dir=tmp_path)
    held = cli.RunLock(cli.store_path(cfg, True).with_suffix(".lock"))
    assert held.acquire()
    try:
        assert await cli.run_engine(cfg, paper=True, duration=0.1, venues=fake_venues()) == cli.EXIT_ERROR
        assert "another farmer process" in capsys.readouterr().err
    finally:
        held.release()


async def test_run_engine_reports_start_failure(tmp_path):
    cfg = AppConfig(data_dir=tmp_path, strategy=fast_strategy(markets=[MarketConfig(symbol="DOGE")]))
    rc = await cli.run_engine(cfg, paper=True, duration=5, venues=fake_venues())
    assert rc == cli.EXIT_ERROR  # no tradeable pair -> clean error, not a traceback


# --------------------------------------------------------------------------- stats
async def test_stats_report(tmp_path):
    cfg = AppConfig(data_dir=tmp_path)
    store = Store(cli.store_path(cfg, True), paper=True)
    store.record_fill(Fill("arcus", "BTC", Side.SELL, D("100000"), D("0.01"), fee=D("0"), is_maker=True,
                           trade_id="a1"), "maker")
    store.record_fill(Fill("lighter", "BTC", Side.BUY, D("100010"), D("0.01"), fee=D("0"), is_maker=False,
                           trade_id="l1"), "hedge")
    store.record_funding(FundingPayment("arcus", "BTC", D("0.05")))
    store.record_event("phase", {"symbol": "BTC"})
    store.close()
    out = io.StringIO()
    rc = await cli.stats_report(cfg, paper=True, since_hours=0, use_marks=False, out=out)
    text = out.getvalue()
    assert rc == cli.EXIT_OK
    assert "arcus" in text and "lighter" in text and "1000" in text
    assert "arcus/maker 1" in text and "lighter/hedge 1" in text
    assert "PnL est $-0.05" in text  # -0.1 cash flow + 0.05 funding, flat net base
    assert "phase 1" in text
    assert await cli.stats_report(AppConfig(data_dir=tmp_path / "none"), paper=False, since_hours=1) == cli.EXIT_ERROR


# --------------------------------------------------------------------------- keygen (local only)
async def test_keygen_arcus_generate_only_and_missing_wallet():
    args = cli.build_parser().parse_args(["keygen", "arcus", "--generate-only"])
    out = io.StringIO()
    assert await cli.keygen_arcus(AppConfig(), args, out=out) == cli.EXIT_OK
    line = [ln for ln in out.getvalue().splitlines() if ln.startswith("ARCUS_API_PRIVATE_KEY=")]
    assert len(line) == 1 and len(line[0].split("=", 1)[1]) == 64
    args = cli.build_parser().parse_args(["keygen", "arcus", "--yes"])
    assert await cli.keygen_arcus(AppConfig(), args, out=io.StringIO()) == cli.EXIT_USAGE


async def test_keygen_lighter_validates_index():
    args = cli.build_parser().parse_args(["keygen", "lighter", "--api-key-index", "2", "--generate-only"])
    assert await cli.keygen_lighter(AppConfig(), args, out=io.StringIO()) == cli.EXIT_USAGE


def test_confirm_requires_yes_without_tty(monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO(""))
    assert cli._confirm("x", True) is True
    assert cli._confirm("x", False) is False


def test_module_entry_point():
    import subprocess
    import sys

    r = subprocess.run([sys.executable, "-m", "farmer", "--version"], capture_output=True, text=True,
                       env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}, timeout=60)
    assert r.returncode == 0 and "farmer" in r.stdout


def test_lock_is_exclusive(tmp_path):
    a, b = cli.RunLock(tmp_path / "x.lock"), cli.RunLock(tmp_path / "x.lock")
    assert a.acquire()
    assert not b.acquire()
    assert a.holder() == str(os.getpid())
    a.release()
    assert b.acquire()
    b.release()


def test_scan_rows_sort_by_carry():
    rows = [cli.ScanRow("A", "A", "A", False, carry_apr=D("0.01")), cli.ScanRow("B", "B", "B", False),
            cli.ScanRow("C", "C", "C", False, carry_apr=D("0.05"))]
    rows.sort(key=lambda r: (r.carry_apr is None, -(r.carry_apr or D(0))))
    assert [r.symbol for r in rows] == ["C", "A", "B"]



# --------------------------------------------------------------------------- status / close-all with fake venues
async def test_status_prints_delta_per_pair(monkeypatch):
    creds_env(monkeypatch)
    cfg = cli._load(cli.build_parser().parse_args(["status"]))
    venues = fake_venues()
    venues["arcus"].positions["BTC"] = D("0.01")
    venues["lighter"].positions["BTC"] = D("-0.008")
    venues["lighter"].positions["XRP"] = D("5")
    monkeypatch.setattr(cli, "build_venues", lambda *a, **k: venues)
    out = io.StringIO()
    assert await cli.status(cfg, out=out) == cli.EXIT_OK
    text = out.getvalue()
    assert "equity 10000" in text and "position BTC" in text
    assert "delta 0.002" in text.replace("  ", " ") or "0.002" in text
    assert "UNHEDGED" in text and "lighter:XRP" in text
    assert not venues["arcus"].connected


async def test_close_all_taker_flattens(monkeypatch, tmp_path):
    creds_env(monkeypatch)
    cfg = cli._load(cli.build_parser().parse_args(["close-all"]))
    cfg = cfg.model_copy(update={"data_dir": tmp_path, "strategy": fast_strategy(markets=[MarketConfig(symbol="BTC")]),
                                 "risk": fast_risk()})
    venues = fake_venues()
    venues["arcus"].positions["BTC"] = D("0.01")
    venues["lighter"].positions["BTC"] = D("-0.01")
    monkeypatch.setattr(cli, "build_venues", lambda *a, **k: venues)
    out = io.StringIO()
    assert await cli.close_all(cfg, taker=True, timeout_s=10, out=out) == cli.EXIT_OK
    assert "FLAT" in out.getvalue()
    assert venues["arcus"].positions.get("BTC", D(0)) == 0 and venues["lighter"].positions.get("BTC", D(0)) == 0
    assert not venues["arcus"].leverage  # close-all never changes leverage
