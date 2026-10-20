"""Dashboard: security layer, credentials, settings and bot control over HTTP (fake venues, no network)."""

from __future__ import annotations

import asyncio
import logging
import os
import stat
from decimal import Decimal as D

import pytest
import yaml
from aiohttp.test_utils import TestClient, TestServer

from farmer.dashboard import credentials, settings
from farmer.dashboard.controller import BotController
from farmer.dashboard.server import _hostname, allowed_hosts_for, create_app
from tests.fakes import FakeExchange, FundingInfo, fast_risk, fast_strategy, make_market

TOKEN = "test-token-0123456789abcdef"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
ARCUS_KEY = "11" * 32
LIGHTER_KEY = "0x" + "ab" * 40


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


def write_config(path, data_dir):
    scfg = fast_strategy(direction_mode="long_maker").model_dump(mode="json", exclude={"markets"})
    path.write_text(yaml.safe_dump({
        "data_dir": str(data_dir), "strategy": {**scfg, "markets": [{"symbol": "BTC"}]},
        "risk": fast_risk().model_dump(mode="json"), "monitoring": {"report_interval_s": 0.2}}))


@pytest.fixture
async def dash(tmp_path):
    cfg_path, env_file = tmp_path / "config.yaml", tmp_path / ".env"
    write_config(cfg_path, tmp_path / "data")
    env_file.write_text("# my secrets\nOTHER=keep\n")
    environ: dict[str, str] = {}
    venues: dict = {}

    def factory(cfg, paper):
        venues.clear()
        venues.update(fake_venues())
        return venues

    ctl = BotController(cfg_path, env_file, environ=environ, venues_factory=factory)
    logging.getLogger().addHandler(ctl.logs)  # as serve() does
    app = create_app(ctl, token=TOKEN, allowed_hosts=allowed_hosts_for("127.0.0.1"))
    client = TestClient(TestServer(app, host="127.0.0.1"))
    await client.start_server()
    try:
        yield client, ctl, environ, venues, tmp_path
    finally:
        await ctl.shutdown(timeout_s=10)
        await client.close()
        logging.getLogger().removeHandler(ctl.logs)


async def wait_for(pred, timeout=5.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not await pred():
        if loop.time() > end:
            raise AssertionError("condition not met")
        await asyncio.sleep(0.05)


# --------------------------------------------------------------------------- security
async def test_api_needs_the_token_and_a_loopback_host(dash):
    client, *_ = dash
    assert (await client.get("/api/state")).status == 401
    assert (await client.get("/api/state", headers={"Authorization": "Bearer wrong"})).status == 401
    r = await client.get("/api/state", headers=AUTH)
    assert r.status == 200 and (await r.json())["bot"]["state"] == "stopped"
    assert r.headers["Cache-Control"] == "no-store" and r.headers["X-Frame-Options"] == "DENY"
    r = await client.get("/api/state", headers={**AUTH, "Host": "evil.example:8080"})  # DNS rebinding
    assert r.status == 403


async def test_index_is_served_with_a_nonce_csp_and_no_external_resources(dash):
    client, *_ = dash
    r = await client.get("/")
    html = await r.text()
    csp = r.headers["Content-Security-Policy"]
    nonce = csp.split("'nonce-")[1].split("'")[0]
    assert r.status == 200 and f'nonce="{nonce}"' in html and "{{NONCE}}" not in html
    assert "default-src 'none'" in csp and "connect-src 'self'" in csp and "frame-ancestors 'none'" in csp
    for external in ("<script src", "<link", "@import", "url(", "https://"):  # no third-party code or assets
        assert external not in html, external
    assert nonce != (await client.get("/")).headers["Content-Security-Policy"].split("'nonce-")[1].split("'")[0]


async def test_writes_need_json_and_same_origin(dash):
    client, *_ = dash
    r = await client.post("/api/bot/stop", data="x", headers={**AUTH, "Content-Type": "text/plain"})
    assert r.status == 415
    r = await client.post("/api/bot/stop", json={}, headers={**AUTH, "Origin": "https://evil.example"})
    assert r.status == 403
    r = await client.post("/api/bot/stop", json={}, headers=AUTH)
    assert r.status == 409 and "çalışmıyor" in (await r.json())["error"]


def test_hostname_and_allowed_hosts():
    assert _hostname("127.0.0.1:8080") == "127.0.0.1" and _hostname("[::1]:8080") == "[::1]"
    assert _hostname("LocalHost") == "localhost"
    assert allowed_hosts_for("0.0.0.0") is None and "localhost" in allowed_hosts_for("127.0.0.1")


# --------------------------------------------------------------------------- credentials
async def test_credentials_are_write_only_and_stored_0600(dash):
    client, ctl, environ, _, tmp = dash
    body = {"ARCUS_ADDRESS": "0x" + "a" * 40, "ARCUS_API_PRIVATE_KEY": ARCUS_KEY, "ARCUS_ACCOUNT_INDEX": "2",
            "LIGHTER_API_PRIVATE_KEY": LIGHTER_KEY, "LIGHTER_ACCOUNT_INDEX": 7, "LIGHTER_API_KEY_INDEX": "5"}
    r = await client.put("/api/credentials", json=body, headers=AUTH)
    data = await r.json()
    assert r.status == 200 and len(data["changed"]) == 6
    cr = data["credentials"]
    assert cr["ARCUS_API_PRIVATE_KEY"] == {**cr["ARCUS_API_PRIVATE_KEY"], "set": True, "value": None}
    assert cr["LIGHTER_API_PRIVATE_KEY"]["value"] is None and cr["ARCUS_ADDRESS"]["value"] == "0x" + "a" * 40
    assert ARCUS_KEY not in await (await client.get("/api/state", headers=AUTH)).text()
    env = tmp / ".env"
    text = env.read_text()
    assert stat.S_IMODE(os.stat(env).st_mode) == 0o600
    assert text.startswith("# my secrets\nOTHER=keep\n") and f"ARCUS_API_PRIVATE_KEY={ARCUS_KEY}" in text
    assert environ["LIGHTER_ACCOUNT_INDEX"] == "7"
    cfg = ctl.load_cfg()
    assert cfg.arcus.has_credentials and cfg.lighter.has_credentials and cfg.arcus.account_index == 2
    # empty field = unchanged; null = remove; a bad value changes nothing at all
    r = await client.put("/api/credentials", json={"ARCUS_ADDRESS": "", "LIGHTER_API_KEY_INDEX": "2"}, headers=AUTH)
    assert r.status == 400 and "4..254" in (await r.json())["error"] and "LIGHTER_API_KEY_INDEX=5" in env.read_text()
    r = await client.put("/api/credentials", json={"ARCUS_API_PRIVATE_KEY": None}, headers=AUTH)
    assert (await r.json())["changed"] == ["ARCUS_API_PRIVATE_KEY"] and "ARCUS_API_PRIVATE_KEY" not in env.read_text()
    assert "ARCUS_API_PRIVATE_KEY" not in environ and "OTHER=keep" in env.read_text()


@pytest.mark.parametrize("name,value,err", [
    ("ARCUS_ADDRESS", "0x1234", "geçersiz adres"),
    ("ARCUS_API_PRIVATE_KEY", "zz" * 32, "Ed25519"),
    ("ARCUS_ACCOUNT_INDEX", "12", "0..9"),
    ("TELEGRAM_BOT_TOKEN", "abc", "bot token"),
    ("LIGHTER_API_PRIVATE_KEY", "ab cd", "boşluk"),
    ("ARCUS_WALLET_PRIVATE_KEY", "11" * 32, "bilinmeyen alan"),
])
def test_credential_validation(name, value, err):
    with pytest.raises(ValueError, match=err):
        credentials.validate({name: value})


# --------------------------------------------------------------------------- settings
async def test_settings_are_validated_and_written_with_a_backup(dash):
    client, ctl, _, _, tmp = dash
    r = await client.get("/api/config", headers=AUTH)
    data = await r.json()
    assert data["values"]["strategy"]["direction_mode"] == "long_maker" and data["markets"][0]["symbol"] == "BTC"
    assert any(f["name"] == "max_daily_loss" for f in data["fields"])
    patch = {"strategy": {"min_edge_bps": "-0.25", "churn": False}, "risk": {"max_daily_loss": "40"},
             "markets": [{"symbol": "btc"}, {"symbol": "eth", "enabled": False, "order_notional": "250"}]}
    r = await client.put("/api/config", json=patch, headers=AUTH)
    assert r.status == 200
    cfg = ctl.load_cfg()
    assert cfg.strategy.min_edge_bps == D("-0.25") and cfg.strategy.churn is False and cfg.risk.max_daily_loss == 40
    assert [(m.symbol, m.enabled, m.order_notional) for m in cfg.strategy.markets] == [("BTC", True, None),
                                                                                       ("ETH", False, D("250"))]
    assert cfg.data_dir == tmp / "data"  # untouched fields survive
    assert (tmp / "config.yaml.bak").exists()
    before = (tmp / "config.yaml").read_text()
    for bad, err in (({"strategy": {"churn_floor_frac": "1.5"}}, "churn_floor_frac"),
                     ({"arcus": {"rest_url": "http://x"}}, "panelden değiştirilemez"),
                     ({"risk": {"max_daily_loss": "abc"}}, "geçersiz değer"),
                     ({"markets": []}, "en az bir piyasa")):
        r = await client.put("/api/config", json=bad, headers=AUTH)
        assert r.status == 400 and err in (await r.json())["error"], bad
    assert (tmp / "config.yaml").read_text() == before


def test_settings_schema_matches_the_config_models():
    from farmer.config import AppConfig

    cfg = AppConfig()
    for sec, name, _, _ in settings.FIELDS:
        assert hasattr(getattr(cfg, sec), name), (sec, name)


# --------------------------------------------------------------------------- bot control
async def _state(client) -> dict:
    return await (await client.get("/api/state", headers=AUTH)).json()


async def test_paper_start_state_history_logs_and_stop(dash):
    client, ctl, _, venues, _ = dash
    r = await client.post("/api/bot/start", json={"mode": "paper"}, headers=AUTH)
    assert r.status == 200 and (await r.json())["bot"]["state"] == "running"
    r = await client.post("/api/bot/start", json={"mode": "paper"}, headers=AUTH)
    assert r.status == 409

    async def started() -> bool:
        st = await _state(client)
        return bool(st["engine"] and st["engine"]["markets"].get("BTC") and st["report"])

    await wait_for(started)
    st = await _state(client)
    assert st["bot"]["mode"] == "paper" and st["engine"]["markets"]["BTC"]["phase"] == "OPENING"
    assert any(e["kind"] == "pairs" for e in st["events"])
    maker = venues["arcus"]
    q = maker.resting("BTC")[0]
    maker.fill_resting(q.client_id)  # the hedger hedges it

    async def hedged() -> bool:
        m = (await _state(client))["engine"]["markets"]["BTC"]
        return D(m["maker_pos"]) > 0 and D(m["delta"]) == 0

    await wait_for(hedged)
    ctl._report = (0.0, None)
    st = await _state(client)
    assert D(st["report"]["all"]["volume"]) > 0 and "pnl_all" in st["report"]
    hist = await (await client.get("/api/history?hours=2", headers=AUTH)).json()
    assert sum(D(r["volume"]) for r in hist["hourly"]) > 0 and hist["samples"]
    logs = await (await client.get("/api/logs?after=0", headers=AUTH)).json()
    assert any("starting the bot in PAPER" in line["msg"] for line in logs["lines"])
    r = await client.post("/api/bot/stop", json={}, headers=AUTH)
    assert r.status == 200

    async def stopped() -> bool:
        return (await _state(client))["bot"]["state"] == "stopped"

    await wait_for(stopped, 15)
    st = await _state(client)
    assert st["bot"]["last_exit"]["code"] == 0 and st["report"]["from_db"] is True
    assert not maker.resting("BTC")  # graceful stop canceled the quote


async def test_live_start_needs_keys_and_the_typed_confirmation(dash):
    client, ctl, environ, _, _ = dash
    r = await client.post("/api/bot/start", json={"mode": "live", "confirm": "CANLI"}, headers=AUTH)
    assert r.status == 400 and "API anahtarı gerekli" in (await r.json())["error"]
    environ.update(ARCUS_ADDRESS="0x" + "a" * 40, ARCUS_API_PRIVATE_KEY=ARCUS_KEY, LIGHTER_API_PRIVATE_KEY=LIGHTER_KEY,
                   LIGHTER_ACCOUNT_INDEX="1", LIGHTER_API_KEY_INDEX="4")
    for confirm in (None, "evet"):
        r = await client.post("/api/bot/start", json={"mode": "live", "confirm": confirm}, headers=AUTH)
        assert r.status == 400 and "CANLI" in (await r.json())["error"]
    assert not ctl.running
    r = await client.post("/api/bot/start", json={"mode": "demo"}, headers=AUTH)
    assert r.status == 400


async def test_close_all_on_the_running_bot(dash):
    client, ctl, _, venues, _ = dash

    def factory(cfg, paper):
        v = fake_venues()
        v["arcus"].positions["BTC"], v["lighter"].positions["BTC"] = D("0.02"), D("-0.02")
        venues.update(v)
        return v

    ctl.venues_factory = factory
    await client.post("/api/bot/start", json={"mode": "paper"}, headers=AUTH)

    async def started() -> bool:
        return (await _state(client))["bot"]["engine_started"]

    await wait_for(started)
    r = await client.post("/api/bot/close-all", json={"mode": "taker", "confirm": "hayır"}, headers=AUTH)
    assert r.status == 400 and "KAPAT" in (await r.json())["error"]
    r = await client.post("/api/bot/close-all", json={"mode": "taker", "confirm": "KAPAT"}, headers=AUTH)
    assert r.status == 200 and (await r.json())["job"]["state"] == "running"

    async def done() -> bool:
        job = (await _state(client))["job"]
        return job["state"] != "running"

    await wait_for(done, 10)
    job = (await _state(client))["job"]
    assert job["state"] == "done" and "BTC" in job["output"] and "FLAT" in job["output"], job
    assert venues["arcus"].positions["BTC"] == 0 and venues["lighter"].positions["BTC"] == 0


async def test_close_all_with_the_bot_stopped_needs_live_keys(dash):
    client, *_ = dash
    r = await client.post("/api/bot/close-all", json={"mode": "maker", "confirm": "KAPAT"}, headers=AUTH)
    assert r.status == 400 and "API anahtarı" in (await r.json())["error"]
