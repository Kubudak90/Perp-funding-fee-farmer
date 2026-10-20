import json
import os
from decimal import Decimal as D
from pathlib import Path

import pytest

from farmer.config import MarketConfig, StrategyConfig
from farmer.exchanges.arcus.parse import parse_market as arcus_parse_market
from farmer.exchanges.lighter.helpers import parse_markets as lighter_parse_markets
from farmer.models import FundingInfo
from farmer.strategy.mapping import candidate_symbols, find_market, resolve_pairs, venue_prices
from tests.fakes import FakeExchange, make_market

DATA = Path(__file__).parent / "data" / "strategy"


def rm(symbol, **kw):
    return StrategyConfig().for_market(MarketConfig(symbol=symbol, **kw))


def venues(maker_syms, taker_syms):
    m = FakeExchange("arcus", [make_market("arcus", s) for s in maker_syms])
    t = FakeExchange("lighter", [make_market("lighter", s) for s in taker_syms])
    return m, t


def test_candidates():
    assert candidate_symbols("kbonk") == ["KBONK", "1000BONK"]
    assert candidate_symbols("1000BONK") == ["1000BONK", "KBONK"]
    assert candidate_symbols("BONK") == ["BONK", "KBONK", "1000BONK"]


async def test_same_name_and_book_price_check():
    m, t = venues(["BTC", "ETH"], ["BTC", "ETH", "SOL"])
    m.set_book("BTC", [(99990, 1)], [(100010, 1)])
    t.set_book("BTC", [(100090, 1)], [(100110, 1)])  # -10 bps structural basis is fine
    m.set_book("ETH", [(2399, 1)], [(2401, 1)])
    t.set_book("ETH", [(2400, 1)], [(2402, 1)])
    pairs, skipped = await resolve_pairs(m, t, [rm("BTC"), rm("ETH"), rm("SOL")])
    assert [p.symbol for p in pairs] == ["BTC", "ETH"]
    assert pairs[0].maker_market is m.markets["BTC"] and pairs[0].taker_market is t.markets["BTC"]
    assert abs(pairs[0].price_ratio - D("0.999")) < D("0.0001")
    assert len(skipped) == 1 and "SOL" in skipped[0] and "not listed on arcus" in skipped[0]


async def test_override_by_venue_ticker():
    m = FakeExchange("arcus", [make_market("arcus", "BTC")])
    m.markets["BTC"].venue_symbol = "BTC-USD"
    t = FakeExchange("lighter", [make_market("lighter", "XBT")])
    pairs, skipped = await resolve_pairs(m, t, [rm("BTC", arcus_symbol="BTC-USD", lighter_symbol="XBT")],
                                         check_prices=False)
    assert skipped == []
    assert (pairs[0].maker_symbol, pairs[0].taker_symbol) == ("BTC", "XBT")
    _, skipped = await resolve_pairs(m, t, [rm("BTC", lighter_symbol="NOPE")], check_prices=False)
    assert "override 'NOPE' not listed on lighter" in skipped[0]


async def test_k_vs_1000_prefix():
    m, t = venues(["KBONK"], ["1000BONK"])
    m.set_book("KBONK", [("0.002919", 1)], [("0.002921", 1)])
    t.set_book("1000BONK", [("0.002924", 1)], [("0.002926", 1)])
    for sym in ("KBONK", "1000BONK", "BONK"):
        pairs, skipped = await resolve_pairs(m, t, [rm(sym)])
        assert skipped == [], sym
        assert (pairs[0].maker_symbol, pairs[0].taker_symbol) == ("KBONK", "1000BONK")


async def test_price_ratio_rejects_different_assets():
    m, t = venues(["QNT"], ["QNT"])
    m.set_book("QNT", [(35.3, 1)], [(35.5, 1)])
    t.set_book("QNT", [(194.4, 1)], [(194.6, 1)])
    pairs, skipped = await resolve_pairs(m, t, [rm("QNT")])
    assert pairs == [] and "price ratio" in skipped[0] and "QNT" in skipped[0]
    pairs, _ = await resolve_pairs(m, t, [rm("QNT")], check_prices=False)
    assert len(pairs) == 1  # the check is what protects us


async def test_inactive_and_missing_price_skipped():
    m, t = venues(["BTC", "ETH"], ["BTC", "ETH"])
    t.markets["BTC"].active = False
    t.markets["BTC"].extra["force_reduce_only"] = True
    pairs, skipped = await resolve_pairs(m, t, [rm("BTC"), rm("ETH")])
    assert pairs == []
    assert any("lighter:BTC inactive (reduce-only)" in s for s in skipped)
    assert any("ETH: no reference price" in s for s in skipped)


async def test_duplicate_config_skipped():
    m, t = venues(["KBONK"], ["1000BONK"])
    pairs, skipped = await resolve_pairs(m, t, [rm("KBONK"), rm("1000BONK")], check_prices=False)
    assert len(pairs) == 1 and "duplicate" in skipped[0]


async def test_price_sources_funding_mark_then_rest_book():
    m, t = venues(["BTC"], ["BTC"])
    m.funding["BTC"] = FundingInfo("arcus", "BTC", D(0), mark_price=D("81871.1"))
    t.books["BTC"] = None  # no live book; fetch_book (REST) returns this snapshot
    from farmer.models import BookLevel, OrderBook
    t.books["BTC"] = OrderBook("lighter", "BTC", [BookLevel(D("81911"), D(1))], [BookLevel(D("81912"), D(1))])
    px = await venue_prices(m, ["BTC"])
    assert px == {"BTC": D("81871.1")}
    px_t = await venue_prices(t, ["BTC", "ETH"])  # ETH: no source at all -> missing, no exception
    assert px_t == {"BTC": D("81911.5")}


def _real_venues():
    arows = json.loads((DATA / "arcus_markets_subset.json").read_text())["markets"]
    lrows = json.loads((DATA / "lighter_obd_subset.json").read_text())
    m = FakeExchange("arcus", [arcus_parse_market(r) for r in arows])
    t = FakeExchange("lighter", list(lighter_parse_markets(lrows).values()))
    for r in arows:
        mk = D(r["markPrice"])
        if mk > 0:
            m.funding[r["baseAsset"]] = FundingInfo("arcus", r["baseAsset"], D(r["nextFundingRate"] or 0), mark_price=mk)
    for d in lrows["order_book_details"]:
        t.funding[d["symbol"]] = FundingInfo("lighter", d["symbol"], D(0), mark_price=D(str(d["mark_price"])))
    return m, t


async def test_real_captured_markets():
    """Live 2026-10-19 metadata: KBONK<->1000BONK maps, QNT is rejected by price, GME (Lighter reduce-only),
    F (Arcus OFFLINE) and GLD (only an XAU proxy on Lighter) are skipped."""
    m, t = _real_venues()
    cfg = [rm(s) for s in ("BTC", "ETH", "KBONK", "QNT", "GME", "F", "GLD", "SPY")]
    pairs, skipped = await resolve_pairs(m, t, cfg)
    got = {p.symbol: (p.maker_symbol, p.taker_symbol) for p in pairs}
    assert got == {"BTC": ("BTC", "BTC"), "ETH": ("ETH", "ETH"), "KBONK": ("KBONK", "1000BONK"), "SPY": ("SPY", "SPY")}
    reasons = " | ".join(skipped)
    assert "QNT: price ratio" in reasons
    assert "GME: lighter:GME inactive (reduce-only)" in reasons
    assert "F: arcus:F inactive" in reasons
    assert "GLD: GLD not listed on lighter" in reasons
    kb = next(p for p in pairs if p.symbol == "KBONK")
    assert D("0.98") <= kb.price_ratio <= D("1.02")
    btc = next(p for p in pairs if p.symbol == "BTC")
    # Arcus step 1e-8 vs Lighter 1e-5 -> common step 1e-5; min = Lighter min base/quote
    assert btc.step == D("0.00001")
    assert btc.min_size(D("80000")) >= t.markets["BTC"].min_size


async def test_allow_reduce_only_maps_close_only_pairs():
    """close-all: a Lighter force_reduce_only market (GME) becomes a close-only pair; Arcus OFFLINE (F) accepts no
    orders at all and stays skipped."""
    m, t = _real_venues()
    assert t.markets["GME"].reduce_only_ok and not t.markets["GME"].active
    assert not m.markets["F"].reduce_only_ok
    cfg = [rm(s) for s in ("BTC", "GME", "F")]
    pairs, skipped = await resolve_pairs(m, t, cfg, allow_reduce_only=True)
    got = {p.symbol: p.close_only for p in pairs}
    assert got == {"BTC": False, "GME": True}
    assert "close only" in next(p for p in pairs if p.symbol == "GME").describe()
    assert len(skipped) == 1 and "F: arcus:F inactive (OFFLINE)" in skipped[0]


def test_find_market_how():
    m, _ = venues(["KBONK"], [])
    assert find_market(m, "1000BONK") == ("KBONK", "prefix rule (KBONK)")
    assert find_market(m, "KBONK") == ("KBONK", "same symbol")
    assert find_market(m, "PEPE")[0] is None


LIVE = os.environ.get("FARMER_LIVE_TESTS") == "1"


@pytest.mark.live
@pytest.mark.skipif(not LIVE, reason="set FARMER_LIVE_TESTS=1 for live read-only tests")
async def test_live_resolve_pairs_public():
    """Read-only: real adapters without credentials, live markets + prices."""
    from farmer.config import ArcusConfig, LighterConfig
    from farmer.exchanges.arcus.client import ArcusExchange
    from farmer.exchanges.lighter.client import LighterExchange

    a = ArcusExchange(ArcusConfig())
    lt = LighterExchange(LighterConfig())
    try:
        await a.connect()
        await lt.connect()
        cfg = [rm(s) for s in ("BTC", "ETH", "SOL", "KBONK", "QNT", "GME", "GLD", "SPY")]
        pairs, skipped = await resolve_pairs(a, lt, cfg)
        got = {p.symbol: p.taker_symbol for p in pairs}
        assert got.get("BTC") == "BTC" and got.get("KBONK") == "1000BONK"
        assert "QNT" not in got and "GLD" not in got
    finally:
        await a.close()
        await lt.close()
