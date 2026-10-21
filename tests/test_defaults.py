"""The built-in defaults are sized for a small account (~$200 of collateral per venue) and must stay consistent:
clips fit the real venue minimums of the default markets, the caps leave room for a full cycle, and a full cycle
on $200 paper venues passes every opening gate with margin to spare (captured public market metadata, no network).
"""

import json
from decimal import Decimal as D
from pathlib import Path

import pytest

from farmer.config import AppConfig, MarketConfig, PaperConfig, RiskConfig, StrategyConfig
from farmer.exchanges.arcus.parse import parse_market as arcus_parse_market
from farmer.exchanges.lighter.helpers import parse_markets as lighter_parse_markets
from farmer.exchanges.paper.client import PaperExchange
from farmer.models import Side
from farmer.risk.manager import RiskManager
from farmer.strategy.mapping import resolve_pairs
from farmer.strategy.pricing import round_pair_size
from tests.fakes import FakeExchange

DATA = Path(__file__).parent / "data"


def _captured_venues() -> tuple[FakeExchange, FakeExchange]:
    """Arcus /v1/markets + Lighter orderBookDetails captures; books at each venue's captured price."""
    arows = json.loads((DATA / "arcus" / "markets.json").read_text())["markets"]
    lrows = json.loads((DATA / "lighter" / "order_book_details.json").read_text())
    maker = FakeExchange("arcus", [arcus_parse_market(r) for r in arows])
    taker = FakeExchange("lighter", list(lighter_parse_markets(lrows).values()))
    prices = {"arcus": {r["baseAsset"]: D(r["markPrice"]) for r in arows},
              "lighter": {d["symbol"]: D(str(d["last_trade_price"])) for d in lrows["order_book_details"]}}
    for ex in (maker, taker):
        for sym, m in ex.markets.items():
            px = prices[ex.name].get(sym)
            if not px or px <= 0:
                continue
            tick = m.tick_for(px)
            bid = (px / tick).to_integral_value() * tick
            ex.set_book(sym, [(bid, 1_000_000)], [(bid + tick, 1_000_000)])
    return maker, taker


async def _default_pairs(cfg: AppConfig):
    maker, taker = _captured_venues()
    pairs, skipped = await resolve_pairs(maker, taker, cfg.enabled_markets())
    assert not skipped, skipped
    return maker, taker, pairs


def _move_books(ex: FakeExchange, factor: D) -> None:
    for sym, b in list(ex.books.items()):
        if b is None or not b.bids:
            continue
        m = ex.markets[sym]
        px = b.bids[0].price * factor
        tick = m.tick_for(px)
        bid = (px / tick).to_integral_value() * tick
        ex.set_book(sym, [(bid, 1_000_000)], [(bid + tick, 1_000_000)])


# --------------------------------------------------------------------------- static consistency
def test_defaults_are_sized_for_200_usd_per_venue():
    cfg = AppConfig()
    s, r = cfg.strategy, cfg.risk
    assert cfg.warnings() == []
    assert [m.symbol for m in s.markets] == ["ETH", "SOL"]
    assert cfg.paper.initial_balance == D("200")
    assert cfg.paper.default_leverage == s.leverage <= 3  # paper mirrors live; low leverage
    mkts = cfg.enabled_markets()
    top = sum((m.target_notional for m in mkts), D(0))
    clip = max(m.order_notional for m in mkts)
    # a full cycle fits the exposure cap with one clip of price-drift headroom
    assert top + clip <= r.max_total_notional
    # effective leverage at the top of a cycle stays low (liquidation needs a very large move)
    assert top / cfg.paper.initial_balance <= D("1.5")
    for m in mkts:
        assert m.order_notional <= m.target_notional
        assert m.target_notional / m.order_notional >= 2  # several clips per cycle (volume per cycle, small deltas)
    # a whole unhedged clip escalates (pause quotes, wider hedge) ...
    assert r.max_unhedged_notional < clip
    # ... and the daily loss limit is a small fraction of the combined collateral
    assert 0 < r.max_daily_loss <= D("0.1") * 2 * cfg.paper.initial_balance


async def test_default_clips_fit_the_real_venue_minimums():
    cfg = AppConfig()
    maker, _, pairs = await _default_pairs(cfg)
    assert [p.symbol for p in pairs] == ["ETH", "SOL"]
    for p in pairs:
        assert p.maker_market.active and p.taker_market.active
        px = maker.get_book(p.maker_symbol).mid
        min_notional = p.min_size(px) * px
        clip_notional = round_pair_size(p.cfg.order_notional / px, p.maker_market, p.taker_market) * px
        # the clip is several pair minimums: partial fills and the hedge venue's minimum are small next to it
        assert clip_notional >= 4 * min_notional, (p.symbol, clip_notional, min_notional)
        # dust (a residual below the hedge minimum) never reaches the escalation threshold; a whole clip does
        assert min_notional < cfg.risk.max_unhedged_notional < clip_notional
        # both venues publish the market's maintenance fraction (the paper margin model uses it)
        assert p.maker_market.maintenance_margin_fraction and p.taker_market.maintenance_margin_fraction


# --------------------------------------------------------------------------- a full cycle on $200 paper venues
@pytest.mark.parametrize("maker_side", [Side.BUY, Side.SELL])
async def test_default_cycle_passes_every_gate_on_200_usd_paper_venues(maker_side):
    cfg = AppConfig()
    maker, taker, pairs = await _default_pairs(cfg)
    zero = {"arcus": 0.0, "lighter": 0.0}
    pcfg = PaperConfig(maker_latency_ms=zero, taker_latency_ms=zero)  # every other field: the default
    pa, pl = PaperExchange(maker, pcfg), PaperExchange(taker, pcfg)
    for p in pairs:
        await pa.set_leverage(p.maker_symbol, p.cfg.leverage)
        await pl.set_leverage(p.taker_symbol, p.cfg.leverage)
    risk = RiskManager(cfg.risk, {"arcus": pa, "lighter": pl}, leverage={p.symbol: p.cfg.leverage for p in pairs},
                       default_leverage=cfg.strategy.leverage)
    hedge_side = Side.SELL if maker_side is Side.BUY else Side.BUY
    far = {Side.BUY: D("1e9"), Side.SELL: D("1e-9")}

    async def expose():
        await risk.refresh_balances()
        for p in pairs:
            for ex, sym in ((pa, p.maker_symbol), (pl, p.taker_symbol)):
                risk.update_exposure(p.symbol, ex.name, abs(ex.position(sym)) * maker.get_book(p.maker_symbol).mid)

    clips = 0
    for p in pairs:  # the trader's OPENING loop: clip by clip up to the target, every clip through check_open
        mid = maker.get_book(p.maker_symbol).mid
        goal = p.cfg.target_notional / mid
        min_sz = p.min_size(mid)
        while goal - abs(pa.position(p.maker_symbol)) >= min_sz:
            size = round_pair_size(min(p.cfg.order_notional / mid, goal - abs(pa.position(p.maker_symbol))),
                                   p.maker_market, p.taker_market)
            size = max(size, min_sz)
            await expose()
            ok, why = risk.check_open(p.symbol, size * mid)
            assert ok, why
            assert (await pa.place_market(p.maker_symbol, maker_side, size, worst_price=far[maker_side])).ok
            assert (await pl.place_market(p.taker_symbol, hedge_side, size, worst_price=far[hedge_side])).ok
            clips += 1
        assert pa.position(p.maker_symbol) == -pl.position(p.taker_symbol) != 0
    assert clips == 6  # 3 clips per market
    await expose()
    for name in ("arcus", "lighter"):
        bal = risk.balance(name)
        assert risk.exposure(name) <= cfg.risk.max_total_notional
        # real maintenance fractions: ~0.02-0.05 at the top (the old notional/(2*lev) model sat AT 0.25 here)
        assert bal.margin_ratio < D("0.1"), (name, bal)
    # a 15 % move against either leg: no margin stop, no forced reduction, and the next cycle can still open
    for factor in (D("0.85"), D("1.15")):
        _move_books(maker, factor)
        _move_books(taker, factor)
        await expose()
        assert not risk.must_reduce()
        for name in ("arcus", "lighter"):
            bal = risk.balance(name)
            assert bal.margin_ratio < cfg.risk.max_margin_ratio, (factor, name, bal)
            need = cfg.strategy.order_notional / cfg.strategy.leverage * cfg.risk.min_free_collateral_mult
            assert bal.available >= need, (factor, name, bal)
        _move_books(maker, 1 / factor)
        _move_books(taker, 1 / factor)


# --------------------------------------------------------------------------- warnings for a scaled-up config
def test_warns_when_the_exposure_cap_cannot_hold_every_target():
    cfg = AppConfig(strategy=StrategyConfig(markets=[MarketConfig(symbol=s) for s in ("ETH", "SOL", "BTC")]))
    w = cfg.warnings()
    assert len(w) == 1 and "max_total_notional" in w[0] and "500" in w[0]  # 3 x 150 + 50
    ok = AppConfig(strategy=cfg.strategy, risk=RiskConfig(max_total_notional=D("500")))
    assert ok.warnings() == []
    # disabled markets and per-market overrides count as configured
    m = [MarketConfig(symbol="ETH", target_notional=D("300")), MarketConfig(symbol="SOL", enabled=False)]
    assert AppConfig(strategy=StrategyConfig(markets=m)).warnings() == []  # 300 + 50 <= 350
    m[0].order_notional = D("100")
    assert any("max_total_notional" in x for x in AppConfig(strategy=StrategyConfig(markets=m)).warnings())


def test_warns_when_paper_leverage_differs_and_is_actually_used():
    s = StrategyConfig(set_leverage_on_start=False, leverage=5)
    w = AppConfig(strategy=s).warnings()
    assert len(w) == 1 and "paper.default_leverage" in w[0]
    assert AppConfig(strategy=s, paper=PaperConfig(default_leverage=5)).warnings() == []
    # with set_leverage_on_start the paper venue gets strategy.leverage anyway
    assert AppConfig(strategy=StrategyConfig(leverage=5, set_leverage_on_start=True)).warnings() == []
