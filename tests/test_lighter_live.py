"""Live READ-ONLY checks against Lighter mainnet (public REST + ``?readonly=true`` websocket).

Skipped unless ``FARMER_LIVE_TESTS=1``. Never places orders, never uses credentials.
"""

import asyncio
import os
import time
from decimal import Decimal as D

import pytest

from farmer.config import LighterConfig
from farmer.exchanges.lighter import LighterExchange
from farmer.exchanges.lighter.helpers import check_market_invariants, hourly_from_funding_rates

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.environ.get("FARMER_LIVE_TESTS") != "1", reason="live tests disabled (set FARMER_LIVE_TESTS=1)"),
]


async def test_live_public_books_and_funding():
    ex = LighterExchange(LighterConfig())  # no credentials -> readonly stream
    assert not ex.has_credentials and ex._ws.url.endswith("readonly=true")
    books = {"BTC": 0, "ETH": 0}
    crossed = []

    def on_book(b):
        books[b.symbol] += 1
        if b.best_bid is not None and b.best_ask is not None and b.best_bid >= b.best_ask:
            crossed.append(b)

    ex.add_book_listener(on_book)
    await ex.connect()
    try:
        assert ex.markets["BTC"].market_id == 1 and ex.markets["ETH"].market_id == 0
        assert all(check_market_invariants(m) == [] for m in ex.markets.values())
        await ex.subscribe_books(["BTC", "ETH"])
        await asyncio.sleep(20)
        for s in ("BTC", "ETH"):
            lb = ex._books[ex.markets[s].market_id]
            assert books[s] > 10 and lb.gaps == 0, (s, books[s], lb.gaps)
            b = ex.get_book(s)
            assert b is not None and b.age() < 5 and len(b.bids) == 20 and len(b.asks) == 20
        assert not crossed
        f = await ex.fetch_funding(["BTC", "ETH"])
        for s in ("BTC", "ETH"):
            ms = ex.market_stats(s)
            assert f[s].rate_hourly == D(ms["current_funding_rate"]) / 100
            assert abs(f[s].rate_hourly) < D("0.005")  # clamp: 0.5 %/h
            assert f[s].next_funding_ts % 3600 == 0 and 0 < f[s].next_funding_ts - time.time() <= 3600
        rows = await ex._funding_rates_rest()
        rest = {r["symbol"]: hourly_from_funding_rates(r["rate"]) for r in rows if r["exchange"] == "lighter"}
        for s in ("BTC", "ETH"):  # REST 8 h fraction / 8 == WS percent / 100 (forecast rate)
            assert abs(rest[s] - f[s].rate_hourly) <= D("0.000002"), (s, rest[s], f[s].rate_hourly)
        assert ex._ws.stats["watchdog_trips"] == 0 and ex._limiter.available() > ex.cfg.read_reserve
    finally:
        await ex.close()
