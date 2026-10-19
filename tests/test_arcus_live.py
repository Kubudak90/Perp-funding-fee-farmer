"""Live READ-ONLY checks against Arcus mainnet (public endpoints only, never trades).

Skipped unless ``FARMER_LIVE_TESTS=1``.
"""

import asyncio
import os
import time

import pytest

from farmer.config import ArcusConfig
from farmer.exchanges.arcus.client import ArcusExchange
from farmer.exchanges.arcus.rest import ArcusAccessError, ArcusRest
from farmer.models import MarketCategory

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.environ.get("FARMER_LIVE_TESTS") != "1", reason="set FARMER_LIVE_TESTS=1 to run"),
]


async def test_live_markets_books_funding():
    ex = ArcusExchange(ArcusConfig())  # no credentials: public data only
    await ex.connect()
    try:
        assert len(ex.markets) >= 50
        btc = ex.markets["BTC"]
        assert btc.venue_symbol == "BTC-USD" and btc.market_id == 1 and btc.category is MarketCategory.CRYPTO
        assert btc.tick_tiers and btc.tick_tiers[-1].up_to_price is None
        assert "KBONK" in ex.markets
        books = {"BTC": 0, "ETH": 0}
        crossed = []

        def on_book(b):
            books[b.symbol] += 1
            if b.best_bid is None or b.best_ask is None or b.best_bid >= b.best_ask:
                crossed.append(b)

        ex.add_book_listener(on_book)
        await ex.subscribe_books(["BTC", "ETH"])
        await asyncio.sleep(12)
        assert books["BTC"] > 5 and books["ETH"] > 5 and not crossed
        for s in ("BTC", "ETH"):
            ob = ex.get_book(s)
            assert ob.age() < 5 and len(ob.bids) >= 10 and len(ob.asks) >= 10
            st = ex.stats()["books"][s]
            assert st["synced"] and st["gaps"] == 0
        fund = await ex.fetch_funding(["BTC", "ETH"])
        for fi in fund.values():
            assert abs(fi.rate_hourly) < 0.04 and fi.next_funding_ts > time.time() - 5
            assert fi.mark_price and fi.index_price
        rb = await ex.fetch_book("BTC", 10)
        assert rb.best_bid < rb.best_ask
    finally:
        await ex.close()


async def test_live_whitelist_gate_is_explicit():
    rest = ArcusRest("https://api.arcus.xyz")
    await rest.start()
    try:
        with pytest.raises(ArcusAccessError):
            await rest.positions("0x0000000000000000000000000000000000000001", 0)
        t = await rest.server_time_ns()
        assert abs(t / 1e9 - time.time()) < 30
    finally:
        await rest.close()
