"""PaperExchange: fill simulation, accounting and funding (no network)."""

from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal as D
from pathlib import Path

import pytest

from farmer.config import PaperConfig
from farmer.exchanges.paper.client import DEFAULT_LEVERAGE, PaperExchange, _PaperPosition
from farmer.models import FundingInfo, MarketInfo, OrderStatus, Side
from farmer.persistence.store import Store
from tests.fakes import FakeExchange, make_market

DATA = Path(__file__).parent / "data" / "paper"


def pcfg(maker_ms: float = 0.0, taker_ms: float = 0.0, **kw) -> PaperConfig:
    return PaperConfig(maker_latency_ms={"arcus": maker_ms, "lighter": maker_ms},
                       taker_latency_ms={"arcus": taker_ms, "lighter": taker_ms}, **kw)


def make(maker_fee="0", taker_fee="0", cfg: PaperConfig | None = None, name="arcus", **kw):
    inner = FakeExchange(name, [make_market(name, "BTC", tick="0.1", step="0.001", min_size="0.001",
                                            min_notional="1", maker_fee=maker_fee, taker_fee=taker_fee)])
    px = PaperExchange(inner, cfg or pcfg(), **kw)
    return inner, px


class Rec:
    """Collects listener events in order."""

    def __init__(self, ex) -> None:
        self.events: list[tuple[str, object]] = []
        ex.add_fill_listener(lambda f: self.events.append(("fill", f)))
        ex.add_order_listener(lambda o: self.events.append(("order", (o.client_id, o.status, o.filled))))
        ex.add_book_listener(lambda b: self.events.append(("book", b)))

    def fills(self):
        return [e[1] for e in self.events if e[0] == "fill"]

    def orders(self):
        return [e[1] for e in self.events if e[0] == "order"]


# ------------------------------------------------------------------ identity / delegation
async def test_identity_and_delegation():
    inner, px = make()
    assert px.name == "arcus" and px.is_paper and px.has_credentials
    assert px.markets is inner.markets and "BTC" in px.markets
    inner.markets["ETH"] = make_market("arcus", "ETH")
    assert "ETH" in px.markets  # always the live dict
    assert px.market("BTC").tick_size == D("0.1")
    b = inner.set_book("BTC", [(100, 1)], [(101, 1)])
    assert px.get_book("BTC") is b
    assert await px.fetch_book("BTC") is b
    await px.subscribe_books(["BTC"])
    assert ("subscribe_books", ("BTC",)) in inner.calls
    await px.connect()
    assert inner.connected
    await px.close()
    await px.close()  # idempotent
    assert not inner.connected
    assert px.can_increase_exposure() == (True, "")
    assert px.market_open("BTC")


async def test_book_reemitted_after_fill_simulation():
    inner, px = make()
    rec = Rec(px)
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    r = await px.place_limit("BTC", Side.BUY, D("0.5"), D("100.5"), client_id="b1")
    assert r.ok
    inner.set_book("BTC", [(99, 1)], [(100.4, 2)])
    kinds = [k for k, _ in rec.events]
    # OPEN ack, book, then for the trade-through: fill -> order update -> book
    assert kinds == ["book", "order", "fill", "order", "book"]


# ------------------------------------------------------------------ post-only
async def test_post_only_rejects_crossing_orders():
    inner, px = make()
    rec = Rec(px)
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    r = await px.place_limit("BTC", Side.BUY, D("0.1"), D("101"), client_id="x1")
    assert not r.ok and r.would_cross and r.order.status is OrderStatus.REJECTED and not r.retryable
    r = await px.place_limit("BTC", Side.SELL, D("0.1"), D("100"), client_id="x2")
    assert not r.ok and r.would_cross
    r = await px.place_limit("BTC", Side.BUY, D("0.1"), D("100.9"), client_id="x3")
    assert r.ok and r.order.status is OrderStatus.OPEN and r.order.order_id
    assert [o[1] for o in rec.orders()] == [OrderStatus.REJECTED, OrderStatus.REJECTED, OrderStatus.OPEN]
    assert [o.client_id for o in await px.fetch_open_orders()] == ["x3"]
    assert px.position("BTC") == 0 and rec.fills() == []


async def test_post_only_check_uses_book_after_maker_latency():
    inner, px = make(cfg=pcfg(maker_ms=40))
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    t0 = time.monotonic()
    task = asyncio.create_task(px.place_limit("BTC", Side.BUY, D("0.1"), D("100.5")))
    await asyncio.sleep(0.01)
    inner.set_book("BTC", [(100, 1)], [(100.5, 1)])  # ask moved onto our price during the latency
    r = await task
    assert time.monotonic() - t0 >= 0.035
    assert not r.ok and r.would_cross


async def test_no_book_is_retryable_rejection():
    _, px = make()
    r = await px.place_limit("BTC", Side.BUY, D("0.1"), D("100"))
    assert not r.ok and r.retryable and "no book" in r.error
    r = await px.place_market("BTC", Side.BUY, D("0.1"), worst_price=D("100"))
    assert not r.ok and r.retryable


async def test_validation_errors():
    inner, px = make()
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    assert "step" in (await px.place_limit("BTC", Side.BUY, D("0.0015"), D("100"))).error
    assert "tick" in (await px.place_limit("BTC", Side.BUY, D("0.1"), D("100.05"))).error
    assert "notional" in (await px.place_limit("BTC", Side.BUY, D("0.001"), D("100"))).error
    assert "unknown market" in (await px.place_limit("DOGE", Side.BUY, D("1"), D("1"))).error
    assert "positive" in (await px.place_limit("BTC", Side.BUY, D("0"), D("100"))).error
    assert "Decimal" in (await px.place_limit("BTC", Side.BUY, 0.1, D("100"))).error  # type: ignore[arg-type]
    r = await px.place_limit("BTC", Side.BUY, D("0.1"), D("99"), client_id="dup")
    assert r.ok
    assert "duplicate" in (await px.place_limit("BTC", Side.BUY, D("0.1"), D("99"), client_id="dup")).error


# ------------------------------------------------------------------ maker fills
async def test_maker_fill_on_trade_through_at_order_price():
    inner, px = make(maker_fee="0.0002")
    rec = Rec(px)
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    r = await px.place_limit("BTC", Side.BUY, D("1"), D("100.5"), client_id="m1")
    inner.set_book("BTC", [(100.1, 1)], [(100.6, 5)])  # no trade-through yet
    assert rec.fills() == []
    inner.set_book("BTC", [(99, 1)], [(100.2, 5)])  # ask trades through 100.5
    (f,) = rec.fills()
    assert f.is_maker and f.price == D("100.5") and f.size == D("1") and f.side is Side.BUY
    assert f.fee == D("100.5") * D("0.0002")
    assert f.client_id == "m1" and f.order_id == r.order.order_id and f.trade_id
    assert r.order.status is OrderStatus.FILLED and r.order.avg_fill_price == D("100.5")
    assert px.position("BTC") == D("1")
    assert await px.fetch_open_orders() == []


async def test_maker_partial_fills_only_against_size_at_or_better():
    inner, px = make()
    rec = Rec(px)
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    r = await px.place_limit("BTC", Side.BUY, D("2"), D("100.5"), client_id="p1")
    inner.set_book("BTC", [(99, 1)], [(100.5, 0.7), (100.6, 10)])  # only 0.7 at <= 100.5
    assert r.order.filled == D("0.7") and r.order.status is OrderStatus.PARTIALLY_FILLED
    inner.set_book("BTC", [(99, 1)], [(100.3, 0.2), (100.4, 0.3), (100.9, 10)])
    assert r.order.filled == D("1.2")
    inner.set_book("BTC", [(99, 1)], [(100.0, 5)])
    assert r.order.filled == D("2") and r.order.status is OrderStatus.FILLED
    fills = rec.fills()
    assert [f.size for f in fills] == [D("0.7"), D("0.5"), D("0.8")]
    assert all(f.price == D("100.5") and f.is_maker for f in fills)
    assert len({f.trade_id for f in fills}) == 3
    statuses = [o[1] for o in rec.orders()]
    assert statuses == [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.PARTIALLY_FILLED,
                        OrderStatus.FILLED]


async def test_maker_sell_symmetric_and_shared_liquidity():
    inner, px = make()
    rec = Rec(px)
    inner.set_book("BTC", [(100, 1)], [(102, 1)])
    a = await px.place_limit("BTC", Side.SELL, D("1"), D("101.5"), client_id="s1")
    b = await px.place_limit("BTC", Side.SELL, D("1"), D("101"), client_id="s2")
    # bid 101.6 x 1.5 trades through both: best-priced (lowest ask) order first, liquidity shared
    inner.set_book("BTC", [(101.6, 1.5)], [(102, 1)])
    assert b.order.filled == D("1") and a.order.filled == D("0.5")
    assert {(f.client_id, f.price) for f in rec.fills()} == {("s2", D("101")), ("s1", D("101.5"))}
    assert px.position("BTC") == D("-1.5")


async def test_maker_fill_rounds_down_to_step():
    inner, px = make()
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    r = await px.place_limit("BTC", Side.BUY, D("1"), D("100.5"))
    inner.set_book("BTC", [(99, 1)], [(100.5, "0.0004")])  # less than one step
    assert r.order.filled == 0
    inner.set_book("BTC", [(99, 1)], [(100.5, "0.0127")])
    assert r.order.filled == D("0.012")


# ------------------------------------------------------------------ taker
async def test_taker_sweeps_levels_within_worst_price_ioc_remainder_canceled():
    inner, px = make(cfg=pcfg(taker_fee={"arcus": D("0.0005")}))
    rec = Rec(px)
    inner.set_book("BTC", [(100, 5)], [(101, 1), (102, 1), (103, 5)])
    r = await px.place_market("BTC", Side.BUY, D("3"), worst_price=D("102.5"), client_id="t1")
    assert r.ok
    o = r.order
    assert o.filled == D("2") and o.status is OrderStatus.CANCELED and o.ioc
    assert o.avg_fill_price == D("101.5")  # VWAP of the consumed levels
    fills = rec.fills()
    assert [(f.price, f.size) for f in fills] == [(D("101"), D("1")), (D("102"), D("1"))]
    assert all(not f.is_maker for f in fills)
    assert [f.fee for f in fills] == [D("101") * D("0.0005"), D("102") * D("0.0005")]
    assert len({f.trade_id for f in fills}) == 2
    assert rec.orders()[-1] == ("t1", OrderStatus.CANCELED, D("2"))
    assert px.position("BTC") == D("2")


async def test_taker_full_fill_sell_and_consumed_liquidity():
    inner, px = make()
    inner.set_book("BTC", [(100, 1), (99, 1)], [(101, 1)])
    r1 = await px.place_market("BTC", Side.SELL, D("1.5"), worst_price=D("98"))
    assert r1.order.status is OrderStatus.FILLED
    assert r1.order.avg_fill_price == (D("100") * 1 + D("99") * D("0.5")) / D("1.5")
    # same book snapshot: 100 x1 and 0.5 of 99 are gone -> only 0.5 left at 99
    r2 = await px.place_market("BTC", Side.SELL, D("1"), worst_price=D("98"))
    assert r2.order.filled == D("0.5") and r2.order.status is OrderStatus.CANCELED
    inner.set_book("BTC", [(100, 1), (99, 1)], [(101, 1)])  # new book -> liquidity refreshed
    r3 = await px.place_market("BTC", Side.SELL, D("1"), worst_price=D("98"))
    assert r3.order.filled == D("1")


async def test_taker_no_liquidity_within_worst_price():
    inner, px = make()
    rec = Rec(px)
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    r = await px.place_market("BTC", Side.BUY, D("1"), worst_price=D("100.5"))
    assert r.ok and r.order.filled == 0 and r.order.status is OrderStatus.CANCELED
    assert rec.fills() == [] and px.position("BTC") == 0


async def test_taker_latency_respected_and_uses_current_book():
    inner, px = make(cfg=pcfg(taker_ms=50))
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    t0 = time.monotonic()
    task = asyncio.create_task(px.place_market("BTC", Side.BUY, D("1"), worst_price=D("105")))
    await asyncio.sleep(0.01)
    inner.set_book("BTC", [(102, 1)], [(103, 1)])  # market moved during the latency
    r = await task
    assert time.monotonic() - t0 >= 0.045
    assert r.order.avg_fill_price == D("103")


async def test_cancel_cannot_stop_inflight_ioc():
    inner, px = make(cfg=pcfg(taker_ms=20))
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    task = asyncio.create_task(px.place_market("BTC", Side.BUY, D("1"), worst_price=D("105"), client_id="io"))
    await asyncio.sleep(0)
    assert await px.cancel_order("BTC", client_id="io")
    r = await task
    assert r.order.status is OrderStatus.FILLED


# ------------------------------------------------------------------ modify / cancel
async def test_modify_reprice_and_resize_in_place():
    inner, px = make()
    rec = Rec(px)
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    r = await px.place_limit("BTC", Side.BUY, D("1"), D("100"), client_id="mo")
    oid = r.order.order_id
    m = await px.modify_order(r.order, price=D("100.5"), size=D("0.6"))
    assert m.ok and m.order is r.order and m.order.order_id == oid
    assert m.order.price == D("100.5") and m.order.size == D("0.6") and m.order.status is OrderStatus.OPEN
    inner.set_book("BTC", [(99, 1)], [(100.5, 0.2)])
    assert r.order.filled == D("0.2")
    m = await px.modify_order(r.order, price=D("100.4"), size=D("0.3"))  # size = new REMAINING
    assert m.ok and r.order.size == D("0.5") and r.order.remaining == D("0.3")
    assert r.order.status is OrderStatus.PARTIALLY_FILLED
    assert rec.orders()[-1][1] is OrderStatus.PARTIALLY_FILLED


async def test_modify_that_would_cross_cancels_the_order():
    inner, px = make()
    rec = Rec(px)
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    r = await px.place_limit("BTC", Side.SELL, D("1"), D("101.5"), client_id="mc")
    m = await px.modify_order(r.order, price=D("100"))
    assert not m.ok and m.would_cross
    assert r.order.status is OrderStatus.CANCELED
    assert rec.orders()[-1] == ("mc", OrderStatus.CANCELED, D("0"))
    assert await px.fetch_open_orders() == []
    m = await px.modify_order(r.order, price=D("102"))
    assert not m.ok and "not open" in m.error


async def test_modify_validation():
    inner, px = make()
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    r = await px.place_limit("BTC", Side.BUY, D("1"), D("100"))
    assert "tick" in (await px.modify_order(r.order, price=D("100.01"))).error
    assert "positive" in (await px.modify_order(r.order, price=D("100"), size=D("0"))).error
    assert "step" in (await px.modify_order(r.order, price=D("100"), size=D("0.0001"))).error
    assert r.order.price == D("100") and r.order.size == D("1")


async def test_cancel_order_cancel_all_and_open_orders():
    inner, px = make()
    rec = Rec(px)
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    a = await px.place_limit("BTC", Side.BUY, D("0.1"), D("99"), client_id="c1")
    await px.place_limit("BTC", Side.BUY, D("0.1"), D("98"), client_id="c2")
    await px.place_limit("BTC", Side.SELL, D("0.1"), D("102"), client_id="c3")
    assert len(await px.fetch_open_orders("BTC")) == 3
    assert await px.cancel_order("BTC", order_id=a.order.order_id)
    assert a.order.status is OrderStatus.CANCELED
    assert await px.cancel_order("BTC", client_id="nope")  # unknown -> already gone
    assert await px.cancel_order("BTC", client_id="c1")  # already canceled
    await px.cancel_all("ETH")
    assert len(await px.fetch_open_orders()) == 2
    await px.cancel_all()
    assert await px.fetch_open_orders() == []
    assert [o[1] for o in rec.orders()].count(OrderStatus.CANCELED) == 3
    # canceled orders never fill
    inner.set_book("BTC", [(90, 1)], [(91, 10)])
    assert rec.fills() == []
    with pytest.raises(ValueError):
        await px.cancel_order("BTC")


async def test_cancel_during_placement_latency():
    inner, px = make(cfg=pcfg(maker_ms=30))
    inner.set_book("BTC", [(100, 1)], [(101, 1)])
    task = asyncio.create_task(px.place_limit("BTC", Side.BUY, D("0.1"), D("99"), client_id="lat"))
    await asyncio.sleep(0.005)
    # same latency: the cancel lands after the ack -> the resting order is canceled
    cancel = asyncio.create_task(px.cancel_order("BTC", client_id="lat"))
    r = await task
    assert r.ok
    await cancel
    assert r.order.status is OrderStatus.CANCELED and await px.fetch_open_orders() == []
    # a cancel that overtakes the placement kills it on arrival
    task = asyncio.create_task(px.place_limit("BTC", Side.BUY, D("0.1"), D("99"), client_id="lat2"))
    await asyncio.sleep(0.005)
    px.cfg.maker_latency_ms["arcus"] = 0.0
    assert await px.cancel_order("BTC", client_id="lat2")
    r = await task
    assert not r.ok and r.order.status is OrderStatus.CANCELED
    assert await px.fetch_open_orders() == []


# ------------------------------------------------------------------ reduce-only / margin
async def test_reduce_only_rules():
    inner, px = make()
    inner.set_book("BTC", [(100, 5)], [(101, 5)])
    r = await px.place_market("BTC", Side.SELL, D("1"), worst_price=D("90"), reduce_only=True)
    assert not r.ok and "reduce-only" in r.error
    r = await px.place_limit("BTC", Side.SELL, D("1"), D("102"), reduce_only=True)
    assert not r.ok and "reduce-only" in r.error
    await px.place_market("BTC", Side.BUY, D("1"), worst_price=D("110"))
    assert px.position("BTC") == D("1")
    r = await px.place_market("BTC", Side.SELL, D("2"), worst_price=D("90"), reduce_only=True)
    assert r.order.filled == D("1") and r.order.status is OrderStatus.CANCELED
    assert px.position("BTC") == 0
    # resting reduce-only order canceled when there's nothing left to reduce
    await px.place_market("BTC", Side.BUY, D("1"), worst_price=D("110"))
    ro = await px.place_limit("BTC", Side.SELL, D("1"), D("102"), reduce_only=True)
    assert ro.ok
    await px.place_market("BTC", Side.SELL, D("1"), worst_price=D("90"))
    inner.set_book("BTC", [(103, 5)], [(104, 5)])
    assert ro.order.status is OrderStatus.CANCELED and px.position("BTC") == 0


async def test_insufficient_margin_rejected():
    inner, px = make(cfg=pcfg(initial_balance=D("1000")))
    inner.set_book("BTC", [(100, 500)], [(101, 500)])
    await px.set_leverage("BTC", 2)
    r = await px.place_market("BTC", Side.BUY, D("30"), worst_price=D("110"))  # IM 1515 > 1000
    assert not r.ok and "margin" in r.error
    r = await px.place_limit("BTC", Side.BUY, D("30"), D("99"))
    assert not r.ok and "margin" in r.error and r.order.status is OrderStatus.REJECTED
    assert (await px.place_market("BTC", Side.BUY, D("10"), worst_price=D("110"))).order.filled == D("10")


# ------------------------------------------------------------------ accounting
async def test_positions_and_balance_math():
    inner, px = make(cfg=pcfg(initial_balance=D("10000"), taker_fee={"arcus": D("0.001")}))
    await px.set_leverage("BTC", 4)
    inner.set_book("BTC", [(99, 10)], [(100, 10)])
    await px.place_market("BTC", Side.BUY, D("2"), worst_price=D("100"))  # buy 2 @ 100, fee 0.2
    pos = (await px.fetch_positions())["BTC"]
    assert pos.size == D("2") and pos.entry_price == D("100") and pos.leverage == D("4")
    assert pos.mark_price == D("99.5") and pos.unrealized_pnl == D("-1.0")
    bal = await px.fetch_balance()
    assert bal.equity == D("10000") - D("200") - D("0.2") + D("2") * D("99.5")  # 9998.8
    assert bal.margin_used == D("2") * D("99.5") / 4
    assert bal.maintenance_margin == D("2") * D("99.5") / 8
    assert bal.available == bal.equity - bal.margin_used
    # sell 0.5 @ 110 -> realized +5 on that part
    inner.set_book("BTC", [(110, 10)], [(111, 10)])
    await px.place_market("BTC", Side.SELL, D("0.5"), worst_price=D("100"))
    pos = (await px.fetch_positions())["BTC"]
    assert pos.size == D("1.5") and pos.entry_price == D("100")
    st = px.stats()
    assert st["realized"] == D("5.0")
    assert st["fees"] == D("0.2") + D("0.055")
    bal = await px.fetch_balance()
    expected = D("10000") + (-D("200") + D("55")) - (D("0.2") + D("0.055")) + D("1.5") * D("110.5")
    assert bal.equity == expected
    # identical to initial + realized - fees + unrealized
    assert bal.equity == D("10000") + D("5") - st["fees"] + (D("110.5") - D("100")) * D("1.5")


async def test_position_flip_and_average_entry():
    inner, px = make()
    inner.set_book("BTC", [(99, 10)], [(100, 10)])
    await px.place_market("BTC", Side.BUY, D("1"), worst_price=D("100"))
    inner.set_book("BTC", [(101, 10)], [(102, 10)])
    await px.place_market("BTC", Side.BUY, D("1"), worst_price=D("102"))
    assert (await px.fetch_positions())["BTC"].entry_price == D("101")
    await px.place_market("BTC", Side.SELL, D("3"), worst_price=D("101"))  # close 2 @ 101, open short 1 @ 101
    pos = (await px.fetch_positions())["BTC"]
    assert pos.size == D("-1") and pos.entry_price == D("101")
    assert px.stats()["realized"] == 0
    await px.place_market("BTC", Side.BUY, D("1"), worst_price=D("102"))
    assert await px.fetch_positions() == {}


async def test_set_leverage_clamped_and_default():
    inner, px = make()
    assert px.leverage("BTC") == DEFAULT_LEVERAGE
    inner.markets["BTC"].max_leverage = D("20")
    await px.set_leverage("BTC", 50)
    assert px.leverage("BTC") == 20
    await px.set_leverage("BTC", 0)
    assert px.leverage("BTC") == 1


async def test_fee_sources_override_then_market():
    _, px = make(maker_fee="0.0002", taker_fee="0.0004")
    assert px.fee_rate("BTC", maker=True) == D("0.0002") and px.fee_rate("BTC", maker=False) == D("0.0004")
    _, px2 = make(maker_fee="0.0002", cfg=pcfg(maker_fee={"arcus": D("0")}))
    assert px2.fee_rate("BTC", maker=True) == 0
    assert px2.fee_rate("XXX", maker=True) == 0


async def test_trade_ids_unique_across_instances_store_keeps_all_fills():
    st = Store(":memory:", paper=True)
    for _ in range(2):  # e.g. a restart: ids must not collide in the store
        inner, px = make()
        px.add_fill_listener(lambda f: st.record_fill(f, "hedge"))
        inner.set_book("BTC", [(99, 10)], [(100, 10)])
        await px.place_market("BTC", Side.BUY, D("1"), worst_price=D("100"))
        await px.place_market("BTC", Side.SELL, D("1"), worst_price=D("99"))
    assert st.summary(0)["arcus"].fills == 4


# ------------------------------------------------------------------ captured real books
def _book_from(path: Path):
    j = json.loads(path.read_text())
    return j, [tuple(x) for x in j["bids"]], [tuple(x) for x in j["asks"]]


async def test_arcus_captured_book_post_only_and_partial_maker_fill():
    j, bids, asks = _book_from(DATA / "arcus_btc_book.json")
    mk = j["market"]
    inner = FakeExchange("arcus", [MarketInfo(venue="arcus", symbol="BTC", venue_symbol="BTC-USD", market_id=1,
                                              tick_size=D(mk["tickSize"]), step_size=D(mk["stepSize"]),
                                              min_size=D(mk["minOrderSize"]), min_notional=D(mk["minOrderNotional"]))])
    px = PaperExchange(inner, pcfg())
    inner.set_book("BTC", bids, asks)
    best_bid, best_ask = D(bids[0][0]), D(asks[0][0])  # one-tick spread 81947.4 / 81947.5
    r = await px.place_limit("BTC", Side.BUY, D("0.005"), best_ask)
    assert not r.ok and r.would_cross
    r = await px.place_limit("BTC", Side.BUY, D("0.005"), best_bid)
    assert r.ok
    # ask ticks down onto our bid with 0.00270139 BTC -> partial maker fill at our price
    inner.set_book("BTC", bids[1:], [(str(best_bid), "0.00270139")] + asks)
    assert r.order.filled == D("0.00270139") and r.order.status is OrderStatus.PARTIALLY_FILLED
    assert r.order.avg_fill_price == best_bid


async def test_lighter_captured_book_taker_sweep():
    j, bids, asks = _book_from(DATA / "lighter_btc_book.json")
    mk = j["market"]
    inner = FakeExchange("lighter", [MarketInfo(venue="lighter", symbol="BTC", venue_symbol="BTC", market_id=1,
                                                tick_size=D("0.1"), step_size=D("0.00001"),
                                                min_size=D(mk["min_base_amount"]), min_notional=D("10"),
                                                taker_fee=D(mk["taker_fee"]))])
    px = PaperExchange(inner, pcfg())
    rec = Rec(px)
    inner.set_book("BTC", bids, asks)
    r = await px.place_market("BTC", Side.BUY, D("0.012"), worst_price=D("82016.0"))
    # 0.00609 @ 82015.3 + 0.00408 @ 82015.7; 82016.9 is beyond the worst price
    assert [(f.price, f.size) for f in rec.fills()] == [(D("82015.3"), D("0.00609")), (D("82015.7"), D("0.00408"))]
    assert r.order.filled == D("0.01017") and r.order.status is OrderStatus.CANCELED
    vwap, _ = inner.get_book("BTC").vwap(Side.BUY, D("0.01017"))
    assert r.order.avg_fill_price == vwap
    assert all(f.fee == 0 for f in rec.fills())


# ------------------------------------------------------------------ funding
def _fi(rate, index=None, mark=None, venue="arcus"):
    return FundingInfo(venue=venue, symbol="BTC", rate_hourly=D(rate), index_price=None if index is None else D(index),
                       mark_price=None if mark is None else D(mark))


async def test_settle_funding_formula_and_sign():
    inner, px = make()
    inner.set_book("BTC", [(99, 10)], [(100, 10)])
    await px.place_market("BTC", Side.BUY, D("2"), worst_price=D("100"))
    inner.funding["BTC"] = _fi("0.0000125", index="81948.1", mark="81946.5")
    await px.fetch_funding(["BTC"])  # also fills the cache
    (p,) = px.settle_funding(time.time() + 1)
    assert p.amount == -D("2") * D("81948.1") * D("0.0000125")  # long pays positive funding, at the index
    assert p.position_size == D("2") and p.rate == D("0.0000125")
    eq0 = D("10000") - D("200") + D("2") * D("99.5")
    assert (await px.fetch_balance()).equity == eq0 + p.amount


async def test_settle_funding_matches_live_arcus_convention():
    """Live Arcus rows: payment = -rate * signed_size * oracle (positive = received)."""
    rows = json.loads((DATA / "arcus_funding_payments.json").read_text())["fundingPayments"]
    for row in rows:
        rate, size, payment = D(row["fundingRate"]), D(row["size"]), D(row["payment"])
        if rate == 0:
            continue
        oracle = payment / (-rate * size)
        assert oracle > 0  # sign convention: long + positive rate => negative payment
        inner, px = make()
        px._pos["BTC"] = _PaperPosition(size=size, entry=oracle)
        px._funding_cache["BTC"] = (time.time(), _fi(row["fundingRate"], index=oracle))
        (p,) = px.settle_funding(time.time())
        assert p.amount.quantize(D("1e-9")) == payment


async def test_settle_funding_skips_without_rate_and_falls_back_to_mark_price():
    inner, px = make()
    inner.set_book("BTC", [(99, 10)], [(100, 10)])
    await px.place_market("BTC", Side.SELL, D("1"), worst_price=D("90"))
    assert px.settle_funding(time.time()) == []  # no cached rate -> skipped (logged)
    px._funding_cache["BTC"] = (time.time(), _fi("0.0001"))
    (p,) = px.settle_funding(time.time())
    assert p.amount == D("1") * D("99.5") * D("0.0001")  # short receives; book mid fallback
    px._funding_cache["BTC"] = (time.time() - 3600, _fi("0.0001"))
    assert px.settle_funding(time.time()) == []  # stale rate


class Clock:
    def __init__(self, t: float) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


async def _wait_for(cond, timeout=2.0):
    t0 = time.monotonic()
    while not cond():
        if time.monotonic() - t0 > timeout:
            raise AssertionError("condition not met")
        await asyncio.sleep(0.005)


async def test_funding_loop_settles_at_top_of_hour_with_controllable_clock():
    base = 1_792_440_000.0  # an exact hour
    clock = Clock(base + 1800)
    inner, px = make(clock=clock, funding_poll_s=0.002)
    inner.funding["BTC"] = _fi("0.00002", index="100")
    inner.set_book("BTC", [(99, 10)], [(100, 10)])
    await px.place_market("BTC", Side.BUY, D("3"), worst_price=D("100"))
    await px.connect()
    try:
        await _wait_for(lambda: "BTC" in px._funding_cache)  # first refresh happens immediately
        await asyncio.sleep(0.02)
        assert await px.fetch_funding_payments() == []  # started mid-hour: nothing settled
        inner.funding["BTC"] = _fi("0.00004", index="100")  # predicted rate changes before the tick
        clock.t = base + 3600 - 10  # inside the pre-settlement window -> refresh
        await _wait_for(lambda: px._funding_cache["BTC"][1].rate_hourly == D("0.00004"))
        clock.t = base + 3600 + 0.2
        await _wait_for(lambda: len(px._funding_payments) == 1)
        (p,) = await px.fetch_funding_payments()
        assert p.ts == base + 3600 and p.amount == -D("3") * D("100") * D("0.00004")
        await asyncio.sleep(0.02)
        assert len(await px.fetch_funding_payments()) == 1  # settles once per hour
        assert await px.fetch_funding_payments(since_ts=base + 3601) == []
        clock.t = base + 7200 + 0.5
        await _wait_for(lambda: len(px._funding_payments) == 2)
        assert px.stats()["funding"] == 2 * (-D("3") * D("100") * D("0.00004"))
    finally:
        await px.close()


async def test_funding_loop_survives_inner_errors():
    clock = Clock(1_792_440_000.0 + 100)
    inner, px = make(clock=clock, funding_poll_s=0.002, funding_refresh_s=0.0)
    calls = {"n": 0}

    async def boom(symbols=None):
        calls["n"] += 1
        raise RuntimeError("venue down")

    inner.fetch_funding = boom  # type: ignore[method-assign]
    await px.subscribe_books(["BTC"])
    await px.connect()
    try:
        await _wait_for(lambda: calls["n"] >= 3)
        assert px._tasks and not px._tasks[0].done()
    finally:
        await px.close()
    assert not px._tasks


# ------------------------------------------------------------------ live (read-only public data)
import os  # noqa: E402

live = [pytest.mark.live,
        pytest.mark.skipif(os.environ.get("FARMER_LIVE_TESTS") != "1", reason="set FARMER_LIVE_TESTS=1 to run")]


async def _live_roundtrip(inner, symbol: str = "BTC") -> None:
    px = PaperExchange(inner, PaperConfig())
    assert not inner.has_credentials  # paper never needs (or uses) venue keys
    books = []
    px.add_book_listener(books.append)
    await px.connect()
    try:
        await px.subscribe_books([symbol])
        t0 = time.monotonic()
        while (px.get_book(symbol) is None or len(books) < 3) and time.monotonic() - t0 < 30:
            await asyncio.sleep(0.2)
        book = px.get_book(symbol)
        assert book is not None and book.best_bid < book.best_ask and books
        m = px.market(symbol)
        size = m.round_size(max(m.min_size, (m.min_notional * 2) / book.mid) * 2)
        size = max(size, m.min_size)
        far = m.round_price(book.best_bid * D("0.97"), Side.BUY)
        r = await px.place_limit(symbol, Side.BUY, size, far)
        assert r.ok and r.order.status is OrderStatus.OPEN, r.error
        cross = await px.place_limit(symbol, Side.BUY, size, m.round_price(book.best_ask, Side.BUY))
        assert not cross.ok and cross.would_cross
        live_book = px.get_book(symbol)
        worst = m.round_price(live_book.best_ask * D("1.002"), Side.BUY, passive=False)
        mk = await px.place_market(symbol, Side.BUY, size, worst_price=worst)
        assert mk.ok and mk.order.filled > 0 and mk.order.avg_fill_price <= worst
        assert px.position(symbol) == mk.order.filled
        await px.refresh_funding()
        fi = px._funding_cache[symbol][1]
        assert abs(fi.rate_hourly) < D("0.04")
        bal = await px.fetch_balance()
        assert abs(bal.equity - D("10000")) < size * book.mid * D("0.01") + 1  # only spread/fees moved equity
        await px.cancel_all()
        assert await px.fetch_open_orders() == []
    finally:
        await px.close()


@live[0]
@live[1]
async def test_live_paper_on_arcus_public_data():
    from farmer.config import ArcusConfig
    from farmer.exchanges.arcus.client import ArcusExchange

    await _live_roundtrip(ArcusExchange(ArcusConfig()))


@live[0]
@live[1]
async def test_live_paper_on_lighter_public_data():
    from farmer.config import LighterConfig
    from farmer.exchanges.lighter import LighterExchange

    await _live_roundtrip(LighterExchange(LighterConfig()))
