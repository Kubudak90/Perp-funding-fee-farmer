"""Deterministic in-memory Exchange for unit tests.

Tests drive it explicitly:
- ``set_book(symbol, bids, asks)`` publishes a book (calls book listeners).
- ``fill_resting(client_id, size=None, price=None)`` fills a resting limit order as maker.
- Taker orders (``place_market``) fill immediately against the current book unless
  ``taker_auto_fill = False`` (then call ``complete_taker(client_id, size, price)``).
- ``fail_next(n, retryable=True)`` makes the next n order placements fail.
"""

from __future__ import annotations

import itertools
import time
from decimal import Decimal

from farmer.exchanges.base import Exchange
from farmer.models import (
    Balance,
    BookLevel,
    Fill,
    FundingInfo,
    MarketInfo,
    Order,
    OrderBook,
    OrderResult,
    OrderStatus,
    Position,
    Side,
)

D = Decimal


def make_market(venue: str, symbol: str, *, tick="0.1", step="0.001", min_size="0.001", min_notional="1",
                maker_fee="0", taker_fee="0", market_id=1) -> MarketInfo:
    return MarketInfo(
        venue=venue, symbol=symbol, venue_symbol=symbol, market_id=market_id,
        tick_size=D(tick), step_size=D(step), min_size=D(min_size), min_notional=D(min_notional),
        maker_fee=D(maker_fee), taker_fee=D(taker_fee),
    )


class FakeExchange(Exchange):
    def __init__(self, name: str, markets: list[MarketInfo] | None = None) -> None:
        self.name = name
        super().__init__()
        for m in markets or []:
            self.markets[m.symbol] = m
        self.books: dict[str, OrderBook] = {}
        self.positions: dict[str, Decimal] = {}
        self.entry: dict[str, Decimal] = {}
        self.orders: dict[str, Order] = {}
        self.funding: dict[str, FundingInfo] = {}
        self.balance = Balance(venue=name, equity=D("10000"), available=D("10000"))
        self.leverage: dict[str, int] = {}
        self.calls: list[tuple] = []
        self.taker_auto_fill = True
        self._fail_next = 0
        self._fail_retryable = True
        self._ids = itertools.count(1)
        self.connected = False

    # lifecycle
    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    @property
    def has_credentials(self) -> bool:
        return True

    # market data
    async def load_markets(self):
        return self.markets

    async def subscribe_books(self, symbols):
        self.calls.append(("subscribe_books", tuple(symbols)))

    def get_book(self, symbol):
        return self.books.get(symbol)

    async def fetch_book(self, symbol, depth=20):
        return self.books[symbol]

    async def fetch_funding(self, symbols=None):
        if symbols is None:
            return dict(self.funding)
        return {s: f for s, f in self.funding.items() if s in symbols}

    # account
    async def fetch_positions(self):
        return {
            s: Position(venue=self.name, symbol=s, size=q, entry_price=self.entry.get(s))
            for s, q in self.positions.items() if q != 0
        }

    async def fetch_balance(self):
        return self.balance

    async def fetch_open_orders(self, symbol=None):
        return [o for o in self.orders.values() if o.is_open and (symbol is None or o.symbol == symbol)]

    # trading
    def fail_next(self, n: int = 1, retryable: bool = True) -> None:
        self._fail_next = n
        self._fail_retryable = retryable

    def _maybe_fail(self) -> OrderResult | None:
        if self._fail_next > 0:
            self._fail_next -= 1
            return OrderResult(ok=False, error="injected failure", retryable=self._fail_retryable)
        return None

    async def place_limit(self, symbol, side, size, price, *, post_only=True, reduce_only=False, client_id=None):
        self.calls.append(("place_limit", symbol, side, size, price, post_only, reduce_only))
        if (r := self._maybe_fail()) is not None:
            return r
        book = self.books.get(symbol)
        if post_only and book is not None:
            if (side is Side.BUY and book.best_ask is not None and price >= book.best_ask) or (
                side is Side.SELL and book.best_bid is not None and price <= book.best_bid
            ):
                return OrderResult(ok=False, error="post-only would cross", would_cross=True)
        cid = client_id or f"{self.name}-c{next(self._ids)}"
        o = Order(venue=self.name, symbol=symbol, side=side, price=price, size=size, client_id=cid,
                  order_id=f"{self.name}-o{next(self._ids)}", status=OrderStatus.OPEN,
                  post_only=post_only, reduce_only=reduce_only)
        self.orders[cid] = o
        self._emit_order(o)
        return OrderResult(ok=True, order=o)

    async def place_market(self, symbol, side, size, *, worst_price, reduce_only=False, client_id=None):
        self.calls.append(("place_market", symbol, side, size, worst_price, reduce_only))
        if (r := self._maybe_fail()) is not None:
            return r
        cid = client_id or f"{self.name}-c{next(self._ids)}"
        o = Order(venue=self.name, symbol=symbol, side=side, price=worst_price, size=size, client_id=cid,
                  order_id=f"{self.name}-o{next(self._ids)}", status=OrderStatus.PENDING, ioc=True,
                  reduce_only=reduce_only)
        self.orders[cid] = o
        if self.taker_auto_fill:
            book = self.books.get(symbol)
            px = (book.best_ask if side is Side.BUY else book.best_bid) if book else worst_price
            self.complete_taker(cid, size, px)
        return OrderResult(ok=True, order=o)

    def complete_taker(self, client_id: str, size: Decimal, price: Decimal) -> None:
        o = self.orders[client_id]
        if size > 0:
            self._apply_fill(o, size, price, is_maker=False)
        o.status = OrderStatus.FILLED if o.filled >= o.size else OrderStatus.CANCELED
        o.updated_ts = time.time()
        self._emit_order(o)

    def fill_resting(self, client_id: str, size: Decimal | None = None, price: Decimal | None = None) -> Fill:
        o = self.orders[client_id]
        q = o.remaining if size is None else size
        f = self._apply_fill(o, q, price if price is not None else o.price, is_maker=True)
        o.status = OrderStatus.FILLED if o.remaining <= 0 else OrderStatus.PARTIALLY_FILLED
        self._emit_order(o)
        return f

    def _apply_fill(self, o: Order, size: Decimal, price: Decimal, *, is_maker: bool) -> Fill:
        prev = o.filled
        o.filled += size
        o.avg_fill_price = price if prev == 0 else (o.avg_fill_price * prev + price * size) / o.filled
        self.positions[o.symbol] = self.positions.get(o.symbol, D(0)) + size * o.side.sign
        self.entry.setdefault(o.symbol, price)
        mkt = self.markets.get(o.symbol)
        fee_rate = (mkt.maker_fee if is_maker else mkt.taker_fee) if mkt else D(0)
        f = Fill(venue=self.name, symbol=o.symbol, side=o.side, price=price, size=size, fee=price * size * fee_rate,
                 is_maker=is_maker, order_id=o.order_id, client_id=o.client_id, trade_id=f"t{next(self._ids)}")
        self._emit_fill(f)
        return f

    async def cancel_order(self, symbol, *, order_id=None, client_id=None):
        self.calls.append(("cancel_order", symbol, order_id, client_id))
        for o in self.orders.values():
            if (client_id and o.client_id == client_id) or (order_id and o.order_id == order_id):
                if o.is_open:
                    o.status = OrderStatus.CANCELED
                    self._emit_order(o)
                return True
        return True

    async def cancel_all(self, symbol=None):
        self.calls.append(("cancel_all", symbol))
        for o in self.orders.values():
            if o.is_open and not o.ioc and (symbol is None or o.symbol == symbol):
                o.status = OrderStatus.CANCELED
                self._emit_order(o)

    async def modify_order(self, order, *, price, size=None):
        self.calls.append(("modify_order", order.client_id, price, size))
        if (r := self._maybe_fail()) is not None:
            return r
        o = self.orders.get(order.client_id)
        if o is None or not o.is_open:
            return OrderResult(ok=False, error="order not open")
        book = self.books.get(o.symbol)
        if o.post_only and book is not None:
            if (o.side is Side.BUY and book.best_ask is not None and price >= book.best_ask) or (
                o.side is Side.SELL and book.best_bid is not None and price <= book.best_bid
            ):
                return OrderResult(ok=False, error="post-only would cross", would_cross=True)
        o.price = price
        if size is not None:
            o.size = o.filled + size
        o.updated_ts = time.time()
        self._emit_order(o)
        return OrderResult(ok=True, order=o)

    async def set_leverage(self, symbol, leverage):
        self.leverage[symbol] = leverage

    # helpers for tests
    def set_book(self, symbol: str, bids: list[tuple], asks: list[tuple]) -> OrderBook:
        b = OrderBook(
            venue=self.name, symbol=symbol,
            bids=[BookLevel(D(str(p)), D(str(s))) for p, s in bids],
            asks=[BookLevel(D(str(p)), D(str(s))) for p, s in asks],
        )
        self.books[symbol] = b
        self._emit_book(b)
        return b

    def resting(self, symbol: str | None = None) -> list[Order]:
        return [o for o in self.orders.values() if o.is_open and not o.ioc and (symbol is None or o.symbol == symbol)]
