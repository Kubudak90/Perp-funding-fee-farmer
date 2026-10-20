"""Deterministic in-memory Exchange for unit tests.

Tests drive it explicitly:
- ``set_book(symbol, bids, asks)`` publishes a book (calls book listeners).
- ``fill_resting(client_id, size=None, price=None)`` fills a resting limit order as maker.
- Taker orders (``place_market``) fill immediately against the current book unless
  ``taker_auto_fill = False`` (then call ``complete_taker(client_id, size, price)``).
- ``fail_next(n, retryable=True)`` makes the next n order placements fail
  (``kinds={"limit","market","modify"}`` restricts which calls fail).

Optional knobs (all default to the original behaviour): ``ack_pending`` (place_limit returns PENDING until
``ack(client_id)``), ``modify_cross_kills`` (a crossing post-only modify kills the order, like Arcus ALO),
``taker_fill_ratio`` (auto-filled IOC fraction), ``closed`` (symbols whose ``market_open`` is False),
``health`` (``budget_health``), ``positions_error`` (``fetch_positions`` raises it), ``stream_ok`` (``private_ok``:
health of the private orders/fills stream).
``FakeRisk`` / ``FakeNotifier`` are small stand-ins for the RiskManager / Notifier interfaces.
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
        self.taker_fill_ratio = D(1)
        self.ack_pending = False
        self.modify_cross_kills = False
        self.closed: set[str] = set()
        self.health = 1.0
        self.positions_error: Exception | None = None
        self.stream_ok = True
        self._fail_next = 0
        self._fail_retryable = True
        self._fail_kinds: set[str] | None = None
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

    def market_open(self, symbol):
        return super().market_open(symbol) and symbol not in self.closed

    def budget_health(self):
        return self.health

    def private_ok(self):
        return (True, "") if self.stream_ok else (False, f"{self.name} private stream down (test)")

    # account
    async def fetch_positions(self):
        self.calls.append(("fetch_positions",))
        if self.positions_error is not None:
            raise self.positions_error
        return {
            s: Position(venue=self.name, symbol=s, size=q, entry_price=self.entry.get(s))
            for s, q in self.positions.items() if q != 0
        }

    async def fetch_balance(self):
        return self.balance

    async def fetch_open_orders(self, symbol=None):
        return [o for o in self.orders.values() if o.is_open and (symbol is None or o.symbol == symbol)]

    # trading
    def fail_next(self, n: int = 1, retryable: bool = True, kinds: set[str] | None = None) -> None:
        self._fail_next = n
        self._fail_retryable = retryable
        self._fail_kinds = set(kinds) if kinds else None

    def _maybe_fail(self, kind: str | None = None) -> OrderResult | None:
        if self._fail_next > 0 and (self._fail_kinds is None or kind in self._fail_kinds):
            self._fail_next -= 1
            return OrderResult(ok=False, error="injected failure", retryable=self._fail_retryable)
        return None

    async def place_limit(self, symbol, side, size, price, *, post_only=True, reduce_only=False, client_id=None):
        self.calls.append(("place_limit", symbol, side, size, price, post_only, reduce_only))
        if (r := self._maybe_fail("limit")) is not None:
            return r
        book = self.books.get(symbol)
        if post_only and book is not None:
            if (side is Side.BUY and book.best_ask is not None and price >= book.best_ask) or (
                side is Side.SELL and book.best_bid is not None and price <= book.best_bid
            ):
                return OrderResult(ok=False, error="post-only would cross", would_cross=True)
        cid = client_id or f"{self.name}-c{next(self._ids)}"
        o = Order(venue=self.name, symbol=symbol, side=side, price=price, size=size, client_id=cid,
                  order_id=f"{self.name}-o{next(self._ids)}",
                  status=OrderStatus.PENDING if self.ack_pending else OrderStatus.OPEN,
                  post_only=post_only, reduce_only=reduce_only)
        self.orders[cid] = o
        if not self.ack_pending:
            self._emit_order(o)
        return OrderResult(ok=True, order=o)

    def ack(self, client_id: str, status: OrderStatus = OrderStatus.OPEN, error: str | None = None) -> Order:
        """Deliver the venue acknowledgement of a PENDING order (``ack_pending`` mode)."""
        o = self.orders[client_id]
        o.status = status
        o.error = error
        o.updated_ts = time.time()
        self._emit_order(o)
        return o

    async def place_market(self, symbol, side, size, *, worst_price, reduce_only=False, client_id=None):
        self.calls.append(("place_market", symbol, side, size, worst_price, reduce_only))
        if (r := self._maybe_fail("market")) is not None:
            return r
        cid = client_id or f"{self.name}-c{next(self._ids)}"
        o = Order(venue=self.name, symbol=symbol, side=side, price=worst_price, size=size, client_id=cid,
                  order_id=f"{self.name}-o{next(self._ids)}", status=OrderStatus.PENDING, ioc=True,
                  reduce_only=reduce_only)
        self.orders[cid] = o
        if self.taker_auto_fill:
            book = self.books.get(symbol)
            px = (book.best_ask if side is Side.BUY else book.best_bid) if book else worst_price
            q = size if self.taker_fill_ratio >= 1 else (size * self.taker_fill_ratio).quantize(size)
            self.complete_taker(cid, q, px)
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
        if (r := self._maybe_fail("modify")) is not None:
            return r
        o = self.orders.get(order.client_id)
        if o is None or not o.is_open:
            return OrderResult(ok=False, error="order not open")
        book = self.books.get(o.symbol)
        if o.post_only and book is not None:
            if (o.side is Side.BUY and book.best_ask is not None and price >= book.best_ask) or (
                o.side is Side.SELL and book.best_bid is not None and price <= book.best_bid
            ):
                if self.modify_cross_kills:  # Arcus: a crossing ALO modify ends the order
                    o.status = OrderStatus.REJECTED
                    o.error = "POST_ONLY_WOULD_CROSS"
                    o.updated_ts = time.time()
                    self._emit_order(o)
                    return OrderResult(ok=False, order=o, error="post-only would cross", would_cross=True)
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


class FakeRisk:
    """Minimal stand-in for ``farmer.risk.manager.RiskManager`` (same method names/semantics, test knobs)."""

    def __init__(self, cfg=None) -> None:
        from farmer.config import RiskConfig

        self.cfg = cfg or RiskConfig()
        self.halted = False
        self.halt_reason: str | None = None
        self.quote_ok = True
        self.quote_reason = ""
        self.open_ok = True
        self.open_reason = ""
        self.reduce = False
        self.errors: list[tuple[str, str]] = []
        self.successes: list[str] = []
        self.exposure: dict[tuple[str, str], Decimal] = {}
        self.quote_checks: list[tuple] = []
        self.open_checks: list[tuple[str, Decimal]] = []
        self.balances: dict[str, Balance] = {}
        self.running = False
        self._stop = None

    async def run(self) -> None:
        import asyncio

        self.running = True
        self._stop = asyncio.Event()
        await self._stop.wait()
        self.running = False

    def stop(self) -> None:
        if self._stop is not None:
            self._stop.set()

    def halt(self, reason: str, *, kind: str = "manual") -> None:
        if not self.halted:
            self.halted = True
            self.halt_reason = reason

    def resume(self) -> None:
        self.halted = False
        self.halt_reason = None

    def check_quote(self, symbol, maker_book, taker_book, basis_dev_bps=None, divergence_bps=None):
        self.quote_checks.append((symbol, basis_dev_bps, divergence_bps))
        if self.halted:
            return False, f"halted: {self.halt_reason}"
        if maker_book is None or taker_book is None:
            return False, "book missing"
        if not self.quote_ok:
            return False, self.quote_reason or "quote blocked"
        return True, ""

    def check_open(self, symbol, add_notional):
        self.open_checks.append((symbol, add_notional))
        if self.halted:
            return False, "halted"
        if not self.open_ok:
            return False, self.open_reason or "open blocked"
        return True, ""

    def must_reduce(self) -> bool:
        return self.reduce

    def record_error(self, venue: str, err: str) -> None:
        self.errors.append((venue, err))

    def record_success(self, venue: str) -> None:
        self.successes.append(venue)

    def update_exposure(self, symbol: str, venue: str, notional: Decimal) -> None:
        self.exposure[(symbol, venue)] = notional

    def balance(self, venue: str):
        return self.balances.get(venue)

    def status(self) -> dict:
        return {"halted": self.halted, "halt_reason": self.halt_reason, "errors": len(self.errors)}


class FakeNotifier:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, text: str) -> None:
        self.messages.append(text)

    async def close(self) -> None:
        return None


# --------------------------------------------------------------------------- strategy test helpers
def _loose_configs():
    from pydantic import ConfigDict

    from farmer.config import RiskConfig, StrategyConfig

    class LooseStrategyConfig(StrategyConfig):
        """StrategyConfig that also accepts the optional knobs strategy code reads via getattr()."""

        model_config = ConfigDict(extra="allow")

    class LooseRiskConfig(RiskConfig):
        model_config = ConfigDict(extra="allow")

    return LooseStrategyConfig, LooseRiskConfig


LooseStrategyConfig, LooseRiskConfig = _loose_configs()

FAST_STRATEGY = dict(
    hedge_aggregate_ms=5, hedge_timeout_s=0.5, hedge_retry_backoff_s=0.02, hedge_retry_max_s=0.2,
    loop_interval_s=0.005, reconcile_interval_s=3600, reconcile_confirm_s=0.03, reconcile_quiet_s=0.0,
    use_basis=False, basis_warmup_s=0, min_quote_lifetime_s=0.0, protect_funding_window_s=0,
    cancel_confirm_timeout_s=0.2, order_ack_timeout_s=0.5, hold_seconds=0.0, funding_refresh_s=0.05,
    shutdown_flush_s=1.0, hedge_fill_wait_s=0.1, reconcile_retry_s=0.05, shutdown_settle_s=0.05,
)
FAST_RISK = dict(unhedged_timeout_s=0.3, unhedged_grace_s=0.05, stale_book_s=30.0)


def fast_strategy(**kw):
    return LooseStrategyConfig(**{**FAST_STRATEGY, **kw})


def fast_risk(**kw):
    return LooseRiskConfig(**{**FAST_RISK, **kw})


async def wait_until(pred, timeout: float = 2.0, interval: float = 0.005) -> bool:
    """Poll ``pred()`` until truthy; raise AssertionError on timeout."""
    import asyncio

    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while True:
        if pred():
            return True
        if loop.time() >= end:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(interval)
