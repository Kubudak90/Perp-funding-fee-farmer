"""Abstract exchange adapter.

Every venue (Arcus, Lighter, Paper) implements this interface. The strategy layer only talks
to ``Exchange`` and never imports venue modules directly.

Threading model: everything runs on ONE asyncio event loop. Listeners are plain (sync)
callables invoked from the adapter's websocket reader task; they must be fast and must not
block. Strategy code that needs to do async work in response schedules it itself
(e.g. by setting an ``asyncio.Event``).
"""

from __future__ import annotations

import abc
import logging
from decimal import Decimal

from farmer.models import (
    Balance,
    BookListener,
    Fill,
    FillListener,
    FundingInfo,
    FundingPayment,
    MarketInfo,
    Order,
    OrderBook,
    OrderListener,
    OrderResult,
    Position,
    Side,
)


class ExchangeError(Exception):
    """Base error raised by adapters for unexpected venue failures."""

    def __init__(self, message: str, *, retryable: bool = False, code: str | int | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.code = code


class AuthRequired(ExchangeError):
    """Raised when a private endpoint is called on an adapter without credentials."""


class Exchange(abc.ABC):
    """Venue adapter contract."""

    name: str = "base"

    def __init__(self) -> None:
        self.log = logging.getLogger(f"farmer.{self.name}")
        self._fill_listeners: list[FillListener] = []
        self._order_listeners: list[OrderListener] = []
        self._book_listeners: list[BookListener] = []
        self.markets: dict[str, MarketInfo] = {}  # canonical symbol -> info

    # ------------------------------------------------------------------ lifecycle
    @abc.abstractmethod
    async def connect(self) -> None:
        """Open HTTP session(s), load markets, authenticate, start private streams if creds exist."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Stop background tasks and close sockets/sessions. Must be idempotent."""

    @property
    @abc.abstractmethod
    def has_credentials(self) -> bool:
        """True if private (trading/account) calls are possible."""

    @property
    def is_paper(self) -> bool:
        return False

    # ------------------------------------------------------------------ market data
    @abc.abstractmethod
    async def load_markets(self) -> dict[str, MarketInfo]:
        """Fetch market metadata and populate ``self.markets`` (canonical symbol -> MarketInfo)."""

    @abc.abstractmethod
    async def subscribe_books(self, symbols: list[str]) -> None:
        """Start streaming order books (at least top 10 levels) for ``symbols``.

        Must reconnect + resubscribe automatically. Each update calls book listeners.
        """

    @abc.abstractmethod
    def get_book(self, symbol: str) -> OrderBook | None:
        """Latest locally maintained book, or None if not yet received."""

    @abc.abstractmethod
    async def fetch_book(self, symbol: str, depth: int = 20) -> OrderBook:
        """One-shot REST snapshot."""

    @abc.abstractmethod
    async def fetch_funding(self, symbols: list[str] | None = None) -> dict[str, FundingInfo]:
        """Current/next funding per symbol, normalized to fraction-per-hour."""

    # ------------------------------------------------------------------ account
    @abc.abstractmethod
    async def fetch_positions(self) -> dict[str, Position]:
        """All non-zero positions keyed by canonical symbol."""

    @abc.abstractmethod
    async def fetch_balance(self) -> Balance:
        ...

    @abc.abstractmethod
    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        ...

    async def fetch_funding_payments(self, since_ts: float | None = None) -> list[FundingPayment]:
        """Optional: realized funding history. Default: unsupported -> empty."""
        return []

    # ------------------------------------------------------------------ trading
    @abc.abstractmethod
    async def place_limit(
        self,
        symbol: str,
        side: Side,
        size: Decimal,
        price: Decimal,
        *,
        post_only: bool = True,
        reduce_only: bool = False,
        client_id: str | None = None,
    ) -> OrderResult:
        """Place a limit order. ``size``/``price`` must already be rounded by the caller
        (adapters re-validate and reject invalid values with ``ok=False``)."""

    @abc.abstractmethod
    async def place_market(
        self,
        symbol: str,
        side: Side,
        size: Decimal,
        *,
        worst_price: Decimal,
        reduce_only: bool = False,
        client_id: str | None = None,
    ) -> OrderResult:
        """Immediate-or-cancel taker order that never executes beyond ``worst_price``."""

    @abc.abstractmethod
    async def cancel_order(self, symbol: str, *, order_id: str | None = None, client_id: str | None = None) -> bool:
        """Cancel one order. Returns True if the cancel was accepted (or order already gone)."""

    @abc.abstractmethod
    async def cancel_all(self, symbol: str | None = None) -> None:
        ...

    async def modify_order(self, order: Order, *, price: Decimal, size: Decimal | None = None) -> OrderResult:
        """Amend price/size. Default implementation: cancel + replace (same client side/flags)."""
        await self.cancel_order(order.symbol, order_id=order.order_id, client_id=order.client_id)
        return await self.place_limit(
            order.symbol,
            order.side,
            size if size is not None else order.remaining,
            price,
            post_only=order.post_only,
            reduce_only=order.reduce_only,
        )

    @abc.abstractmethod
    async def set_leverage(self, symbol: str, leverage: int) -> None:
        ...

    async def arm_dead_mans_switch(self, timeout_s: float) -> None:
        """Optional venue-side cancel-all-after timer. Default: no-op."""
        return None

    # ------------------------------------------------------------------ listeners
    def add_fill_listener(self, cb: FillListener) -> None:
        self._fill_listeners.append(cb)

    def add_order_listener(self, cb: OrderListener) -> None:
        self._order_listeners.append(cb)

    def add_book_listener(self, cb: BookListener) -> None:
        self._book_listeners.append(cb)

    def _emit_fill(self, fill: Fill) -> None:
        for cb in list(self._fill_listeners):
            try:
                cb(fill)
            except Exception:  # pragma: no cover - listener bugs must not kill the reader
                self.log.exception("fill listener failed")

    def _emit_order(self, order: Order) -> None:
        for cb in list(self._order_listeners):
            try:
                cb(order)
            except Exception:  # pragma: no cover
                self.log.exception("order listener failed")

    def _emit_book(self, book: OrderBook) -> None:
        for cb in list(self._book_listeners):
            try:
                cb(book)
            except Exception:  # pragma: no cover
                self.log.exception("book listener failed")

    # ------------------------------------------------------------------ helpers
    def market(self, symbol: str) -> MarketInfo:
        try:
            return self.markets[symbol]
        except KeyError as exc:
            raise ExchangeError(f"{self.name}: unknown market {symbol!r}") from exc
