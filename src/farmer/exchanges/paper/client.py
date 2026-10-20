"""Paper trading adapter: live PUBLIC data from a real venue adapter, simulated PRIVATE side.

``PaperExchange(inner, paper_cfg)`` wraps a real adapter (normally built without credentials).
Markets, books and funding come from ``inner``; orders, fills, positions, balance and funding
payments are simulated locally. The paper venue never calls a private/trading method of ``inner``.

Fill model (deliberately conservative, queue-agnostic):
- Resting post-only orders are checked against the CURRENT inner book after
  ``maker_latency_ms`` and rejected (``would_cross``) if they would take liquidity.
- A resting BUY at ``P`` fills as MAKER at ``P`` only when the inner book trades through it
  (best ask <= P), for at most the ask size visible at prices <= P (partial fills allowed);
  SELL symmetric. Fills are rounded down to the market step.
- ``place_market`` sleeps ``taker_latency_ms`` and then sweeps the CURRENT inner book level by
  level up to ``worst_price`` (IOC: the remainder is canceled), one Fill per level.
- Liquidity consumed by simulated fills is remembered per (side, price) level across book updates
  (our simulated trades never reach the real book, so an unchanged level - e.g. a heartbeat re-emission or a
  change at another level - is NOT fresh liquidity). A level that grows offers only the increase; a level that
  shrinks keeps our consumption (capped at its size); a level that disappears is forgotten after
  ``CONSUMED_GONE_GRACE_S`` unless it comes back with the identical size, or the identical size appears at another
  price on that side, first (a market maker's cancel/re-post or moved order: the same liquidity).
- Fills are book-based: a resting quote fills when the real book trades through its price. Our quote is invisible
  to other participants, so quotes inside the spread (negative ``min_edge_bps``) still fill optimistically.

Rate budget: with ``paper.emulate_rate_limits`` and an inner adapter whose config has ``max_tx_per_minute``
(Lighter), order actions take a slot of the inner adapter's shared request bucket (``max_requests_per_minute``,
also used by its public REST reads) and of a local tx limiter, waiting up to ``tx_budget_wait_s`` and then failing
retryably unsent (live semantics). ``budget_health`` reports the shared bucket.

Funding: a background task settles at every top of the UTC hour:
``payment = -signed_size * price * rate_hourly`` (price = index/oracle, else mark, else book
mid; both venues value funding at the index), rates cached from ``inner.fetch_funding``.

Accounting (cash method): ``equity = initial_balance + cash_flow + funding + sum(size * mid)`` with
``cash_flow = sum(-signed_size * price) - fees``; initial margin = notional / leverage,
maintenance = notional / (2 * leverage).

Limitations: state is in memory only (a restart starts flat with ``initial_balance``); no queue
position / trade-tape fills (a maker order resting inside the spread only fills when the book
moves through it); simulated trades do not move the real book.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import os
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable

from farmer.config import PaperConfig
from farmer.exchanges.base import Exchange
from farmer.models import (
    ZERO,
    Balance,
    BookLevel,
    Fill,
    FundingInfo,
    FundingPayment,
    MarketInfo,
    Order,
    OrderBook,
    OrderResult,
    OrderStatus,
    Position,
    Side,
)
from farmer.utils.ids import new_client_id
from farmer.utils.ratelimit import SlidingWindowLimiter

# a consumed book level that vanishes and returns with the identical size (or reappears at another price with the
# identical size) within this window is the same market-maker order: not fresh liquidity
CONSUMED_GONE_GRACE_S = 1.0

DEFAULT_LEVERAGE = 5  # used until set_leverage() is called for a symbol
FUNDING_REFRESH_S = 60.0  # how often the funding-rate cache is refreshed from the inner adapter
FUNDING_PRE_SETTLE_S = 15.0  # extra refresh this close before a settlement (rates can change late)
FUNDING_MAX_AGE_S = 600.0  # cached rate older than this at settlement -> skip (logged)
MAX_TERMINAL_ORDERS = 5000  # terminal orders kept for lookups before being pruned
MAX_FILL_HISTORY = 10000


@dataclass
class _PaperPosition:
    size: Decimal = ZERO  # signed base
    entry: Decimal | None = None  # average entry price of the open size
    realized: Decimal = ZERO  # realized price PnL (excl. fees / funding)
    last_px: Decimal | None = None  # last fill price (mark fallback)


class PaperExchange(Exchange):
    """Simulated private side on top of a real adapter's public data."""

    def __init__(
        self,
        inner: Exchange,
        paper_cfg: PaperConfig,
        *,
        clock: Callable[[], float] = time.time,
        funding_refresh_s: float = FUNDING_REFRESH_S,
        funding_poll_s: float = 1.0,
    ) -> None:
        self.inner = inner
        self.name = inner.name
        self.cfg = paper_cfg
        super().__init__()
        self.log = logging.getLogger(f"farmer.paper.{self.name}")
        self._clock = clock
        self._funding_refresh_s = float(funding_refresh_s)
        self._funding_poll_s = max(0.001, float(funding_poll_s))

        self.initial_balance: Decimal = Decimal(paper_cfg.initial_balance)
        self.default_leverage = max(1, int(paper_cfg.default_leverage))
        self._cash: Decimal = ZERO  # sum(-signed * price) - fees
        self._fees: Decimal = ZERO
        self._funding_total: Decimal = ZERO
        self._volume: dict[str, Decimal] = {"maker": ZERO, "taker": ZERO}
        self._pos: dict[str, _PaperPosition] = {}
        self._leverage: dict[str, int] = {}

        self._orders: dict[str, Order] = {}  # client_id -> order (live + recent terminal)
        self._open: dict[str, Order] = {}  # client_id -> resting order
        self._by_oid: dict[str, str] = {}  # order_id -> client_id
        self._terminal_q: deque[str] = deque()
        self._fills: deque[Fill] = deque(maxlen=MAX_FILL_HISTORY)
        self._funding_payments: list[FundingPayment] = []

        # liquidity our simulated fills took from inner book levels:
        # symbol -> {(level side, px): [seen size, used, vanished-at clock ts or None]}
        self._consumed: dict[str, dict[tuple[str, Decimal], list]] = {}
        self._req_limiter: SlidingWindowLimiter | None = None
        self._tx_limiter: SlidingWindowLimiter | None = None
        self._tx_wait_s = 0.0
        self._setup_rate_limits()

        self._funding_cache: dict[str, tuple[float, FundingInfo]] = {}  # symbol -> (clock ts, info)
        self._funding_attempt_ts: float = float("-inf")
        self._last_settled_hour: int | None = None
        self._subscribed: set[str] = set()

        session = os.urandom(3).hex()
        self._oid_prefix = f"P{session}o"
        self._tid_prefix = f"P{session}t"
        self._ids = itertools.count(1)
        self._tasks: list[asyncio.Task] = []
        self._closed = False
        self._connected = False

        inner.add_book_listener(self._on_inner_book)

    # ================================================================== identity
    @property
    def markets(self) -> dict[str, MarketInfo]:  # type: ignore[override]
        """Mirrors the inner adapter's market map (always the live object)."""
        return self.inner.markets

    @markets.setter
    def markets(self, value: dict[str, MarketInfo]) -> None:
        # Exchange.__init__ assigns {}; the paper venue never owns market metadata.
        return None

    @property
    def has_credentials(self) -> bool:
        return True

    @property
    def is_paper(self) -> bool:
        return True

    # ================================================================== lifecycle
    async def connect(self) -> None:
        await self.inner.connect()
        self._closed = False
        self._connected = True
        self.start_background()
        self.log.info("paper %s connected (initial balance %s)", self.name, self.initial_balance)

    def start_background(self) -> None:
        """Start the funding settlement task (idempotent; ``connect`` calls it)."""
        if any(not t.done() for t in self._tasks):
            return
        self._tasks = [asyncio.create_task(self._funding_loop(), name=f"paper-{self.name}-funding")]

    async def close(self) -> None:
        self._closed = True
        tasks, self._tasks = self._tasks, []
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        if self._connected:
            self._connected = False
            try:
                await self.inner.close()
            except Exception:
                self.log.exception("inner close failed")

    # ================================================================== market data (delegated)
    async def load_markets(self) -> dict[str, MarketInfo]:
        await self.inner.load_markets()
        return self.markets

    async def subscribe_books(self, symbols: list[str]) -> None:
        self._subscribed.update(symbols)
        await self.inner.subscribe_books(symbols)

    def get_book(self, symbol: str) -> OrderBook | None:
        return self.inner.get_book(symbol)

    async def fetch_book(self, symbol: str, depth: int = 20) -> OrderBook:
        return await self.inner.fetch_book(symbol, depth)

    async def fetch_funding(self, symbols: list[str] | None = None) -> dict[str, FundingInfo]:
        res = await self.inner.fetch_funding(symbols)
        now = self._clock()
        for sym, fi in res.items():
            self._funding_cache[sym] = (now, fi)
        return res

    def market_open(self, symbol: str) -> bool:
        return self.inner.market_open(symbol)

    def _setup_rate_limits(self) -> None:
        icfg = getattr(self.inner, "cfg", None)
        max_tx = getattr(icfg, "max_tx_per_minute", None)
        if not getattr(self.cfg, "emulate_rate_limits", False) or not isinstance(max_tx, int) or max_tx <= 0:
            return
        shared = getattr(self.inner, "_limiter", None)  # the inner adapter's bucket also counts its public reads
        if not isinstance(shared, SlidingWindowLimiter):
            shared = SlidingWindowLimiter(max(1, int(getattr(icfg, "max_requests_per_minute", 60) or 60)), 60.0)
        self._req_limiter = shared
        self._tx_limiter = SlidingWindowLimiter(max_tx, 60.0)
        self._tx_wait_s = max(0.0, float(getattr(icfg, "tx_budget_wait_s", 10.0) or 0.0))

    async def _tx_budget(self) -> str | None:
        """Take one tx slot (local tx cap, then the shared request bucket). Returns an error when the budget did
        not free up within ``tx_budget_wait_s`` (the order is then refused unsent, retryable)."""
        if self._tx_limiter is None or self._req_limiter is None:
            return None
        try:
            await asyncio.wait_for(self._tx_limiter.acquire(), self._tx_wait_s)
            await asyncio.wait_for(self._req_limiter.acquire(), self._tx_wait_s)
        except asyncio.TimeoutError:
            self.log.warning("paper %s: rate budget exhausted (waited %.1fs); order refused unsent", self.name,
                             self._tx_wait_s)
            return f"paper {self.name}: rate budget exhausted (tx not sent)"
        return None

    def _tx_budget_nowait(self) -> None:
        """Cancels also spend a slot when one is free (never delayed or refused here)."""
        if self._tx_limiter is not None and self._req_limiter is not None and self._tx_limiter.try_acquire():
            self._req_limiter.try_acquire()

    def budget_health(self) -> float:
        lim, tx = self._req_limiter, self._tx_limiter
        if lim is None or tx is None:
            return 1.0  # no budget emulation for this venue
        return max(0.0, min(1.0, lim.available() / lim.max_calls, tx.available() / tx.max_calls))

    def can_increase_exposure(self) -> tuple[bool, str]:
        return True, ""

    # ================================================================== book listener / maker sim
    def _on_inner_book(self, book: OrderBook) -> None:
        try:
            self._reconcile_consumed(book)
            self._match_resting(book)
        except Exception:  # never let a simulation bug block book delivery
            self.log.exception("paper fill simulation failed for %s", book.symbol)
        self._emit_book(book)

    def _match_resting(self, book: OrderBook) -> None:
        resting = [o for o in self._open.values() if o.symbol == book.symbol]
        if not resting:
            return
        m = self.markets.get(book.symbol)
        if m is None:
            return
        buys = sorted((o for o in resting if o.side is Side.BUY), key=lambda o: (-(o.price or ZERO), o.created_ts))
        sells = sorted((o for o in resting if o.side is Side.SELL), key=lambda o: (o.price or ZERO, o.created_ts))
        for o in buys + sells:
            if o.is_open and o.client_id in self._open:
                self._maker_fill(o, book, m)

    def _maker_fill(self, o: Order, book: OrderBook, m: MarketInfo) -> None:
        assert o.price is not None
        levels = book.asks if o.side is Side.BUY else book.bids
        best = levels[0].price if levels else None
        if best is None:
            return
        if (o.side is Side.BUY and best > o.price) or (o.side is Side.SELL and best < o.price):
            return  # not traded through
        want = o.remaining
        if o.reduce_only:
            cap = self._reduce_cap(o.symbol, o.side)
            if cap <= 0:
                self._finish(o, OrderStatus.CANCELED, "reduce-only order canceled: no position to reduce")
                self._emit_order(o)
                return
            want = min(want, cap)
        taken = ZERO
        plan: list[tuple[BookLevel, Decimal]] = []
        for lvl in levels:
            if want - taken <= 0:
                break
            if (o.side is Side.BUY and lvl.price > o.price) or (o.side is Side.SELL and lvl.price < o.price):
                break
            avail = lvl.size - self._used(o.symbol, o.side.opposite, lvl.price, lvl.size)
            if avail <= 0:
                continue
            q = min(want - taken, avail)
            plan.append((lvl, q))
            taken += q
        qty = m.round_size(taken)
        if qty <= 0:
            return
        left = qty
        for lvl, q in plan:  # consume the levels the fill is attributed to
            use = min(q, left)
            self._consume(o.symbol, o.side.opposite, lvl.price, use, lvl.size)
            left -= use
            if left <= 0:
                break
        self._fill(o, qty, o.price, maker=True)
        if o.remaining <= 0:
            self._finish(o, OrderStatus.FILLED)
        else:
            o.status = OrderStatus.PARTIALLY_FILLED
        self._emit_order(o)

    @staticmethod
    def _level_update(entry: list, size: Decimal) -> None:
        """Carry our consumption of one level to its newly displayed ``size``: growth is new liquidity (only the
        increase), a shrink keeps what we took (capped at the size left)."""
        if size < entry[0]:
            entry[1] = min(entry[1], size)
        entry[0] = size

    def _reconcile_consumed(self, book: OrderBook) -> None:
        """Carry consumption across book updates. A consumed level that vanishes is remembered for
        ``CONSUMED_GONE_GRACE_S``: if it comes back with the identical size (a market maker's cancel/re-post, seen
        live on Arcus ~2x/s with ~25 ms gaps) or the identical size shows up at another price on the same side (the
        order was moved), it is the same liquidity we already took. Anything else is fresh."""
        d = self._consumed.get(book.symbol)
        if not d:
            return
        now = self._clock()
        sizes: dict[tuple[str, Decimal], Decimal] = {(Side.SELL.value, lvl.price): lvl.size for lvl in book.asks}
        sizes.update({(Side.BUY.value, lvl.price): lvl.size for lvl in book.bids})
        gone: list[tuple[str, Decimal]] = []
        for key, entry in list(d.items()):
            size = sizes.get(key)
            if size is None or size <= 0:
                if entry[2] is None:
                    entry[2] = now
                if now - entry[2] > CONSUMED_GONE_GRACE_S:
                    del d[key]
                else:
                    gone.append(key)
                continue
            if entry[2] is not None:  # vanished and back: the same order only if the size is identical
                if size != entry[0] or now - entry[2] > CONSUMED_GONE_GRACE_S:
                    del d[key]
                    continue
                entry[2] = None
            self._level_update(entry, size)
            if entry[1] <= 0:
                del d[key]
        for key in gone:  # moved orders: identical size at a new price on the same side
            entry = d.get(key)
            if entry is None:
                continue
            for k2, size2 in sizes.items():
                if k2[0] == key[0] and k2 not in d and size2 == entry[0]:
                    d[k2] = [size2, min(entry[1], size2), None]
                    del d[key]
                    break
        if not d:
            del self._consumed[book.symbol]

    def _used(self, symbol: str, level_side: Side, price: Decimal, size: Decimal | None = None) -> Decimal:
        entry = self._consumed.get(symbol, {}).get((level_side.value, price))
        if entry is None:
            return ZERO
        if entry[2] is not None:  # vanished meanwhile (book read without a listener update)
            same = size is not None and size == entry[0] and self._clock() - entry[2] <= CONSUMED_GONE_GRACE_S
            return entry[1] if same else ZERO
        return entry[1]

    def _consume(self, symbol: str, level_side: Side, price: Decimal, qty: Decimal, level_size: Decimal) -> None:
        d = self._consumed.setdefault(symbol, {})
        key = (level_side.value, price)
        entry = d.get(key)
        if entry is None or (entry[2] is not None and self._used(symbol, level_side, price, level_size) <= 0):
            d[key] = [level_size, qty, None]
            return
        entry[2] = None
        self._level_update(entry, level_size)  # the book may have changed without a listener update
        entry[1] += qty

    # ================================================================== helpers
    def _maker_latency_s(self) -> float:
        return max(0.0, float(self.cfg.maker_latency_ms.get(self.name, 0.0))) / 1000.0

    def _taker_latency_s(self) -> float:
        return max(0.0, float(self.cfg.taker_latency_ms.get(self.name, 0.0))) / 1000.0

    @staticmethod
    async def _sleep(seconds: float) -> None:
        if seconds > 0:
            await asyncio.sleep(seconds)

    def fee_rate(self, symbol: str, *, maker: bool) -> Decimal:
        over = self.cfg.maker_fee if maker else self.cfg.taker_fee
        if self.name in over:
            return Decimal(over[self.name])
        m = self.markets.get(symbol)
        if m is None:
            return ZERO
        return m.maker_fee if maker else m.taker_fee

    def leverage(self, symbol: str) -> int:
        return self._leverage.get(symbol, self.default_leverage)

    def position(self, symbol: str) -> Decimal:
        p = self._pos.get(symbol)
        return p.size if p else ZERO

    def _reduce_cap(self, symbol: str, side: Side) -> Decimal:
        """Max size a reduce-only order of ``side`` may still execute."""
        pos = self.position(symbol)
        if pos == 0 or (pos > 0) == (side is Side.BUY):
            return ZERO
        return abs(pos)

    def _increases(self, symbol: str, side: Side) -> bool:
        pos = self.position(symbol)
        return pos == 0 or (pos > 0) == (side is Side.BUY)

    def _mark(self, symbol: str) -> Decimal | None:
        try:
            book = self.inner.get_book(symbol)
        except Exception:
            book = None
        mid = book.mid if book is not None else None
        if mid is not None and mid > 0:
            return mid
        p = self._pos.get(symbol)
        if p is None:
            return None
        return p.last_px or p.entry

    def _validate(self, m: MarketInfo, size: Decimal, price: Decimal, *, reduce_only: bool,
                  check_tick: bool = True, notional_price: Decimal | None = None) -> str | None:
        if not m.active:
            return f"market {m.symbol} is not active"
        if not isinstance(size, Decimal) or not isinstance(price, Decimal):
            return "size and price must be Decimal"
        if size <= 0:
            return "size must be positive"
        if price <= 0:
            return "price must be positive"
        if size % m.step_size != 0:
            return f"size {size} is not a multiple of step {m.step_size}"
        if check_tick:
            tick = m.tick_for(price)
            if price % tick != 0:
                return f"price {price} is not a multiple of tick {tick}"
        if m.max_size is not None and size > m.max_size:
            return f"size {size} exceeds max size {m.max_size}"
        if not reduce_only:
            if size < m.min_size:
                return f"size {size} below min size {m.min_size}"
            px = notional_price if notional_price is not None else price
            if size * px < m.min_notional:
                return f"notional {size * px} below min notional {m.min_notional}"
        return None

    def _margin_error(self, symbol: str, side: Side, size: Decimal, price: Decimal) -> str | None:
        if not self._increases(symbol, side):
            return None
        need = size * price / Decimal(self.leverage(symbol))
        avail = self.balance_now().available
        if need > avail:
            return f"insufficient margin: need {need:.2f}, available {avail:.2f}"
        return None

    def _new_order_id(self) -> str:
        return f"{self._oid_prefix}{next(self._ids)}"

    def _new_trade_id(self) -> str:
        return f"{self._tid_prefix}{next(self._ids)}"

    def _find(self, order_id: str | None, client_id: str | None) -> Order | None:
        if client_id and client_id in self._orders:
            return self._orders[client_id]
        if order_id and order_id in self._by_oid:
            return self._orders.get(self._by_oid[order_id])
        return None

    def _register(self, o: Order) -> None:
        self._orders[o.client_id] = o
        if o.order_id:
            self._by_oid[o.order_id] = o.client_id

    def _forget(self, o: Order) -> None:
        if self._orders.get(o.client_id) is o:
            del self._orders[o.client_id]
        self._open.pop(o.client_id, None)

    def _finish(self, o: Order, status: OrderStatus, error: str | None = None) -> None:
        o.status = status
        o.updated_ts = self._clock()
        if error is not None:
            o.error = error
        if self._open.get(o.client_id) is o:
            del self._open[o.client_id]
        self._terminal_q.append(o.client_id)
        while len(self._terminal_q) > MAX_TERMINAL_ORDERS:
            cid = self._terminal_q.popleft()
            old = self._orders.get(cid)
            if old is not None and old.status.is_terminal:
                del self._orders[cid]
                if old.order_id:
                    self._by_oid.pop(old.order_id, None)

    def _duplicate(self, cid: str) -> bool:
        prev = self._orders.get(cid)
        return prev is not None and not prev.status.is_terminal

    # ================================================================== fills / accounting
    def _fill(self, o: Order, qty: Decimal, price: Decimal, *, maker: bool) -> Fill:
        now = self._clock()
        fee = price * qty * self.fee_rate(o.symbol, maker=maker)
        prev = o.filled
        o.filled = prev + qty
        o.avg_fill_price = price if prev == 0 or o.avg_fill_price is None else (
            (o.avg_fill_price * prev + price * qty) / o.filled)
        o.updated_ts = now
        self._apply_position(o.symbol, qty * o.side.sign, price)
        self._cash += -o.side.sign * qty * price - fee
        self._fees += fee
        self._volume["maker" if maker else "taker"] += price * qty
        fill = Fill(venue=self.name, symbol=o.symbol, side=o.side, price=price, size=qty, fee=fee, is_maker=maker,
                    order_id=o.order_id, client_id=o.client_id, trade_id=self._new_trade_id(), ts=now)
        self._fills.append(fill)
        self.log.info("paper fill %s %s %s %s @ %s (%s, fee %s)", self.name, o.symbol, o.side.value, qty, price,
                      "maker" if maker else "taker", fee)
        self._emit_fill(fill)
        return fill

    def _apply_position(self, symbol: str, signed_qty: Decimal, price: Decimal) -> None:
        p = self._pos.setdefault(symbol, _PaperPosition())
        old = p.size
        new = old + signed_qty
        if old == 0 or (old > 0) == (signed_qty > 0):  # open / increase
            p.entry = price if old == 0 or p.entry is None else (p.entry * abs(old) + price * abs(signed_qty)) / abs(new)
        else:  # reduce / close / flip
            closed = min(abs(old), abs(signed_qty))
            if p.entry is not None:
                p.realized += (price - p.entry) * closed * (1 if old > 0 else -1)
            if new == 0:
                p.entry = None
            elif (new > 0) != (old > 0):
                p.entry = price
        p.size = new
        p.last_px = price

    def _take(self, o: Order, book: OrderBook, limit: Decimal, qty: Decimal, m: MarketInfo) -> Decimal:
        """Sweep the book as TAKER up to ``limit`` for at most ``qty``; one Fill per level. Returns filled."""
        levels = book.asks if o.side is Side.BUY else book.bids
        level_side = o.side.opposite
        left = qty
        for lvl in levels:
            if left <= 0:
                break
            if (o.side is Side.BUY and lvl.price > limit) or (o.side is Side.SELL and lvl.price < limit):
                break
            avail = lvl.size - self._used(o.symbol, level_side, lvl.price, lvl.size)
            take = m.round_size(min(left, avail))
            if take <= 0:
                continue
            self._consume(o.symbol, level_side, lvl.price, take, lvl.size)
            self._fill(o, take, lvl.price, maker=False)
            left -= take
        return qty - left

    # ================================================================== trading
    async def place_limit(self, symbol: str, side: Side, size: Decimal, price: Decimal, *, post_only: bool = True,
                          reduce_only: bool = False, client_id: str | None = None) -> OrderResult:
        m = self.markets.get(symbol)
        if m is None:
            return OrderResult(ok=False, error=f"paper {self.name}: unknown market {symbol!r}")
        err = self._validate(m, size, price, reduce_only=reduce_only)
        if err:
            return OrderResult(ok=False, error=err)
        cid = client_id or new_client_id("pp")
        if self._duplicate(cid):
            return OrderResult(ok=False, error=f"duplicate live client_id {cid}")
        now = self._clock()
        o = Order(venue=self.name, symbol=symbol, side=side, price=price, size=size, client_id=cid,
                  status=OrderStatus.PENDING, post_only=post_only, reduce_only=reduce_only, created_ts=now,
                  updated_ts=now)
        self._register(o)  # blocks duplicates while "in flight"
        berr = await self._tx_budget()
        if berr:
            self._forget(o)
            return OrderResult(ok=False, retryable=True, error=berr)
        await self._sleep(self._maker_latency_s())
        if o.status.is_terminal:  # canceled while in flight
            return OrderResult(ok=False, order=o, error=o.error or f"order {o.status.value}")
        if self._closed:
            self._forget(o)
            return OrderResult(ok=False, error="paper exchange closed")
        book = self.inner.get_book(symbol)
        if book is None or (not book.bids and not book.asks):
            self._forget(o)
            return OrderResult(ok=False, retryable=True, error=f"paper {self.name}: no book for {symbol}")
        if reduce_only and self._reduce_cap(symbol, side) <= 0:
            self._forget(o)
            return OrderResult(ok=False, error="reduce-only order would increase position")
        crosses = (side is Side.BUY and book.best_ask is not None and price >= book.best_ask) or (
            side is Side.SELL and book.best_bid is not None and price <= book.best_bid)
        o.order_id = self._new_order_id()
        self._register(o)
        if crosses and post_only:
            self._finish(o, OrderStatus.REJECTED, "post-only order would cross")
            self._emit_order(o)
            return OrderResult(ok=False, order=o, error=o.error, would_cross=True)
        merr = self._margin_error(symbol, side, size, price)
        if merr:
            self._finish(o, OrderStatus.REJECTED, merr)
            self._emit_order(o)
            return OrderResult(ok=False, order=o, error=merr)
        o.status = OrderStatus.OPEN
        o.updated_ts = self._clock()
        self._open[cid] = o
        if crosses:  # plain limit taking liquidity: execute the crossing part now, rest the remainder
            qty = o.size if not reduce_only else min(o.size, self._reduce_cap(symbol, side))
            self._take(o, book, price, qty, m)
            if o.remaining <= 0:
                self._finish(o, OrderStatus.FILLED)
            elif reduce_only and self._reduce_cap(symbol, side) <= 0:
                self._finish(o, OrderStatus.CANCELED, "reduce-only remainder canceled")
            elif o.filled > 0:
                o.status = OrderStatus.PARTIALLY_FILLED
        self._emit_order(o)
        return OrderResult(ok=True, order=o)

    async def place_market(self, symbol: str, side: Side, size: Decimal, *, worst_price: Decimal,
                           reduce_only: bool = False, client_id: str | None = None) -> OrderResult:
        m = self.markets.get(symbol)
        if m is None:
            return OrderResult(ok=False, error=f"paper {self.name}: unknown market {symbol!r}")
        book0 = self.inner.get_book(symbol)
        ref = None
        if book0 is not None:
            ref = book0.best_ask if side is Side.BUY else book0.best_bid
        err = self._validate(m, size, worst_price, reduce_only=reduce_only, check_tick=False, notional_price=ref)
        if err:
            return OrderResult(ok=False, error=err)
        cid = client_id or new_client_id("pt")
        if self._duplicate(cid):
            return OrderResult(ok=False, error=f"duplicate live client_id {cid}")
        now = self._clock()
        o = Order(venue=self.name, symbol=symbol, side=side, price=worst_price, size=size, client_id=cid,
                  status=OrderStatus.PENDING, ioc=True, reduce_only=reduce_only, created_ts=now, updated_ts=now)
        self._register(o)
        berr = await self._tx_budget()
        if berr:
            self._forget(o)
            return OrderResult(ok=False, retryable=True, error=berr)
        await self._sleep(self._taker_latency_s())
        if self._closed:
            self._forget(o)
            return OrderResult(ok=False, error="paper exchange closed")
        book = self.inner.get_book(symbol)
        if book is None or (not book.bids and not book.asks):
            self._forget(o)
            return OrderResult(ok=False, retryable=True, error=f"paper {self.name}: no book for {symbol}")
        qty = size
        if reduce_only:
            qty = min(size, self._reduce_cap(symbol, side))
            if qty <= 0:
                self._forget(o)
                return OrderResult(ok=False, error="reduce-only order would increase position")
        touch = book.best_ask if side is Side.BUY else book.best_bid
        merr = self._margin_error(symbol, side, qty, touch or worst_price)
        if merr:
            self._forget(o)
            return OrderResult(ok=False, error=merr)
        o.order_id = self._new_order_id()
        self._register(o)
        o.status = OrderStatus.OPEN
        filled = self._take(o, book, worst_price, qty, m)
        if o.remaining <= 0:
            self._finish(o, OrderStatus.FILLED)
        else:
            self._finish(o, OrderStatus.CANCELED,
                         "IOC remainder canceled" if filled > 0 else "IOC canceled: no liquidity within worst price")
        self._emit_order(o)
        return OrderResult(ok=True, order=o)

    async def modify_order(self, order: Order, *, price: Decimal, size: Decimal | None = None) -> OrderResult:
        """Amend in place. ``size`` is the new REMAINING size. A crossing post-only amend CANCELS the
        order (Arcus semantics) and returns ``would_cross``."""
        o = self._find(order.order_id, order.client_id)
        if o is None or not o.is_open or o.ioc:
            return OrderResult(ok=False, order=o or order, error="order not open")
        m = self.markets.get(o.symbol)
        if m is None:
            return OrderResult(ok=False, order=o, error=f"paper {self.name}: unknown market {o.symbol!r}")
        remaining = size if size is not None else o.remaining
        if not isinstance(remaining, Decimal) or remaining <= 0:
            return OrderResult(ok=False, order=o, error="new size must leave a positive remainder")
        err = self._validate(m, o.filled + remaining, price, reduce_only=o.reduce_only)
        if err is None and remaining % m.step_size != 0:
            err = f"size {remaining} is not a multiple of step {m.step_size}"
        if err is None and not o.reduce_only and remaining * price < m.min_notional:
            err = f"remaining notional {remaining * price} below min notional {m.min_notional}"
        if err:
            return OrderResult(ok=False, order=o, error=err)
        berr = await self._tx_budget()
        if berr:
            return OrderResult(ok=False, order=o, retryable=True, error=berr)
        await self._sleep(self._maker_latency_s())
        if not o.is_open:
            return OrderResult(ok=False, order=o, error=f"order is {o.status.value}")
        book = self.inner.get_book(o.symbol)
        if book is None or (not book.bids and not book.asks):
            return OrderResult(ok=False, order=o, retryable=True, error=f"paper {self.name}: no book for {o.symbol}")
        crosses = (o.side is Side.BUY and book.best_ask is not None and price >= book.best_ask) or (
            o.side is Side.SELL and book.best_bid is not None and price <= book.best_bid)
        if crosses and o.post_only:
            self._finish(o, OrderStatus.CANCELED, "post-only modify would cross; order canceled")
            self._emit_order(o)
            return OrderResult(ok=False, order=o, error=o.error, would_cross=True)
        o.price = price
        o.size = o.filled + remaining
        o.error = None
        o.updated_ts = self._clock()
        if crosses:  # non-post-only amend through the book: take what crosses now
            self._take(o, book, price, o.remaining, m)
            if o.remaining <= 0:
                self._finish(o, OrderStatus.FILLED)
        if o.is_open:
            o.status = OrderStatus.PARTIALLY_FILLED if o.filled > 0 else OrderStatus.OPEN
        self._emit_order(o)
        return OrderResult(ok=True, order=o)

    async def cancel_order(self, symbol: str, *, order_id: str | None = None, client_id: str | None = None) -> bool:
        if not order_id and not client_id:
            raise ValueError("cancel_order needs order_id or client_id")
        self._tx_budget_nowait()
        await self._sleep(self._maker_latency_s())
        o = self._find(order_id, client_id)
        if o is None or not o.is_open:
            return True  # unknown / already gone
        if o.client_id not in self._open:
            if o.ioc:  # an IOC in flight cannot be canceled (it executes on arrival)
                return True
            # a limit order still "in flight" (placement latency): it dies on arrival
            self._finish(o, OrderStatus.CANCELED, "canceled before ack")
            self._emit_order(o)
            return True
        self._finish(o, OrderStatus.CANCELED)
        self._emit_order(o)
        return True

    async def cancel_all(self, symbol: str | None = None) -> None:
        self._tx_budget_nowait()
        await self._sleep(self._maker_latency_s())
        for o in list(self._open.values()):
            if symbol is None or o.symbol == symbol:
                if o.is_open:
                    self._finish(o, OrderStatus.CANCELED)
                    self._emit_order(o)

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        lev = max(1, int(leverage))
        m = self.markets.get(symbol)
        if m is not None and m.max_leverage is not None:
            lev = min(lev, max(1, int(m.max_leverage)))
        self._leverage[symbol] = lev

    # ================================================================== account
    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        return [o for o in self._open.values() if o.is_open and (symbol is None or o.symbol == symbol)]

    async def fetch_positions(self) -> dict[str, Position]:
        out: dict[str, Position] = {}
        now = self._clock()
        for sym, p in self._pos.items():
            if p.size == 0:
                continue
            mark = self._mark(sym)
            upnl = (mark - p.entry) * p.size if mark is not None and p.entry is not None else None
            out[sym] = Position(venue=self.name, symbol=sym, size=p.size, entry_price=p.entry, mark_price=mark,
                                unrealized_pnl=upnl, leverage=Decimal(self.leverage(sym)), ts=now)
        return out

    def balance_now(self) -> Balance:
        """Synchronous balance snapshot (marks = current inner book mids)."""
        value = ZERO
        im = ZERO
        mm = ZERO
        for sym, p in self._pos.items():
            if p.size == 0:
                continue
            mark = self._mark(sym) or ZERO
            notional = abs(p.size) * mark
            lev = Decimal(self.leverage(sym))
            value += p.size * mark
            im += notional / lev
            mm += notional / (2 * lev)
        equity = self.initial_balance + self._cash + self._funding_total + value
        return Balance(venue=self.name, equity=equity, available=equity - im, margin_used=im, maintenance_margin=mm,
                       ts=self._clock())

    async def fetch_balance(self) -> Balance:
        return self.balance_now()

    async def fetch_funding_payments(self, since_ts: float | None = None) -> list[FundingPayment]:
        return [p for p in self._funding_payments if since_ts is None or p.ts >= since_ts]

    # ================================================================== funding simulation
    def _funding_symbols(self) -> list[str]:
        syms = set(self._subscribed) | {s for s, p in self._pos.items() if p.size != 0}
        return sorted(s for s in syms if s in self.markets)

    async def refresh_funding(self) -> None:
        """Refresh the funding-rate cache from the inner adapter (errors are logged, not raised)."""
        self._funding_attempt_ts = self._clock()
        syms = self._funding_symbols()
        if not syms:
            return
        try:
            res = await self.inner.fetch_funding(syms)
        except Exception as exc:
            self.log.warning("paper %s funding refresh failed: %r", self.name, exc)
            return
        now = self._clock()
        for sym, fi in res.items():
            self._funding_cache[sym] = (now, fi)

    def settle_funding(self, settle_ts: float) -> list[FundingPayment]:
        """Apply one hourly settlement at ``settle_ts`` to every open position."""
        out: list[FundingPayment] = []
        for sym, p in self._pos.items():
            if p.size == 0:
                continue
            cached = self._funding_cache.get(sym)
            if cached is None or settle_ts - cached[0] > FUNDING_MAX_AGE_S:
                self.log.warning("paper %s: no fresh funding rate for %s at settlement; skipped", self.name, sym)
                continue
            fi = cached[1]
            px = fi.index_price or fi.mark_price or self._mark(sym)
            if px is None or px <= 0:
                self.log.warning("paper %s: no price to value funding for %s; skipped", self.name, sym)
                continue
            amount = -p.size * px * fi.rate_hourly
            fp = FundingPayment(venue=self.name, symbol=sym, amount=amount, rate=fi.rate_hourly,
                                position_size=p.size, ts=settle_ts)
            self._funding_total += amount
            self._funding_payments.append(fp)
            out.append(fp)
            self.log.info("paper funding %s %s: pos %s rate %s px %s -> %s", self.name, sym, p.size,
                          fi.rate_hourly, px, amount)
        return out

    async def funding_step(self) -> None:
        """One iteration of the funding loop: settle a newly started hour, then refresh rates if due."""
        now = self._clock()
        hour = int(now // 3600)
        if self._last_settled_hour is None:
            self._last_settled_hour = hour  # never settle the hour we started in
        elif hour > self._last_settled_hour:
            if hour - self._last_settled_hour > 1:
                self.log.warning("paper %s: skipped %d funding settlement(s) (loop stalled)", self.name,
                                 hour - self._last_settled_hour - 1)
            self._last_settled_hour = hour
            settle_ts = hour * 3600.0
            stale = [s for s, p in self._pos.items() if p.size != 0 and (
                s not in self._funding_cache or settle_ts - self._funding_cache[s][0] > FUNDING_MAX_AGE_S)]
            if stale:  # better an approximate (post-tick) rate than a silently skipped payment
                self.log.info("paper %s: refreshing stale funding rates for %s at settlement", self.name, stale)
                await self.refresh_funding()
            self.settle_funding(settle_ts)
        to_settle = (hour + 1) * 3600.0 - now
        since = now - self._funding_attempt_ts
        if since >= self._funding_refresh_s or (to_settle <= FUNDING_PRE_SETTLE_S and since >= FUNDING_PRE_SETTLE_S):
            await self.refresh_funding()

    async def _funding_loop(self) -> None:
        backoff = 1.0
        while not self._closed:
            try:
                await self.funding_step()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("paper %s funding loop error", self.name)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
            await asyncio.sleep(self._funding_poll_s)

    # ================================================================== diagnostics
    def stats(self) -> dict:
        bal = self.balance_now()
        return {
            "paper": True,
            "equity": bal.equity,
            "available": bal.available,
            "cash_flow": self._cash,
            "fees": self._fees,
            "funding": self._funding_total,
            "maker_volume": self._volume["maker"],
            "taker_volume": self._volume["taker"],
            "realized": sum((p.realized for p in self._pos.values()), ZERO),
            "positions": {s: p.size for s, p in self._pos.items() if p.size != 0},
            "open_orders": len(self._open),
        }


__all__ = ["PaperExchange", "DEFAULT_LEVERAGE"]
