"""Local Lighter order book maintained from the ``order_book/{market_id}`` WS channel.

Protocol (specs/lighter-data.md §7.4, verified live): the first frame after subscribing
(``subscribed/order_book``) is the FULL book; every later ``update/order_book`` frame carries changed levels
where ``size`` is the level's NEW TOTAL and a zero size deletes it. Continuity: ``update.begin_nonce`` must equal
the previous frame's ``nonce`` (a global matching-engine counter, so it jumps by more than 1); otherwise an update
was missed and the channel must be re-subscribed. ``offset`` is per API server and must be ignored.
"""

from __future__ import annotations

import bisect
import time
from decimal import Decimal, InvalidOperation

from farmer.models import BookLevel, OrderBook


class LocalBook:
    """Sorted price levels for one market. Not thread-safe (single event loop)."""

    def __init__(self, symbol: str, market_id: int, venue: str = "lighter") -> None:
        self.symbol = symbol
        self.market_id = market_id
        self.venue = venue
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._bid_px: list[Decimal] = []  # ascending (best bid is the last element)
        self._ask_px: list[Decimal] = []  # ascending (best ask is the first element)
        self.nonce: int = -1  # -1 = waiting for a snapshot
        self.updates = 0
        self.gaps = 0
        self.last_ts: float = 0.0  # local receive time of the last applied frame

    # ------------------------------------------------------------------ state
    @property
    def valid(self) -> bool:
        return self.nonce >= 0

    def invalidate(self) -> None:
        self.nonce = -1

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self._bid_px.clear()
        self._ask_px.clear()

    # ------------------------------------------------------------------ frames
    def apply_snapshot(self, ob: dict, ts: float | None = None) -> None:
        self.clear()
        self._merge(ob)
        self.nonce = int(ob.get("nonce", 0))
        self.last_ts = time.time() if ts is None else ts

    def apply_update(self, ob: dict, ts: float | None = None) -> bool:
        """Apply a delta. Returns False on a continuity gap (book invalidated; caller must resubscribe)."""
        if self.nonce < 0:
            return True  # still waiting for the snapshot: ignore
        try:
            begin = int(ob.get("begin_nonce", -2))
        except (TypeError, ValueError):
            begin = -2
        if begin != self.nonce:
            self.gaps += 1
            self.invalidate()
            return False
        self._merge(ob)
        self.nonce = int(ob.get("nonce", self.nonce))
        self.updates += 1
        self.last_ts = time.time() if ts is None else ts
        return True

    def _merge(self, ob: dict) -> None:
        for levels, book, keys in ((ob.get("asks") or [], self._asks, self._ask_px), (ob.get("bids") or [], self._bids, self._bid_px)):
            for lvl in levels:
                try:
                    px = Decimal(str(lvl["price"]))
                    sz = Decimal(str(lvl["size"]))
                except (KeyError, InvalidOperation, TypeError, ValueError):
                    continue
                if sz <= 0:
                    if book.pop(px, None) is not None:
                        i = bisect.bisect_left(keys, px)
                        if i < len(keys) and keys[i] == px:
                            del keys[i]
                else:
                    if px not in book:
                        bisect.insort(keys, px)
                    book[px] = sz

    # ------------------------------------------------------------------ queries
    @property
    def best_bid(self) -> Decimal | None:
        return self._bid_px[-1] if self._bid_px else None

    @property
    def best_ask(self) -> Decimal | None:
        return self._ask_px[0] if self._ask_px else None

    def crossed(self) -> bool:
        bb, ba = self.best_bid, self.best_ask
        return bb is not None and ba is not None and bb >= ba

    def depth(self) -> tuple[int, int]:
        return len(self._bid_px), len(self._ask_px)

    def top(self, n: int = 20) -> tuple[list[BookLevel], list[BookLevel]]:
        bids = [BookLevel(px, self._bids[px]) for px in reversed(self._bid_px[-n:])] if n > 0 else []
        asks = [BookLevel(px, self._asks[px]) for px in self._ask_px[:n]] if n > 0 else []
        return bids, asks

    def to_orderbook(self, depth: int = 20, ts: float | None = None) -> OrderBook:
        bids, asks = self.top(depth)
        return OrderBook(
            venue=self.venue,
            symbol=self.symbol,
            bids=bids,
            asks=asks,
            ts=self.last_ts if ts is None else ts,
            seq=self.nonce if self.nonce >= 0 else None,
        )
