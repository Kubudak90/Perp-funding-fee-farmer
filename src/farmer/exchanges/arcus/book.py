"""Local L2 book for the Arcus ``l2OrderbookUpdates`` channel.

Protocol (spec arcus-data §5.3.3, live-verified):
- the ``subscribed`` frame carries a snapshot of ``nLevels`` per side plus ``lastSequenceId``;
- every ``channel_data`` frame is a delta with ABSOLUTE sizes (``"0"`` = remove), applied in array
  order, covering the WHOLE book (not only the seeded window);
- the first delta after a snapshot may jump ahead of ``snapshotSeq + 1`` (documented boundary gap,
  NOT an error); after that sequences are contiguous and a gap means the book is broken ->
  resubscribe for a fresh snapshot;
- ``bbo`` frames share the per-market sequence space: a bbo frame whose ``lastSequenceId`` equals
  the book's is authoritative for the top of book at that sequence.

LIVE FINDING (2026-10-19): the boundary gap is a real loss window. Changes between the snapshot's
``lastSequenceId`` and the first delta (seen: +93 sequences on ETH-USD) are never delivered, so some
snapshot levels are stale (e.g. an ask that was filled in the window stays in the book and later
crosses it). Hence snapshot levels are tracked as *unconfirmed* until a delta touches their price:
- a crossed book is repaired by dropping unconfirmed levels that conflict with fresher data;
- a same-sequence ``bbo`` frame repairs the top (drops unconfirmed levels better than the bbo,
  inserts/resizes the bbo level). Only a disagreement involving a CONFIRMED level means the stream is
  broken -> resnapshot.

Prices are kept in two sorted lists (bisect) so top-N extraction is O(N) at ~100 deltas/s.
"""

from __future__ import annotations

import bisect
import time
from decimal import Decimal
from enum import Enum

from farmer.models import BookLevel, OrderBook

D = Decimal
ZERO = Decimal(0)


class DeltaResult(str, Enum):
    APPLIED = "applied"
    STALE = "stale"  # sequence <= current (pre-snapshot or duplicate) -> ignored
    GAP = "gap"  # mid-stream sequence gap -> caller must resnapshot
    UNSYNCED = "unsynced"  # no snapshot yet -> ignored
    CROSSED = "crossed"  # applied but the book is crossed/locked -> caller must resnapshot


class L2Book:
    """Full-depth book maintained from one snapshot + contiguous deltas."""

    def __init__(self, symbol: str, ticker: str, *, venue: str = "arcus", depth: int = 20,
                 max_levels: int = 2000) -> None:
        self.symbol = symbol
        self.ticker = ticker
        self.venue = venue
        self.depth = depth
        self.max_levels = max_levels
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._bid_px: list[Decimal] = []  # ascending; best bid = last
        self._ask_px: list[Decimal] = []  # ascending; best ask = first
        self.seq: int | None = None
        self.synced = False
        self.applying = False  # True once the first post-snapshot delta was applied
        self.snapshot_ts: float | None = None
        self.last_update_ts: float | None = None
        self.seeded_low_bid: Decimal | None = None
        self.seeded_high_ask: Decimal | None = None
        self.gaps = 0
        self.resyncs = 0
        self.repairs = 0  # stale snapshot levels corrected (crossed repair / bbo reconcile)
        self._unconfirmed: set[tuple[str, Decimal]] = set()  # ("b"|"a", price) seen only in the snapshot

    # ------------------------------------------------------------------ state
    def reset(self) -> None:
        """Forget everything; the book is unusable until the next snapshot."""
        self._bids.clear()
        self._asks.clear()
        self._bid_px.clear()
        self._ask_px.clear()
        self._unconfirmed.clear()
        self.seq = None
        self.synced = False
        self.applying = False

    def apply_snapshot(self, contents: dict, *, now: float | None = None) -> None:
        self.reset()
        for p, s in contents.get("bids") or []:
            self._set(self._bids, self._bid_px, D(p), D(s))
        for p, s in contents.get("asks") or []:
            self._set(self._asks, self._ask_px, D(p), D(s))
        seq = contents.get("lastSequenceId")
        self.seq = int(seq) if seq is not None else None
        self.synced = self.seq is not None
        self.applying = False
        self.snapshot_ts = self.last_update_ts = time.time() if now is None else now
        self.seeded_low_bid = self._bid_px[0] if self._bid_px else None
        self.seeded_high_ask = self._ask_px[-1] if self._ask_px else None
        self._unconfirmed = {("b", p) for p in self._bid_px} | {("a", p) for p in self._ask_px}

    def apply_delta(self, contents: dict, *, now: float | None = None) -> DeltaResult:
        if not self.synced or self.seq is None:
            return DeltaResult.UNSYNCED
        seq = contents.get("lastSequenceId")
        if seq is None:
            return DeltaResult.STALE
        seq = int(seq)
        if seq <= self.seq:
            return DeltaResult.STALE
        if self.applying and seq != self.seq + 1:
            self.gaps += 1
            self.synced = False
            return DeltaResult.GAP
        unconf = self._unconfirmed
        for p, s in contents.get("bids") or []:
            px = D(p)
            self._set(self._bids, self._bid_px, px, D(s))
            if unconf:
                unconf.discard(("b", px))
        for p, s in contents.get("asks") or []:
            px = D(p)
            self._set(self._asks, self._ask_px, px, D(s))
            if unconf:
                unconf.discard(("a", px))
        self.seq = seq
        self.applying = True
        self.last_update_ts = time.time() if now is None else now
        self._trim()
        if self.is_crossed() and not self._repair_cross():
            self.synced = False
            return DeltaResult.CROSSED
        return DeltaResult.APPLIED

    def _drop(self, side: str, price: Decimal) -> None:
        if side == "b":
            self._set(self._bids, self._bid_px, price, ZERO)
        else:
            self._set(self._asks, self._ask_px, price, ZERO)
        self._unconfirmed.discard((side, price))

    def _repair_cross(self) -> bool:
        """Drop stale (unconfirmed) snapshot levels until uncrossed; False if confirmed levels cross."""
        while self.is_crossed():
            pb, pa = self._bid_px[-1], self._ask_px[0]
            if ("b", pb) in self._unconfirmed:
                self._drop("b", pb)
            elif ("a", pa) in self._unconfirmed:
                self._drop("a", pa)
            else:
                return False
            self.repairs += 1
        return True

    @staticmethod
    def _set(book: dict, px: list, price: Decimal, size: Decimal) -> None:
        if size <= ZERO:
            if price in book:
                del book[price]
                i = bisect.bisect_left(px, price)
                if i < len(px) and px[i] == price:
                    px.pop(i)
            return
        if price not in book:
            bisect.insort(px, price)
        book[price] = size

    def _trim(self) -> None:
        # Bound memory / insert cost: drop the levels farthest from the touch.
        if len(self._bid_px) > self.max_levels:
            for p in self._bid_px[: len(self._bid_px) - self.max_levels]:
                del self._bids[p]
            del self._bid_px[: len(self._bid_px) - self.max_levels]
        if len(self._ask_px) > self.max_levels:
            for p in self._ask_px[self.max_levels:]:
                del self._asks[p]
            del self._ask_px[self.max_levels:]

    # ------------------------------------------------------------------ reads
    @property
    def best_bid(self) -> tuple[Decimal, Decimal] | None:
        if not self._bid_px:
            return None
        p = self._bid_px[-1]
        return p, self._bids[p]

    @property
    def best_ask(self) -> tuple[Decimal, Decimal] | None:
        if not self._ask_px:
            return None
        p = self._ask_px[0]
        return p, self._asks[p]

    def is_crossed(self) -> bool:
        return bool(self._bid_px and self._ask_px and self._bid_px[-1] >= self._ask_px[0])

    def levels(self) -> tuple[int, int]:
        return len(self._bid_px), len(self._ask_px)

    def top(self, n: int | None = None) -> tuple[list[BookLevel], list[BookLevel]]:
        n = self.depth if n is None else n
        bids = [BookLevel(p, self._bids[p]) for p in reversed(self._bid_px[-n:])] if n > 0 else []
        asks = [BookLevel(p, self._asks[p]) for p in self._ask_px[:n]] if n > 0 else []
        return bids, asks

    def to_order_book(self, n: int | None = None, *, ts: float | None = None) -> OrderBook:
        bids, asks = self.top(n)
        return OrderBook(venue=self.venue, symbol=self.symbol, bids=bids, asks=asks,
                         ts=time.time() if ts is None else ts, seq=self.seq)

    def check_bbo(self, contents: dict) -> bool | None:
        """Read-only cross-check of a ``bbo`` frame. True = consistent, False = top differs,
        None = not comparable (different sequence / book not synced)."""
        if not self.synced or self.seq is None:
            return None
        seq = contents.get("lastSequenceId")
        if seq is None or int(seq) != self.seq:
            return None
        for key, best in (("bestBid", self.best_bid), ("bestAsk", self.best_ask)):
            lvl = contents.get(key)
            if lvl is None:
                if best is not None:
                    return False
                continue
            if best is None:
                return False
            if D(lvl["price"]) != best[0] or D(lvl["size"]) != best[1]:
                return False
        return True

    def reconcile_bbo(self, contents: dict) -> bool | None:
        """Repair the top from a same-sequence ``bbo`` frame (authoritative at that sequence).

        Returns None (not comparable), True (consistent, possibly after repairing stale unconfirmed
        levels; see ``repairs``) or False (a CONFIRMED level disagrees -> the stream is broken).
        """
        if not self.synced or self.seq is None or not self.applying:
            # before the first delta a bbo at the snapshot's sequence is as stale as the snapshot itself
            return None
        seq = contents.get("lastSequenceId")
        if seq is None or int(seq) != self.seq:
            return None
        for side, key in (("b", "bestBid"), ("a", "bestAsk")):
            book, px = (self._bids, self._bid_px) if side == "b" else (self._asks, self._ask_px)
            lvl = contents.get(key)
            ref = D(lvl["price"]) if lvl else None
            # levels strictly better than the bbo (or any level when the bbo side is empty) cannot exist
            while px:
                best = px[-1] if side == "b" else px[0]
                if ref is not None and (best <= ref if side == "b" else best >= ref):
                    break
                if (side, best) not in self._unconfirmed:
                    return False
                self._drop(side, best)
                self.repairs += 1
            if lvl is None:
                continue
            size = D(lvl["size"])
            cur = book.get(ref)
            if cur == size:
                self._unconfirmed.discard((side, ref))
                continue
            if cur is not None and (side, ref) not in self._unconfirmed:
                return False  # a delta-confirmed level disagrees with the bbo
            self._set(book, px, ref, size)  # missing (inserted in the loss window) or stale size
            self._unconfirmed.discard((side, ref))
            self.repairs += 1
        if self.is_crossed():
            return False
        return True


def bbo_to_order_book(symbol: str, contents: dict, *, venue: str = "arcus", ts: float | None = None) -> OrderBook:
    """Top-of-book OrderBook from a ``bbo`` frame / REST ``/v1/bbo`` body."""
    bids, asks = [], []
    if contents.get("bestBid"):
        bids.append(BookLevel(D(contents["bestBid"]["price"]), D(contents["bestBid"]["size"])))
    if contents.get("bestAsk"):
        asks.append(BookLevel(D(contents["bestAsk"]["price"]), D(contents["bestAsk"]["size"])))
    seq = contents.get("lastSequenceId")
    return OrderBook(venue=venue, symbol=symbol, bids=bids, asks=asks, ts=time.time() if ts is None else ts,
                     seq=int(seq) if seq is not None else None)


def levels_to_order_book(symbol: str, contents: dict, *, venue: str = "arcus", depth: int = 20,
                         ts: float | None = None) -> OrderBook:
    """OrderBook from a REST ``/v1/l2OrderBook`` body or an ``l2Orderbook`` snapshot."""
    bids = sorted((BookLevel(D(p), D(s)) for p, s in contents.get("bids") or [] if D(s) > 0),
                  key=lambda lv: lv.price, reverse=True)[:depth]
    asks = sorted((BookLevel(D(p), D(s)) for p, s in contents.get("asks") or [] if D(s) > 0),
                  key=lambda lv: lv.price)[:depth]
    seq = contents.get("lastSequenceId")
    return OrderBook(venue=venue, symbol=symbol, bids=bids, asks=asks, ts=time.time() if ts is None else ts,
                     seq=int(seq) if seq is not None else None)


__all__ = ["DeltaResult", "L2Book", "bbo_to_order_book", "levels_to_order_book"]
