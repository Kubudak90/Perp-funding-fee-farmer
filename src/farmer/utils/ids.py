"""Client order id helpers."""

from __future__ import annotations

import itertools
import os
import time

_counter = itertools.count(1)
_prefix = os.urandom(2).hex()


def new_client_id(tag: str = "ff") -> str:
    """Short unique string id: <tag>-<pid-ish random>-<ms>-<counter>."""
    return f"{tag}{_prefix}{int(time.time() * 1000) % 10**10:010d}{next(_counter) % 10**6:06d}"


class IntIdGenerator:
    """Monotonic integer ids within [lo, hi] (Lighter client_order_index is an integer)."""

    def __init__(self, lo: int = 1, hi: int = 2**48 - 1) -> None:
        self.lo, self.hi = lo, hi
        # seed from wall clock (ms) so restarts don't reuse recent ids
        self._next = lo + (int(time.time() * 1000) % (hi - lo))

    def next(self) -> int:
        v = self._next
        self._next += 1
        if self._next > self.hi:
            self._next = self.lo
        return v
