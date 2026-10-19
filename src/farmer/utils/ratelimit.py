"""Async rate limiters."""

from __future__ import annotations

import asyncio
import collections
import time


class SlidingWindowLimiter:
    """At most ``max_calls`` (weighted) acquisitions per rolling ``period`` seconds.

    Matches venue limits expressed as "N requests per rolling minute".
    """

    def __init__(self, max_calls: int, period: float = 60.0, *, clock=time.monotonic) -> None:
        if max_calls <= 0:
            raise ValueError("max_calls must be positive")
        self.max_calls = max_calls
        self.period = period
        self._clock = clock
        self._events: collections.deque[tuple[float, int]] = collections.deque()
        self._used = 0
        self._lock = asyncio.Lock()

    def _prune(self, now: float) -> None:
        while self._events and now - self._events[0][0] >= self.period:
            _, w = self._events.popleft()
            self._used -= w

    def available(self) -> int:
        self._prune(self._clock())
        return self.max_calls - self._used

    def try_acquire(self, weight: int = 1) -> bool:
        now = self._clock()
        self._prune(now)
        if self._used + weight > self.max_calls:
            return False
        self._events.append((now, weight))
        self._used += weight
        return True

    async def acquire(self, weight: int = 1) -> None:
        if weight > self.max_calls:
            raise ValueError("weight exceeds limiter capacity")
        async with self._lock:
            while True:
                now = self._clock()
                self._prune(now)
                if self._used + weight <= self.max_calls:
                    self._events.append((now, weight))
                    self._used += weight
                    return
                # wait until enough weight expires
                need = self._used + weight - self.max_calls
                freed = 0
                wait = self.period
                for ts, w in self._events:
                    freed += w
                    if freed >= need:
                        wait = max(0.0, self.period - (now - ts)) + 0.001
                        break
                await asyncio.sleep(wait)


class TokenBucket:
    """Classic token bucket: ``rate`` tokens/second, burst ``capacity``."""

    def __init__(self, rate: float, capacity: float, *, clock=time.monotonic) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._clock = clock
        self._last = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
        self._last = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    async def acquire(self, tokens: float = 1.0) -> None:
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                await asyncio.sleep((tokens - self._tokens) / self.rate)
