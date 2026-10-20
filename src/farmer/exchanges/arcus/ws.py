"""Managed Arcus WebSocket connection.

One instance = one socket that is kept alive forever:
- exponential backoff with jitter on (re)connect (respects 50 new connections/min/IP);
- every registered subscription is re-sent after each reconnect (``subscribed`` carries the snapshot);
- protocol pings (library) + JSON ``{"type":"ping"}`` every ``ping_interval_s`` (the ``pong`` doubles
  as an application-level liveness probe);
- silent-drop watchdog: no frame at all for ``watchdog_s`` -> force reconnect;
- subscriptions not acknowledged within ``subscribe_timeout_s`` are re-sent (bounded); a ``degraded``
  frame means "pending, server retries": the timer is re-armed with its ``retryAfterMs``;
- proactive roll before the server's 24 h connection lifetime;
- RPC ``get``/``post`` with strictly increasing integer ids per connection (reset to 1 on reconnect,
  pending futures failed on disconnect).

Incoming frames are passed to ``on_message`` synchronously from the reader task; handlers must be
fast and must not block.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from decimal import Decimal
from typing import Any, Awaitable, Callable

import websockets

log = logging.getLogger("farmer.arcus.ws")

ACCOUNT_CHANNELS = {"account", "positions", "orders", "userFills", "funding", "accountAttributeUpdates",
                    "accountTransferUpdates"}

SubKey = tuple[str, str, int | None, str | None]


def sub_key(msg: dict) -> SubKey:
    """Subscription identity: (channel, id, accountIndex, market). Account ids compare lowercase."""
    ch = str(msg.get("channel") or "")
    ident = str(msg.get("id") or "")
    if ch in ACCOUNT_CHANNELS:
        ident = ident.lower()
    ai = msg.get("accountIndex")
    mk = msg.get("market")
    return ch, ident, (int(ai) if ai is not None and ch in ACCOUNT_CHANNELS else None), (
        str(mk) if mk and ch in ACCOUNT_CHANNELS else None)


class ArcusWsConnection:
    """Self-healing socket with subscription registry and RPC."""

    def __init__(
        self,
        url: str,
        *,
        name: str = "ws",
        on_message: Callable[[dict], None],
        on_connect: Callable[[], Awaitable[None] | None] | None = None,
        on_disconnect: Callable[[], None] | None = None,
        watchdog_s: float = 45.0,
        ping_interval_s: float = 20.0,
        max_lifetime_s: float = 23.5 * 3600,
        backoff_initial_s: float = 0.5,
        backoff_max_s: float = 30.0,
        open_timeout_s: float = 15.0,
        subscribe_timeout_s: float = 15.0,
        max_subscribe_retries: int = 3,
        connect: Callable[..., Any] | None = None,
    ) -> None:
        self.url = url
        self.name = name
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.watchdog_s = watchdog_s
        self.ping_interval_s = ping_interval_s
        self.max_lifetime_s = max_lifetime_s
        self.backoff_initial_s = backoff_initial_s
        self.backoff_max_s = backoff_max_s
        self.open_timeout_s = open_timeout_s
        self.subscribe_timeout_s = subscribe_timeout_s
        self.max_subscribe_retries = max_subscribe_retries
        self._connect = connect or websockets.connect
        self.log = logging.getLogger(f"farmer.arcus.ws.{name}")

        self._subs: dict[SubKey, dict] = {}
        self._pending_subs: dict[SubKey, float] = {}  # key -> time the ack timeout counts from
        self._sub_retries: dict[SubKey, int] = {}
        self.degraded: dict[SubKey, dict] = {}  # key -> last ``degraded`` frame (pending on the server)
        self._ws: Any = None
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._connected = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._next_id = 1
        self._pending_rpc: dict[int, asyncio.Future] = {}
        self.last_frame_mono: float = 0.0
        self.connected_since_mono: float | None = None
        self.connects = 0
        self.disconnects = 0
        self.connection_id: str | None = None

    # ------------------------------------------------------------------ public api
    @property
    def connected(self) -> bool:
        return self._connected.is_set() and self._ws is not None

    def frame_age(self) -> float:
        """Seconds since the last received frame (inf if never)."""
        return time.monotonic() - self.last_frame_mono if self.last_frame_mono else float("inf")

    def is_healthy(self, max_silence_s: float) -> bool:
        return self.connected and self.frame_age() <= max_silence_s

    async def wait_connected(self, timeout: float | None = None) -> bool:
        try:
            await asyncio.wait_for(self._connected.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name=f"arcus-ws-{self.name}")

    async def stop(self) -> None:
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:  # pragma: no cover - best effort
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        self._fail_pending(ConnectionError("arcus ws stopped"))

    def has_subscription(self, key: SubKey) -> bool:
        return key in self._subs

    async def subscribe(self, msg: dict) -> SubKey:
        """Register (and send, if connected) a subscription; it is re-sent on every reconnect."""
        m = dict(msg)
        m["type"] = "subscribe"
        key = sub_key(m)
        self._subs[key] = m
        if self.connected:
            self._pending_subs[key] = time.monotonic()
            await self._send_json(m)
        return key

    async def unsubscribe(self, key: SubKey) -> None:
        m = self._subs.pop(key, None)
        self._pending_subs.pop(key, None)
        self._sub_retries.pop(key, None)
        self.degraded.pop(key, None)
        if m is not None and self.connected:
            await self._send_json(self._unsub_msg(m))

    async def resubscribe(self, key: SubKey) -> None:
        """Force a fresh snapshot: unsubscribe + subscribe on the same socket (processed in order)."""
        m = self._subs.get(key)
        if m is None or not self.connected:
            return
        self._pending_subs[key] = time.monotonic()
        await self._send_json(self._unsub_msg(m))
        await self._send_json(m)

    async def request(self, kind: str, method: str, payload: dict, *, timeout: float = 10.0,
                      envelope: dict | None = None) -> dict:
        """RPC: ``kind`` is ``get`` or ``post``; ``envelope`` adds apiKey/timestamp/signature."""
        if not self.connected:
            raise ConnectionError(f"arcus ws {self.name} not connected")
        loop = asyncio.get_running_loop()
        async with self._send_lock:
            rid = self._next_id
            self._next_id += 1
            fut = loop.create_future()
            self._pending_rpc[rid] = fut
            req = {"type": method, "payload": payload}
            if envelope:
                req.update(envelope)
            try:
                await self._ws.send(json.dumps({"type": kind, "id": rid, "request": req}, separators=(",", ":"),
                                               default=_json_default))
            except Exception:
                self._pending_rpc.pop(rid, None)
                raise
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._pending_rpc.pop(rid, None)

    async def get(self, method: str, payload: dict | None = None, *, timeout: float = 10.0) -> dict:
        return await self.request("get", method, payload or {}, timeout=timeout)

    def force_reconnect(self, reason: str = "") -> None:
        """Close the current socket; the run loop reconnects with backoff."""
        ws = self._ws
        if ws is not None:
            self.log.warning("forcing reconnect: %s", reason)
            asyncio.ensure_future(self._close_quietly(ws))

    # ------------------------------------------------------------------ internals
    @staticmethod
    def _unsub_msg(m: dict) -> dict:
        u = {k: v for k, v in m.items() if k in ("channel", "id", "accountIndex", "market", "sigFigs", "roundStep")}
        u["type"] = "unsubscribe"
        return u

    async def _send_json(self, obj: dict) -> None:
        ws = self._ws
        if ws is None:
            raise ConnectionError(f"arcus ws {self.name} not connected")
        async with self._send_lock:
            await ws.send(json.dumps(obj, separators=(",", ":"), default=_json_default))

    async def _close_quietly(self, ws: Any) -> None:
        try:
            await asyncio.wait_for(ws.close(), 5.0)
        except Exception:
            pass

    def _fail_pending(self, exc: Exception) -> None:
        for fut in self._pending_rpc.values():
            if not fut.done():
                fut.set_exception(exc)
        self._pending_rpc.clear()

    async def _run(self) -> None:
        backoff = self.backoff_initial_s
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                await self._session()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning("arcus ws %s error: %r", self.name, exc)
            finally:
                if self._connected.is_set():
                    self.disconnects += 1
                self._connected.clear()
                self._ws = None
                self._fail_pending(ConnectionError(f"arcus ws {self.name} reconnecting"))
                self._pending_subs.clear()
                self._sub_retries.clear()
                self.degraded.clear()
                if self.on_disconnect is not None:
                    try:
                        self.on_disconnect()
                    except Exception:
                        self.log.exception("on_disconnect hook failed")
            if self._stop.is_set():
                break
            if time.monotonic() - started > 60.0:
                backoff = self.backoff_initial_s  # it was a healthy connection
            delay = min(backoff, self.backoff_max_s) * (0.5 + random.random())
            self.log.info("arcus ws %s reconnecting in %.1fs", self.name, delay)
            try:
                await asyncio.wait_for(self._stop.wait(), delay)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, self.backoff_max_s)

    async def _session(self) -> None:
        ws = await asyncio.wait_for(
            self._connect(self.url, max_size=2**25, ping_interval=20, ping_timeout=20, close_timeout=5,
                          open_timeout=self.open_timeout_s),
            self.open_timeout_s + 5,
        )
        self._ws = ws
        self._next_id = 1
        now = time.monotonic()
        self.connected_since_mono = now
        self.last_frame_mono = now
        self.connects += 1
        aux: list[asyncio.Task] = []
        try:
            # resubscribe everything before announcing the connection (loop until no subscription
            # registered concurrently during an await is left unsent)
            sent: set[SubKey] = set()
            while True:
                todo = [(k, m) for k, m in self._subs.items() if k not in sent]
                if not todo:
                    break
                for key, m in todo:
                    sent.add(key)
                    self._pending_subs[key] = time.monotonic()
                    await self._send_json(m)
            self._connected.set()
            self.log.info("arcus ws %s connected (%d subscriptions)", self.name, len(self._subs))
            if self.on_connect is not None:
                res = self.on_connect()
                if asyncio.iscoroutine(res):
                    aux.append(asyncio.create_task(_guard(res, self.log, "on_connect")))
            aux.append(asyncio.create_task(self._pinger(ws)))
            aux.append(asyncio.create_task(self._watchdog(ws)))
            async for raw in ws:
                self.last_frame_mono = time.monotonic()
                self._dispatch(raw)
        finally:
            for t in aux:
                t.cancel()
            await self._close_quietly(ws)

    def _dispatch(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw, parse_float=Decimal)
        except ValueError:
            self.log.warning("non-JSON frame: %.200r", raw)
            return
        if not isinstance(msg, dict):
            return
        rid = msg.get("id")
        if "method" in msg and isinstance(rid, int):
            fut = self._pending_rpc.pop(rid, None)
            if fut is not None and not fut.done():
                fut.set_result(msg)
            return
        t = msg.get("type")
        if t == "connected":
            self.connection_id = str(msg.get("connection_id"))
        elif t in ("subscribed", "degraded", "unsubscribed"):
            if t == "subscribed":
                key = sub_key(msg)
                self._pending_subs.pop(key, None)
                self._sub_retries.pop(key, None)
                self.degraded.pop(key, None)
            elif t == "degraded":
                key = sub_key(msg)
                self.degraded[key] = msg
                try:
                    retry_s = max(0.0, int(msg.get("retryAfterMs") or 0) / 1000.0)
                except (TypeError, ValueError):
                    retry_s = 0.0
                if key in self._subs:  # still pending: the server retries, so restart the ack timer later
                    self._pending_subs[key] = time.monotonic() + retry_s
                self.log.warning("subscription degraded: %s",
                                 {k: msg.get(k) for k in ("channel", "id", "reason", "retryAfterMs")})
        elif t == "error":
            self.log.warning("arcus ws %s error frame: %.300s", self.name, msg.get("message"))
        try:
            self.on_message(msg)
        except Exception:
            self.log.exception("arcus ws %s handler failed for %.200s", self.name, str(msg)[:200])

    async def _pinger(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(self.ping_interval_s)
            try:
                async with self._send_lock:
                    await ws.send('{"type":"ping"}')
            except Exception:
                return

    async def _watchdog(self, ws: Any) -> None:
        period = max(0.05, min(self.watchdog_s / 4, self.subscribe_timeout_s / 4, 5.0))
        while True:
            await asyncio.sleep(period)
            now = time.monotonic()
            if now - self.last_frame_mono > self.watchdog_s:
                self.log.warning("arcus ws %s silent for %.0fs -> reconnect", self.name, now - self.last_frame_mono)
                await self._close_quietly(ws)
                return
            if self.connected_since_mono is not None and now - self.connected_since_mono > self.max_lifetime_s:
                self.log.info("arcus ws %s lifetime roll", self.name)
                await self._close_quietly(ws)
                return
            for key, sent in list(self._pending_subs.items()):
                if now - sent <= self.subscribe_timeout_s:
                    continue
                n = self._sub_retries.get(key, 0)
                if key not in self._subs or n >= self.max_subscribe_retries:
                    self.log.error("subscription %s not acknowledged after %d attempt(s) -> giving up until the "
                                   "next reconnect", key, n + 1)
                    self._pending_subs.pop(key, None)
                    continue
                self._sub_retries[key] = n + 1
                self.log.warning("subscription %s not acknowledged after %.0fs -> re-sending (%d/%d)", key,
                                 now - sent, n + 1, self.max_subscribe_retries)
                try:
                    await self.resubscribe(key)  # unsubscribe + subscribe, re-arms the timer
                except Exception as exc:  # the socket is broken: reconnect (resubscribes everything)
                    self.log.warning("re-sending subscription %s failed: %r -> reconnect", key, exc)
                    await self._close_quietly(ws)
                    return


async def _guard(coro: Awaitable, logger: logging.Logger, what: str) -> None:
    try:
        await coro
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("%s failed", what)


def _json_default(o: Any) -> Any:
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(f"not JSON serializable: {type(o).__name__}")


__all__ = ["ACCOUNT_CHANNELS", "ArcusWsConnection", "SubKey", "sub_key"]
