"""Lighter websocket client: ONE connection carrying market data, account channels and ``jsonapi/sendtx``.

Behaviour (specs/lighter-data.md §7, lighter-trading.md §8.3):
- first server frame is ``{"type":"connected","session_id":...}``; subscriptions are (re)sent after it;
- the server drops clients that SEND nothing for ~120 s (receiving data does not count) -> app-level
  ``{"type":"ping"}`` every ``ping_interval_s`` (plus protocol pings);
- a silent-drop watchdog closes the socket when nothing was received for ``watchdog_s`` (after one probe ping);
- ``{"type":"shutdown","close_in_ms":N}`` (sent before deployments) -> reconnect immediately;
- reconnect with exponential backoff + jitter, resubscribing every registered channel (auth channels get a
  FRESH token from their provider on every (re)subscribe);
- client->server messages (subscribe/unsubscribe/ping) are capped at 200/min per IP -> local limiter;
  ``sendtx`` is excluded from that budget (it counts against the REST budget instead, handled by the caller);
- a handshake rejected with HTTP 400 ``{"code":20558}`` (restricted jurisdiction) marks ``restricted`` and, when
  configured, falls back to the ``?readonly=true`` URL so public data keeps flowing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from collections.abc import Callable
from typing import Any

from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

from farmer.exchanges.lighter.helpers import ERR_RESTRICTED, TxNotSent, parse_error_code
from farmer.utils.ratelimit import SlidingWindowLimiter

AuthProvider = Callable[[], str | None]
MessageHandler = Callable[[dict], None]

# id-less error frames that can only belong to the single in-flight sendtx (malformed request / tx info)
_TX_ERROR_CODES_WITHOUT_ID = frozenset({30007, 21501})
# market_stats/all streams continuously: a socket silent for longer than this is stalled (downlink dead or
# lagging), so its acks cannot be relied on -> transactions go over REST instead
TX_RX_FRESH_S = 3.0


class LighterWS:
    """Resilient websocket session. Handlers are sync callables run on the reader task (keep them fast)."""

    def __init__(
        self,
        url: str,
        *,
        on_message: MessageHandler,
        on_connect: Callable[[], None] | None = None,
        on_disconnect: Callable[[str], None] | None = None,
        readonly_fallback_url: str | None = None,
        ping_interval_s: float = 30.0,
        watchdog_s: float = 20.0,
        max_msgs_per_min: int = 180,
        open_timeout_s: float = 10.0,
        backoff_initial_s: float = 0.5,
        backoff_max_s: float = 30.0,
        stable_after_s: float = 5.0,
        protocol_ping_s: float | None = 20.0,
        tx_rx_fresh_s: float = TX_RX_FRESH_S,
        name: str = "farmer.lighter.ws",
    ) -> None:
        self.url = url
        self.readonly_fallback_url = readonly_fallback_url
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.ping_interval_s = ping_interval_s
        self.watchdog_s = watchdog_s
        self.open_timeout_s = open_timeout_s
        self.backoff_initial_s = backoff_initial_s
        self.backoff_max_s = backoff_max_s
        self.stable_after_s = stable_after_s
        self.protocol_ping_s = protocol_ping_s
        self.tx_rx_fresh_s = tx_rx_fresh_s
        self.log = logging.getLogger(name)
        self._msg_limiter = SlidingWindowLimiter(max(1, max_msgs_per_min), 60.0)
        self._subs: dict[str, AuthProvider | None] = {}
        self._ws: Any = None
        self._connected = asyncio.Event()
        self._first_attempt_done = asyncio.Event()
        self._stopping = False
        self._task: asyncio.Task | None = None
        self._bg: set[asyncio.Task] = set()
        self._send_lock = asyncio.Lock()
        self._last_rx = 0.0
        self._fast_reconnect = False
        self._draining = False
        self._pending: dict[str, asyncio.Future] = {}
        self._pending_hash: dict[str, str] = {}
        self.restricted = False
        self.restricted_body: str | None = None
        self.session_id: str | None = None
        self.stats: dict[str, int] = {
            "connects": 0,
            "disconnects": 0,
            "watchdog_trips": 0,
            "shutdown_frames": 0,
            "messages": 0,
            "handler_errors": 0,
            "handshake_rejections": 0,
        }

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stopping = False
            self._task = asyncio.create_task(self._run(), name="lighter-ws")

    async def stop(self) -> None:
        self._stopping = True
        ws = self._ws
        if ws is not None:
            try:
                await asyncio.wait_for(ws.close(), timeout=3)
            except Exception:  # pragma: no cover - best effort
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        for t in list(self._bg):
            t.cancel()
        self._bg.clear()
        self._fail_pending(ConnectionError("websocket stopped"))

    @property
    def connected(self) -> bool:
        return self._connected.is_set() and self._ws is not None

    @property
    def can_send_tx(self) -> bool:
        return (self.connected and not self._draining and not self.restricted_mode
                and self.last_rx_age() < self.tx_rx_fresh_s)

    @property
    def restricted_mode(self) -> bool:
        """True when running on the readonly fallback URL after a geo-block."""
        return self.restricted and self.readonly_fallback_url is not None and self.url == self.readonly_fallback_url

    async def wait_connected(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._connected.wait(), timeout)
            return True
        except TimeoutError:
            return False

    async def wait_first_attempt(self, timeout: float) -> bool:
        """Wait until the first connection attempt finished (connected OR rejected)."""
        try:
            await asyncio.wait_for(self._first_attempt_done.wait(), timeout)
            return True
        except TimeoutError:
            return False

    def last_rx_age(self) -> float:
        return time.monotonic() - self._last_rx if self._last_rx else float("inf")

    # ------------------------------------------------------------------ subscriptions
    def subscribe(self, channel: str, auth: AuthProvider | None = None) -> None:
        """Register ``channel`` (kept across reconnects) and subscribe now if connected."""
        new = channel not in self._subs
        self._subs[channel] = auth
        if new and self.connected:
            self._spawn(self._send_subscribe(channel))

    def unsubscribe(self, channel: str) -> None:
        known = channel in self._subs
        self._subs.pop(channel, None)
        if known and self.connected:
            self._spawn(self._send_ctrl({"type": "unsubscribe", "channel": channel}))

    def resubscribe(self, channel: str) -> None:
        """Unsubscribe + subscribe (fresh snapshot / fresh auth token)."""
        if channel not in self._subs or not self.connected:
            return

        async def _do() -> None:
            await self._send_ctrl({"type": "unsubscribe", "channel": channel})
            await self._send_subscribe(channel)

        self._spawn(_do())

    @property
    def channels(self) -> list[str]:
        return list(self._subs)

    def _spawn(self, coro) -> None:
        async def guarded() -> None:
            try:
                await coro
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("lighter ws background send failed")

        t = asyncio.get_running_loop().create_task(guarded())
        self._bg.add(t)
        t.add_done_callback(self._bg.discard)

    async def _send_subscribe(self, channel: str) -> None:
        if channel not in self._subs:
            return
        msg: dict[str, Any] = {"type": "subscribe", "channel": channel}
        provider = self._subs.get(channel)
        if provider is not None:
            try:
                tok = provider()
            except Exception:
                self.log.exception("auth token provider failed for %s", channel)
                return
            if not tok:
                self.log.warning("no auth token for %s; not subscribing", channel)
                return
            msg["auth"] = tok
        await self._send_ctrl(msg)

    async def _send_ctrl(self, msg: dict) -> bool:
        """Budgeted client->server message (subscribe/unsubscribe/ping). Returns False if not sent."""
        ws = self._ws
        if ws is None:
            return False
        await self._msg_limiter.acquire()
        ws = self._ws
        if ws is None:
            return False
        try:
            async with self._send_lock:
                await ws.send(json.dumps(msg))
            return True
        except (ConnectionClosed, OSError) as exc:
            self.log.debug("ws send failed (%s): %s", msg.get("type"), exc)
            return False

    # ------------------------------------------------------------------ transactions
    async def send_tx(self, tx_type: int, tx_info: dict, tx_hash: str, timeout: float) -> dict:
        """Send ``jsonapi/sendtx`` and wait for the ack frame (correlated by ``id`` or ``tx_hash``).

        Raises TxNotSent if the socket is not usable (nothing left this process), ``asyncio.TimeoutError`` if no
        ack arrived (tx may have been accepted) and ``ConnectionError`` if the socket died meanwhile.
        """
        ws = self._ws
        if ws is None or not self.can_send_tx:
            raise TxNotSent("websocket not connected")
        rid = uuid.uuid4().hex[:24]
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        self._pending_hash[tx_hash] = rid
        try:
            payload = json.dumps({"type": "jsonapi/sendtx", "data": {"id": rid, "tx_type": int(tx_type), "tx_info": tx_info}})
            try:
                async with self._send_lock:
                    await ws.send(payload)
            except (ConnectionClosed, OSError) as exc:
                raise ConnectionError(f"websocket closed while sending tx: {exc}") from exc
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._pending.pop(rid, None)
            if self._pending_hash.get(tx_hash) == rid:
                self._pending_hash.pop(tx_hash, None)

    def _resolve(self, rid: str, msg: dict) -> None:
        fut = self._pending.pop(rid, None)
        if fut is not None and not fut.done():
            fut.set_result(msg)

    def _fail_pending(self, exc: Exception) -> None:
        for rid in list(self._pending):
            fut = self._pending.pop(rid)
            if not fut.done():
                fut.set_exception(exc)
        self._pending_hash.clear()

    def _match_tx_ack(self, msg: dict) -> bool:
        if not self._pending:
            return False
        rid = msg.get("id")
        if isinstance(rid, str) and rid in self._pending:
            self._resolve(rid, msg)
            return True
        h = msg.get("tx_hash")
        if isinstance(h, str) and h in self._pending_hash:
            self._resolve(self._pending_hash[h], msg)
            return True
        err = msg.get("error")
        if rid is None and isinstance(err, dict) and len(self._pending) == 1:
            try:
                code = int(err.get("code", -1))
            except (TypeError, ValueError):
                code = -1
            if code in _TX_ERROR_CODES_WITHOUT_ID:  # e.g. 30007 frames do not echo the id
                self._resolve(next(iter(self._pending)), msg)
                return True
        return False

    # ------------------------------------------------------------------ connection loop
    def _connect_kwargs(self) -> dict:
        return dict(
            max_size=None,
            max_queue=1024,
            open_timeout=self.open_timeout_s,
            ping_interval=self.protocol_ping_s,
            ping_timeout=self.protocol_ping_s,
            close_timeout=2,
            compression="deflate",
        )

    async def _run(self) -> None:
        backoff = self.backoff_initial_s
        while not self._stopping:
            started = time.monotonic()
            reason = "closed"
            was_connected = False
            try:
                async with ws_connect(self.url, **self._connect_kwargs()) as ws:
                    self._ws = ws
                    was_connected = await self._session(ws)
                    reason = f"closed code={getattr(ws, 'close_code', None)} reason={getattr(ws, 'close_reason', '')}"
            except asyncio.CancelledError:
                raise
            except InvalidStatus as exc:
                reason = self._on_handshake_rejected(exc)
            except (OSError, ConnectionClosed, InvalidHandshake, TimeoutError) as exc:
                reason = f"{type(exc).__name__}: {exc}"
                self.log.warning("lighter ws connection error: %s", reason)
            except Exception as exc:  # never let the loop die
                reason = f"{type(exc).__name__}: {exc}"
                self.log.exception("lighter ws unexpected error")
            finally:
                self._ws = None
                self._connected.clear()
                self._first_attempt_done.set()
                self._fail_pending(ConnectionError(f"websocket disconnected ({reason})"))
            if was_connected:
                self.stats["disconnects"] += 1
                self._draining = False
                if self.on_disconnect is not None:
                    try:
                        self.on_disconnect(reason)
                    except Exception:
                        self.log.exception("on_disconnect handler failed")
            if self._stopping:
                break
            stable = was_connected and time.monotonic() - started >= self.stable_after_s
            if stable:
                backoff = self.backoff_initial_s
            if self._fast_reconnect or stable:
                # a healthy session that dropped (or a shutdown notice / watchdog trip): reconnect right away;
                # repeated failures fall back to exponential backoff
                self._fast_reconnect = False
                delay = random.random() * 0.1
            else:
                delay = backoff + random.random() * min(1.0, backoff)
                backoff = min(backoff * 2, self.backoff_max_s)
            if was_connected or delay > 1:
                self.log.info("lighter ws reconnecting in %.2fs (%s)", delay, reason)
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise

    def _on_handshake_rejected(self, exc: Exception) -> str:
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", None)
        body = getattr(resp, "body", b"") or b""
        text = body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else str(body)
        self.stats["handshake_rejections"] += 1
        code = None
        try:
            code = int(json.loads(text).get("code"))
        except Exception:
            code = parse_error_code(text)
        if code == ERR_RESTRICTED:
            self.restricted = True
            self.restricted_body = text[:300]
            if self.readonly_fallback_url and self.url != self.readonly_fallback_url:
                self.log.critical(
                    "lighter ws handshake rejected: restricted jurisdiction (20558). Trading is impossible from "
                    "this region; falling back to the read-only stream for market data. body=%s", text[:200]
                )
                self.url = self.readonly_fallback_url
                self._fast_reconnect = True
            else:
                self.log.error("lighter ws handshake rejected: restricted jurisdiction (20558) body=%s", text[:200])
        else:
            self.log.error("lighter ws handshake rejected: HTTP %s %s", status, text[:200])
        return f"handshake HTTP {status} code={code}"

    async def _session(self, ws) -> bool:
        """Run one connected session. Returns True if the 'connected' frame was received."""
        raw = await asyncio.wait_for(ws.recv(), timeout=self.open_timeout_s)
        try:
            first = json.loads(raw)
        except ValueError:
            first = {}
        if first.get("type") != "connected":
            self.log.warning("lighter ws: unexpected first frame %s", str(raw)[:200])
        self.session_id = first.get("session_id")
        self._last_rx = time.monotonic()
        self._connected.set()
        self._first_attempt_done.set()
        self.stats["connects"] += 1
        self.log.info("lighter ws connected (%s, session %s)", self.url.split("?")[0], self.session_id)
        if self.on_connect is not None:
            try:
                self.on_connect()
            except Exception:
                self.log.exception("on_connect handler failed")
        helpers = [
            asyncio.create_task(self._subscribe_all(), name="lighter-ws-subscribe"),
            asyncio.create_task(self._pinger(ws), name="lighter-ws-ping"),
            asyncio.create_task(self._watchdog(ws), name="lighter-ws-watchdog"),
        ]
        try:
            async for raw in ws:
                self._last_rx = time.monotonic()
                self.stats["messages"] += 1
                try:
                    msg = json.loads(raw)
                except ValueError:
                    self.log.warning("lighter ws: non-JSON frame %s", str(raw)[:120])
                    continue
                if not isinstance(msg, dict):
                    continue
                self._dispatch(ws, msg)
        except ConnectionClosed as exc:
            self.log.warning("lighter ws closed: %s", exc)
        finally:
            for t in helpers:
                t.cancel()
            await asyncio.gather(*helpers, return_exceptions=True)
        return True

    def _dispatch(self, ws, msg: dict) -> None:
        t = msg.get("type")
        if t == "ping":
            self._spawn(self._reply_pong(ws))
            return
        if t == "pong":
            return
        if t == "shutdown":
            self.stats["shutdown_frames"] += 1
            self.log.warning("lighter ws: server shutdown notice (close_in_ms=%s); reconnecting now", msg.get("close_in_ms"))
            self._draining = True
            self._fast_reconnect = True
            self._spawn(self._close(ws))
            return
        if self._match_tx_ack(msg):
            return
        try:
            self.on_message(msg)
        except Exception:
            self.stats["handler_errors"] += 1
            self.log.exception("lighter ws handler failed for %s", str(msg)[:200])

    async def _reply_pong(self, ws) -> None:
        try:
            async with self._send_lock:
                await ws.send('{"type":"pong"}')
        except Exception:
            pass

    async def _close(self, ws) -> None:
        try:
            await ws.close()
        except Exception:
            pass

    async def _subscribe_all(self) -> None:
        for ch in list(self._subs):
            await self._send_subscribe(ch)

    async def _pinger(self, ws) -> None:
        while True:
            await asyncio.sleep(self.ping_interval_s)
            if not await self._send_ctrl({"type": "ping"}):
                return

    async def _watchdog(self, ws) -> None:
        probe_sent = False
        tick = max(0.05, min(1.0, self.watchdog_s / 4))
        while True:
            await asyncio.sleep(tick)
            idle = time.monotonic() - self._last_rx
            if idle > self.watchdog_s:
                self.stats["watchdog_trips"] += 1
                self.log.warning("lighter ws: no frame for %.1fs; forcing reconnect", idle)
                self._fast_reconnect = True
                await self._close(ws)
                return
            if idle > self.watchdog_s / 2:
                if not probe_sent:
                    probe_sent = True
                    await self._send_ctrl({"type": "ping"})
            else:
                probe_sent = False
