"""Notifications (Telegram optional)."""

from __future__ import annotations

import asyncio
import logging

import aiohttp

log = logging.getLogger("farmer.notify")


class Notifier:
    """No-op notifier (logs only)."""

    async def send(self, text: str) -> None:
        log.info("NOTIFY: %s", text)

    async def close(self) -> None:
        return None


class TelegramNotifier(Notifier):
    def __init__(self, token: str, chat_id: str, *, timeout_s: float = 10.0) -> None:
        self._url = f"https://api.telegram.org/bot{token}/sendMessage"
        self._chat_id = chat_id
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()

    async def send(self, text: str) -> None:
        log.info("NOTIFY: %s", text)
        try:
            async with self._lock:
                if self._session is None or self._session.closed:
                    self._session = aiohttp.ClientSession(timeout=self._timeout)
            async with self._session.post(
                self._url, json={"chat_id": self._chat_id, "text": text[:4000], "disable_web_page_preview": True}
            ) as resp:
                if resp.status != 200:
                    log.warning("telegram send failed: HTTP %s %s", resp.status, (await resp.text())[:200])
        except Exception as exc:  # never let notifications break trading
            log.warning("telegram send failed: %s", exc)

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()


def make_notifier(token: str | None, chat_id: str | None) -> Notifier:
    if token and chat_id:
        return TelegramNotifier(token, chat_id)
    return Notifier()
