"""Dashboard back end: owns at most one ``BotSession`` plus one-shot jobs (connection test, close-all).

Everything runs on the dashboard's event loop: the engine exactly as ``farmer run`` runs it (same run lock, same
graceful shutdown), so the dashboard is a different front end, not a different bot.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import enum
import io
import logging
import math
import os
import time
from collections.abc import Callable, MutableMapping
from decimal import Decimal
from pathlib import Path
from typing import Any

from farmer import __version__, cli
from farmer.config import AppConfig, load_config
from farmer.dashboard import credentials

log = logging.getLogger("farmer.dashboard")

REPORT_CACHE_S = 3.0  # store summaries are reused this long (the page polls every ~2 s)
SAMPLE_EVERY_S = 30.0  # PnL / volume samples kept for the session chart
CLOSE_TIMEOUT = {"maker": 600.0, "taker": 120.0}


class DashboardError(Exception):
    """A request that cannot be served; ``status`` is the HTTP status."""

    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


def jsonable(x: Any) -> Any:
    """Decimals as strings, enums as values, dataclasses / sets / tuples as plain JSON types."""
    if isinstance(x, Decimal):
        return str(x) if x.is_finite() else None
    if isinstance(x, float):
        return x if math.isfinite(x) else None
    if isinstance(x, enum.Enum):
        return jsonable(x.value)
    if isinstance(x, dict):
        return {str(k): jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set, frozenset, collections.deque)):
        return [jsonable(v) for v in x]
    if dataclasses.is_dataclass(x) and not isinstance(x, type):
        return jsonable(dataclasses.asdict(x))
    if x is None or isinstance(x, (str, int, bool)):
        return x
    return str(x)


class LogBuffer(logging.Handler):
    """The last ``capacity`` log records (INFO and up) for the dashboard's log view."""

    def __init__(self, capacity: int = 2000, level: int = logging.INFO) -> None:
        super().__init__(level)
        self.lines: collections.deque[dict] = collections.deque(maxlen=capacity)
        self.seq = 0

    def emit(self, record: logging.LogRecord) -> None:
        if record.name == "aiohttp.access":  # the page's own polling
            return
        try:
            msg = record.getMessage()
            if record.exc_info and record.exc_info[1] is not None:
                msg += f" [{record.exc_info[1]!r}]"
        except Exception:  # pragma: no cover - a broken log call must not break the bot
            return
        self.seq += 1
        self.lines.append({"seq": self.seq, "ts": record.created, "level": record.levelname, "name": record.name,
                           "msg": msg})

    def since(self, seq: int, limit: int = 500) -> list[dict]:
        rows = [r for r in self.lines if r["seq"] > seq]
        return rows[-limit:]


VenuesFactory = Callable[[AppConfig, bool], dict[str, Any]]


class BotController:
    def __init__(self, config_path: Path, env_file: Path, *, environ: MutableMapping[str, str] | None = None,
                 venues_factory: VenuesFactory | None = None, logs: LogBuffer | None = None) -> None:
        self.config_path = Path(config_path)
        self.env_file = Path(env_file)
        self.environ = os.environ if environ is None else environ
        self.venues_factory = venues_factory  # tests: fake venues instead of the real adapters
        self.logs = logs or LogBuffer()
        self.session: cli.BotSession | None = None
        self.task: asyncio.Task | None = None
        self.bot_state = "stopped"  # stopped | running | stopping
        self.mode: str | None = None  # paper | live of the running (or last) session
        self.started_ts: float | None = None
        self.last_exit: dict | None = None
        self.job: dict | None = None
        self._job_task: asyncio.Task | None = None
        self.samples: collections.deque[tuple[float, Any, Any]] = collections.deque(maxlen=2880)
        self._report: tuple[float, dict | None] = (0.0, None)
        self._db_cache: tuple[float, str | None, dict | None] = (0.0, None, None)

    # ------------------------------------------------------------------ config
    def load_cfg(self, *, warn: bool = False) -> AppConfig:
        """config.yaml (when present) + secrets from the process environment (``.env`` was loaded at startup and
        is kept in sync by ``save_credentials``). Config warnings are logged only when ``warn`` (bot start), not on
        every poll."""
        path = self.config_path if self.config_path.exists() else None
        try:
            return load_config(path, env_file=None, environ=self.environ, warn=warn)
        except Exception as exc:
            raise DashboardError(f"config: {exc}") from exc

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    @property
    def job_running(self) -> bool:
        return self._job_task is not None and not self._job_task.done()

    # ------------------------------------------------------------------ bot
    async def start(self, mode: str, confirm: str | None = None) -> None:
        if mode not in ("paper", "live"):
            raise DashboardError("mode paper ya da live olmalı")
        if self.running:
            raise DashboardError("bot zaten çalışıyor", 409)
        if self.job_running:
            raise DashboardError("bir işlem sürüyor (close-all / bağlantı testi); bitmesini bekleyin", 409)
        cfg = self.load_cfg(warn=True)
        paper = mode == "paper"
        if not paper:
            missing = cli._missing_credentials(cfg)
            if missing:
                raise DashboardError("canlı mod için iki borsada da API anahtarı gerekli; eksik: " + "; ".join(missing))
            if (confirm or "").strip().upper() != "CANLI":
                raise DashboardError("canlı işlem için onay kutusuna CANLI yazın")
        venues = self.venues_factory(cfg, paper) if self.venues_factory else None
        session = cli.BotSession(cfg, paper=paper, venues=venues)
        if not session.acquire():
            raise DashboardError(f"başka bir farmer süreci bu veritabanında çalışıyor (pid {session.lock.holder()})",
                                 409)
        self.session = session
        self.mode = mode
        self.bot_state = "running"
        self.started_ts = time.time()
        self.last_exit = None
        self.samples.clear()
        self._report = (0.0, None)
        log.warning("dashboard: starting the bot in %s mode", mode.upper())
        self.task = asyncio.create_task(self._run(session), name="dashboard:bot")

    async def _run(self, session: cli.BotSession) -> None:
        rc: int | None = None
        error: str | None = None
        try:
            rc = await session.run()
            error = session.error
        except asyncio.CancelledError:
            error = "cancelled"
            raise
        except Exception as exc:
            log.exception("bot session failed")
            rc, error = cli.EXIT_ERROR, f"{exc}"
        finally:
            self.last_exit = {"code": rc, "error": error, "ts": time.time(), "mode": self.mode}
            self.bot_state = "stopped"
            self.session = None
            self._report = (0.0, None)

    def stop(self) -> None:
        if not self.running or self.session is None:
            raise DashboardError("bot çalışmıyor", 409)
        log.warning("dashboard: stop requested (cancel quotes, flush hedges)")
        self.bot_state = "stopping"
        self.session.request_stop()

    async def shutdown(self, timeout_s: float = 300.0) -> None:
        """Dashboard exit: stop the bot gracefully and end any job (close-all cancels like Ctrl+C would)."""
        if self._job_task is not None and not self._job_task.done():
            self._job_task.cancel()  # a close-all ends through its own graceful path
        if self.running and self.session is not None:
            self.bot_state = "stopping"
            self.session.request_stop()
        for t in (self.task, self._job_task):
            if t is not None and not t.done():
                with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    await asyncio.wait_for(asyncio.shield(t), timeout_s)

    # ------------------------------------------------------------------ jobs
    def _start_job(self, kind: str, coro: Any) -> None:
        if self.job_running:
            coro.close()
            raise DashboardError("başka bir işlem sürüyor", 409)
        job = {"kind": kind, "state": "running", "output": "", "started": time.time(), "ended": None}
        self.job = job

        async def runner() -> None:
            try:
                ok, text = await coro
                job["state"], job["output"] = ("done" if ok else "failed"), text
            except asyncio.CancelledError:
                job["state"], job["output"] = "failed", (job["output"] or "") + "\nişlem iptal edildi"
                raise
            except Exception as exc:
                log.exception("dashboard job %s failed", kind)
                job["state"], job["output"] = "failed", f"{exc!r}"
            finally:
                job["ended"] = time.time()

        self._job_task = asyncio.create_task(runner(), name=f"dashboard:{kind}")

    def close_all(self, mode: str, confirm: str | None = None) -> None:
        if mode not in ("maker", "taker"):
            raise DashboardError("mode maker ya da taker olmalı")
        if (confirm or "").strip().upper() != "KAPAT":
            raise DashboardError("tüm pozisyonları kapatmak için onay kutusuna KAPAT yazın")
        timeout = CLOSE_TIMEOUT[mode]
        if self.running and self.session is not None and self.session.engine is not None:
            engine = self.session.engine
            log.warning("dashboard: close_all(%s) on the running bot", mode)

            async def on_engine() -> tuple[bool, str]:
                res = await engine.close_all(mode, timeout_s=timeout)
                lines = [f"{s}: maker {r['maker_pos']} taker {r['taker_pos']} {'FLAT' if r['flat'] else 'NOT FLAT'}"
                         for s, r in res.items()]
                ok = all(r["flat"] for r in res.values())
                tail = ("Pozisyonlar kapatıldı. Bot çalışmaya devam ediyor ama yeni pozisyon açmaz (force close); "
                        "yeniden işlem için durdurup başlatın." if mode == "maker" else
                        "Pozisyonlar IOC ile kapatıldı. Bot çalışıyor; kotasyon için durdurup yeniden başlatın.")
                return ok, "\n".join([*lines, tail if ok else "Zaman aşımı: hepsi kapanmadı."])

            self._start_job(f"close-all ({mode})", on_engine())
            return
        if self.running:
            raise DashboardError("bot henüz başlıyor; birkaç saniye sonra tekrar deneyin", 409)
        cfg = self.load_cfg()
        if cli._missing_credentials(cfg):
            raise DashboardError("close-all canlı hesaplarda çalışır; iki borsada da API anahtarı gerekli")
        log.warning("dashboard: close-all (%s) with the bot stopped", mode)

        async def standalone() -> tuple[bool, str]:
            buf = io.StringIO()
            rc = await cli.close_all(cfg, taker=mode == "taker", timeout_s=timeout, out=buf, signals=False)
            return rc == cli.EXIT_OK, buf.getvalue()

        self._start_job(f"close-all ({mode})", standalone())

    def test_connection(self) -> None:
        cfg = self.load_cfg()
        if not (cfg.arcus.has_credentials or cfg.lighter.has_credentials):
            raise DashboardError("önce en az bir borsanın API anahtarlarını kaydedin")

        async def run_status() -> tuple[bool, str]:
            buf = io.StringIO()
            errs = io.StringIO()
            with contextlib.redirect_stderr(errs):
                rc = await cli.status(cfg, out=buf)
            text = buf.getvalue() + (("\n" + errs.getvalue()) if errs.getvalue() else "")
            return rc == cli.EXIT_OK, text.strip()

        self._start_job("bağlantı testi", run_status())

    # ------------------------------------------------------------------ credentials
    def credentials_status(self) -> dict:
        return credentials.status(self.environ)

    def save_credentials(self, updates: dict) -> list[str]:
        try:
            changed = credentials.save(self.env_file, updates, self.environ)
        except ValueError as exc:
            raise DashboardError(str(exc)) from None
        if changed:
            log.warning("dashboard: credentials updated (%s)%s", ", ".join(changed),
                        "; the running bot keeps its keys until restarted" if self.running else "")
        return changed

    # ------------------------------------------------------------------ data
    async def report(self) -> dict | None:
        """Volume / fees / funding / PnL: the running session's stats reporter, else the database of the last
        (or configured) mode without marks (open positions are then excluded from the PnL)."""
        now = time.monotonic()
        ts, cached = self._report
        if cached is not None and now - ts < REPORT_CACHE_S:
            return cached
        session = self.session
        rep: dict | None = None
        if session is not None and session.stats is not None and session.engine is not None and session.engine.started:
            try:
                rep = jsonable(await session.stats.snapshot())
            except Exception as exc:
                log.debug("dashboard report failed: %r", exc)
            if rep is not None and (not self.samples or time.time() - self.samples[-1][0] >= SAMPLE_EVERY_S):
                self.samples.append((time.time(), rep.get("pnl_all"), (rep.get("all") or {}).get("volume")))
        elif not self.running:
            rep = await self._db_report()
        self._report = (now, rep)
        return rep

    def _db_paper(self) -> bool:
        if self.mode is not None:
            return self.mode == "paper"
        try:
            return self.load_cfg().mode == "paper"
        except DashboardError:
            return True

    async def _db_report(self) -> dict | None:
        from farmer.monitoring.stats import pnl_from_summary, summaries_snapshot
        from farmer.persistence.store import Store

        paper = self._db_paper()
        try:
            cfg = self.load_cfg()
        except DashboardError:
            return None
        path = cli.store_path(cfg, paper)
        if not path.exists():
            return None

        def read() -> dict:
            store = Store(path, paper=paper)
            try:
                now = time.time()
                s1h, s24, sall = store.summary(now - 3600), store.summary(now - 86400), store.summary(0.0)
            finally:
                store.close()
            out = summaries_snapshot(s1h, s24, sall, ["arcus", "lighter"])
            out.update(ts=now, pnl_24h=pnl_from_summary(s24, {}), pnl_all=pnl_from_summary(sall, {}),
                       missing_marks=["bot durduruldu: açık pozisyonlar PnL'e dahil değil"], from_db=True)
            return out

        try:
            return jsonable(await asyncio.to_thread(read))
        except Exception as exc:
            log.debug("dashboard database report failed: %r", exc)
            return None

    def _store_for_reads(self) -> tuple[Any, bool]:
        """(store, owned): the running session's store, else a fresh one on the last / configured mode's db."""
        from farmer.persistence.store import Store

        if self.session is not None and self.session.store is not None:
            return self.session.store, False
        paper = self._db_paper()
        path = cli.store_path(self.load_cfg(), paper)
        if not path.exists():
            return None, False
        return Store(path, paper=paper), True

    def events(self, limit: int = 40) -> list[dict]:
        try:
            store, owned = self._store_for_reads()
        except DashboardError:
            return []
        if store is None:
            return []
        try:
            return [{"ts": ts, "kind": kind, "data": jsonable(data)} for ts, kind, data in store.recent_events(limit)]
        finally:
            if owned:
                store.close()

    async def history(self, hours: float = 24.0) -> dict:
        since = time.time() - max(1.0, min(hours, 24 * 30)) * 3600

        def read() -> list:
            store, owned = self._store_for_reads()
            if store is None:
                return []
            try:
                return store.hourly(since)
            finally:
                if owned:
                    store.close()

        try:
            rows = await asyncio.to_thread(read)
        except Exception as exc:
            log.debug("dashboard history failed: %r", exc)
            rows = []
        return {"hourly": [{"hour": h, "venue": v, "volume": jsonable(vol), "funding": jsonable(fund), "fills": n}
                           for h, v, vol, fund, n in rows],
                "samples": [[t, p, v] for t, p, v in self.samples]}

    async def state(self) -> dict:
        session = self.session
        engine = session.engine if session is not None else None
        eng_status = None
        if engine is not None and engine.started:
            try:
                eng_status = jsonable(engine.status())
            except Exception as exc:
                eng_status = {"error": repr(exc)}
        try:
            cfg_mode = self.load_cfg().mode
            cfg_error = None
        except DashboardError as exc:
            cfg_mode, cfg_error = None, str(exc)
        return {
            "version": __version__,
            "now": time.time(),
            "bot": {"state": self.bot_state, "mode": self.mode if self.running else None, "started_ts": self.started_ts
                    if self.running else None, "engine_started": bool(engine is not None and engine.started),
                    "last_exit": self.last_exit, "last_mode": self.mode},
            "engine": eng_status,
            "report": await self.report(),
            "events": self.events(),
            "job": self.job,
            "credentials": self.credentials_status(),
            "config": {"mode": cfg_mode, "error": cfg_error, "path": str(self.config_path)},
        }


__all__ = ["BotController", "DashboardError", "LogBuffer", "jsonable"]
