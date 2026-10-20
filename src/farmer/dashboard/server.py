"""``farmer dashboard``: a local, token-protected web UI on top of ``BotController``.

Security model (the page can start live trading and store API keys):
- binds 127.0.0.1 by default; reach a remote host through an SSH tunnel rather than exposing the port;
- every ``/api`` request needs ``Authorization: Bearer <token>`` (printed at startup as ``/#token=...``; the page
  keeps it in sessionStorage, never in a cookie, so other sites cannot ride on it);
- on a loopback bind the ``Host`` header must name the loopback address (DNS rebinding), and a write request's
  ``Origin`` must be this server; JSON bodies only;
- the page loads no external resources; a per-response CSP nonce allows only its own inline script;
- secrets are write-only (never returned) and live in ``.env`` (mode 0600), exactly as with the CLI.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import ipaddress
import json
import logging
import os
import secrets
import sys
from pathlib import Path
from typing import Any

from aiohttp import web

from farmer.dashboard import settings
from farmer.dashboard.controller import BotController, DashboardError, jsonable

log = logging.getLogger("farmer.dashboard")

STATIC = Path(__file__).parent / "static"
MAX_BODY = 64 * 1024

CONTROLLER = web.AppKey("controller", BotController)
TOKEN = web.AppKey("token", str)
ALLOWED_HOSTS = web.AppKey("allowed_hosts", object)


def _json(data: Any, status: int = 200) -> web.Response:
    return web.json_response(jsonable(data), status=status, dumps=lambda o: json.dumps(o, ensure_ascii=False))


def _err(message: str, status: int) -> web.Response:
    return _json({"error": message}, status)


def _loopback(host: str) -> bool:
    if host in ("localhost", ""):
        return True
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        return False


def allowed_hosts_for(host: str) -> set[str] | None:
    """Host names accepted on a loopback bind (None = any: a non-loopback bind relies on the token alone)."""
    return {"127.0.0.1", "localhost", "[::1]", "::1"} if _loopback(host) else None


def _hostname(host_header: str) -> str:
    """``Host`` header without the port (``[::1]:8080`` -> ``[::1]``)."""
    h = host_header.strip().lower()
    if h.startswith("["):
        return h[: h.find("]") + 1] if "]" in h else h
    return h.rsplit(":", 1)[0] if h.count(":") == 1 else h


@web.middleware
async def security(request: web.Request, handler: Any) -> web.StreamResponse:
    allowed = request.app[ALLOWED_HOSTS]
    if allowed is not None and _hostname(request.host or "") not in allowed:
        return web.Response(status=403, text="forbidden host")
    if request.path.startswith("/api/"):
        auth = request.headers.get("Authorization", "")
        given = auth[7:] if auth.startswith("Bearer ") else ""
        if not given or not hmac.compare_digest(given.encode(), request.app[TOKEN].encode()):
            resp: web.StreamResponse = _err("yetkisiz: panel bağlantısındaki token gerekli", 401)
            return _harden(resp)
        if request.method not in ("GET", "HEAD"):
            origin = request.headers.get("Origin")
            if origin is not None and origin not in (f"http://{request.host}", f"https://{request.host}"):
                return _harden(_err("başka bir siteden gelen istek reddedildi", 403))
            if request.content_type != "application/json":
                return _harden(_err("JSON gövde gerekli", 415))
    try:
        resp = await handler(request)
    except DashboardError as exc:
        resp = _err(str(exc), exc.status)
    except web.HTTPException:
        raise
    except Exception as exc:
        log.exception("dashboard request %s %s failed", request.method, request.path)
        resp = _err(f"sunucu hatası: {exc!r}", 500)
    return _harden(resp)


def _harden(resp: web.StreamResponse) -> web.StreamResponse:
    h = resp.headers
    h.setdefault("X-Content-Type-Options", "nosniff")
    h.setdefault("Referrer-Policy", "no-referrer")
    h.setdefault("X-Frame-Options", "DENY")
    h.setdefault("Cache-Control", "no-store")
    return resp


async def _body(request: web.Request) -> dict:
    if request.content_length is not None and request.content_length > MAX_BODY:
        raise DashboardError("istek çok büyük", 413)
    try:
        data = await request.json()
    except Exception:
        raise DashboardError("geçersiz JSON") from None
    if not isinstance(data, dict):
        raise DashboardError("JSON nesnesi bekleniyordu")
    return data


# --------------------------------------------------------------------------- handlers
async def index(request: web.Request) -> web.Response:
    nonce = secrets.token_urlsafe(16)
    html = (STATIC / "index.html").read_text(encoding="utf-8").replace("{{NONCE}}", nonce)
    resp = web.Response(text=html, content_type="text/html", charset="utf-8")
    resp.headers["Content-Security-Policy"] = (
        f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'unsafe-inline'; "
        "connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
    return resp


async def no_content(request: web.Request) -> web.Response:
    return web.Response(status=204)


async def get_state(request: web.Request) -> web.Response:
    return _json(await request.app[CONTROLLER].state())


async def get_history(request: web.Request) -> web.Response:
    try:
        hours = float(request.query.get("hours", "24"))
    except ValueError:
        raise DashboardError("hours sayı olmalı") from None
    return _json(await request.app[CONTROLLER].history(hours))


async def get_logs(request: web.Request) -> web.Response:
    try:
        after = int(request.query.get("after", "0"))
        limit = max(1, min(2000, int(request.query.get("limit", "500"))))
    except ValueError:
        raise DashboardError("after / limit tam sayı olmalı") from None
    logs = request.app[CONTROLLER].logs
    return _json({"lines": logs.since(after, limit), "seq": logs.seq})


async def get_config(request: web.Request) -> web.Response:
    ctl = request.app[CONTROLLER]
    try:
        cur = settings.current(ctl.config_path)
    except Exception as exc:
        raise DashboardError(f"config okunamadı: {exc}") from None
    return _json({**cur, **settings.schema(), "running": ctl.running})


async def put_config(request: web.Request) -> web.Response:
    ctl = request.app[CONTROLLER]
    patch = await _body(request)
    try:
        cur = settings.save(ctl.config_path, patch)
    except ValueError as exc:
        raise DashboardError(str(exc)) from None
    log.warning("dashboard: config saved to %s%s", ctl.config_path,
                " (applies at the next start)" if ctl.running else "")
    return _json({**cur, **settings.schema(), "running": ctl.running, "saved": True})


async def get_credentials(request: web.Request) -> web.Response:
    return _json(request.app[CONTROLLER].credentials_status())


async def put_credentials(request: web.Request) -> web.Response:
    ctl = request.app[CONTROLLER]
    changed = ctl.save_credentials(await _body(request))
    return _json({"changed": changed, "credentials": ctl.credentials_status(), "running": ctl.running})


async def post_test(request: web.Request) -> web.Response:
    ctl = request.app[CONTROLLER]
    ctl.test_connection()
    return _json({"job": ctl.job})


async def post_start(request: web.Request) -> web.Response:
    ctl = request.app[CONTROLLER]
    body = await _body(request)
    await ctl.start(str(body.get("mode") or ""), body.get("confirm"))
    return _json(await ctl.state())


async def post_stop(request: web.Request) -> web.Response:
    ctl = request.app[CONTROLLER]
    await _body(request)
    ctl.stop()
    return _json(await ctl.state())


async def post_close_all(request: web.Request) -> web.Response:
    ctl = request.app[CONTROLLER]
    body = await _body(request)
    ctl.close_all(str(body.get("mode") or ""), body.get("confirm"))
    return _json({"job": ctl.job})


def create_app(controller: BotController, *, token: str, allowed_hosts: set[str] | None) -> web.Application:
    app = web.Application(middlewares=[security], client_max_size=MAX_BODY)
    app[CONTROLLER] = controller
    app[TOKEN] = token
    app[ALLOWED_HOSTS] = allowed_hosts
    app.router.add_get("/", index)
    app.router.add_get("/favicon.ico", no_content)
    app.router.add_get("/api/state", get_state)
    app.router.add_get("/api/history", get_history)
    app.router.add_get("/api/logs", get_logs)
    app.router.add_get("/api/config", get_config)
    app.router.add_put("/api/config", put_config)
    app.router.add_get("/api/credentials", get_credentials)
    app.router.add_put("/api/credentials", put_credentials)
    app.router.add_post("/api/credentials/test", post_test)
    app.router.add_post("/api/bot/start", post_start)
    app.router.add_post("/api/bot/stop", post_stop)
    app.router.add_post("/api/bot/close-all", post_close_all)
    return app


async def serve(config_path: Path, env_file: Path, *, host: str = "127.0.0.1", port: int = 8080,
                token: str | None = None, out: Any = None) -> int:
    """Run the dashboard until SIGINT / SIGTERM; a running bot is then stopped gracefully first."""
    from farmer import cli

    out = out or sys.stdout
    root = logging.getLogger()
    controller = BotController(config_path, env_file)
    root.addHandler(controller.logs)
    root.setLevel(min(root.level or logging.INFO, logging.INFO))
    token = token or os.environ.get("FARMER_DASHBOARD_TOKEN") or secrets.token_urlsafe(24)
    app = create_app(controller, token=token, allowed_hosts=allowed_hosts_for(host))
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    try:
        await site.start()
    except OSError as exc:
        await runner.cleanup()
        print(f"ERROR: cannot listen on {host}:{port}: {exc}", file=sys.stderr)
        return cli.EXIT_ERROR
    shown = "127.0.0.1" if host in ("0.0.0.0", "::", "") else host
    print(f"farmer dashboard: http://{shown}:{port}/#token={token}", file=out)
    print("Bu bağlantı paneli açar ve botu yönetme yetkisi verir; kimseyle paylaşmayın.", file=out)
    if not _loopback(host):
        print("UYARI: panel yerel olmayan bir adreste dinliyor ve trafik şifresiz (HTTP). Mümkünse 127.0.0.1'de "
              "çalıştırıp SSH tüneli kullanın: ssh -L 8080:127.0.0.1:8080 <sunucu>", file=out)
    out.flush()
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    installed = cli._install_stop_signals(loop, stop.set, "stopping the dashboard (the bot stops gracefully)")
    try:
        await stop.wait()
    finally:
        await controller.shutdown()
        cli._remove_stop_signals(loop, installed)
        with contextlib.suppress(Exception):
            await runner.cleanup()
        root.removeHandler(controller.logs)
    return cli.EXIT_OK


__all__ = ["allowed_hosts_for", "create_app", "serve"]
