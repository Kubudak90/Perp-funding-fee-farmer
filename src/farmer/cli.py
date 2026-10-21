"""Command line entry point: ``farmer scan|run|status|close-all|stats|dashboard|keygen``.

- ``scan``      public data only: every Arcus market that maps onto Lighter, funding, carry, spreads, depth, basis.
- ``run``       the engine (``--paper`` = live public data + simulated orders; ``--live`` = real orders).
- ``status``    balances, positions, open orders and the delta per pair (credentials required).
- ``close-all`` flatten every configured pair (maker mode by default, ``--taker`` = immediate IOC).
- ``stats``     volume / fees / funding / PnL report from the SQLite store.
- ``dashboard`` local web UI (``farmer.dashboard``): status, settings, API keys, start / stop / close-all.
- ``keygen``    create + register API keys (wallet keys are read from the environment, never printed).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from farmer import __version__
from farmer.config import AppConfig, ArcusConfig, LighterConfig, MarketConfig, ResolvedMarket, load_config
from farmer.models import BPS, ZERO, OrderBook, Side
from farmer.utils.decimals import fmt
from farmer.utils.logging import setup_logging

log = logging.getLogger("farmer.cli")

DEFAULT_CONFIG = "config.yaml"
EXIT_OK, EXIT_ERROR, EXIT_USAGE = 0, 1, 2

# Common venue error codes -> operator hint (specs: lighter-trading.md, arcus-trading.md)
ERROR_HINTS: dict[str, str] = {
    "20558": "Lighter: restricted jurisdiction. Trading (sendTx / authenticated WS) is geo-blocked from this "
             "IP. Run the bot from a permitted jurisdiction and check Lighter's terms of use.",
    "21110": "Lighter: invalid api_key_index / key not registered. Run `farmer keygen lighter` or check "
             "LIGHTER_API_KEY_INDEX.",
    "21100": "Lighter: no account for this L1 address (deposit first) or wrong LIGHTER_ACCOUNT_INDEX.",
    "20013": "Lighter: invalid auth token (API key does not match the registered public key?).",
    "52002": "Lighter Standard: undocumented trading limit reached. New exposure is blocked for a while; the "
             "venue decides. Reduce volume or wait.",
    "23000": "Lighter: too many requests (Standard = 60 req/min shared by REST and sendTx). Lower request rates.",
    "whitelist": "Arcus mainnet: this address is not whitelisted (invite code / access program required).",
    "403": "Arcus: access denied (address not whitelisted, or API key inactive / for another account index).",
    "429": "Rate limited by the venue. Wait a minute and lower request rates.",
}


# =========================================================================== helpers
def _hint_for(exc: BaseException | str) -> str | None:
    text = str(exc)
    code = str(getattr(exc, "code", "") or "")
    for key, hint in ERROR_HINTS.items():
        if key == code or key in text.lower():
            return hint
    return None


def _print_error(prefix: str, exc: BaseException | str) -> None:
    print(f"ERROR {prefix}: {exc}", file=sys.stderr)
    hint = _hint_for(exc)
    if hint:
        print(f"  hint: {hint}", file=sys.stderr)


def _load(args: argparse.Namespace) -> AppConfig:
    """Load the YAML config (the default path may be absent: built-in defaults then) + env secrets."""
    path = args.config
    explicit = path != DEFAULT_CONFIG
    if path is not None and not Path(path).exists():
        if explicit:
            raise FileNotFoundError(f"config file not found: {path}")
        path = None
    env_file = args.env_file if args.env_file and Path(args.env_file).exists() else None
    cfg = load_config(path, env_file=env_file)
    if getattr(args, "log_level", None):
        cfg = cfg.model_copy(update={"log_level": args.log_level})
    return cfg


def _setup_logging(cfg: AppConfig, *, quiet_console: bool) -> None:
    log_file = Path(cfg.data_dir) / "logs" / "farmer.log"
    try:
        setup_logging(cfg.log_level, log_file, console=sys.stderr if quiet_console else sys.stdout,
                      console_level="WARNING" if quiet_console else None)
    except OSError:  # read-only data dir: console only
        setup_logging(cfg.log_level, None, console=sys.stderr if quiet_console else sys.stdout,
                      console_level="WARNING" if quiet_console else None)


def public_venue_configs(cfg: AppConfig) -> tuple[ArcusConfig, LighterConfig]:
    """Credential-free copies of the venue configs (public data only; never starts private streams)."""
    a = cfg.arcus.model_copy(update={"address": None, "api_private_key": None, "dead_mans_switch_s": 0.0})
    lt = cfg.lighter.model_copy(update={"api_private_key": None, "account_index": None, "api_key_index": None,
                                        "verify_api_key": False})
    return a, lt


def build_venues(cfg: AppConfig, *, paper: bool, arcus_cfg: ArcusConfig | None = None,
                 lighter_cfg: LighterConfig | None = None) -> dict[str, Any]:
    """``{"arcus": Exchange, "lighter": Exchange}``. Paper wraps credential-free public adapters."""
    from farmer.exchanges.arcus import ArcusExchange
    from farmer.exchanges.lighter import LighterExchange

    if paper:
        from farmer.exchanges.paper.client import PaperExchange

        a, lt = public_venue_configs(cfg)
        return {"arcus": PaperExchange(ArcusExchange(a), cfg.paper),
                "lighter": PaperExchange(LighterExchange(lt), cfg.paper)}
    return {"arcus": ArcusExchange(arcus_cfg or cfg.arcus), "lighter": LighterExchange(lighter_cfg or cfg.lighter)}


def store_path(cfg: AppConfig, paper: bool) -> Path:
    return Path(cfg.data_dir) / ("farmer-paper.db" if paper else "farmer.db")


def _missing_credentials(cfg: AppConfig) -> list[str]:
    out = []
    if not cfg.arcus.has_credentials:
        out.append("Arcus (ARCUS_ADDRESS, ARCUS_API_PRIVATE_KEY)")
    if not cfg.lighter.has_credentials:
        out.append("Lighter (LIGHTER_API_PRIVATE_KEY, LIGHTER_ACCOUNT_INDEX, LIGHTER_API_KEY_INDEX)")
    return out


def _confirm(prompt: str, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        print(f"{prompt}\nRefusing without a terminal; pass --yes to confirm.", file=sys.stderr)
        return False
    try:
        ans = input(f"{prompt} Type 'yes' to continue: ")
    except EOFError:
        return False
    return ans.strip().lower() == "yes"


class RunLock:
    """Advisory per-database lock so ``close-all`` never fights a running engine (and two engines never share
    one API key / nonce stream)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh: Any = None

    def acquire(self) -> bool:
        try:
            import fcntl
        except ImportError:  # pragma: no cover - non-POSIX: no locking
            return True
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "a+")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return False
        fh.seek(0)
        fh.truncate()
        fh.write(f"{os.getpid()}\n")
        fh.flush()
        self._fh = fh
        return True

    def holder(self) -> str:
        try:
            return self.path.read_text().strip() or "?"
        except OSError:
            return "?"

    def release(self) -> None:
        if self._fh is not None:
            with contextlib.suppress(Exception):
                self._fh.close()
            self._fh = None


def _install_stop_signals(loop: asyncio.AbstractEventLoop, on_first: Callable[[], None], what: str) -> list[int]:
    """SIGINT / SIGTERM / SIGHUP -> ``on_first()`` (graceful); the second only warns, the third exits at once
    (resting quotes are then left to the venue's dead man's switch). Returns the signals to remove afterwards.
    SIGHUP is left alone when it is ignored (``nohup``)."""
    seen = 0

    def on_signal(signame: str) -> None:
        nonlocal seen
        seen += 1
        if seen == 1:
            log.warning("%s received: %s", signame, what)
            on_first()
        elif seen < 3:
            log.warning("%s again: shutdown in progress; once more forces an immediate exit (resting quotes "
                        "are then left to the venue's dead man's switch)", signame)
        else:
            log.error("forced exit")
            os._exit(130)

    sigs = [signal.SIGINT, signal.SIGTERM]
    hup = getattr(signal, "SIGHUP", None)
    if hup is not None and signal.getsignal(hup) is not signal.SIG_IGN:
        sigs.append(hup)
    installed = []
    for sig in sigs:
        try:
            loop.add_signal_handler(sig, on_signal, sig.name)
            installed.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):  # pragma: no cover - non-POSIX / not main thread
            pass
    return installed


def _remove_stop_signals(loop: asyncio.AbstractEventLoop, installed: list[int]) -> None:
    for sig in installed:
        with contextlib.suppress(Exception):
            loop.remove_signal_handler(sig)


async def _close_quietly(*venues: Any) -> None:
    for v in venues:
        if v is None:
            continue
        try:
            await asyncio.wait_for(v.close(), 15.0)
        except Exception as exc:  # pragma: no cover - best effort
            log.debug("%s close failed: %r", getattr(v, "name", "?"), exc)


def _all_markets(cfg: AppConfig, maker: Any) -> list[ResolvedMarket]:
    """Configured markets first (their overrides win), then every other maker-venue market."""
    out = list(cfg.enabled_markets())
    seen = {m.symbol for m in out} | {(m.arcus_symbol or "").upper() for m in out}
    for key in sorted(maker.markets):
        if key not in seen:
            out.append(cfg.strategy.for_market(MarketConfig(symbol=key)))
    return out


# =========================================================================== scan
@dataclass
class ScanRow:
    symbol: str
    maker_symbol: str
    taker_symbol: str
    configured: bool
    rate_m: Decimal | None = None
    rate_t: Decimal | None = None
    carry_apr: Decimal | None = None
    direction: int | None = None
    spread_m: Decimal | None = None
    spread_t: Decimal | None = None
    depth_t: Decimal | None = None
    basis_bps: Decimal | None = None
    cost_bps: Decimal | None = None
    kind: str = "CRYPTO"
    open: bool = True


def tob_depth_usd(book: OrderBook | None) -> Decimal | None:
    """min(best bid size x price, best ask size x price)."""
    if book is None or not book.bids or not book.asks:
        return None
    return min(book.bids[0].size * book.bids[0].price, book.asks[0].size * book.asks[0].price)


def round_trip_cost_bps(maker_book: OrderBook | None, taker_book: OrderBook | None, notional: Decimal) -> Decimal | None:
    """Style-A cost per round trip (bps of one leg) before adverse selection:
    ``impact_L(N, buy) + impact_L(N, sell) - s_A`` with impacts measured against the Lighter mid."""
    if maker_book is None or taker_book is None:
        return None
    mid_t, s_m = taker_book.mid, maker_book.spread_bps
    if mid_t is None or mid_t <= 0 or s_m is None:
        return None
    size = notional / mid_t
    buy, _ = taker_book.vwap(Side.BUY, size)
    sell, _ = taker_book.vwap(Side.SELL, size)
    if buy is None or sell is None:
        return None
    return (buy / mid_t - 1) / BPS + (1 - sell / mid_t) / BPS - s_m


def _f(x: Decimal | None, spec: str, scale: Decimal | int = 1) -> str:
    if x is None:
        return "-"
    return format(x * scale, spec)


def format_scan_table(rows: list[ScanRow], *, clip: Decimal) -> str:
    head = (f"{'pair':<15} {'arcus':>9} {'lighter':>9} {'carry':>7} {'best direction':<15} {'A spr':>6} {'L spr':>6} "
            f"{'L ToB $':>9} {'basis':>7} {'RT@$' + fmt(clip, 0):>9} {'type':<10}")
    sub = (f"{'':<15} {'%/h':>9} {'%/h':>9} {'APR%':>7} {'':<15} {'bps':>6} {'bps':>6} {'':>9} {'bps':>7} "
           f"{'bps':>9} {'':<10}")
    lines = [head, sub, "-" * len(head)]
    for r in rows:
        name = ("*" if r.configured else " ") + (r.symbol if r.maker_symbol == r.taker_symbol
                                                 else f"{r.maker_symbol}/{r.taker_symbol}")
        dirs = {1: "long A/short L", -1: "short A/long L"}.get(r.direction or 0, "-")
        kind = r.kind if r.kind == "CRYPTO" else f"RWA {'open' if r.open else 'CLOSED'}"
        lines.append(
            f"{name[:15]:<15} {_f(r.rate_m, '.5f', 100):>9} {_f(r.rate_t, '.5f', 100):>9} "
            f"{_f(r.carry_apr, '.2f', 100):>7} {dirs:<15} {_f(r.spread_m, '.2f'):>6} {_f(r.spread_t, '.2f'):>6} "
            f"{_f(r.depth_t, ',.0f'):>9} {_f(r.basis_bps, '.1f'):>7} {_f(r.cost_bps, '.2f'):>9} {kind:<10}")
    return "\n".join(lines)


async def _wait_books(ex: Any, symbols: list[str], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(ex.get_book(s) is not None for s in symbols):
            return
        await asyncio.sleep(0.25)


async def scan(cfg: AppConfig, *, top: int = 0, max_books: int = 60, wait_s: float = 8.0, rest_fallback: int = 10,
               show_skipped: bool = False, out=None) -> int:
    from farmer.exchanges.arcus import ArcusExchange
    from farmer.exchanges.lighter import LighterExchange
    from farmer.strategy.funding import FundingSnapshot
    from farmer.strategy.mapping import resolve_pairs

    out = out or sys.stdout
    a_cfg, l_cfg = public_venue_configs(cfg)
    arcus, lighter = ArcusExchange(a_cfg), LighterExchange(l_cfg)
    try:
        res = await asyncio.gather(arcus.connect(), lighter.connect(), return_exceptions=True)
        for v, r in zip((arcus, lighter), res, strict=True):
            if isinstance(r, BaseException):
                _print_error(f"{v.name} connect failed", r)
                return EXIT_ERROR
        markets = _all_markets(cfg, arcus)
        configured = {m.symbol for m in cfg.enabled_markets()}
        cands, _ = await resolve_pairs(arcus, lighter, markets, check_prices=False)

        # Lighter books over the readonly WS (no REST budget); configured markets first
        cands.sort(key=lambda p: (p.symbol not in configured, p.symbol))
        sample = cands[:max(0, max_books)]
        if sample:
            await lighter.subscribe_books([p.taker_symbol for p in sample])

        sem = asyncio.Semaphore(8)
        a_books: dict[str, OrderBook] = {}

        async def arcus_book(sym: str) -> None:
            async with sem:
                try:
                    a_books[sym] = await asyncio.wait_for(arcus.fetch_book(sym, depth=20), 10.0)
                except Exception as exc:
                    log.debug("arcus book %s failed: %r", sym, exc)

        await asyncio.gather(_wait_books(lighter, [p.taker_symbol for p in sample], wait_s),
                             *(arcus_book(p.maker_symbol) for p in cands))
        l_books: dict[str, OrderBook] = {}
        fallback = 0
        for p in sample:
            b = lighter.get_book(p.taker_symbol)
            if b is None and fallback < rest_fallback:
                fallback += 1
                with contextlib.suppress(Exception):
                    b = await asyncio.wait_for(lighter.fetch_book(p.taker_symbol, depth=20), 10.0)
            if b is not None:
                l_books[p.taker_symbol] = b

        pairs, skipped = await resolve_pairs(arcus, lighter, markets, check_prices=True)
        fa, fl = await asyncio.gather(arcus.fetch_funding([p.maker_symbol for p in pairs]),
                                      lighter.fetch_funding([p.taker_symbol for p in pairs]))
        rows: list[ScanRow] = []
        for p in pairs:
            r = ScanRow(symbol=p.symbol, maker_symbol=p.maker_symbol, taker_symbol=p.taker_symbol,
                        configured=p.symbol in configured)
            m_f, t_f = fa.get(p.maker_symbol), fl.get(p.taker_symbol)
            r.rate_m = m_f.rate_hourly if m_f else None
            r.rate_t = t_f.rate_hourly if t_f else None
            if m_f and t_f:
                snap = FundingSnapshot(p.symbol, m_f, t_f)
                r.direction = snap.best_direction
                r.carry_apr = snap.apr(r.direction)
            mb, tb = a_books.get(p.maker_symbol), l_books.get(p.taker_symbol)
            r.spread_m = mb.spread_bps if mb else None
            r.spread_t = tb.spread_bps if tb else None
            r.depth_t = tob_depth_usd(tb)
            if mb is not None and tb is not None and mb.mid and tb.mid:
                r.basis_bps = (mb.mid / tb.mid - 1) / BPS
            r.cost_bps = round_trip_cost_bps(mb, tb, p.cfg.order_notional)
            r.kind = p.maker_market.category.value
            r.open = bool(arcus.market_open(p.maker_symbol) and lighter.market_open(p.taker_symbol))
            rows.append(r)
        rows.sort(key=lambda r: (r.carry_apr is None, -(r.carry_apr or ZERO)))
        shown = rows[:top] if top and top > 0 else rows

        n_on = sum(1 for m in arcus.markets.values() if m.active)
        n_l = sum(1 for m in lighter.markets.values() if m.active)
        print(f"farmer scan {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())} | Arcus {len(arcus.markets)} "
              f"markets ({n_on} online) | Lighter {len(lighter.markets)} perps ({n_l} active) | "
              f"{len(pairs)} hedgeable pairs | Lighter books sampled {len(l_books)}/{len(cands)}", file=out)
        print(format_scan_table(shown, clip=cfg.strategy.order_notional), file=out)
        print("\n* = configured market. carry = funding APR of the best direction on one leg's notional (forecast, "
              "changes hourly).\nRT = style-A round-trip cost estimate for one clip before adverse selection "
              "(Lighter impact buy+sell - Arcus spread).\nbasis = Arcus mid / Lighter mid - 1 (crypto ~ -10 bps: "
              "Lighter's index is USDT-based).", file=out)
        real_skips = [s for s in skipped if "duplicate of" not in s]
        if show_skipped and real_skips:
            print(f"\nskipped ({len(real_skips)}):", file=out)
            for s in real_skips:
                print(f"  {s}", file=out)
        elif real_skips:
            print(f"\n{len(real_skips)} Arcus markets not hedgeable (use --skipped to list).", file=out)
        return EXIT_OK
    finally:
        await _close_quietly(arcus, lighter)


# =========================================================================== run
class BotSession:
    """One engine run that a caller starts and stops programmatically (``farmer run`` and the dashboard).

    ``acquire()`` takes the per-database run lock; ``run()`` builds the store, notifier, risk manager, engine and
    stats reporter, trades until ``request_stop()`` (or ``duration``), always cleans up and releases the lock.
    """

    def __init__(self, cfg: AppConfig, *, paper: bool, venues: dict[str, Any] | None = None) -> None:
        self.cfg = cfg.model_copy(update={"mode": "paper" if paper else "live"})
        self.paper = paper
        self.venues: dict[str, Any] = venues or {}
        self.lock = RunLock(store_path(self.cfg, paper).with_suffix(".lock"))
        self.engine: Any = None
        self.stats: Any = None
        self.store: Any = None
        self.error: str | None = None
        self._stop_requested = False

    def acquire(self) -> bool:
        return self.lock.acquire()

    def request_stop(self) -> None:
        """Graceful stop (cancel quotes, flush hedges); safe before ``run()`` started and from a signal handler."""
        self._stop_requested = True
        if self.engine is not None:
            self.engine.request_stop()

    async def run(self, duration: float | None = None) -> int:
        from farmer.monitoring.notifier import make_notifier
        from farmer.monitoring.stats import StatsReporter
        from farmer.persistence.store import Store
        from farmer.strategy.engine import Engine

        cfg, paper = self.cfg, self.paper
        try:
            venues = self.venues = self.venues or build_venues(cfg, paper=paper)
            store = self.store = Store(store_path(cfg, paper), paper=paper)
        except BaseException:
            self.lock.release()
            raise
        mon = cfg.monitoring
        notifier = make_notifier(mon.telegram_bot_token, mon.telegram_chat_id)
        risk = make_risk(cfg, venues, store, notifier)
        engine = self.engine = Engine(cfg, venues, store, notifier, risk)
        # margin ratio at reduce_margin_ratio: the risk manager halts and flattens every pair with reduce-only IOCs
        risk.set_reducer(lambda: engine.close_all("taker", timeout_s=cfg.strategy.close_on_exit_timeout_s))
        stats = self.stats = StatsReporter(store, notifier, engine.status, venues, interval_s=mon.report_interval_s,
                                           notify_every=mon.notify_every, funding_interval_s=mon.funding_collect_s,
                                           funding_lookback_s=mon.funding_lookback_s)
        if self._stop_requested:
            engine.request_stop()

        async def stats_loop() -> None:
            while not engine.started:
                await asyncio.sleep(0.5)
            await stats.run()

        async def stop_after(seconds: float) -> None:
            await asyncio.sleep(seconds)
            log.info("--duration %.0fs elapsed: stopping", seconds)
            engine.request_stop()

        helpers = [asyncio.create_task(stats_loop(), name="farmer:stats")]
        if duration:
            helpers.append(asyncio.create_task(stop_after(float(duration)), name="farmer:duration"))
        rc = EXIT_OK
        log.info("farmer %s starting in %s mode (pairs: %s)", __version__, cfg.mode.upper(),
                 ", ".join(m.symbol for m in cfg.enabled_markets()))
        try:
            await engine.run()
        except Exception as exc:
            rc = EXIT_ERROR
            self.error = str(exc)
            log.error("engine stopped with an error: %s", exc)
            hint = _hint_for(exc)
            if hint:
                self.error += f" (hint: {hint})"
                log.error("hint: %s", hint)
        finally:
            stats.stop()
            for t in helpers:
                t.cancel()
            await asyncio.gather(*helpers, return_exceptions=True)
            if engine.started:
                with contextlib.suppress(Exception):
                    await stats.report_once()
            with contextlib.suppress(Exception):
                await risk.drain_notifications()
            with contextlib.suppress(Exception):
                await notifier.close()
            store.close()
            self.lock.release()
        return rc


async def run_engine(cfg: AppConfig, *, paper: bool, duration: float | None = None,
                     venues: dict[str, Any] | None = None) -> int:
    """Build venues/store/risk/engine/stats and trade until a signal or ``duration`` elapses."""
    session = BotSession(cfg, paper=paper, venues=venues)
    if not session.acquire():
        print(f"ERROR: another farmer process (pid {session.lock.holder()}) is running on "
              f"{store_path(session.cfg, paper)}", file=sys.stderr)
        return EXIT_ERROR
    loop = asyncio.get_running_loop()
    installed = _install_stop_signals(loop, session.request_stop, "graceful shutdown (cancel quotes, flush hedges)")
    try:
        return await session.run(duration)
    finally:
        _remove_stop_signals(loop, installed)


def make_risk(cfg: AppConfig, venues: dict[str, Any], store: Any, notifier: Any) -> Any:
    """RiskManager with the per-pair leverage (free-collateral check)."""
    from farmer.risk.manager import RiskManager

    return RiskManager(cfg.risk, venues, store, notifier, interval_s=cfg.risk.check_interval_s,
                       leverage={m.symbol: m.leverage for m in cfg.enabled_markets()},
                       default_leverage=cfg.strategy.leverage)


# =========================================================================== status
async def status(cfg: AppConfig, out=None) -> int:
    from farmer.strategy.mapping import resolve_pairs

    out = out or sys.stdout
    missing = _missing_credentials(cfg)
    if len(missing) == 2:
        print("ERROR: no credentials configured: " + "; ".join(missing) + ". Fill in .env (see .env.example).",
              file=sys.stderr)
        return EXIT_USAGE
    # status never arms the Arcus dead man's switch (a running engine owns it)
    a_cfg = cfg.arcus.model_copy(update={"dead_mans_switch_s": 0.0})
    venues = build_venues(cfg, paper=False, arcus_cfg=a_cfg)
    usable: dict[str, Any] = {}
    rc = EXIT_OK
    try:
        names = [n for n, v in venues.items() if v.has_credentials]
        for n in venues:
            if n not in names:
                print(f"{n}: no credentials (skipped)", file=out)
        res = await asyncio.gather(*(venues[n].connect() for n in names), return_exceptions=True)
        for n, r in zip(names, res, strict=True):
            if isinstance(r, BaseException):
                _print_error(f"{n} connect failed", r)
                rc = EXIT_ERROR
            else:
                usable[n] = venues[n]
        positions: dict[str, dict] = {}
        for n, ex in usable.items():
            print(f"\n== {n} ==", file=out)
            try:
                bal = await asyncio.wait_for(ex.fetch_balance(), 20.0)
                print(f"equity {fmt(bal.equity, 2)}  available {fmt(bal.available, 2)}  margin used "
                      f"{fmt(bal.margin_used, 2)}  maintenance {fmt(bal.maintenance_margin, 2)}  margin ratio "
                      f"{bal.margin_ratio:.4f}", file=out)
            except Exception as exc:
                _print_error(f"{n} balance", exc)
                rc = EXIT_ERROR
            try:
                pos = await asyncio.wait_for(ex.fetch_positions(), 20.0)
                positions[n] = pos
                if not pos:
                    print("positions: none", file=out)
                for s, p in sorted(pos.items()):
                    print(f"position {s:<10} {fmt(p.size):>14} entry {fmt(p.entry_price)} mark {fmt(p.mark_price)} "
                          f"uPnL {fmt(p.unrealized_pnl, 2)} liq {fmt(p.liquidation_price)}", file=out)
            except Exception as exc:
                _print_error(f"{n} positions", exc)
                rc = EXIT_ERROR
            try:
                orders = await asyncio.wait_for(ex.fetch_open_orders(), 20.0)
                print(f"open orders: {len(orders)}", file=out)
                for o in orders[:50]:
                    print(f"  {o.symbol:<10} {o.side.value:<4} {fmt(o.remaining)} @ {fmt(o.price)} "
                          f"{o.status.value} id={o.order_id} cid={o.client_id}", file=out)
            except Exception as exc:
                _print_error(f"{n} open orders", exc)
                rc = EXIT_ERROR
            ok, why = ex.can_increase_exposure()
            if not ok:
                print(f"NOTE: {n} blocks new exposure: {why}", file=out)
        maker_n, taker_n = cfg.strategy.maker_venue, cfg.strategy.taker_venue
        if maker_n in positions and taker_n in positions:
            pairs, skipped = await resolve_pairs(usable[maker_n], usable[taker_n], cfg.enabled_markets(),
                                                 check_prices=False, allow_reduce_only=True)
            print("\n== delta per pair (maker + taker, base units) ==", file=out)
            for p in pairs:
                mp = positions[maker_n].get(p.maker_symbol)
                tp = positions[taker_n].get(p.taker_symbol)
                m_sz = mp.size if mp else ZERO
                t_sz = tp.size if tp else ZERO
                delta = m_sz + t_sz
                px = next((x for x in (mp and mp.mark_price, tp and tp.mark_price, mp and mp.entry_price,
                                       tp and tp.entry_price) if x), None)
                hedged = abs(delta) < p.min_size(px) if px else delta == 0
                usd = f" (${fmt(abs(delta) * px, 2)})" if px and delta else ""
                print(f"{p.symbol:<10} maker {fmt(m_sz):>14} taker {fmt(t_sz):>14} delta {fmt(delta):>12}{usd}"
                      f"{'' if hedged else '  <-- UNHEDGED'}"
                      f"{'  (reduce-only market: close with farmer close-all)' if p.close_only and (m_sz or t_sz) else ''}",
                      file=out)
            for s in skipped:
                print(f"  (skipped {s})", file=out)
            known = {p.maker_symbol for p in pairs} | {p.taker_symbol for p in pairs}
            extra = [f"{n}:{s}" for n in (maker_n, taker_n) for s in positions[n] if s not in known]
            if extra:
                print("positions outside the configured pairs: " + ", ".join(extra), file=out)
        return rc
    finally:
        await _close_quietly(*venues.values())


# =========================================================================== close-all
async def _close_with_fallback(engine: Any, risk: Any, mode: str, timeout_s: float, out: Any) -> tuple[str, dict]:
    """``engine.close_all(mode)``; a maker close whose risk manager halts (circuit breaker, margin, price move) can
    never quote again, so it switches to taker (IOC) mode for the remaining time instead of waiting out the timeout
    (and starts in taker mode when the risk manager is halted already)."""
    deadline = time.monotonic() + max(0.0, timeout_s)
    if mode == "maker" and getattr(risk, "halted", False):
        print(f"risk is halted ({risk.halt_reason}); closing in taker (IOC) mode", file=out)
        mode = "taker"
    task = asyncio.ensure_future(engine.close_all(mode, timeout_s=timeout_s))
    try:
        while mode == "maker" and not task.done():
            await asyncio.wait({task}, timeout=0.25)
            if task.done() or not getattr(risk, "halted", False):
                continue
            print(f"risk halted during the maker close ({risk.halt_reason}); switching to taker (IOC) mode", file=out)
            task.cancel()
            await asyncio.wait({task})
            mode = "taker"
            task = asyncio.ensure_future(engine.close_all(mode, timeout_s=max(30.0, deadline - time.monotonic())))
        return mode, await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.wait({task})


async def _unmanaged_positions(engine: Any, cfg: AppConfig) -> tuple[list[str], list[str], list[str]]:
    """Positions close-all did not handle: (configured markets without a closed pair, positions outside the
    configured markets, read errors)."""
    from farmer.strategy.mapping import candidate_symbols

    maker, taker = engine.maker, engine.taker
    managed = {(maker.name, p.maker_symbol) for p in engine.pairs if p.symbol in engine.hedgers}
    managed |= {(taker.name, p.taker_symbol) for p in engine.pairs if p.symbol in engine.hedgers}
    configured: set[str] = set()
    for m in cfg.enabled_markets():
        configured.update(candidate_symbols(m.symbol))
        configured.update(o.strip().upper() for o in (m.arcus_symbol, m.lighter_symbol) if o)
    unmanaged: list[str] = []
    other: list[str] = []
    errors: list[str] = []
    for ex in dict.fromkeys([maker, taker]):
        try:
            pos = await asyncio.wait_for(ex.fetch_positions(), 20.0)
        except Exception as exc:
            errors.append(f"{ex.name}: {exc!r}")
            continue
        for sym, p in sorted(pos.items()):
            if p.size == 0 or (ex.name, sym) in managed:
                continue
            (unmanaged if sym.upper() in configured else other).append(f"{ex.name}:{sym} {fmt(p.size)}")
    return unmanaged, other, errors


async def close_all(cfg: AppConfig, *, taker: bool, timeout_s: float, out=None, signals: bool = True) -> int:
    """Flatten every configured pair (see ``Engine.close_all``). ``signals=False``: the caller owns the signal
    handlers (the dashboard) and interrupts by cancelling the task; the shutdown path is the same."""
    from farmer.monitoring.notifier import make_notifier
    from farmer.persistence.store import Store
    from farmer.strategy.engine import Engine

    out = out or sys.stdout
    missing = _missing_credentials(cfg)
    if missing:
        print("ERROR: close-all needs credentials on both venues; missing: " + "; ".join(missing), file=sys.stderr)
        return EXIT_USAGE
    cfg = cfg.model_copy(update={
        "mode": "live",
        "strategy": cfg.strategy.model_copy(update={"set_leverage_on_start": False}),
        # today's loss / a stored price-move reference must not re-halt the closing engine (a halted maker close
        # never quotes, and close-all is the reduction: the guard has no reducer here); references of the pairs it
        # leaves flat are still dropped, the others stay for the next `farmer run`
        "risk": cfg.risk.model_copy(update={"close_on_exit": False, "max_daily_loss": Decimal(0),
                                            "auto_close_move_frac": Decimal(0)}),
    })
    lock = RunLock(store_path(cfg, False).with_suffix(".lock"))
    if not lock.acquire():
        print(f"ERROR: a farmer engine (pid {lock.holder()}) is running. Stop it first (Ctrl+C / SIGTERM, or set "
              "risk.close_on_exit: true); two engines would fight over the same positions.", file=sys.stderr)
        return EXIT_ERROR
    venues = build_venues(cfg, paper=False)
    store = Store(store_path(cfg, False))
    notifier = make_notifier(cfg.monitoring.telegram_bot_token, cfg.monitoring.telegram_chat_id)
    risk = make_risk(cfg, venues, store, notifier)
    # markets that only accept reduce-only orders (e.g. Lighter force_reduce_only) are closed too, with IOCs
    engine = Engine(cfg, venues, store, notifier, risk, include_reduce_only=True)
    mode = "taker" if taker else "maker"
    rc = EXIT_OK
    loop = asyncio.get_running_loop()
    main = asyncio.current_task()
    closing = True

    def interrupt() -> None:
        if closing and main is not None:
            main.cancel()

    # SIGTERM (systemd / docker / timeout) or SIGHUP must not kill the process mid-flatten: cancel the close, then
    # the finally below cancels quotes, flushes the hedgers and closes the venues (signals stay handled meanwhile)
    installed = (_install_stop_signals(loop, interrupt, "interrupting close-all (cancel quotes, flush hedges)")
                 if signals else [])
    try:
        try:
            mode, result = await _close_with_fallback(engine, risk, mode, timeout_s, out)
            print(f"close-all ({mode}) result:", file=out)
            for sym, r in result.items():
                print(f"  {sym:<10} maker {fmt(r['maker_pos']):>14} taker {fmt(r['taker_pos']):>14} "
                      f"{'FLAT' if r['flat'] else 'NOT FLAT'}", file=out)
            if not all(r["flat"] for r in result.values()):
                rc = EXIT_ERROR
                print("Not flat within the timeout." + (" Retry with --taker to close immediately with IOC orders."
                                                        if mode != "taker" else ""), file=out)
        except Exception as exc:
            _print_error("close-all failed", exc)
            rc = EXIT_ERROR
        if engine.pairs or engine.skipped:  # markets were mapped: report positions no pair could close
            unmanaged, other, errors = await _unmanaged_positions(engine, cfg)
            for u in unmanaged:
                print(f"  {u:<24} NOT FLAT (UNMANAGED: no tradeable pair was mapped; close it on the venue)", file=out)
            if unmanaged:
                rc = EXIT_ERROR
                for why in engine.skipped:
                    print(f"  skipped: {why}", file=out)
            if other:
                print("note: positions outside the configured markets were not touched: " + ", ".join(other),
                      file=out)
            for e in errors:
                print(f"WARNING: could not verify the remaining positions on {e}", file=out)
    except asyncio.CancelledError:
        if main is not None:
            main.uncancel()
        rc = EXIT_ERROR
        print("close-all interrupted: positions may NOT be flat (check `farmer status`).", file=out)
    finally:
        closing = False
        with contextlib.suppress(Exception):
            await engine.shutdown()
        _remove_stop_signals(loop, installed)
        with contextlib.suppress(Exception):
            await risk.drain_notifications()
        with contextlib.suppress(Exception):
            await notifier.close()
        store.close()
        lock.release()
    return rc


# =========================================================================== stats
def _role_summary(store: Any, since: float) -> dict[tuple[str, str], tuple[int, Decimal]]:
    out: dict[tuple[str, str], tuple[int, Decimal]] = {}
    for venue, role, notional in store._db.execute(
            "SELECT venue, role, notional FROM fills WHERE ts>=? AND paper=?", (since, store.paper)):
        n, v = out.get((venue, role), (0, ZERO))
        out[(venue, role)] = (n + 1, v + Decimal(notional))
    return out


async def _public_marks(cfg: AppConfig, symbols: set[str]) -> dict[str, Decimal]:
    """Mark prices from Arcus public ``/v1/markets`` (Lighter-style ``1000X`` keys map to Arcus ``KX``)."""
    if not symbols:
        return {}
    from farmer.exchanges.arcus.parse import canonical_symbol, funding_info
    from farmer.exchanges.arcus.rest import ArcusRest
    from farmer.strategy.mapping import candidate_symbols

    rest = ArcusRest(cfg.arcus.rest_url, timeout_s=cfg.arcus.request_timeout_s)
    await rest.start()
    try:
        rows = await asyncio.wait_for(rest.markets(), 15.0)
    finally:
        await rest.close()
    by_sym: dict[str, Decimal] = {}
    for row in rows:
        with contextlib.suppress(Exception):
            fi = funding_info(row)
            px = fi.mark_price or fi.index_price
            if px:
                by_sym[canonical_symbol(row)] = px
    marks = {}
    for s in symbols:
        for c in candidate_symbols(s):
            if c in by_sym:
                marks[s] = by_sym[c]
                break
    return marks


async def stats_report(cfg: AppConfig, *, paper: bool, since_hours: float, use_marks: bool = True, out=None) -> int:
    from farmer.monitoring.stats import net_base, pnl_from_summary
    from farmer.persistence.store import Store

    out = out or sys.stdout
    path = store_path(cfg, paper)
    if not path.exists():
        print(f"no database at {path} (run the bot first)", file=sys.stderr)
        return EXIT_ERROR
    store = Store(path, paper=paper)
    try:
        now = time.time()
        since = now - since_hours * 3600 if since_hours > 0 else 0.0
        summ = store.summary(since)
        span = f"last {since_hours:g} h" if since_hours > 0 else "all time"
        print(f"farmer stats ({'paper' if paper else 'live'}, {span}) from {path}", file=out)
        if not summ:
            print("no fills or funding in this window", file=out)
            return EXIT_OK
        print(f"{'venue':<9} {'volume $':>14} {'maker $':>14} {'taker $':>14} {'fills':>7} {'fees $':>10} "
              f"{'funding $':>10} {'cash flow $':>14}", file=out)
        for v, s in sorted(summ.items()):
            print(f"{v:<9} {fmt(s.volume, 2):>14} {fmt(s.maker_volume, 2):>14} {fmt(s.taker_volume, 2):>14} "
                  f"{s.fills:>7} {fmt(s.fees, 4):>10} {fmt(s.funding, 4):>10} {fmt(s.cash_flow, 2):>14}", file=out)
        roles = _role_summary(store, since)
        if roles:
            print("fills by role: " + ", ".join(f"{v}/{r} {n} (${fmt(x, 0)})" for (v, r), (n, x) in sorted(roles.items())),
                  file=out)
        net = {s: q for s, q in net_base(summ).items() if q != 0}
        marks: dict[str, Decimal] = {}
        if net and use_marks:
            try:
                marks = await _public_marks(cfg, set(net))
            except Exception as exc:
                print(f"(mark prices unavailable: {exc!r})", file=out)
        pnl = pnl_from_summary(summ, marks)
        if net:
            print("net base traded (both venues): " + ", ".join(f"{s} {fmt(q)}" for s, q in sorted(net.items())),
                  file=out)
        missing = sorted(s for s in net if s not in marks)
        vol = sum((s.volume for s in summ.values()), ZERO)
        fees = sum((s.fees for s in summ.values()), ZERO)
        fund = sum((s.funding for s in summ.values()), ZERO)
        maker_n = cfg.strategy.maker_venue
        mvol = summ[maker_n].volume if maker_n in summ else ZERO
        per_m = (pnl / vol * Decimal(1_000_000)) if vol > 0 else None
        per_mm = (pnl / mvol * Decimal(1_000_000)) if mvol > 0 else None
        per = []
        if per_mm is not None:
            per.append(f"${fmt(per_mm, 2)} per $1M {maker_n} volume")
        if per_m is not None:
            per.append(f"${fmt(per_m, 2)} per $1M volume of both venues")
        print(f"total volume ${fmt(vol, 2)} | fees ${fmt(fees, 4)} | funding ${fmt(fund, 4)} | PnL est ${fmt(pnl, 4)}"
              + (f" ({'; '.join(per)})" if per else "")
              + (f" | no mark for {', '.join(missing)} (excluded)" if missing else ""), file=out)
        if net:
            print("(open net base is valued at one common public mark; the cross-venue basis of open legs shows "
                  "until they are closed)", file=out)
        kinds: dict[str, int] = {}
        for ts, kind, _data in store.recent_events(limit=5000):
            if ts >= since:
                kinds[kind] = kinds.get(kind, 0) + 1
        if kinds:
            print("events: " + ", ".join(f"{k} {n}" for k, n in sorted(kinds.items())), file=out)
        return EXIT_OK
    finally:
        store.close()


# =========================================================================== keygen
UNCONFIRMED_NOTE = ("WARNING: the registration was sent but NOT confirmed; it may still take effect. The API private "
                    "key below is shown ONCE so it is not lost: verify with `farmer status` (or the venue) before "
                    "relying on it, and discard it if the registration did not happen.")


def _arcus_key_rejected(exc: BaseException) -> bool:
    """True when ``exc`` proves the Arcus key was NOT registered (local validation or an HTTP 4xx rejection)."""
    from farmer.exchanges.arcus.rest import ArcusHTTPError

    if isinstance(exc, ArcusHTTPError):
        return 400 <= int(exc.status) < 500 and int(exc.status) != 429
    return isinstance(exc, (ValueError, RuntimeError)) and not isinstance(exc, TimeoutError)


def _lighter_key_rejected(exc: BaseException, lk: Any) -> bool:
    """True when ``exc`` proves the ChangePubKey tx was never accepted (signing failed or the venue rejected it)."""
    msg = str(exc)
    restricted = getattr(lk, "RESTRICTED_MESSAGE", None)
    return msg.startswith(("sign ChangePubKey failed", "ChangePubKey rejected")) or (bool(restricted)
                                                                                     and msg == restricted)


async def keygen_arcus(cfg: AppConfig, args: argparse.Namespace, out=None) -> int:
    from farmer.exchanges.arcus import keys as ak

    out = out or sys.stdout
    account_index = cfg.arcus.account_index if args.account_index is None else int(args.account_index)
    if args.generate_only:
        priv, pub = ak.generate_ed25519_keypair()
        print(f"Generated Arcus Ed25519 API key (NOT registered). Public key: {pub}", file=out)
        print("WARNING: the private key below is shown ONCE. Store it in .env only; never share it.", file=out)
        print(f"ARCUS_API_PRIVATE_KEY={priv}", file=out)
        print("Register it with `farmer keygen arcus --api-private-key-env ...` or in the Arcus web app.", file=out)
        return EXIT_OK
    wallet = os.environ.get("ARCUS_WALLET_PRIVATE_KEY", "").strip()
    if not wallet:
        print("ERROR: set ARCUS_WALLET_PRIVATE_KEY (the master ETH wallet key) in the environment/.env. It is only "
              "used locally for the EIP-712 signature and never printed or sent.", file=sys.stderr)
        return EXIT_USAGE
    from eth_account import Account

    try:
        address = Account.from_key(wallet).address
    except Exception:
        print("ERROR: ARCUS_WALLET_PRIVATE_KEY is not a valid private key", file=sys.stderr)
        return EXIT_USAGE
    existing = os.environ.get(args.api_private_key_env or "", "").strip() if args.api_private_key_env else None
    print(f"Arcus {cfg.arcus.rest_url} chainId {cfg.arcus.chain_id}: register an API key for {address} "
          f"subaccount {account_index} (valid {args.valid_days} days)", file=out)
    if not _confirm("This sends a signed createApiKey request.", args.yes):
        return EXIT_ERROR
    # generated here (not inside register_api_key) so a key whose registration went out is never lost
    api_priv = existing or ak.generate_ed25519_keypair()[0]
    try:
        reg = await ak.register_api_key(cfg.arcus.rest_url, cfg.arcus.chain_id, wallet, account_index,
                                        name=args.name, valid_days=args.valid_days, api_private_key=api_priv)
    except Exception as exc:
        _print_error("Arcus key registration failed", exc)
        if not _arcus_key_rejected(exc):
            print(UNCONFIRMED_NOTE, file=out)
            print(f"ARCUS_ADDRESS={address}\nARCUS_ACCOUNT_INDEX={account_index}", file=out)
            if not existing:  # an existing key is already in the environment
                print(f"ARCUS_API_PRIVATE_KEY={api_priv}", file=out)
        return EXIT_ERROR
    print(f"Registered Arcus API key {reg.api_key} ({reg.name}) status={reg.status}", file=out)
    print("WARNING: the API private key below is shown ONCE. Put these lines in .env and keep it private:", file=out)
    print(f"ARCUS_ADDRESS={reg.address}\nARCUS_ACCOUNT_INDEX={reg.account_index}\n"
          f"ARCUS_API_PRIVATE_KEY={reg.api_private_key}", file=out)
    return EXIT_OK


async def keygen_lighter(cfg: AppConfig, args: argparse.Namespace, out=None) -> int:
    from farmer.exchanges.lighter import keys as lk

    out = out or sys.stdout
    url = cfg.lighter.url
    api_key_index = args.api_key_index if args.api_key_index is not None else (cfg.lighter.api_key_index or 4)
    try:
        lk.validate_api_key_index(int(api_key_index), rh=lk.is_rh_url(url))
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_USAGE
    try:
        priv, pub = lk.generate_api_key()
    except Exception as exc:
        _print_error("Lighter key generation failed", exc)
        return EXIT_ERROR
    if args.generate_only:
        print(f"Generated Lighter API key (NOT registered). Public key: {pub}", file=out)
        print("WARNING: the private key below is shown ONCE. Store it in .env only; never share it.", file=out)
        print(f"LIGHTER_API_PRIVATE_KEY={priv}\nLIGHTER_API_KEY_INDEX={api_key_index}", file=out)
        return EXIT_OK
    eth = os.environ.get("LIGHTER_ETH_PRIVATE_KEY", "").strip()
    if not eth:
        print("ERROR: set LIGHTER_ETH_PRIVATE_KEY (the L1 wallet that owns the Lighter account). It is only used "
              "locally to sign ChangePubKey and never printed or sent.", file=sys.stderr)
        return EXIT_USAGE
    try:
        address = lk.address_from_eth_key(eth)
    except Exception:
        print("ERROR: LIGHTER_ETH_PRIVATE_KEY is not a valid private key", file=sys.stderr)
        return EXIT_USAGE
    try:
        account_index = int(args.account_index) if args.account_index is not None else (
            cfg.lighter.account_index if cfg.lighter.account_index is not None
            else await lk.lookup_account_index(address, url=url))
    except Exception as exc:
        _print_error("Lighter account lookup failed", exc)
        return EXIT_ERROR
    print(f"Lighter {url} chain {cfg.lighter.chain_id}: register API key index {api_key_index} for account "
          f"{account_index} ({address}). This REPLACES any key at that index.", file=out)
    if (cfg.lighter.api_key_index is not None and int(api_key_index) == int(cfg.lighter.api_key_index)
            and not getattr(args, "force", False)):
        lock = RunLock(store_path(cfg, False).with_suffix(".lock"))
        if not lock.acquire():
            print(f"ERROR: a running farmer engine (pid {lock.holder()}) signs with API key index {api_key_index} "
                  "(LIGHTER_API_KEY_INDEX); replacing it would break every order. Stop the bot first, use another "
                  "--api-key-index, or pass --force.", file=sys.stderr)
            return EXIT_ERROR
        lock.release()
    if not _confirm("This sends a signed ChangePubKey transaction (must run from a permitted jurisdiction).",
                    args.yes):
        return EXIT_ERROR
    try:
        tx = await lk.register_api_key(eth_private_key=eth, api_private_key=priv, api_public_key=pub,
                                       account_index=account_index, api_key_index=int(api_key_index), url=url,
                                       chain_id=cfg.lighter.chain_id)
    except Exception as exc:
        _print_error("Lighter key registration failed", exc)
        if not _lighter_key_rejected(exc, lk):  # e.g. sent and accepted, but not visible within the wait
            print(UNCONFIRMED_NOTE, file=out)
            print(f"LIGHTER_ACCOUNT_INDEX={account_index}\nLIGHTER_API_KEY_INDEX={api_key_index}\n"
                  f"LIGHTER_API_PRIVATE_KEY={priv}", file=out)
        return EXIT_ERROR
    print(f"Registered Lighter API key index {api_key_index} (tx {tx}).", file=out)
    print("WARNING: the API private key below is shown ONCE. Put these lines in .env and keep it private:", file=out)
    print(f"LIGHTER_ACCOUNT_INDEX={account_index}\nLIGHTER_API_KEY_INDEX={api_key_index}\n"
          f"LIGHTER_API_PRIVATE_KEY={priv}", file=out)
    return EXIT_OK


# =========================================================================== argparse
def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", "-c", default=DEFAULT_CONFIG, help="YAML config (default: config.yaml if present)")
    common.add_argument("--env-file", default=".env", help="secrets file (default: .env)")
    common.add_argument("--log-level", default=None, help="override log_level (DEBUG, INFO, ...)")

    p = argparse.ArgumentParser(prog="farmer", description="Arcus <-> Lighter delta-neutral funding & volume farmer")
    p.add_argument("--version", action="version", version=f"farmer {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("scan", parents=[common], help="public market scan (funding, carry, spreads, basis)")
    s.add_argument("--top", type=int, default=0, help="show only the N best rows by carry (0 = all)")
    s.add_argument("--books", type=int, default=60, help="max Lighter books sampled over the readonly websocket")
    s.add_argument("--wait", type=float, default=8.0, help="seconds to wait for Lighter book snapshots")
    s.add_argument("--skipped", action="store_true", help="list markets that cannot be hedged and why")

    r = sub.add_parser("run", parents=[common], help="run the engine")
    g = r.add_mutually_exclusive_group()
    g.add_argument("--paper", action="store_true", help="simulated orders on live public data")
    g.add_argument("--live", action="store_true", help="real orders (needs credentials and --yes or mode: live)")
    r.add_argument("--yes", action="store_true", help="confirm live trading")
    r.add_argument("--duration", type=float, default=None, help="stop automatically after SECONDS")

    st = sub.add_parser("status", parents=[common], help="balances, positions, open orders, delta per pair")
    del st

    ca = sub.add_parser("close-all", parents=[common], help="flatten all configured pairs")
    ca.add_argument("--taker", action="store_true", help="close immediately with IOC orders on both legs")
    ca.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    ca.add_argument("--timeout", type=float, default=None, help="seconds (default 600 maker / 120 taker)")

    sa = sub.add_parser("stats", parents=[common], help="report from the database")
    sa.add_argument("--since", type=float, default=24.0, help="window in hours (0 = all time)")
    sa.add_argument("--paper", action="store_true", default=None, help="read the paper database")
    sa.add_argument("--live", action="store_true", default=None, help="read the live database")
    sa.add_argument("--no-marks", action="store_true", help="do not fetch public mark prices for the PnL")

    d = sub.add_parser("dashboard", parents=[common], help="local web dashboard: status, settings, API keys, start/stop")
    d.add_argument("--host", default="127.0.0.1", help="listen address (default 127.0.0.1; use an SSH tunnel remotely)")
    d.add_argument("--port", type=int, default=8080)

    k = sub.add_parser("keygen", parents=[common], help="create and register API keys")
    k.add_argument("venue", choices=["arcus", "lighter"])
    k.add_argument("--account-index", type=int, default=None,
                   help="Arcus subaccount (0-9) / Lighter account index (default: config or L1 lookup)")
    k.add_argument("--api-key-index", type=int, default=None, help="Lighter API key slot 4..254 (default 4)")
    k.add_argument("--name", default=None, help="Arcus key name (default farmer-<pub8>; reusing a name revokes)")
    k.add_argument("--valid-days", type=float, default=180, help="Arcus key validity (max 180)")
    k.add_argument("--api-private-key-env", default=None,
                   help="Arcus: register an existing key read from this env var instead of generating one")
    k.add_argument("--generate-only", action="store_true", help="only generate a key pair locally (no network)")
    k.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    k.add_argument("--force", action="store_true",
                   help="Lighter: replace the configured key slot even while a farmer engine holds the run lock")
    return p


def _dashboard(args: argparse.Namespace) -> int:
    """The dashboard also starts with a broken config.yaml (it shows the error and can rewrite the file)."""
    from dotenv import load_dotenv

    from farmer.dashboard.server import serve

    env_file = Path(args.env_file or ".env")
    if env_file.exists():
        load_dotenv(env_file, override=False)
    try:
        cfg = _load(args)
    except Exception as exc:
        print(f"WARNING: config: {exc} (fix it in the dashboard)", file=sys.stderr)
        cfg = AppConfig()
    _setup_logging(cfg, quiet_console=True)
    return asyncio.run(serve(Path(args.config or DEFAULT_CONFIG), env_file, host=args.host, port=args.port))


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "dashboard":
        try:
            return _dashboard(args)
        except KeyboardInterrupt:
            return 130
    try:
        cfg = _load(args)
    except Exception as exc:
        print(f"ERROR: config: {exc}", file=sys.stderr)
        return EXIT_USAGE
    quiet = args.command != "run"
    _setup_logging(cfg, quiet_console=quiet)
    try:
        if args.command == "scan":
            return asyncio.run(scan(cfg, top=args.top, max_books=args.books, wait_s=args.wait,
                                    show_skipped=args.skipped))
        if args.command == "run":
            paper = args.paper or (not args.live and cfg.mode == "paper")
            if not paper:
                missing = _missing_credentials(cfg)
                if missing:
                    print("ERROR: live mode needs credentials on both venues; missing: " + "; ".join(missing)
                          + ". Use --paper to simulate.", file=sys.stderr)
                    return EXIT_USAGE
                if not (args.yes or cfg.mode == "live"):
                    print("ERROR: live trading needs an explicit confirmation: pass --yes or set `mode: live` in the "
                          "config.", file=sys.stderr)
                    return EXIT_USAGE
            return asyncio.run(run_engine(cfg, paper=paper, duration=args.duration))
        if args.command == "status":
            return asyncio.run(status(cfg))
        if args.command == "close-all":
            timeout = args.timeout if args.timeout is not None else (120.0 if args.taker else 600.0)
            what = "IOC (taker) orders on BOTH legs" if args.taker else "hedged post-only maker orders"
            if not _confirm(f"close-all will flatten every configured pair with {what}.", args.yes):
                return EXIT_ERROR
            return asyncio.run(close_all(cfg, taker=args.taker, timeout_s=timeout))
        if args.command == "stats":
            paper = True if args.paper else (False if args.live else cfg.mode == "paper")
            return asyncio.run(stats_report(cfg, paper=paper, since_hours=args.since, use_marks=not args.no_marks))
        if args.command == "keygen":
            fn = keygen_arcus if args.venue == "arcus" else keygen_lighter
            return asyncio.run(fn(cfg, args))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    return EXIT_USAGE


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
