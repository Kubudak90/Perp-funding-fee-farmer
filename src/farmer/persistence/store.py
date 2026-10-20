"""SQLite persistence for fills, funding payments and events.

Synchronous sqlite3 is fine for writes: they are tiny (one row per fill plus its hourly aggregate) and WAL mode
keeps them cheap. All amounts are stored as TEXT (exact Decimal round-trip).

Reads stay cheap as the database grows: every inserted fill / funding row also updates its per-(venue, symbol,
hour) row in ``agg_hourly`` in the same transaction, so ``summary`` sums aggregate rows for the whole hours of a
window and scans raw rows only for the partial hours at its edges (24 h / all-time windows cost O(hours), not
O(fills)). File databases also get a second, query-only connection: ``summary`` may then run in a worker thread
(``concurrent_reads``; callers use ``asyncio.to_thread``) without blocking the event loop - WAL readers never block
the writer.
"""

from __future__ import annotations

import contextlib
import json
import logging
import math
import sqlite3
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterator

from farmer.models import Fill, FundingPayment

D = Decimal
log = logging.getLogger("farmer.store")

HOUR = 3600
AGG_VERSION = "1"  # bump to force a rebuild of agg_hourly from the raw rows

_SCHEMA = """
CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    price TEXT NOT NULL,
    size TEXT NOT NULL,
    notional TEXT NOT NULL,
    fee TEXT NOT NULL,
    is_maker INTEGER,
    role TEXT NOT NULL,
    order_id TEXT,
    client_id TEXT,
    trade_id TEXT,
    paper INTEGER NOT NULL DEFAULT 0,
    UNIQUE (venue, symbol, trade_id, side)
);
CREATE INDEX IF NOT EXISTS fills_ts ON fills (ts);
CREATE TABLE IF NOT EXISTS funding (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    amount TEXT NOT NULL,
    rate TEXT,
    position_size TEXT,
    paper INTEGER NOT NULL DEFAULT 0,
    UNIQUE (venue, symbol, ts)
);
CREATE INDEX IF NOT EXISTS funding_ts ON funding (ts);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,
    data TEXT
);
CREATE INDEX IF NOT EXISTS events_ts ON events (ts);
CREATE TABLE IF NOT EXISTS agg_hourly (
    paper INTEGER NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    hour INTEGER NOT NULL,
    fills INTEGER NOT NULL,
    volume TEXT NOT NULL,
    maker_volume TEXT NOT NULL,
    taker_volume TEXT NOT NULL,
    fees TEXT NOT NULL,
    cash_flow TEXT NOT NULL,
    net_base TEXT NOT NULL,
    funding TEXT NOT NULL,
    PRIMARY KEY (paper, venue, symbol, hour)
);
CREATE INDEX IF NOT EXISTS agg_hourly_hour ON agg_hourly (paper, hour);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

# agg_hourly value columns after ``fills`` (all Decimal TEXT)
_AGG_COLS = ("volume", "maker_volume", "taker_volume", "fees", "cash_flow", "net_base", "funding")


@dataclass
class VenueSummary:
    volume: Decimal = D(0)
    maker_volume: Decimal = D(0)
    taker_volume: Decimal = D(0)
    fees: Decimal = D(0)
    funding: Decimal = D(0)
    fills: int = 0
    cash_flow: Decimal = D(0)  # sum(-signed_size*price) - fees  (excludes funding)
    net_base: dict[str, Decimal] | None = None  # symbol -> net signed base traded in window


def _fill_values(side: str, price: str, size: str, fee: str, is_maker: int | None) -> tuple[Decimal, ...]:
    """(volume, maker_volume, taker_volume, fees, cash_flow, net_base, funding) of one stored fill row."""
    px, sz, fe = D(price), D(size), D(fee)
    notional = px * sz
    sign = 1 if side == "BUY" else -1
    return (notional, notional if is_maker == 1 else D(0), notional if is_maker == 0 else D(0), fe,
            -sign * notional - fe, sign * sz, D(0))


class Store:
    def __init__(self, path: Path | str, *, paper: bool = False) -> None:
        self.path = Path(path)
        memory = str(path) == ":memory:"
        if not memory:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.paper = 1 if paper else 0
        self._db = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(_SCHEMA)
        self._ensure_aggregates()
        # second connection for summaries in a worker thread (an in-memory database cannot be shared)
        self._ro: sqlite3.Connection | None = None
        self._ro_lock = threading.Lock()
        if not memory:
            self._ro = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
            self._ro.execute("PRAGMA query_only=ON")

    @property
    def concurrent_reads(self) -> bool:
        """True when ``summary`` may be called from a worker thread while the loop keeps writing."""
        return self._ro is not None

    def close(self) -> None:
        with self._ro_lock:  # a summary still running in a worker thread finishes first
            for db in (self._ro, self._db):
                if db is None:
                    continue
                try:
                    db.close()
                except Exception:  # pragma: no cover
                    pass

    # ------------------------------------------------------------------ transactions
    @contextlib.contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        db = self._db
        db.execute("BEGIN IMMEDIATE")
        try:
            yield db
        except BaseException:
            with contextlib.suppress(sqlite3.Error):
                db.execute("ROLLBACK")
            raise
        db.execute("COMMIT")

    @contextlib.contextmanager
    def _reader(self) -> Iterator[sqlite3.Connection]:
        """One consistent read snapshot (the read-only connection when there is one)."""
        if self._ro is None:
            yield self._db
            return
        with self._ro_lock:
            self._ro.execute("BEGIN")
            try:
                yield self._ro
            finally:
                self._ro.execute("COMMIT")

    # ------------------------------------------------------------------ hourly aggregates
    def _agg_add(self, db: sqlite3.Connection, venue: str, symbol: str, ts: float, fills: int,
                 vals: tuple[Decimal, ...]) -> None:
        hour = int(ts // HOUR)
        key = (self.paper, venue, symbol, hour)
        row = db.execute("SELECT fills, " + ", ".join(_AGG_COLS) + " FROM agg_hourly WHERE paper=? AND venue=? AND "
                         "symbol=? AND hour=?", key).fetchone()
        if row is not None:
            fills += int(row[0])
            vals = tuple(D(old) + v for old, v in zip(row[1:], vals, strict=True))
        db.execute("INSERT OR REPLACE INTO agg_hourly (paper, venue, symbol, hour, fills, " + ", ".join(_AGG_COLS)
                   + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (*key, fills, *(str(v) for v in vals)))

    def _ensure_aggregates(self) -> None:
        """(Re)build ``agg_hourly`` from the raw rows once (databases written before it existed)."""
        def built(db: sqlite3.Connection) -> bool:
            row = db.execute("SELECT value FROM meta WHERE key='agg_hourly'").fetchone()
            return row is not None and row[0] == AGG_VERSION

        if built(self._db):
            return
        t0 = time.monotonic()
        n = 0
        with self._write() as db:
            if built(db):  # another process built it meanwhile
                return
            db.execute("DELETE FROM agg_hourly")
            acc: dict[tuple, list] = {}
            zero = (D(0),) * len(_AGG_COLS)

            def add(key: tuple, fills: int, vals: tuple[Decimal, ...]) -> None:
                cur = acc.get(key)
                if cur is None:
                    acc[key] = [fills, *vals]
                else:
                    cur[0] += fills
                    for i, v in enumerate(vals, start=1):
                        cur[i] += v

            for paper, venue, symbol, ts, side, price, size, fee, is_maker in db.execute(
                    "SELECT paper, venue, symbol, ts, side, price, size, fee, is_maker FROM fills").fetchall():
                add((paper, venue, symbol, int(ts // HOUR)), 1, _fill_values(side, price, size, fee, is_maker))
                n += 1
            for paper, venue, symbol, ts, amount in db.execute(
                    "SELECT paper, venue, symbol, ts, amount FROM funding").fetchall():
                add((paper, venue, symbol, int(ts // HOUR)), 0, zero[:-1] + (D(amount),))
                n += 1
            db.executemany("INSERT INTO agg_hourly (paper, venue, symbol, hour, fills, " + ", ".join(_AGG_COLS)
                           + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                           [(*k, v[0], *(str(x) for x in v[1:])) for k, v in acc.items()])
            db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('agg_hourly', ?)", (AGG_VERSION,))
        if n:
            log.info("built hourly aggregates from %d stored rows in %.2fs", n, time.monotonic() - t0)

    # ------------------------------------------------------------------ writes
    def record_fill(self, fill: Fill, role: str) -> bool:
        """role: 'maker' | 'hedge' | 'emergency' | 'manual'. Returns False for a duplicate (already stored)."""
        side, price, size, fee = fill.side.value, str(fill.price), str(fill.size), str(fill.fee)
        is_maker = None if fill.is_maker is None else int(fill.is_maker)
        with self._write() as db:
            cur = db.execute(
                "INSERT OR IGNORE INTO fills (ts, venue, symbol, side, price, size, notional, fee, is_maker, role,"
                " order_id, client_id, trade_id, paper) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    fill.ts, fill.venue, fill.symbol, side, price, size, str(fill.notional), fee, is_maker, role,
                    fill.order_id, fill.client_id, fill.trade_id, self.paper,
                ),
            )
            if cur.rowcount != 1:
                return False
            self._agg_add(db, fill.venue, fill.symbol, fill.ts, 1, _fill_values(side, price, size, fee, is_maker))
        return True

    def record_funding(self, p: FundingPayment) -> bool:
        """Returns False for a duplicate (same venue, symbol and ts already stored)."""
        amount = str(p.amount)
        with self._write() as db:
            cur = db.execute(
                "INSERT OR IGNORE INTO funding (ts, venue, symbol, amount, rate, position_size, paper)"
                " VALUES (?,?,?,?,?,?,?)",
                (p.ts, p.venue, p.symbol, amount, None if p.rate is None else str(p.rate),
                 None if p.position_size is None else str(p.position_size), self.paper),
            )
            if cur.rowcount != 1:
                return False
            self._agg_add(db, p.venue, p.symbol, p.ts, 0, (D(0),) * (len(_AGG_COLS) - 1) + (D(amount),))
        return True

    def record_event(self, kind: str, data: dict | None = None) -> None:
        self._db.execute(
            "INSERT INTO events (ts, kind, data) VALUES (?,?,?)",
            (time.time(), kind, json.dumps(data or {}, default=str)),
        )

    def set_meta(self, key: str, value: str) -> None:
        self._db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))

    # ------------------------------------------------------------------ reads
    def get_meta(self, key: str) -> str | None:
        row = self._db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return None if row is None else row[0]

    def last_funding_ts(self, venue: str) -> float | None:
        row = self._db.execute("SELECT MAX(ts) FROM funding WHERE venue=? AND paper=?", (venue, self.paper)).fetchone()
        return row[0] if row and row[0] is not None else None

    def first_funding_ts(self, venue: str) -> float | None:
        row = self._db.execute("SELECT MIN(ts) FROM funding WHERE venue=? AND paper=?", (venue, self.paper)).fetchone()
        return row[0] if row and row[0] is not None else None

    def summary(self, since_ts: float = 0.0, until_ts: float | None = None) -> dict[str, VenueSummary]:
        """Per-venue totals of fills and funding with ``since_ts <= ts < until_ts``.

        Whole hours come from ``agg_hourly``; only the partial hours at the window edges scan raw rows. Safe to
        call from a worker thread when ``concurrent_reads`` is True.
        """
        since = float(since_ts)
        until = float(until_ts) if until_ts is not None else time.time() + 1
        out: dict[str, VenueSummary] = {}
        if until <= since:
            return out
        with self._reader() as db:
            h0, h1 = math.ceil(since / HOUR), math.floor(until / HOUR)
            if h1 > h0:
                self._scan_raw(db, out, since, h0 * float(HOUR))
                self._scan_agg(db, out, h0, h1)
                self._scan_raw(db, out, h1 * float(HOUR), until)
            else:
                self._scan_raw(db, out, since, until)
        return out

    def _scan_raw(self, db: sqlite3.Connection, out: dict[str, VenueSummary], since: float, until: float) -> None:
        if until <= since:
            return
        for venue, side, price, size, fee, is_maker, symbol in db.execute(
            "SELECT venue, side, price, size, fee, is_maker, symbol FROM fills WHERE ts>=? AND ts<? AND paper=?",
            (since, until, self.paper),
        ):
            s = out.setdefault(venue, VenueSummary(net_base={}))
            vol, mvol, tvol, fe, cash, base, _ = _fill_values(side, price, size, fee, is_maker)
            s.volume += vol
            s.maker_volume += mvol
            s.taker_volume += tvol
            s.fees += fe
            s.fills += 1
            s.cash_flow += cash
            assert s.net_base is not None
            s.net_base[symbol] = s.net_base.get(symbol, D(0)) + base
        for venue, amount in db.execute(
            "SELECT venue, amount FROM funding WHERE ts>=? AND ts<? AND paper=?", (since, until, self.paper)
        ):
            s = out.setdefault(venue, VenueSummary(net_base={}))
            s.funding += D(amount)

    def _scan_agg(self, db: sqlite3.Connection, out: dict[str, VenueSummary], h0: int, h1: int) -> None:
        for venue, symbol, fills, vol, mvol, tvol, fees, cash, base, funding in db.execute(
            "SELECT venue, symbol, fills, " + ", ".join(_AGG_COLS) + " FROM agg_hourly WHERE paper=? AND hour>=? AND "
            "hour<?", (self.paper, h0, h1),
        ):
            s = out.setdefault(venue, VenueSummary(net_base={}))
            if fills:
                s.volume += D(vol)
                s.maker_volume += D(mvol)
                s.taker_volume += D(tvol)
                s.fees += D(fees)
                s.fills += int(fills)
                s.cash_flow += D(cash)
                assert s.net_base is not None
                s.net_base[symbol] = s.net_base.get(symbol, D(0)) + D(base)
            s.funding += D(funding)

    def total_volume(self, since_ts: float = 0.0) -> Decimal:
        return sum((s.volume for s in self.summary(since_ts).values()), D(0))

    def pnl(self, since_ts: float, marks: dict[str, Decimal]) -> Decimal:
        """Realized+unrealized PnL of activity since ``since_ts``: cash flow + funding + mark value
        of the net base traded in the window (both venues combined, so hedged legs net out)."""
        total = D(0)
        net: dict[str, Decimal] = {}
        for s in self.summary(since_ts).values():
            total += s.cash_flow + s.funding
            for sym, q in (s.net_base or {}).items():
                net[sym] = net.get(sym, D(0)) + q
        for sym, q in net.items():
            if q != 0 and sym in marks:
                total += q * marks[sym]
        return total

    def recent_events(self, limit: int = 50) -> list[tuple[float, str, dict]]:
        rows = self._db.execute("SELECT ts, kind, data FROM events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [(ts, kind, json.loads(data or "{}")) for ts, kind, data in rows]
