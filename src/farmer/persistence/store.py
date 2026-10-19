"""SQLite persistence for fills, funding payments and events.

Synchronous sqlite3 is fine here: writes are tiny and infrequent (one row per fill); WAL mode
keeps them cheap. All amounts are stored as TEXT (exact Decimal round-trip).
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from farmer.models import Fill, FundingPayment

D = Decimal

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
"""


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


class Store:
    def __init__(self, path: Path | str, *, paper: bool = False) -> None:
        self.path = Path(path)
        if str(path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.paper = 1 if paper else 0
        self._db = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(_SCHEMA)

    def close(self) -> None:
        try:
            self._db.close()
        except Exception:  # pragma: no cover
            pass

    # ------------------------------------------------------------------ writes
    def record_fill(self, fill: Fill, role: str) -> None:
        """role: 'maker' | 'hedge' | 'emergency' | 'manual'."""
        self._db.execute(
            "INSERT OR IGNORE INTO fills (ts, venue, symbol, side, price, size, notional, fee, is_maker, role,"
            " order_id, client_id, trade_id, paper) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                fill.ts, fill.venue, fill.symbol, fill.side.value, str(fill.price), str(fill.size),
                str(fill.notional), str(fill.fee), None if fill.is_maker is None else int(fill.is_maker), role,
                fill.order_id, fill.client_id, fill.trade_id, self.paper,
            ),
        )

    def record_funding(self, p: FundingPayment) -> None:
        self._db.execute(
            "INSERT OR IGNORE INTO funding (ts, venue, symbol, amount, rate, position_size, paper) VALUES (?,?,?,?,?,?,?)",
            (p.ts, p.venue, p.symbol, str(p.amount), None if p.rate is None else str(p.rate),
             None if p.position_size is None else str(p.position_size), self.paper),
        )

    def record_event(self, kind: str, data: dict | None = None) -> None:
        self._db.execute(
            "INSERT INTO events (ts, kind, data) VALUES (?,?,?)",
            (time.time(), kind, json.dumps(data or {}, default=str)),
        )

    # ------------------------------------------------------------------ reads
    def last_funding_ts(self, venue: str) -> float | None:
        row = self._db.execute("SELECT MAX(ts) FROM funding WHERE venue=? AND paper=?", (venue, self.paper)).fetchone()
        return row[0] if row and row[0] is not None else None

    def summary(self, since_ts: float = 0.0, until_ts: float | None = None) -> dict[str, VenueSummary]:
        until = until_ts if until_ts is not None else time.time() + 1
        out: dict[str, VenueSummary] = {}
        for venue, side, price, size, fee, is_maker, symbol in self._db.execute(
            "SELECT venue, side, price, size, fee, is_maker, symbol FROM fills WHERE ts>=? AND ts<? AND paper=?",
            (since_ts, until, self.paper),
        ):
            s = out.setdefault(venue, VenueSummary(net_base={}))
            px, sz, fe = D(price), D(size), D(fee)
            notional = px * sz
            s.volume += notional
            if is_maker == 1:
                s.maker_volume += notional
            elif is_maker == 0:
                s.taker_volume += notional
            s.fees += fe
            s.fills += 1
            sign = 1 if side == "BUY" else -1
            s.cash_flow += -sign * notional - fe
            s.net_base[symbol] = s.net_base.get(symbol, D(0)) + sign * sz
        for venue, amount in self._db.execute(
            "SELECT venue, amount FROM funding WHERE ts>=? AND ts<? AND paper=?", (since_ts, until, self.paper)
        ):
            s = out.setdefault(venue, VenueSummary(net_base={}))
            s.funding += D(amount)
        return out

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
