"""Arcus request signing (Ed25519) and the server-synced nanosecond clock.

Two signing schemes (mirrors the official TS SDK bundled in app.arcus.xyz):

- **Scheme 1** ("ordersign") for ``placeOrder`` / ``cancelOrder`` / ``modifyOrder`` (and each batch
  element): the signed message is a compact, alphabetically keyed JSON object built from
  engine-native integers (``p`` = price / top-level tickSize, ``q`` = size / stepSize,
  ``g`` = goodTilTime_us * 1000, ``ct`` = X-Timestamp ns, ``ad`` lower-cased address).
- **Scheme 2** (legacy) for ``cancelAllOrders`` / ``setLeverage`` / ``scheduleCancel`` /
  ``adjustIsolatedMargin``: ``ed25519(str(ts_ns) + action + canonical_json(body))``.

Auth travels in the headers ``X-API-Key`` (64-hex public key), ``X-Timestamp`` (Unix ns string,
within +-30 s of the server, unique and strictly increasing per key) and ``X-Signature``
(128 lowercase hex).
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric import ed25519

from farmer.models import Side
from farmer.utils.decimals import to_plain

OP_PLACE = 1
OP_CANCEL = 2
OP_MODIFY = 3
OP_PLACE_TPSL = 4
PAYLOAD_VERSION = 1

SIDE_CODES = {"BUY": 0, "SELL": 1}
# Submit values are GTT/FOK/IOC/ALO; reads report resting GTT orders as "GTC".
TIF_CODES = {"GTT": 0, "GTC": 0, "FOK": 1, "IOC": 2, "ALO": 3}

CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,36}$")
_HEX_RE = re.compile(r"^[0-9a-fA-F]+$")
_ADDRESS_RE = re.compile(r"^(0x|0X)?[0-9a-fA-F]{40}$")
_ORDER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

US_PER_DAY = 86_400 * 1_000_000


# --------------------------------------------------------------------------- clock
class NsClock:
    """Unique, strictly increasing Unix-nanosecond stamps, offset-corrected to server time.

    One instance per API key. ``sync`` takes the server's ``GET /v1/time`` value and the local
    send/receive times of that request (offset = server - midpoint). ``now_ns`` never returns the
    same value twice, even if the offset moves backwards after a re-sync.
    """

    def __init__(self, *, time_ns: Callable[[], int] = time.time_ns) -> None:
        self._time_ns = time_ns
        self.offset_ns = 0
        self.rtt_ns: int | None = None
        self.synced_at: float | None = None
        self._last = 0

    def sync(self, server_time_ns: int, t0_ns: int, t1_ns: int) -> int:
        """Apply a server time sample. Returns the new offset (ns)."""
        self.rtt_ns = max(0, t1_ns - t0_ns)
        self.offset_ns = int(server_time_ns) - (t0_ns + t1_ns) // 2
        self.synced_at = time.time()
        return self.offset_ns

    def now_ns(self) -> int:
        t = self._time_ns() + self.offset_ns
        if t <= self._last:
            t = self._last + 1
        self._last = t
        return t

    def now_us(self) -> int:
        """Server-corrected wall clock in microseconds (not uniqueness-guarded)."""
        return (self._time_ns() + self.offset_ns) // 1000

    def now_s(self) -> float:
        return (self._time_ns() + self.offset_ns) / 1e9


# --------------------------------------------------------------------------- encoding helpers
def to_units(value: Decimal, unit: Decimal) -> int:
    """Exact ``value / unit`` as int; raises ``ValueError`` if not an integer multiple."""
    if unit <= 0:
        raise ValueError(f"unit must be positive, got {unit}")
    n = Decimal(value) / Decimal(unit)
    if n != n.to_integral_value():
        raise ValueError(f"{value} is not a multiple of {unit}")
    return int(n)


def dec_str(x: Decimal) -> str:
    """Decimal -> API string (no exponent, no trailing zeros), e.g. ``60000.0 -> "60000"``."""
    return to_plain(Decimal(x))


def side_str(side: Side | str) -> str:
    s = side.value if isinstance(side, Side) else str(side).upper()
    if s not in SIDE_CODES:
        raise ValueError(f"invalid side {side!r}")
    return s


def submit_tif(tif: str) -> str:
    """Normalize a read TIF value into a submit value (``GTC`` -> ``GTT``)."""
    t = str(tif).upper()
    if t == "GTC":
        return "GTT"
    if t not in TIF_CODES:
        raise ValueError(f"invalid timeInForce {tif!r}")
    return t


def _check_str(name: str, v: str, rx: re.Pattern) -> str:
    if not isinstance(v, str) or not rx.match(v):
        raise ValueError(f"invalid {name}: {v!r}")
    return v


def _obj(fields: list[tuple[str, Any]]) -> str:
    out = []
    for k, v in fields:
        if isinstance(v, bool):
            raise TypeError("encode booleans as 0/1 in signed payloads")
        if isinstance(v, int):
            out.append(f'"{k}":{v}')
        elif isinstance(v, str):
            if '"' in v or "\\" in v or not v.isascii():
                raise ValueError(f"unsafe string in signed payload field {k!r}")
            out.append(f'"{k}":"{v}"')
        else:
            raise TypeError(f"unsupported type for {k!r}: {type(v).__name__}")
    return "{" + ",".join(out) + "}"


def place_payload(*, address: str, account_index: int, ct: int, gtt_us: int, market_id: int, price_ticks: int,
                  qty_quantums: int, reduce_only: bool, side: str, tif: str, client_id: str | None = None,
                  op: int = OP_PLACE) -> str:
    """Scheme-1 payload for a place (op 1) / untriggered TPSL place (op 4)."""
    f: list[tuple[str, Any]] = [("ad", address.lower()), ("ai", int(account_index))]
    if client_id:
        f.append(("c", client_id))
    f += [("ct", int(ct)), ("g", int(gtt_us) * 1000), ("m", int(market_id)), ("op", int(op)), ("p", int(price_ticks)),
          ("q", int(qty_quantums)), ("r", 1 if reduce_only else 0), ("s", SIDE_CODES[side_str(side)]),
          ("t", TIF_CODES[submit_tif(tif)]), ("v", PAYLOAD_VERSION)]
    return _obj(f)


def cancel_payload(*, address: str, account_index: int, ct: int, market_id: int, order_id: str | None = None,
                   client_id: str | None = None) -> str:
    """Scheme-1 payload for a cancel (op 2). Exactly one of ``order_id`` / ``client_id``."""
    if bool(order_id) == bool(client_id):
        raise ValueError("exactly one of order_id / client_id is required")
    f: list[tuple[str, Any]] = [("ad", address.lower()), ("ai", int(account_index))]
    if client_id:
        f.append(("c", client_id.strip()))
    f.append(("ct", int(ct)))
    if order_id:
        f.append(("id", order_id.strip()))
    f += [("m", int(market_id)), ("op", OP_CANCEL), ("v", PAYLOAD_VERSION)]
    return _obj(f)


def modify_payload(*, address: str, account_index: int, ct: int, gtt_us: int, market_id: int, price_ticks: int,
                   qty_quantums: int, reduce_only: bool, side: str, tif: str, order_id: str | None = None,
                   client_id: str | None = None) -> str:
    """Scheme-1 payload for a modify (op 3); identity mirrors the body (exactly one of id / c)."""
    if bool(order_id) == bool(client_id):
        raise ValueError("exactly one of order_id / client_id is required")
    f: list[tuple[str, Any]] = [("ad", address.lower()), ("ai", int(account_index))]
    if client_id:
        f.append(("c", client_id.strip()))
    f += [("ct", int(ct)), ("g", int(gtt_us) * 1000)]
    if order_id:
        f.append(("id", order_id.strip()))
    f += [("m", int(market_id)), ("op", OP_MODIFY), ("p", int(price_ticks)), ("q", int(qty_quantums)),
          ("r", 1 if reduce_only else 0), ("s", SIDE_CODES[side_str(side)]), ("t", TIF_CODES[submit_tif(tif)]),
          ("v", PAYLOAD_VERSION)]
    return _obj(f)


def _check_signable(obj: Any) -> None:
    """Only ASCII strings, ints, bools and null: the encodings where Python == JS ``JSON.stringify``."""
    if isinstance(obj, float):
        raise TypeError("floats are not allowed in signed bodies (use ints / decimal strings)")
    if isinstance(obj, str):
        if not obj.isascii():
            raise ValueError("signed bodies must be ASCII")
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _check_signable(str(k))
            _check_signable(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            _check_signable(v)


def canonical_json(obj: Any) -> str:
    """Sorted keys at every level, no whitespace (== SDK ``JSON.stringify(sortKeys(body))`` for ASCII)."""
    _check_signable(obj)
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def legacy_message(ts_ns: int, action: str, body: dict | None) -> bytes:
    """Scheme-2 message ``str(ts) + action + canonical_json(body)``."""
    return (str(int(ts_ns)) + action + (canonical_json(body) if body is not None else "")).encode()


def auth_headers(api_key: str, ts_ns: int, signature: str) -> dict[str, str]:
    return {"X-API-Key": api_key, "X-Timestamp": str(int(ts_ns)), "X-Signature": signature}


# --------------------------------------------------------------------------- key
class Ed25519Signer:
    """Ed25519 API key. Accepts the 32-byte seed as 64 hex (``0x`` optional) or a 64-byte
    NaCl-style secret key (seed || public key) as 128 hex."""

    def __init__(self, private_key_hex: str) -> None:
        h = private_key_hex.strip()
        if h.startswith(("0x", "0X")):
            h = h[2:]
        if not _HEX_RE.match(h) or len(h) not in (64, 128):
            raise ValueError("Arcus API private key must be 32-byte (64 hex) Ed25519 seed")
        raw = bytes.fromhex(h)
        self._sk = ed25519.Ed25519PrivateKey.from_private_bytes(raw[:32])
        self.api_key = self._sk.public_key().public_bytes_raw().hex()
        if len(raw) == 64 and raw[32:].hex() != self.api_key:
            raise ValueError("64-byte secret key: embedded public key does not match the seed")

    def sign(self, msg: str | bytes) -> str:
        return self._sk.sign(msg.encode() if isinstance(msg, str) else msg).hex()

    def verify(self, msg: str | bytes, signature_hex: str) -> bool:
        try:
            self._sk.public_key().verify(bytes.fromhex(signature_hex), msg.encode() if isinstance(msg, str) else msg)
            return True
        except (InvalidSignature, ValueError):
            return False

    def __repr__(self) -> str:  # never leak the secret
        return f"Ed25519Signer(api_key={self.api_key[:8]}...)"


def verify_signature(api_key_hex: str, msg: str | bytes, signature_hex: str) -> bool:
    """Verify an Ed25519 signature with a public key (64 hex)."""
    try:
        pk = ed25519.Ed25519PublicKey.from_public_bytes(bytes.fromhex(api_key_hex))
        pk.verify(bytes.fromhex(signature_hex), msg.encode() if isinstance(msg, str) else msg)
        return True
    except (InvalidSignature, ValueError):
        return False


# --------------------------------------------------------------------------- request builder
@dataclass(frozen=True)
class SignedRequest:
    """A signed write: ``body`` is sent as JSON (canonical bytes), ``ts_ns`` is X-Timestamp."""

    body: dict
    signature: str
    ts_ns: int
    message: bytes  # exactly what was signed (debug / tests)

    def headers(self, api_key: str) -> dict[str, str]:
        return auth_headers(api_key, self.ts_ns, self.signature)


class ArcusSigner:
    """Builds signed REST bodies for one ``(address, accountIndex)`` with one key and clock."""

    def __init__(self, key: Ed25519Signer, address: str, account_index: int, clock: NsClock) -> None:
        _check_str("address", address, _ADDRESS_RE)
        if not 0 <= int(account_index) <= 9:
            raise ValueError("accountIndex must be 0..9")
        self.key = key
        self.address = address
        self.account_index = int(account_index)
        self.clock = clock

    @property
    def api_key(self) -> str:
        return self.key.api_key

    def _sign(self, message: str | bytes) -> str:
        return self.key.sign(message)

    def place(self, *, market_id: int, tick_size: Decimal, step_size: Decimal, side: Side | str, price: Decimal,
              size: Decimal, tif: str, gtt_us: int, order_type: str = "LIMIT", reduce_only: bool = False,
              client_id: str | None = None, ts_ns: int | None = None) -> SignedRequest:
        s, t = side_str(side), submit_tif(tif)
        if client_id is not None:
            _check_str("clientId", client_id, CLIENT_ID_RE)
        p, q = to_units(price, tick_size), to_units(size, step_size)
        if p <= 0 or q <= 0:
            raise ValueError("price and size must be positive")
        ct = ts_ns if ts_ns is not None else self.clock.now_ns()
        msg = place_payload(address=self.address, account_index=self.account_index, ct=ct, gtt_us=gtt_us,
                            market_id=market_id, price_ticks=p, qty_quantums=q, reduce_only=reduce_only, side=s,
                            tif=t, client_id=client_id)
        body: dict[str, Any] = {
            "address": self.address, "accountIndex": self.account_index, "marketId": int(market_id),
            "orderSide": s, "orderType": order_type, "quantity": dec_str(size), "price": dec_str(price),
            "timeInForce": t, "goodTilTime": str(int(gtt_us)), "reduceOnly": bool(reduce_only), "timestamp": ct,
        }
        if client_id:
            body["clientId"] = client_id
        return SignedRequest(body, self._sign(msg), ct, msg.encode())

    def cancel(self, *, market_id: int, order_id: str | None = None, client_id: str | None = None,
               ts_ns: int | None = None) -> SignedRequest:
        if order_id is not None:
            _check_str("orderId", order_id, _ORDER_ID_RE)
        if client_id is not None:
            _check_str("clientId", client_id, CLIENT_ID_RE)
        ct = ts_ns if ts_ns is not None else self.clock.now_ns()
        msg = cancel_payload(address=self.address, account_index=self.account_index, ct=ct, market_id=market_id,
                             order_id=order_id, client_id=client_id)
        body: dict[str, Any] = {"address": self.address, "accountIndex": self.account_index,
                                "marketId": int(market_id), "timestamp": ct}
        if order_id:
            body.update(kind="orderId", orderId=order_id)
        else:
            body.update(kind="clientId", clientId=client_id)
        return SignedRequest(body, self._sign(msg), ct, msg.encode())

    def modify(self, *, market_id: int, tick_size: Decimal, step_size: Decimal, side: Side | str, price: Decimal,
               size: Decimal, tif: str, reduce_only: bool, gtt_us: int, order_id: str | None = None,
               client_id: str | None = None, ts_ns: int | None = None) -> SignedRequest:
        s, t = side_str(side), submit_tif(tif)
        if order_id is not None:
            _check_str("orderId", order_id, _ORDER_ID_RE)
        if client_id is not None:
            _check_str("clientId", client_id, CLIENT_ID_RE)
        p, q = to_units(price, tick_size), to_units(size, step_size)
        if p <= 0 or q <= 0:
            raise ValueError("price and size must be positive")
        ct = ts_ns if ts_ns is not None else self.clock.now_ns()
        msg = modify_payload(address=self.address, account_index=self.account_index, ct=ct, gtt_us=gtt_us,
                             market_id=market_id, price_ticks=p, qty_quantums=q, reduce_only=reduce_only, side=s,
                             tif=t, order_id=order_id, client_id=client_id)
        # ModifyOrderRequest has additionalProperties:false -> no "timestamp"; clientTime carries ct.
        body: dict[str, Any] = {
            "address": self.address, "accountIndex": self.account_index, "marketId": int(market_id),
            "side": s, "quantity": dec_str(size), "price": dec_str(price), "timeInForce": t,
            "reduceOnly": bool(reduce_only), "goodTilTime": str(int(gtt_us)), "clientTime": str(ct),
        }
        body["orderId" if order_id else "clientId"] = order_id or client_id
        return SignedRequest(body, self._sign(msg), ct, msg.encode())

    def legacy(self, action: str, body: dict, ts_ns: int | None = None) -> SignedRequest:
        """Scheme 2: sign ``ts + action + canonical_json(body)``; send exactly ``canonical_json(body)``."""
        ct = ts_ns if ts_ns is not None else self.clock.now_ns()
        msg = legacy_message(ct, action, body)
        return SignedRequest(dict(body), self._sign(msg), ct, msg)


__all__ = [
    "ArcusSigner",
    "CLIENT_ID_RE",
    "Ed25519Signer",
    "NsClock",
    "OP_CANCEL",
    "OP_MODIFY",
    "OP_PLACE",
    "SIDE_CODES",
    "SignedRequest",
    "TIF_CODES",
    "US_PER_DAY",
    "auth_headers",
    "cancel_payload",
    "canonical_json",
    "dec_str",
    "legacy_message",
    "modify_payload",
    "place_payload",
    "side_str",
    "submit_tif",
    "to_units",
    "verify_signature",
]
