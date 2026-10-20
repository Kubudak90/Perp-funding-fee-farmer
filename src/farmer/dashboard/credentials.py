"""API credentials entered in the dashboard: validation and the ``.env`` file.

Secrets are write-only: ``status`` reports whether a secret is set, never its value. ``save`` validates every
value, rewrites ``.env`` atomically with mode 0600 (other lines and comments are kept) and updates the process
environment, which ``load_config`` reads on the next start. Master wallet keys (``ARCUS_WALLET_PRIVATE_KEY`` /
``LIGHTER_ETH_PRIVATE_KEY``) are deliberately not accepted here: registering API keys stays in ``farmer keygen``.
"""

from __future__ import annotations

import contextlib
import os
import re
import tempfile
from collections.abc import Callable, MutableMapping
from dataclasses import dataclass
from pathlib import Path


def _int_in(lo: int, hi: int | None) -> Callable[[str], str]:
    def check(v: str) -> str:
        try:
            n = int(v)
        except ValueError:
            raise ValueError("tam sayı olmalı") from None
        if n < lo or (hi is not None and n > hi):
            raise ValueError(f"{lo}..{hi} aralığında olmalı" if hi is not None else f"en az {lo} olmalı")
        return str(n)
    return check


def _regex(rx: str, what: str) -> Callable[[str], str]:
    pat = re.compile(rx)

    def check(v: str) -> str:
        if not pat.fullmatch(v):
            raise ValueError(f"geçersiz {what}")
        return v
    return check


def _arcus_key(v: str) -> str:
    from farmer.exchanges.arcus.auth import Ed25519Signer

    try:
        Ed25519Signer(v)
    except ValueError:
        raise ValueError("Ed25519 özel anahtarı olmalı (64 hex karakter, 0x isteğe bağlı)") from None
    return v


def _lighter_api_key_index(v: str) -> str:
    from farmer.exchanges.lighter.keys import validate_api_key_index

    n = int(_int_in(0, 255)(v))
    try:
        validate_api_key_index(n)
    except ValueError:
        raise ValueError("4..254 aralığında olmalı (0-3 Lighter uygulamalarına ayrılmış)") from None
    return str(n)


@dataclass(frozen=True)
class Field:
    name: str
    label: str
    secret: bool
    check: Callable[[str], str]
    group: str


FIELDS: tuple[Field, ...] = (
    Field("ARCUS_ADDRESS", "Arcus cüzdan adresi (0x...)", False, _regex(r"0x[0-9a-fA-F]{40}", "adres"), "arcus"),
    Field("ARCUS_ACCOUNT_INDEX", "Arcus alt hesap numarası (0-9)", False, _int_in(0, 9), "arcus"),
    Field("ARCUS_API_PRIVATE_KEY", "Arcus API özel anahtarı", True, _arcus_key, "arcus"),
    Field("LIGHTER_ACCOUNT_INDEX", "Lighter hesap numarası", False, _int_in(0, None), "lighter"),
    Field("LIGHTER_API_KEY_INDEX", "Lighter API anahtar yuvası (4-254)", False, _lighter_api_key_index, "lighter"),
    Field("LIGHTER_API_PRIVATE_KEY", "Lighter API özel anahtarı", True,
          _regex(r"(0x)?[0-9a-fA-F]{64,128}", "Lighter anahtarı (hex)"), "lighter"),
    Field("TELEGRAM_BOT_TOKEN", "Telegram bot token", True, _regex(r"\d{3,}:[A-Za-z0-9_-]{20,}", "bot token"),
          "telegram"),
    Field("TELEGRAM_CHAT_ID", "Telegram sohbet ID", False, _regex(r"-?\d{3,}|@[A-Za-z0-9_]{4,}", "sohbet ID"),
          "telegram"),
)
BY_NAME = {f.name: f for f in FIELDS}


def status(environ: MutableMapping[str, str]) -> dict:
    """{name: {set, value (non-secret only), label, secret, group}}."""
    out = {}
    for f in FIELDS:
        v = (environ.get(f.name) or "").strip()
        out[f.name] = {"set": bool(v), "value": None if f.secret or not v else v, "label": f.label,
                       "secret": f.secret, "group": f.group}
    return out


def validate(updates: dict) -> dict[str, str | None]:
    """{name: normalized value, or None = remove}. Raises ValueError naming the first bad field."""
    clean: dict[str, str | None] = {}
    for name, raw in updates.items():
        f = BY_NAME.get(name)
        if f is None:
            raise ValueError(f"{name}: bilinmeyen alan")
        if raw is None:
            clean[name] = None
            continue
        if not isinstance(raw, (str, int)) or isinstance(raw, bool):
            raise ValueError(f"{f.label}: metin olmalı")
        v = str(raw).strip()
        if not v:
            continue  # empty field in the form = leave unchanged
        if any(c.isspace() for c in v) or "#" in v or "'" in v or '"' in v:
            raise ValueError(f"{f.label}: boşluk, tırnak veya # içeremez")
        try:
            clean[name] = f.check(v)
        except ValueError as exc:
            raise ValueError(f"{f.label}: {exc}") from None
    return clean


_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")


def write_env_file(path: Path, updates: dict[str, str | None]) -> None:
    """Replace / append / remove ``NAME=value`` lines in ``path`` (created if missing), keeping every other line.
    Written to a temporary file in the same directory with mode 0600 and renamed over the original."""
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    pending = dict(updates)
    out: list[str] = []
    for line in lines:
        m = _LINE.match(line)
        if m and m.group(1) in pending:
            v = pending.pop(m.group(1))
            if v is not None:
                out.append(f"{m.group(1)}={v}")
            continue
        out.append(line)
    out += [f"{k}={v}" for k, v in pending.items() if v is not None]
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".env.", dir=str(path.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write("\n".join(out) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise
    os.chmod(path, 0o600)


def save(path: Path, updates: dict, environ: MutableMapping[str, str]) -> list[str]:
    """Validate, write ``.env`` and update ``environ``. Returns the names changed."""
    clean = validate(updates)
    if not clean:
        return []
    write_env_file(path, clean)
    for k, v in clean.items():
        if v is None:
            environ.pop(k, None)
        else:
            environ[k] = v
    return sorted(clean)


__all__ = ["FIELDS", "save", "status", "validate", "write_env_file"]
