"""Settings edited in the dashboard: a curated subset of ``config.yaml``.

``current`` returns the effective values (file + defaults). ``save`` applies a patch to the raw YAML, validates the
whole result with ``AppConfig`` (nothing is written when it fails), keeps a ``.bak`` copy of the previous file and
writes the new one atomically. Comments of a hand-written ``config.yaml`` are not preserved (the backup keeps
them; every field is documented in ``config.example.yaml``). Changes apply to the next bot start.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from farmer.config import AppConfig, load_config

# (section, field, kind, label); kind: decimal | int | float | bool | choice:a,b,c
FIELDS: tuple[tuple[str, str, str, str], ...] = (
    ("strategy", "target_notional", "decimal", "Hedef pozisyon (USD, bacak başına)"),
    ("strategy", "order_notional", "decimal", "Maker emir büyüklüğü / clip (USD)"),
    ("strategy", "leverage", "int", "Kaldıraç (iki borsada)"),
    ("strategy", "min_edge_bps", "decimal", "Minimum kâr, bps (negatif = hacim için küçük maliyet)"),
    ("strategy", "max_slippage_bps", "decimal", "Hedge en kötü fiyat kayması, bps"),
    ("strategy", "direction_mode", "choice:auto,long_maker,short_maker", "Yön (auto = funding'e göre)"),
    ("strategy", "churn", "bool", "Aç / tut / kapat döngüsü (hacim üretir)"),
    ("strategy", "churn_floor_frac", "decimal", "Kapanışta açık kalacak oran (0 = tamamen kapat)"),
    ("strategy", "hold_seconds", "float", "Hedefte en az bekleme (sn)"),
    ("strategy", "hold_through_funding", "bool", "Funding ödemesine kadar pozisyonu tut"),
    ("strategy", "protect_funding_window_s", "float", "Funding öncesi kapanış erteleme penceresi (sn)"),
    ("risk", "max_unhedged_notional", "decimal", "Hedgelenmemiş delta sınırı (USD)"),
    ("risk", "max_total_notional", "decimal", "Borsa başına toplam pozisyon sınırı (USD)"),
    ("risk", "max_daily_loss", "decimal", "Günlük zarar limiti (USD, 0 = kapalı)"),
    ("risk", "max_margin_ratio", "decimal", "Yeni pozisyon açmama marj oranı"),
    ("risk", "reduce_margin_ratio", "decimal", "Durup her şeyi kapatma marj oranı"),
    ("risk", "min_free_collateral_mult", "decimal", "Serbest teminat çarpanı"),
    ("risk", "max_consecutive_errors", "int", "Devre kesici: art arda hata sayısı"),
    ("risk", "error_halt_cooldown_s", "float", "Devre kesici otomatik devam süresi (sn, 0 = kapalı)"),
    ("risk", "close_on_exit", "bool", "Kapanışta iki bacağı da kapat"),
    ("risk", "skip_rwa_off_hours", "bool", "RWA piyasalarında yalnızca işlem saatlerinde kotasyon"),
    ("monitoring", "report_interval_s", "float", "Rapor aralığı (sn)"),
    ("monitoring", "notify_every", "int", "Her N. raporu Telegram'a gönder"),
    ("paper", "initial_balance", "decimal", "Paper başlangıç bakiyesi (USD, borsa başına)"),
)
_BY_KEY = {(s, f): k for s, f, k, _ in FIELDS}
# per-market overrides (empty = the global strategy value)
MARKET_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("symbol", "str", "Sembol"),
    ("enabled", "bool", "Açık"),
    ("target_notional", "decimal", "Hedef (USD)"),
    ("order_notional", "decimal", "Clip (USD)"),
    ("leverage", "int", "Kaldıraç"),
    ("min_edge_bps", "decimal", "Min. kâr (bps)"),
)


def _plain(v: Any) -> Any:
    if isinstance(v, Decimal):
        return str(v)
    return v


def schema() -> dict:
    return {"fields": [{"section": s, "name": f, "kind": k, "label": lbl} for s, f, k, lbl in FIELDS],
            "market_fields": [{"name": f, "kind": k, "label": lbl} for f, k, lbl in MARKET_FIELDS]}


def current(path: Path) -> dict:
    """Effective values of the editable fields (file + defaults), the configured markets and the config's
    consistency warnings (``AppConfig.warnings``)."""
    cfg = load_config(path if path.exists() else None, env_file=None, environ={}, warn=False)
    values = {s: {} for s, *_ in FIELDS}
    for s, f, _, _ in FIELDS:
        values[s][f] = _plain(getattr(getattr(cfg, s), f))
    markets = [{f: _plain(getattr(m, f)) for f, _, _ in MARKET_FIELDS} for m in cfg.strategy.markets]
    return {"values": values, "markets": markets, "path": str(path), "exists": path.exists(),
            "warnings": cfg.warnings()}


def _coerce(kind: str, v: Any, label: str) -> Any:
    try:
        if kind == "decimal":
            if isinstance(v, bool) or v is None or str(v).strip() == "":
                raise ValueError
            d = Decimal(str(v).strip().replace(",", "."))
            if not d.is_finite():
                raise ValueError
            return d
        if kind == "int":
            if isinstance(v, bool):
                raise ValueError
            return int(str(v).strip())
        if kind == "float":
            if isinstance(v, bool):
                raise ValueError
            return float(str(v).strip().replace(",", "."))
        if kind == "bool":
            if not isinstance(v, bool):
                raise ValueError
            return v
        if kind.startswith("choice:"):
            if v not in kind[7:].split(","):
                raise ValueError
            return v
        if kind == "str":
            s = str(v).strip().upper()
            if not s or not s.replace("_", "").isalnum():
                raise ValueError
            return s
    except (ValueError, InvalidOperation, TypeError):
        raise ValueError(f"{label}: geçersiz değer {v!r}") from None
    raise ValueError(f"{label}: bilinmeyen tür {kind}")


def _markets(rows: Any) -> list[dict]:
    if not isinstance(rows, list) or not rows:
        raise ValueError("en az bir piyasa gerekli")
    out = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("piyasa satırı geçersiz")
        m: dict[str, Any] = {}
        for f, kind, label in MARKET_FIELDS:
            v = row.get(f)
            if v is None or (isinstance(v, str) and not v.strip()):
                if f == "symbol":
                    raise ValueError("her piyasanın bir sembolü olmalı")
                continue
            m[f] = _coerce(kind, v, f"{row.get('symbol') or '?'} {label}")
        out.append(m)
    return out


class _Dumper(yaml.SafeDumper):
    pass


def _repr_decimal(dumper: yaml.SafeDumper, d: Decimal) -> yaml.Node:
    if d == d.to_integral_value():
        return dumper.represent_int(int(d))
    return dumper.represent_scalar("tag:yaml.org,2002:float", format(d.normalize(), "f"))


_Dumper.add_representer(Decimal, _repr_decimal)

HEADER = ("# farmer paneli tarafından yazıldı. Önceki dosya: config.yaml.bak\n"
          "# Tüm alanların açıklaması: config.example.yaml\n")


def save(path: Path, patch: dict) -> dict:
    """Apply ``patch`` ({section: {field: value}}, optional "markets": [...]) to ``path``. Raises ValueError (with
    a readable message) and writes nothing when a value or the resulting config is invalid."""
    if not isinstance(patch, dict):
        raise ValueError("geçersiz istek")
    raw: dict = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}) if path.exists() else {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path} bir YAML sözlüğü değil")
    for section, fields in patch.items():
        if section == "markets":
            raw.setdefault("strategy", {})["markets"] = _markets(fields)
            continue
        if not isinstance(fields, dict):
            raise ValueError(f"{section}: geçersiz bölüm")
        for name, v in fields.items():
            kind = _BY_KEY.get((section, name))
            if kind is None:
                raise ValueError(f"{section}.{name}: panelden değiştirilemez")
            raw.setdefault(section, {})[name] = _coerce(kind, v, f"{section}.{name}")
    try:
        AppConfig.model_validate(raw)
    except ValidationError as exc:
        e = exc.errors()[0]
        loc = ".".join(str(x) for x in e.get("loc", ()))
        raise ValueError(f"{loc}: {e.get('msg')}" if loc else str(e.get("msg"))) from None
    text = HEADER + yaml.dump(raw, Dumper=_Dumper, sort_keys=False, allow_unicode=True, default_flow_style=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        shutil.copy2(path, path.with_name(path.name + ".bak"))
    fd, tmp = tempfile.mkstemp(prefix=".config.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    return current(path)


__all__ = ["FIELDS", "MARKET_FIELDS", "current", "save", "schema"]
