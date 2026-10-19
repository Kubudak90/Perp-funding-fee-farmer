"""Decimal helpers."""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal, InvalidOperation


def D(x) -> Decimal:
    """Convert str/int/float/Decimal to Decimal safely (floats via str to avoid binary noise)."""
    if isinstance(x, Decimal):
        return x
    if isinstance(x, float):
        return Decimal(repr(x))
    if x is None or x == "":
        raise InvalidOperation(f"cannot convert {x!r} to Decimal")
    return Decimal(str(x))


def D_or_none(x) -> Decimal | None:
    try:
        return D(x)
    except (InvalidOperation, ValueError, TypeError):
        return None


def common_step(a: Decimal, b: Decimal) -> Decimal:
    """Smallest positive size that is an integer multiple of both steps (decimal LCM)."""
    if a <= 0 or b <= 0:
        raise ValueError("steps must be positive")
    exp = max(-a.as_tuple().exponent, -b.as_tuple().exponent, 0)
    scale = Decimal(10) ** exp
    ia, ib = int(a * scale), int(b * scale)
    from math import gcd

    l = ia * ib // gcd(ia, ib)
    return Decimal(l) / scale


def round_down(x: Decimal, step: Decimal) -> Decimal:
    if x <= 0:
        return Decimal(0)
    return (x / step).to_integral_value(rounding=ROUND_DOWN) * step


def fmt(x: Decimal | None, places: int = 8) -> str:
    """Human formatting without exponent notation, at most ``places`` decimals."""
    if x is None:
        return "-"
    s = format(x.quantize(Decimal(1).scaleb(-places)), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return "0" if s in ("", "-0") else s


def to_plain(x: Decimal) -> str:
    """Decimal -> plain string for venue APIs (no exponent, no trailing zeros)."""
    s = format(x.normalize(), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s not in ("", "-0") else "0"
