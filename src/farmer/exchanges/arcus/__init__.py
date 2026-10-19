"""Arcus adapter package. ``from farmer.exchanges.arcus import ArcusExchange`` (lazy import)."""

from __future__ import annotations

__all__ = ["ArcusExchange"]


def __getattr__(name: str):
    if name == "ArcusExchange":
        from farmer.exchanges.arcus.client import ArcusExchange

        return ArcusExchange
    raise AttributeError(name)
