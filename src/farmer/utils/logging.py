"""Logging setup."""

from __future__ import annotations

import logging
import sys
from pathlib import Path


def setup_logging(level: str = "INFO", log_file: Path | None = None) -> None:
    fmt = "%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt, datefmt=datefmt, handlers=handlers, force=True)
    for noisy in ("websockets", "aiohttp", "urllib3", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
