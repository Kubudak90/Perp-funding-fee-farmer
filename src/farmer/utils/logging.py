"""Logging setup."""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import TextIO


def setup_logging(level: str = "INFO", log_file: Path | None = None, *, console: TextIO | None = None,
                  console_level: str | None = None) -> None:
    """Root logging: console (``console`` stream, default stdout) + optional rotating ``log_file``.

    ``console_level`` lets table-printing commands keep the console quiet while the file still gets ``level``.
    """
    fmt = "%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    lvl = getattr(logging, level.upper(), logging.INFO)
    stream = logging.StreamHandler(console if console is not None else sys.stdout)
    stream.setLevel(getattr(logging, console_level.upper(), lvl) if console_level else lvl)
    handlers: list[logging.Handler] = [stream]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(log_file, maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8")
        fh.setLevel(lvl)
        handlers.append(fh)
    root_level = min(h.level for h in handlers) if handlers else lvl
    logging.basicConfig(level=root_level, format=fmt, datefmt=datefmt, handlers=handlers, force=True)
    for noisy in ("websockets", "aiohttp", "urllib3", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
