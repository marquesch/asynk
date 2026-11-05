from __future__ import annotations

import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configure_logging(level: str | int = "INFO") -> None:
    """Configure root logging for asynk CLI processes (stderr, shared format)."""
    if isinstance(level, str):
        resolved = logging.getLevelName(level.upper())
        if not isinstance(resolved, int):
            raise ValueError(f"unknown log level: {level!r}")
        level = resolved
    logging.basicConfig(level=level, stream=sys.stderr, format=LOG_FORMAT)
