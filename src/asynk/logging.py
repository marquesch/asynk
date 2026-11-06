from __future__ import annotations

import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configure_logging(level: str | int = "INFO") -> None:
    """Configures root logging to stderr in the asynk log format.

    Args:
        level: A level name, such as "INFO", or a level number.

    Raises:
        ValueError: If `level` is not a known level name.
    """
    if isinstance(level, str):
        resolved = logging.getLevelName(level.upper())
        if not isinstance(resolved, int):
            raise ValueError(f"unknown log level: {level!r}")
        level = resolved
    logging.basicConfig(level=level, stream=sys.stderr, format=LOG_FORMAT)
