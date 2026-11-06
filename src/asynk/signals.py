"""Signal handling for graceful shutdown."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable, Iterator
from contextlib import contextmanager

_SHUTDOWN_SIGNALS = (signal.SIGTERM, signal.SIGINT)


@contextmanager
def shutdown_signals(request_shutdown: Callable[[], None]) -> Iterator[None]:
    """Makes SIGTERM and SIGINT call `request_shutdown` on the running loop.

    The previous handlers are restored on exit.

    Args:
        request_shutdown: Called when a signal arrives.

    Yields:
        None, while the handlers are installed.
    """
    loop = asyncio.get_running_loop()
    previous = {sig: signal.getsignal(sig) for sig in _SHUTDOWN_SIGNALS}
    for sig in _SHUTDOWN_SIGNALS:
        loop.add_signal_handler(sig, request_shutdown)
    try:
        yield
    finally:
        for sig, handler in previous.items():
            loop.remove_signal_handler(sig)
            signal.signal(sig, handler)
