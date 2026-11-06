"""`poll`, a helper for stores that can only check a key, not wait on it."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable


async def poll(
    fetch: Callable[[], Awaitable[bytes | None]], *, timeout: float | None, max_interval: float
) -> bytes | None:
    """Calls `fetch` until it returns a value, pausing longer after each miss.

    Args:
        fetch: Coroutine function that returns a value, or None if there is none yet.
        timeout: Seconds to keep trying, or None to try until there is a value.
        max_interval: Longest pause in seconds between two calls.

    Returns:
        The value, or None if `timeout` elapsed first.
    """
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    pause = min(0.05, max_interval)
    while True:
        value = await fetch()
        if value is not None:
            return value
        remaining = None if deadline is None else deadline - loop.time()
        if remaining is not None and remaining <= 0:
            return None
        await asyncio.sleep(pause if remaining is None else min(pause, remaining))
        pause = min(pause * 2, max_interval)
