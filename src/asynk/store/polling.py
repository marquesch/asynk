from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable


async def poll_wait(
    get: Callable[[str], Awaitable[bytes | None]],
    key: str,
    timeout: float | None,
    interval: float,
) -> bytes | None:
    """Implements `Store.wait` for stores that can only poll.

    Args:
        get: Coroutine function that reads a key, like `Store.get`.
        key: The key to wait for.
        timeout: Seconds to wait, or None to wait indefinitely.
        interval: Seconds to sleep between reads.

    Returns:
        The value, or None if `timeout` elapsed first.
    """
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        value = await get(key)
        if value is not None:
            return value
        if deadline is None:
            await asyncio.sleep(interval)
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        await asyncio.sleep(min(interval, remaining))
