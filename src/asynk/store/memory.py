from __future__ import annotations

import asyncio
import time
from collections.abc import Callable


class MemoryStore:
    """In-memory `Store` for a single process.

    Values are not shared between processes, so it only suits tests and
    setups where producer and worker run in the same process.

    Args:
        ttl: Seconds a key lives after it is written.
        clock: Monotonic time source, replaceable for tests.
    """

    def __init__(
        self, ttl: float = 86400.0, *, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._ttl = ttl
        self._clock = clock
        self._data: dict[str, tuple[bytes, float]] = {}
        self._events: dict[str, asyncio.Event] = {}

    async def start(self) -> None:
        """Does nothing; the store needs no resources."""

    async def stop(self) -> None:
        """Does nothing; the store holds no resources."""

    async def get(self, key: str) -> bytes | None:
        """Reads a value; see `Store.get`."""
        entry = self._data.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if self._clock() >= expires_at:
            del self._data[key]
            return None
        return value

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        """Writes a value; see `Store.set`."""
        if not overwrite and await self.get(key) is not None:
            return False
        self._data[key] = (value, self._clock() + self._ttl)
        event = self._events.pop(key, None)
        if event is not None:
            event.set()
        return True

    async def wait(self, key: str, timeout: float | None = None) -> bytes | None:
        """Waits for a value using per-key events; see `Store.wait`."""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            value = await self.get(key)
            if value is not None:
                return value
            remaining = None if deadline is None else deadline - loop.time()
            if remaining is not None and remaining <= 0:
                return None
            event = self._events.setdefault(key, asyncio.Event())
            try:
                await asyncio.wait_for(event.wait(), remaining)
            except TimeoutError:
                return await self.get(key)
