from __future__ import annotations

import asyncio
import time
from collections.abc import Callable


class MemoryStore:
    """Store that keeps values in memory within one process."""

    def __init__(
        self, ttl: float = 86400.0, *, clock: Callable[[], float] = time.monotonic
    ) -> None:
        """Initializes the store.

        Args:
            ttl: Seconds a key lives after it is written.
            clock: Monotonic time source, replaceable for tests.
        """
        self._ttl = ttl
        self._clock = clock
        # Every entry has the same ttl, so insertion order is expiry order.
        self._data: dict[str, tuple[bytes, float]] = {}
        self._events: dict[str, asyncio.Event] = {}
        self._waiters: dict[str, int] = {}

    async def start(self) -> None:
        """Does nothing."""

    async def stop(self) -> None:
        """Does nothing."""

    def _purge(self) -> None:
        now = self._clock()
        while self._data:
            key = next(iter(self._data))
            if now < self._data[key][1]:
                break
            del self._data[key]

    async def get(self, key: str) -> bytes | None:
        """Reads a value, or returns None if the key is missing or expired."""
        self._purge()
        entry = self._data.get(key)
        return None if entry is None else entry[0]

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        """Writes a value; returns False if the key exists and `overwrite` is false."""
        self._purge()
        if key in self._data:
            if not overwrite:
                return False
            del self._data[key]  # re-insert at the end to keep expiry order
        self._data[key] = (value, self._clock() + self._ttl)
        event = self._events.pop(key, None)
        if event is not None:
            event.set()
        return True

    async def delete(self, key: str) -> bool:
        """Removes a key; returns False if it is missing or expired."""
        if await self.get(key) is None:
            return False
        del self._data[key]
        return True

    async def wait(self, key: str, timeout: float | None = None) -> bytes | None:
        """Waits until the key has a value; returns None if `timeout` elapses first."""
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
            self._waiters[key] = self._waiters.get(key, 0) + 1
            try:
                async with asyncio.timeout(remaining):
                    await event.wait()
            except TimeoutError:
                return await self.get(key)
            finally:
                # `set` may have popped the event already; the last waiter out
                # removes whatever event is left, which only waiters could have made.
                self._waiters[key] -= 1
                if self._waiters[key] == 0:
                    del self._waiters[key]
                    self._events.pop(key, None)
