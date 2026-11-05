from __future__ import annotations

from typing import Protocol


class Store(Protocol):
    """A key-value store for task results and other small pieces of state.

    Values are bytes. Keys expire after a time-to-live fixed when the store is
    constructed; an expired key is never returned by `get` or `wait`.
    """

    async def start(self) -> None:
        """Acquires any resources the store needs (connections, files)."""
        ...

    async def stop(self) -> None:
        """Releases the resources acquired by `start`."""
        ...

    async def get(self, key: str) -> bytes | None:
        """Reads a value.

        Args:
            key: The key to read.

        Returns:
            The stored value, or None if the key is missing or has expired.
        """
        ...

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        """Writes a value.

        An existing, unexpired key is only replaced when `overwrite` is true.

        Args:
            key: The key to write.
            value: The value to store.
            overwrite: Whether to replace an existing value.

        Returns:
            True if the value was written, False if the key already existed
            and `overwrite` was false.
        """
        ...

    async def wait(self, key: str, timeout: float | None = None) -> bytes | None:
        """Blocks until a key has a value.

        This exists so `TaskHandle.result()` can block until a task's result
        is stored. A store that cannot support it must raise a clear error
        or document that it is a no-op.

        Args:
            key: The key to wait for.
            timeout: Seconds to wait, or None to wait indefinitely.

        Returns:
            The value, or None if `timeout` elapsed first.
        """
        ...
