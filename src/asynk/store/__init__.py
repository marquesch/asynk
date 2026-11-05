from __future__ import annotations

from typing import Protocol


class Store(Protocol):
    """Key-value storage for task results and other small pieces of state.

    Values are bytes. Keys expire after the time-to-live the store was
    constructed with; an expired key reads as missing.
    """

    async def start(self) -> None:
        """Acquires the resources the store needs."""
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
        """Waits until a key has a value.

        Args:
            key: The key to wait for.
            timeout: Seconds to wait.

        Returns:
            The value, or None if `timeout` elapsed first.
        """
        ...
