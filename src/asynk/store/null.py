from __future__ import annotations

from asynk.errors import NotConfiguredError


class NullStore:
    """Store for apps with no store configured.

    `set` always reports a write, and `get` and `wait` raise `NotConfiguredError`.
    """

    async def start(self) -> None:
        """Does nothing."""

    async def stop(self) -> None:
        """Does nothing."""

    async def get(self, key: str) -> bytes | None:
        """Raises `NotConfiguredError`."""
        raise NotConfiguredError("no store configured")

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        """Discards the value and returns True."""
        return True

    async def wait(self, key: str, timeout: float | None = None) -> bytes | None:
        """Raises `NotConfiguredError`."""
        raise NotConfiguredError("no store configured")
