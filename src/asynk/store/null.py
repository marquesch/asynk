from __future__ import annotations

from asynk.errors import NotConfiguredError


class NullStore:
    """Store for apps with no store configured.

    `set` always reports a write, `delete` reports that nothing was removed, and
    `get` and `wait` raise `NotConfiguredError`.
    """

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def get(self, key: str) -> bytes | None:
        """Raises `NotConfiguredError`."""
        raise NotConfiguredError("no store configured")

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        """Discards the value and returns True."""
        return True

    async def wait(self, key: str, timeout: float | None = None) -> bytes | None:
        """Raises `NotConfiguredError`."""
        raise NotConfiguredError("no store configured")

    async def delete(self, key: str) -> bool:
        """Returns False: nothing is stored."""
        return False
