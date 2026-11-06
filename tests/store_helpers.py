"""Test helpers for the store lifecycle shared by the worker and scheduler tests."""

from __future__ import annotations

import pytest

from asynk.store.memory import MemoryStore


class StrictStore(MemoryStore):
    """MemoryStore that rejects reads and writes outside `start`/`stop`."""

    def __init__(self) -> None:
        super().__init__()
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def get(self, key: str) -> bytes | None:
        if not self.started:
            raise RuntimeError("store not started")
        return await super().get(key)

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        if not self.started:
            raise RuntimeError("store not started")
        return await super().set(key, value, overwrite=overwrite)


class FailingStopStore(MemoryStore):
    """MemoryStore whose `stop` raises `ConnectionError`."""

    async def stop(self) -> None:
        raise ConnectionError("stop failed")


def assert_store_stop_failure_logged(caplog: pytest.LogCaptureFixture) -> None:
    """Asserts that one failing `stop` of the store was logged with its error."""
    [record] = [r for r in caplog.records if r.getMessage() == "failed to stop the store"]
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], ConnectionError)
