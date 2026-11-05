from __future__ import annotations

import asyncio

import pytest

from asynk.app import App
from asynk.results import start_store, stop_store
from asynk.store.memory import MemoryStore


class _CountingStore(MemoryStore):
    def __init__(self, fail_starts: int = 0) -> None:
        super().__init__()
        self.starts = 0
        self.stops = 0
        self._fail_starts = fail_starts

    async def start(self) -> None:
        await asyncio.sleep(0)
        if self._fail_starts:
            self._fail_starts -= 1
            raise RuntimeError("start failed")
        self.starts += 1

    async def stop(self) -> None:
        self.stops += 1


async def test_concurrent_start_store_starts_once() -> None:
    store = _CountingStore()
    app = App("t", store=store)
    a, b = await asyncio.gather(start_store(app), start_store(app))
    assert a is store and b is store
    assert store.starts == 1


async def test_failed_start_leaves_store_unstarted_so_later_start_retries() -> None:
    store = _CountingStore(fail_starts=1)
    app = App("t", store=store)
    with pytest.raises(RuntimeError, match="start failed"):
        await start_store(app)
    await stop_store(app)
    assert store.stops == 0
    assert await start_store(app) is store
    assert store.starts == 1


async def test_stop_store_stops_once_and_allows_restart() -> None:
    store = _CountingStore()
    app = App("t", store=store)
    await start_store(app)
    await stop_store(app)
    await stop_store(app)
    assert store.stops == 1
    await start_store(app)
    assert store.starts == 2


async def test_stop_store_without_start_is_noop() -> None:
    store = _CountingStore()
    await stop_store(App("t", store=store))
    assert store.stops == 0


async def test_stop_store_without_store_is_noop() -> None:
    await stop_store(App("t"))
