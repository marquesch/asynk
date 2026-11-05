from __future__ import annotations

import asyncio

from asynk.store.memory import MemoryStore


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


async def test_get_missing_returns_none() -> None:
    assert await MemoryStore().get("k") is None


async def test_set_writes_and_returns_true() -> None:
    store = MemoryStore()
    assert await store.set("k", b"v") is True
    assert await store.get("k") == b"v"


async def test_set_without_overwrite_keeps_existing_and_returns_false() -> None:
    store = MemoryStore()
    await store.set("k", b"first")
    assert await store.set("k", b"second") is False
    assert await store.get("k") == b"first"


async def test_set_overwrite_replaces_and_returns_true() -> None:
    store = MemoryStore()
    await store.set("k", b"first")
    assert await store.set("k", b"second", overwrite=True) is True
    assert await store.get("k") == b"second"


async def test_expired_key_is_not_returned_and_can_be_set_again() -> None:
    clock = FakeClock()
    store = MemoryStore(ttl=10.0, clock=clock)
    await store.set("k", b"old")
    clock.now = 11.0
    assert await store.get("k") is None
    assert await store.set("k", b"new") is True
    assert await store.get("k") == b"new"


async def test_wait_returns_existing_value() -> None:
    store = MemoryStore()
    await store.set("k", b"v")
    assert await store.wait("k") == b"v"


async def test_wait_returns_value_set_later() -> None:
    store = MemoryStore()
    waiter = asyncio.create_task(store.wait("k"))
    await asyncio.sleep(0)
    await store.set("k", b"v")
    assert await asyncio.wait_for(waiter, 1) == b"v"


async def test_wait_times_out_with_none() -> None:
    assert await MemoryStore().wait("k", timeout=0.05) is None


async def test_wait_ignores_expired_value() -> None:
    clock = FakeClock()
    store = MemoryStore(ttl=10.0, clock=clock)
    await store.set("k", b"old")
    clock.now = 11.0
    assert await store.wait("k", timeout=0.05) is None
