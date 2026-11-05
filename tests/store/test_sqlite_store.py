from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from pathlib import Path

import pytest

from asynk.store.sqlite import SqliteStore


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
async def make(tmp_path: Path) -> AsyncIterator[Callable[..., SqliteStore]]:
    stores: list[SqliteStore] = []

    def factory(**kwargs: object) -> SqliteStore:
        store = SqliteStore(tmp_path / "store.db", poll_interval=0.01, **kwargs)  # type: ignore[arg-type]
        stores.append(store)
        return store

    yield factory
    for store in stores:
        await store.stop()


async def test_get_missing_returns_none(make: Callable[..., SqliteStore]) -> None:
    store = make()
    await store.start()
    assert await store.get("k") is None


async def test_set_writes_and_returns_true(make: Callable[..., SqliteStore]) -> None:
    store = make()
    await store.start()
    assert await store.set("k", b"v") is True
    assert await store.get("k") == b"v"


async def test_set_without_overwrite_keeps_existing_and_returns_false(
    make: Callable[..., SqliteStore],
) -> None:
    store = make()
    await store.start()
    await store.set("k", b"first")
    assert await store.set("k", b"second") is False
    assert await store.get("k") == b"first"


async def test_set_overwrite_replaces_and_returns_true(
    make: Callable[..., SqliteStore],
) -> None:
    store = make()
    await store.start()
    await store.set("k", b"first")
    assert await store.set("k", b"second", overwrite=True) is True
    assert await store.get("k") == b"second"


async def test_expired_key_is_not_returned_and_can_be_set_again(
    make: Callable[..., SqliteStore],
) -> None:
    clock = FakeClock()
    store = make(ttl=10.0, clock=clock)
    await store.start()
    await store.set("k", b"old")
    clock.now = 11.0
    assert await store.get("k") is None
    assert await store.set("k", b"new") is True
    assert await store.get("k") == b"new"


async def test_wait_returns_existing_value(make: Callable[..., SqliteStore]) -> None:
    store = make()
    await store.start()
    await store.set("k", b"v")
    assert await store.wait("k") == b"v"


async def test_wait_returns_value_set_later(make: Callable[..., SqliteStore]) -> None:
    store = make()
    await store.start()
    waiter = asyncio.create_task(store.wait("k"))
    await asyncio.sleep(0)
    await store.set("k", b"v")
    assert await asyncio.wait_for(waiter, 1) == b"v"


async def test_wait_times_out_with_none(make: Callable[..., SqliteStore]) -> None:
    store = make()
    await store.start()
    assert await store.wait("k", timeout=0.05) is None


async def test_wait_ignores_expired_value(make: Callable[..., SqliteStore]) -> None:
    clock = FakeClock()
    store = make(ttl=10.0, clock=clock)
    await store.start()
    await store.set("k", b"old")
    clock.now = 11.0
    assert await store.wait("k", timeout=0.05) is None


async def test_two_instances_share_one_file_atomic_claim(
    make: Callable[..., SqliteStore],
) -> None:
    first, second = make(), make()
    await first.start()
    await second.start()
    results = await asyncio.gather(first.set("k", b"a"), second.set("k", b"b"))
    assert sorted(results) == [False, True]
    assert await first.get("k") == await second.get("k")


async def test_data_survives_stop_and_new_instance(
    make: Callable[..., SqliteStore],
) -> None:
    first = make()
    await first.start()
    await first.set("k", b"v")
    await first.stop()
    second = make()
    await second.start()
    assert await second.get("k") == b"v"


async def test_get_of_missing_or_live_key_does_not_write(
    make: Callable[..., SqliteStore], tmp_path: Path
) -> None:
    import sqlite3

    store = make()
    await store.start()
    await store.set("live", b"v")
    blocker = sqlite3.connect(tmp_path / "store.db", isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")  # holds the write lock
    try:
        # a write attempt would wait for the 30s busy timeout
        assert await asyncio.wait_for(store.get("missing"), 2) is None
        assert await asyncio.wait_for(store.get("live"), 2) == b"v"
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()


async def test_expired_row_reads_as_none_and_can_be_set_again(
    make: Callable[..., SqliteStore],
) -> None:
    clock = FakeClock()
    store = make(ttl=10, clock=clock)
    await store.start()
    await store.set("k", b"old")
    clock.now = 10
    assert await store.get("k") is None
    assert await store.set("k", b"new") is True
    assert await store.get("k") == b"new"


async def test_start_twice_does_not_leak_or_raise(make: Callable[..., SqliteStore]) -> None:
    store = make()
    await store.start()
    first = store._conn
    await store.start()
    assert store._conn is not first
    assert await store.get("k") is None
    with pytest.raises(Exception):  # noqa: B017
        first.execute("SELECT 1")  # type: ignore[union-attr]
