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


async def test_delete_existing_key_removes_it_and_returns_true() -> None:
    store = MemoryStore()
    await store.set("k", b"v")
    assert await store.delete("k") is True
    assert await store.get("k") is None
    assert await store.set("k", b"new") is True


async def test_delete_missing_key_returns_false() -> None:
    assert await MemoryStore().delete("k") is False


async def test_delete_expired_key_returns_false() -> None:
    clock = FakeClock()
    store = MemoryStore(ttl=10.0, clock=clock)
    await store.set("k", b"v")
    clock.now = 11.0
    assert await store.delete("k") is False


# The tests below look at the private `_data` and `_events` attributes: the
# leaks they guard against are not observable through the public interface.


async def test_unread_expired_keys_are_purged_by_later_operations() -> None:
    clock = FakeClock()
    store = MemoryStore(ttl=10.0, clock=clock)
    await store.set("a", b"1")
    await store.set("b", b"2")
    clock.now = 11.0
    await store.get("other")
    assert store._data == {}


async def test_set_purges_expired_keys() -> None:
    clock = FakeClock()
    store = MemoryStore(ttl=10.0, clock=clock)
    await store.set("a", b"1")
    clock.now = 11.0
    await store.set("b", b"2")
    assert list(store._data) == ["b"]


async def test_wait_purges_expired_keys() -> None:
    clock = FakeClock()
    store = MemoryStore(ttl=10.0, clock=clock)
    await store.set("a", b"1")
    clock.now = 11.0
    assert await store.wait("b", timeout=0.01) is None
    assert store._data == {}


async def test_rewritten_key_moves_to_the_end_and_expires_later() -> None:
    clock = FakeClock()
    store = MemoryStore(ttl=10.0, clock=clock)
    await store.set("a", b"1")
    clock.now = 5.0
    await store.set("b", b"2")
    await store.set("a", b"3", overwrite=True)
    clock.now = 12.0  # "b" (written at 5) is alive; so is "a" rewritten at 5
    assert await store.get("a") == b"3"
    assert await store.get("b") == b"2"
    clock.now = 16.0
    assert await store.get("a") is None
    assert store._data == {}


async def test_purge_stops_at_first_live_entry() -> None:
    clock = FakeClock()
    store = MemoryStore(ttl=10.0, clock=clock)
    await store.set("a", b"1")
    clock.now = 5.0
    await store.set("b", b"2")
    clock.now = 11.0
    await store.get("x")
    assert list(store._data) == ["b"]


async def test_wait_timeout_leaves_no_event_behind() -> None:
    store = MemoryStore()
    assert await store.wait("k", timeout=0.01) is None
    assert store._events == {}


async def test_wait_success_leaves_no_event_behind() -> None:
    store = MemoryStore()
    waiter = asyncio.create_task(store.wait("k"))
    await asyncio.sleep(0)
    await store.set("k", b"v")
    await waiter
    assert store._events == {}


async def test_timed_out_waiter_does_not_stop_set_from_waking_the_other() -> None:
    store = MemoryStore()
    patient = asyncio.create_task(store.wait("k"))
    impatient = asyncio.create_task(store.wait("k", timeout=0.01))
    assert await impatient is None
    await store.set("k", b"v")
    assert await asyncio.wait_for(patient, 1) == b"v"
    assert store._events == {}


async def test_waiters_arriving_after_a_set_are_woken_by_the_next_one() -> None:
    clock = FakeClock()
    store = MemoryStore(ttl=10.0, clock=clock)
    first = asyncio.create_task(store.wait("k"))
    await asyncio.sleep(0)
    await store.set("k", b"v")  # pops the event; `first` has not run yet
    clock.now = 11.0  # the value expires before `first` sees it
    second = asyncio.create_task(store.wait("k"))
    await asyncio.sleep(0.01)
    assert not first.done() and not second.done()
    await store.set("k", b"new")
    assert await asyncio.wait_for(first, 1) == b"new"
    assert await asyncio.wait_for(second, 1) == b"new"
    assert store._events == {}
