from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from test_scheduler import FakeClock, _at, _published, _run, _slots

from asynk.app import App
from asynk.scheduler import Scheduler
from asynk.store.memory import MemoryStore
from asynk.transport.memory import MemoryConsumer, memory_transport

SLOT = "2026-11-04T11:30:00+00:00"
CLAIM = f"claim:job:{SLOT}"


class _RecordingStore(MemoryStore):
    """MemoryStore that records lifecycle calls and claims, and can fail claims."""

    def __init__(self, *, fail_claims: bool = False) -> None:
        super().__init__()
        self.events: list[str] = []
        self.fail_claims = fail_claims

    async def start(self) -> None:
        self.events.append("start")

    async def stop(self) -> None:
        self.events.append("stop")

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        if key.startswith("claim:"):
            self.events.append(f"set {key} overwrite={overwrite}")
            if self.fail_claims:
                raise ConnectionError("store down")
        return await super().set(key, value, overwrite=overwrite)


def _app(store: Any) -> tuple[App, MemoryConsumer]:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, store=store)

    @app.scheduled_task(cron="30 * * * *", tz="UTC", name="job")
    def job() -> None: ...

    return app, consumer


def _clock() -> FakeClock:
    return FakeClock(_at("2026-11-04T11:29:00"), until=_at("2026-11-04T11:31:00"))


async def test_two_schedulers_sharing_a_store_publish_each_slot_once() -> None:
    app, consumer = _app(MemoryStore())
    clock = _clock()
    first = Scheduler(app, clock=clock)
    second = Scheduler(app, clock=clock)

    def stop_both() -> None:
        first.request_shutdown()
        second.request_shutdown()

    clock.on_limit = stop_both
    await asyncio.wait_for(asyncio.gather(first.run(), second.run()), 1)
    assert _slots(await _published(consumer)) == [("job", SLOT)]


async def test_claim_loser_skips_without_publishing(caplog: pytest.LogCaptureFixture) -> None:
    store = MemoryStore()
    await store.set(CLAIM, b"1")
    app, consumer = _app(store)
    clock = FakeClock(_at("2026-11-04T11:29:00"), until=_at("2026-11-04T12:31:00"))
    with caplog.at_level(logging.INFO, logger="asynk"):
        await _run(app, clock)
    # The claimed slot is treated as handled: the loop moves on to the next one.
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T12:30:00+00:00")]
    [record] = [r for r in caplog.records if "already claimed" in r.getMessage()]
    assert record.levelno == logging.INFO
    assert SLOT in record.getMessage()


async def test_claim_uses_idempotency_key_without_overwrite() -> None:
    store = _RecordingStore()
    app, consumer = _app(store)
    await _run(app, _clock())
    assert _slots(await _published(consumer)) == [("job", SLOT)]
    assert f"set {CLAIM} overwrite=False" in store.events
    assert await store.get(CLAIM) == b"1"


async def test_store_error_fails_open_and_still_publishes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, consumer = _app(_RecordingStore(fail_claims=True))
    with caplog.at_level(logging.ERROR, logger="asynk"):
        await _run(app, _clock())
    assert _slots(await _published(consumer)) == [("job", SLOT)]
    [record] = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], ConnectionError)


async def test_scheduler_with_null_store_always_publishes() -> None:
    app, consumer = _app(None)
    clock = _clock()
    first = Scheduler(app, clock=clock)
    second = Scheduler(app, clock=clock)

    def stop_both() -> None:
        first.request_shutdown()
        second.request_shutdown()

    clock.on_limit = stop_both
    await asyncio.wait_for(asyncio.gather(first.run(), second.run()), 1)
    assert _slots(await _published(consumer)) == [("job", SLOT), ("job", SLOT)]


async def test_scheduler_starts_and_stops_store_through_app_hooks() -> None:
    store = _RecordingStore()
    app, consumer = _app(store)

    @app.before_startup
    def user_startup(runner: Any) -> None:
        store.events.append("user.before_startup")

    @app.after_shutdown
    def user_shutdown(runner: Any) -> None:
        store.events.append("user.after_shutdown")

    await _run(app, _clock())
    assert store.events == [
        "start",
        "user.before_startup",
        f"set {CLAIM} overwrite=False",
        "stop",
        "user.after_shutdown",
    ]


async def test_store_start_failure_propagates() -> None:
    class Broken(MemoryStore):
        async def start(self) -> None:
            raise RuntimeError("start failed")

    app, consumer = _app(Broken())
    with pytest.raises(RuntimeError, match="start failed"):
        await _run(app, _clock())
    assert await _published(consumer) == []


async def test_store_stop_failure_propagates() -> None:
    class Broken(MemoryStore):
        async def stop(self) -> None:
            raise RuntimeError("stop failed")

    app, consumer = _app(Broken())
    with pytest.raises(RuntimeError, match="stop failed"):
        await _run(app, _clock())
    assert _slots(await _published(consumer)) == [("job", SLOT)]


async def test_store_stopped_even_when_a_hook_raises() -> None:
    store = _RecordingStore()
    app, _ = _app(store)

    @app.after_startup
    async def boom(runner: Any) -> None:
        raise RuntimeError("hook")

    with pytest.raises(RuntimeError, match="hook"):
        await _run(app, _clock())
    assert store.events == ["start", "stop"]
