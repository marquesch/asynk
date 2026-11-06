from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from test_scheduler import BlockingClock, FakeClock, HangingProducer, _at, _published, _run, _slots
from test_worker_store import _StrictStore

from asynk.app import App
from asynk.runner import Runner
from asynk.scheduler import Scheduler
from asynk.store.memory import MemoryStore
from asynk.transport.memory import MemoryConsumer, memory_transport

SLOT = "2026-11-04T11:30:00+00:00"
CLAIM = f"claim:job:{SLOT}"


class _RecordingStore(MemoryStore):
    """MemoryStore that records lifecycle calls and claims, and can fail claims."""

    def __init__(self, *, fail_claims: bool = False, fail_deletes: bool = False) -> None:
        super().__init__()
        self.events: list[str] = []
        self.fail_claims = fail_claims
        self.fail_deletes = fail_deletes

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

    async def delete(self, key: str) -> bool:
        self.events.append(f"delete {key}")
        if self.fail_deletes:
            raise ConnectionError("store down")
        return await super().delete(key)


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


async def test_scheduler_owns_the_store_lifecycle() -> None:
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
        "user.after_shutdown",
        "stop",
    ]


async def test_failing_before_startup_hook_stops_the_store() -> None:
    store = _RecordingStore()
    app, consumer = _app(store)

    @app.before_startup
    def no_telemetry(runner: Any) -> None:
        raise RuntimeError("no telemetry")

    @app.after_shutdown
    def user_shutdown(runner: Any) -> None:
        store.events.append("user.after_shutdown")

    with pytest.raises(RuntimeError):
        await _run(app, _clock())
    assert store.events == ["start", "stop"]
    assert await _published(consumer) == []


async def test_cancelling_run_during_startup_stops_the_store() -> None:
    store = _RecordingStore()
    app, _ = _app(store)
    entered = asyncio.Event()

    @app.before_startup
    async def hang(runner: Any) -> None:
        entered.set()
        await asyncio.Event().wait()

    @app.after_shutdown
    def user_shutdown(runner: Any) -> None:
        store.events.append("user.after_shutdown")

    run = asyncio.create_task(Scheduler(app, clock=_clock()).run())
    await asyncio.wait_for(entered.wait(), 1)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert store.events == ["start", "stop"]


async def test_cancelling_a_started_scheduler_runs_after_shutdown_then_stops_the_store() -> None:
    store = _RecordingStore()
    app, _ = _app(store)
    clock = BlockingClock(_at("2026-11-04T11:29:30"))

    @app.after_shutdown
    def user_shutdown(runner: Any) -> None:
        store.events.append("user.after_shutdown")

    run = asyncio.create_task(Scheduler(app, clock=clock).run())
    await asyncio.wait_for(clock.sleeping.wait(), 1)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert store.events == ["start", "user.after_shutdown", "stop"]


async def test_hooks_use_the_started_store() -> None:
    store = _StrictStore()
    app, _ = _app(store)
    seen: list[bytes | None] = []

    @app.before_startup
    async def write(runner: Runner) -> None:
        await runner.app.store.set("hook", b"up")

    @app.after_shutdown
    async def read(runner: Runner) -> None:
        seen.append(await runner.app.store.get("hook"))

    await _run(app, _clock())
    assert seen == [b"up"]
    assert store.started is False


async def test_store_start_failure_does_not_stop_the_store() -> None:
    class Broken(_RecordingStore):
        async def start(self) -> None:
            self.events.append("start")
            raise RuntimeError("start failed")

    store = Broken()
    app, _ = _app(store)

    @app.after_shutdown
    def user_shutdown(runner: Any) -> None:
        store.events.append("user.after_shutdown")

    with pytest.raises(RuntimeError, match="start failed"):
        await _run(app, _clock())
    assert store.events == ["start"]


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


class _FailingProducer:
    """Producer whose first `failures` publishes raise."""

    def __init__(self, inner: Any, failures: int = 1_000) -> None:
        self.inner = inner
        self.failures = failures

    async def publish(self, envelope: Any, body: bytes) -> Any:
        if self.failures > 0:
            self.failures -= 1
            raise ConnectionError("broker down")
        return await self.inner.publish(envelope, body)


async def test_failed_publish_releases_the_claim() -> None:
    store = _RecordingStore()
    app, consumer = _app(store)
    app.producer = _FailingProducer(app.producer)
    await _run(app, _clock())
    assert await _published(consumer) == []
    assert store.events[1:-1] == [f"set {CLAIM} overwrite=False", f"delete {CLAIM}"]
    assert await store.get(CLAIM) is None


async def test_self_cancelled_publish_releases_the_claim() -> None:
    class SelfCancelling:
        async def publish(self, envelope: Any, body: bytes) -> Any:
            raise asyncio.CancelledError

    store = _RecordingStore()
    app, _ = _app(store)
    app.producer = SelfCancelling()
    await _run(app, _clock())
    assert store.events[1:-1] == [f"set {CLAIM} overwrite=False", f"delete {CLAIM}"]


async def test_shutdown_cancelling_the_publish_releases_the_claim() -> None:
    store = _RecordingStore()
    app, _ = _app(store)
    producer = HangingProducer(app.producer, ok=0)
    app.producer = producer
    scheduler = Scheduler(
        app, clock=FakeClock(_at("2026-11-04T11:29:30"), _at("2026-11-04T12:00:00"))
    )
    run = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(producer.hanging.wait(), 1)
    scheduler.request_shutdown()
    await asyncio.wait_for(run, 1)
    assert store.events[1:-1] == [f"set {CLAIM} overwrite=False", f"delete {CLAIM}"]
    assert await store.get(CLAIM) is None


async def test_external_cancellation_of_run_releases_the_claim() -> None:
    store = _RecordingStore()
    app, _ = _app(store)
    producer = HangingProducer(app.producer, ok=0)
    app.producer = producer
    scheduler = Scheduler(
        app, clock=FakeClock(_at("2026-11-04T11:29:30"), _at("2026-11-04T12:00:00"))
    )
    run = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(producer.hanging.wait(), 1)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert await store.get(CLAIM) is None


async def test_another_scheduler_can_publish_a_released_slot() -> None:
    store = MemoryStore()
    app, consumer = _app(store)
    failing = _FailingProducer(app.producer)
    app.producer = failing
    await _run(app, _clock())
    failing.failures = 0
    await _run(app, _clock())
    assert _slots(await _published(consumer)) == [("job", SLOT)]


async def test_failing_release_is_logged_and_the_schedule_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = _RecordingStore(fail_deletes=True)
    app, consumer = _app(store)
    app.producer = _FailingProducer(app.producer, failures=1)
    clock = FakeClock(_at("2026-11-04T11:29:00"), until=_at("2026-11-04T12:31:00"))
    with caplog.at_level(logging.ERROR, logger="asynk"):
        await _run(app, clock)
    assert _slots(await _published(consumer)) == [("job", "2026-11-04T12:30:00+00:00")]
    assert any(
        r.exc_info is not None
        and isinstance(r.exc_info[1], ConnectionError)
        and "release" in r.getMessage()
        for r in caplog.records
    )
    assert "delete " + CLAIM in store.events


async def test_no_release_when_the_claim_write_failed() -> None:
    store = _RecordingStore(fail_claims=True)
    app, _ = _app(store)
    app.producer = _FailingProducer(app.producer)
    await _run(app, _clock())
    assert not [e for e in store.events if e.startswith("delete")]


async def test_published_slot_keeps_its_claim() -> None:
    store = _RecordingStore()
    app, consumer = _app(store)
    await _run(app, _clock())
    assert _slots(await _published(consumer)) == [("job", SLOT)]
    assert not [e for e in store.events if e.startswith("delete")]
    assert await store.get(CLAIM) == b"1"
