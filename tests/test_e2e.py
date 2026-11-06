from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime, timedelta

import asynk
from asynk import (
    App,
    Delivery,
    Envelope,
    Next,
    PublishOptions,
    Runner,
    TaskCall,
    TaskResult,
    Worker,
)
from asynk.scheduler import Scheduler
from asynk.transport.memory import memory_transport

NAMES = [
    "App",
    "TaskCall",
    "Next",
    "Task",
    "Envelope",
    "Codec",
    "JsonCodec",
    "Producer",
    "PublishOptions",
    "RetryPolicy",
    "Exponential",
    "Backoff",
    "Consumer",
    "Delivery",
    "TaskStatus",
    "TaskResult",
    "Worker",
    "__version__",
    "EncodeError",
    "NotConfiguredError",
    "DeliveryFailedError",
    "AsynkError",
    "CronError",
    "DecodeError",
    "InvalidEnvelopeError",
    "ScheduledTask",
    "Scheduler",
    "Runner",
    "Store",
    "MemoryStore",
    "NullStore",
    "TaskHandle",
    "TaskOptions",
    "TaskState",
    "TaskError",
    "TaskNotFoundError",
    "TaskTimeoutError",
]


def test_import_surface() -> None:
    assert sorted(asynk.__all__) == sorted(NAMES)
    for name in NAMES:
        assert getattr(asynk, name) is not None


async def test_delay_to_worker_roundtrip() -> None:
    producer, consumer = memory_transport()
    app = App("e2e", producer=producer, consumer=consumer)
    done = asyncio.Event()
    seen: list[tuple[int, int]] = []

    @app.task()
    async def add(a: int, *, b: int) -> None:
        seen.append((a, b))
        done.set()

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    await add.delay(2, b=3)
    await asyncio.wait_for(done.wait(), 5)
    worker.request_shutdown()
    await asyncio.wait_for(run, 5)
    assert seen == [(2, 3)]


class ListProducer:
    def __init__(self, wire: list[str]) -> None:
        self._wire = wire

    async def publish(
        self, envelope: Envelope, body: bytes, options: PublishOptions | None = None
    ) -> str:
        self._wire.append(body.decode())
        return envelope.id


class _ListDelivery:
    def __init__(self, body: bytes) -> None:
        self.body_ = body
        self.settled_ = False

    @property
    def body(self) -> bytes:
        return self.body_

    @property
    def settled(self) -> bool:
        return self.settled_

    async def ack(self) -> None:
        self.settled_ = True

    async def reject(self, *, requeue: bool = False) -> None:
        self.settled_ = True


class ListConsumer:
    def __init__(self, wire: list[str]) -> None:
        self._wire = wire
        self._connected = False
        self._stopped = asyncio.Event()

    @property
    def connected(self) -> bool:
        return self._connected

    async def start(self) -> None:
        self._connected = True

    async def stop(self) -> None:
        self._connected = False
        self._stopped.set()

    async def deliveries(self) -> AsyncIterator[Delivery]:
        while not self._stopped.is_set():
            if self._wire:
                yield _ListDelivery(self._wire.pop(0).encode())
            else:
                await asyncio.sleep(0)

    def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]:
        return self._process(delivery)

    @asynccontextmanager
    async def _process(self, delivery: Delivery) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if not delivery.settled:
                await delivery.ack()


async def test_custom_producer_and_consumer_are_decoupled() -> None:
    wire: list[str] = []
    app = App("custom", producer=ListProducer(wire), consumer=ListConsumer(wire))
    done = asyncio.Event()
    loop = asyncio.get_running_loop()
    seen: list[str] = []

    @app.task()
    async def greet(name: str) -> None:
        seen.append(name)
        # Sync tasks run in a worker thread; asyncio.Event is not thread-safe.
        loop.call_soon_threadsafe(done.set)

    await greet.delay("ada")
    assert json.loads(wire[0])["task"] == greet.name

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(done.wait(), 5)
    worker.request_shutdown()
    await asyncio.wait_for(run, 5)
    assert seen == ["ada"]


class _StepClock:
    """Fake clock: `sleep` advances time; stops the scheduler once `until` is reached."""

    def __init__(self, start: datetime, until: datetime) -> None:
        self.value = start
        self.until = until
        self.on_limit: Callable[[], None] = lambda: None

    def now(self) -> datetime:
        return self.value

    async def sleep(self, seconds: float) -> None:
        if self.value >= self.until:
            self.on_limit()
            await asyncio.Event().wait()
        self.value += timedelta(seconds=seconds)
        await asyncio.sleep(0)


async def test_scheduler_publishes_and_worker_runs() -> None:
    producer, consumer = memory_transport()
    app = App("e2e-scheduled-task", producer=producer, consumer=consumer)
    runs: list[int] = []
    seen_keys: set[str] = set()
    keys: list[str] = []
    done = asyncio.Event()

    @app.middleware
    async def dedupe(call: TaskCall, next: Next) -> TaskResult:
        key = str(call.envelope.metadata["idempotency_key"])
        keys.append(key)
        if key in seen_keys:
            result = call.skip()
        else:
            seen_keys.add(key)
            result = await next(call)
        if len(keys) == 2:
            done.set()
        return result

    @app.scheduled_task(cron="30 * * * *", tz="UTC", args=[7], name="job")
    async def job(n: int) -> None:
        runs.append(n)

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    # Two schedulers over the same app simulate an accidental duplicate scheduler.
    schedulers = []
    for _ in range(2):
        clock = _StepClock(
            datetime(2026, 11, 4, 11, 29, tzinfo=UTC),
            datetime(2026, 11, 4, 11, 31, tzinfo=UTC),
        )
        scheduler = Scheduler(app, clock=clock)
        clock.on_limit = scheduler.request_shutdown
        schedulers.append(scheduler)
    await asyncio.wait_for(asyncio.gather(*(t.run() for t in schedulers)), 5)
    await asyncio.wait_for(done.wait(), 5)
    worker.request_shutdown()
    await asyncio.wait_for(run, 5)
    assert keys == ["job:2026-11-04T11:30:00+00:00"] * 2
    assert runs == [7]


async def test_lifecycle_hooks_fire_in_the_worker_and_the_scheduler() -> None:
    producer, consumer = memory_transport()
    app = App("e2e-hooks", producer=producer, consumer=consumer)
    events: list[tuple[str, str]] = []

    @app.scheduled_task(cron="30 * * * *", tz="UTC", name="job")
    async def job() -> None: ...

    @app.before_startup
    def before_startup(runner: Runner) -> None:
        events.append((runner.role, "before_startup"))

    @app.after_shutdown
    async def after_shutdown(runner: Runner) -> None:
        events.append((runner.role, "after_shutdown"))

    clock = _StepClock(
        datetime(2026, 11, 4, 11, 29, tzinfo=UTC),
        datetime(2026, 11, 4, 11, 31, tzinfo=UTC),
    )
    scheduler = Scheduler(app, clock=clock)
    clock.on_limit = scheduler.request_shutdown
    await asyncio.wait_for(scheduler.run(), 5)
    assert events == [("scheduler", "before_startup"), ("scheduler", "after_shutdown")]

    worker_started = asyncio.Event()

    @app.after_startup
    def mark_worker_started(runner: Runner) -> None:
        if runner.role == "worker":
            worker_started.set()

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker_started.wait(), 5)
    worker.request_shutdown()
    await asyncio.wait_for(run, 5)
    assert events[2:] == [("worker", "before_startup"), ("worker", "after_shutdown")]


async def test_delay_then_await_result() -> None:
    producer, consumer = memory_transport()
    app = App("e2e", producer=producer, consumer=consumer, store=asynk.MemoryStore())

    @app.task()
    async def add(a: int, *, b: int) -> int:
        return a + b

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    handle = await add.delay(1, b=2)
    assert await asyncio.wait_for(handle.result(), 5) == 3
    assert await handle.state() is asynk.TaskState.SUCCESS
    assert await app.get_task(handle.task_id).result() == 3
    worker.request_shutdown()
    await asyncio.wait_for(run, 5)
