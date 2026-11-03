from __future__ import annotations

import asyncio
import os
import signal
import threading
import time
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import Any

import pytest

from asynk.app import App
from asynk.envelope import Envelope
from asynk.hooks import TaskResult, TaskStatus
from asynk.transport import Delivery
from asynk.transports.memory import MemoryConsumer, MemoryProducer, memory_transport
from asynk.worker import Worker


def _setup(**kwargs: Any) -> tuple[App, MemoryProducer, MemoryConsumer, Worker]:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer)
    return app, producer, consumer, Worker(app, **kwargs)


def _collect(app: App) -> list[tuple[TaskResult, Delivery]]:
    seen: list[tuple[TaskResult, Delivery]] = []

    @app.after_task
    async def hook(result: TaskResult, delivery: Delivery) -> None:
        seen.append((result, delivery))

    return seen


async def _drain(consumer: MemoryConsumer) -> list[Delivery]:
    """Collect whatever is left in the queue; ends at the stop sentinel run() enqueued."""
    return [d async for d in consumer.deliveries()]


async def _stop(worker: Worker, run: asyncio.Task[None]) -> None:
    worker.request_shutdown()
    await asyncio.wait_for(run, 1)


async def test_run_processes_published_tasks() -> None:
    app, producer, consumer, worker = _setup()
    seen = _collect(app)
    done = asyncio.Event()
    values: list[int] = []

    @app.task(name="rec")
    async def rec(n: int) -> int:
        values.append(n)
        if len(values) == 3:
            done.set()
        return n

    for n in range(3):
        await producer.publish(Envelope.create("rec", (n,), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(done.wait(), 1)
    await _stop(worker, run)
    assert sorted(values) == [0, 1, 2]
    assert len(seen) == 3
    assert all(r.status is TaskStatus.SUCCESS and d.settled for r, d in seen)
    assert not consumer.connected
    assert await _drain(consumer) == []


async def test_concurrency_limit_respected() -> None:
    app, producer, consumer, worker = _setup(concurrency=2)
    running = 0
    max_seen = 0
    finished = 0
    two_running = asyncio.Event()
    gate = asyncio.Event()
    all_done = asyncio.Event()

    @app.task(name="t")
    async def t() -> None:
        nonlocal running, max_seen, finished
        running += 1
        max_seen = max(max_seen, running)
        if running == 2:
            two_running.set()
        await asyncio.wait_for(gate.wait(), 1)
        running -= 1
        finished += 1
        if finished == 5:
            all_done.set()

    for _ in range(5):
        await producer.publish(Envelope.create("t", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(two_running.wait(), 1)
    # Give the run loop ample iterations to start a third task if the limit allowed it.
    for _ in range(100):
        await asyncio.sleep(0)
    assert running == 2
    gate.set()
    await asyncio.wait_for(all_done.wait(), 1)
    await _stop(worker, run)
    assert max_seen == 2


async def test_tasks_run_concurrently() -> None:
    app, producer, consumer, worker = _setup()
    seen = _collect(app)
    barrier = asyncio.Barrier(2)
    both = asyncio.Event()
    count = 0

    @app.task(name="meet")
    async def meet() -> None:
        nonlocal count
        await asyncio.wait_for(barrier.wait(), 1)
        count += 1
        if count == 2:
            both.set()

    await producer.publish(Envelope.create("meet", (), {}))
    await producer.publish(Envelope.create("meet", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(both.wait(), 1)
    await _stop(worker, run)
    assert [r.status for r, _ in seen] == [TaskStatus.SUCCESS, TaskStatus.SUCCESS]


async def test_idle_worker_shuts_down_promptly() -> None:
    _, _, consumer, worker = _setup()
    run = asyncio.create_task(worker.run())
    for _ in range(5):
        await asyncio.sleep(0)
    assert consumer.connected
    await _stop(worker, run)
    assert not consumer.connected


async def test_shutdown_waits_for_in_flight_task() -> None:
    app, producer, consumer, worker = _setup()
    seen = _collect(app)
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task(name="slow")
    async def slow() -> str:
        started.set()
        await release.wait()
        return "done"

    await producer.publish(Envelope.create("slow", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    worker.request_shutdown()
    for _ in range(10):
        await asyncio.sleep(0)
    assert not run.done()
    release.set()
    await asyncio.wait_for(run, 1)
    assert len(seen) == 1
    result, delivery = seen[0]
    assert result.status is TaskStatus.SUCCESS
    assert result.value == "done"
    assert delivery.settled
    assert await _drain(consumer) == []


async def test_shutdown_timeout_requeues_unfinished() -> None:
    app, producer, consumer, worker = _setup(shutdown_timeout=0.1)
    started = asyncio.Event()

    @app.task(name="sleepy")
    async def sleepy() -> None:
        started.set()
        await asyncio.sleep(5)

    env = Envelope.create("sleepy", (), {})
    await producer.publish(env)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    t0 = time.monotonic()
    await _stop(worker, run)
    assert time.monotonic() - t0 < 1
    remaining = await _drain(consumer)
    assert [d.envelope for d in remaining] == [env]


async def test_failed_task_does_not_stop_loop() -> None:
    app, producer, consumer, worker = _setup()
    seen = _collect(app)
    ok_done = asyncio.Event()

    @app.task(name="boom")
    async def boom() -> None:
        raise ValueError("x")

    @app.task(name="ok")
    async def ok() -> None:
        ok_done.set()

    await producer.publish(Envelope.create("boom", (), {}))
    await producer.publish(Envelope.create("ok", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(ok_done.wait(), 1)
    await _stop(worker, run)
    assert {r.envelope.task: r.status for r, _ in seen} == {
        "boom": TaskStatus.FAILED,
        "ok": TaskStatus.SUCCESS,
    }


async def test_sync_blocking_task_does_not_block_loop() -> None:
    app, producer, consumer, worker = _setup()
    async_done = threading.Event()
    sync_saw_async: list[bool] = []
    order: list[str] = []
    both_done = asyncio.Event()

    @app.after_task
    async def hook(result: TaskResult, delivery: Delivery) -> None:
        order.append(result.envelope.task)
        if len(order) == 2:
            both_done.set()

    @app.task(name="blocking")
    def blocking() -> None:
        time.sleep(0.3)
        sync_saw_async.append(async_done.is_set())

    @app.task(name="quick")
    async def quick() -> None:
        async_done.set()

    await producer.publish(Envelope.create("blocking", (), {}))
    await producer.publish(Envelope.create("quick", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(both_done.wait(), 2)
    await _stop(worker, run)
    assert sync_saw_async == [True]
    assert order == ["quick", "blocking"]


async def test_request_shutdown_is_idempotent() -> None:
    _, _, consumer, worker = _setup()
    worker.request_shutdown()
    worker.request_shutdown()
    run = asyncio.create_task(worker.run())
    worker.request_shutdown()
    await asyncio.wait_for(run, 1)
    worker.request_shutdown()
    assert not consumer.connected


async def test_run_with_signals_handles_sigterm() -> None:
    _, _, consumer, worker = _setup()
    before_term = signal.getsignal(signal.SIGTERM)
    before_int = signal.getsignal(signal.SIGINT)
    run = asyncio.create_task(worker.run_with_signals())
    for _ in range(5):
        await asyncio.sleep(0)
    assert consumer.connected
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(run, 1)
    assert not consumer.connected
    assert signal.getsignal(signal.SIGTERM) == before_term
    assert signal.getsignal(signal.SIGINT) == before_int


class _FailingConsumer:
    """Yields the inner consumer's first delivery, then raises once `fail` is set."""

    def __init__(self, inner: MemoryConsumer, events: list[str]) -> None:
        self.inner = inner
        self.events = events
        self.fail = asyncio.Event()

    @property
    def connected(self) -> bool:
        return self.inner.connected

    async def start(self) -> None:
        await self.inner.start()

    async def stop(self) -> None:
        self.events.append("stop")
        await self.inner.stop()

    async def deliveries(self) -> AsyncIterator[Delivery]:
        yield await anext(self.inner.deliveries())
        await self.fail.wait()
        raise ConnectionError("broker gone")

    def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]:
        return self.inner.process(delivery)


async def test_consumer_error_cancels_and_requeues_in_flight() -> None:
    producer, inner = memory_transport()
    events: list[str] = []
    consumer = _FailingConsumer(inner, events)
    app = App("t", producer=producer, consumer=consumer)
    worker = Worker(app)
    started = asyncio.Event()

    @app.task(name="slow")
    async def slow() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            events.append("task_exit")

    env = Envelope.create("slow", (), {})
    await producer.publish(env)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    consumer.fail.set()
    with pytest.raises(ConnectionError, match="broker gone"):
        await asyncio.wait_for(run, 1)
    assert events == ["task_exit", "stop"]
    remaining = await _drain(inner)
    assert [d.envelope for d in remaining] == [env]
