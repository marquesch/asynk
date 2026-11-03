from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from asynk.app import App, NotConfiguredError
from asynk.envelope import Envelope
from asynk.hooks import TaskResult, TaskStatus
from asynk.transport import Delivery
from asynk.transports.memory import memory_transport
from asynk.worker import Worker


def _setup() -> tuple[App, Any, Any, Worker]:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer)
    return app, producer, consumer, Worker(app)


async def _next(consumer: Any) -> Delivery:
    return await asyncio.wait_for(anext(consumer.deliveries()), 1)


async def _run(producer: Any, consumer: Any, worker: Worker, env: Envelope) -> TaskResult:
    await producer.publish(env)
    delivery = await _next(consumer)
    result = await worker.handle(delivery)
    return result


async def test_success_acks_and_returns_value() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="add")
    async def add(a: int, b: int) -> int:
        return a + b

    await producer.publish(Envelope.create("add", (1, 2), {}))
    delivery = await _next(consumer)
    result = await worker.handle(delivery)
    assert result.status is TaskStatus.SUCCESS
    assert result.value == 3
    assert result.error is None
    assert delivery.settled


async def test_async_and_sync_tasks_both_run() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="a")
    async def a() -> str:
        return "async"

    @app.task(name="s")
    def s() -> bool:
        return threading.current_thread() is not threading.main_thread()

    r1 = await _run(producer, consumer, worker, Envelope.create("a", (), {}))
    r2 = await _run(producer, consumer, worker, Envelope.create("s", (), {}))
    assert r1.value == "async"
    assert r2.value is True


async def test_failure_sets_failed_and_rejects_without_requeue() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="boom")
    async def boom() -> None:
        raise ValueError("x")

    await producer.publish(Envelope.create("boom", (), {}))
    delivery = await _next(consumer)
    result = await worker.handle(delivery)
    assert result.status is TaskStatus.FAILED
    assert isinstance(result.error, ValueError)
    assert delivery.settled
    with pytest.raises(TimeoutError):
        await _next(consumer)


async def test_retries_exceeded_status() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="limited", max_retries=2)
    async def limited() -> None:
        raise RuntimeError

    @app.task(name="unlimited")
    async def unlimited() -> None:
        raise RuntimeError

    base = Envelope.create("limited", (), {})
    r = await _run(producer, consumer, worker, base.retry())
    assert r.status is TaskStatus.FAILED
    r = await _run(producer, consumer, worker, base.retry().retry())
    assert r.status is TaskStatus.RETRIES_EXCEEDED
    u = Envelope.create("unlimited", (), {})
    for _ in range(5):
        u = u.retry()
    r = await _run(producer, consumer, worker, u)
    assert r.status is TaskStatus.FAILED


async def test_unknown_task_status_and_after_task_called() -> None:
    app, producer, consumer, worker = _setup()
    seen: list[TaskResult] = []

    @app.after_task
    async def hook(result: TaskResult, delivery: Delivery) -> None:
        seen.append(result)

    result = await _run(producer, consumer, worker, Envelope.create("nope", (), {}))
    assert result.status is TaskStatus.UNKNOWN_TASK
    assert seen == [result]


async def test_after_task_receives_result_and_may_settle() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="boom")
    async def boom() -> None:
        raise ValueError

    @app.after_task
    async def hook(result: TaskResult, delivery: Delivery) -> None:
        assert result.status is TaskStatus.FAILED
        await delivery.ack()

    await producer.publish(Envelope.create("boom", (), {}))
    delivery = await _next(consumer)
    result = await worker.handle(delivery)
    assert result.status is TaskStatus.FAILED
    assert delivery.settled
    with pytest.raises(TimeoutError):
        await _next(consumer)


async def test_after_task_may_requeue_via_producer() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="boom")
    async def boom() -> None:
        raise ValueError

    @app.after_task
    async def hook(result: TaskResult, delivery: Delivery) -> None:
        assert app.producer is not None
        await app.producer.publish(result.envelope.retry())
        await delivery.ack()

    await producer.publish(Envelope.create("boom", (), {}))
    await worker.handle(await _next(consumer))
    again = await _next(consumer)
    assert again.envelope.attempt == 1


async def test_failing_hook_requeues_delivery_and_does_not_raise(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="ok")
    async def ok() -> int:
        return 1

    @app.after_task
    async def hook(result: TaskResult, delivery: Delivery) -> None:
        raise RuntimeError("hook broke")

    with caplog.at_level("ERROR", logger="asynk"):
        result = await _run(producer, consumer, worker, Envelope.create("ok", (), {}))
    assert result.status is TaskStatus.SUCCESS
    assert any(r.name == "asynk" for r in caplog.records)
    redelivered = await _next(consumer)
    assert redelivered.envelope.task == "ok"


async def test_cancelled_handle_requeues_delivery_and_reraises() -> None:
    app, producer, consumer, worker = _setup()
    started = asyncio.Event()

    @app.task(name="hang")
    async def hang() -> None:
        started.set()
        await asyncio.Event().wait()

    env = Envelope.create("hang", (), {})
    await producer.publish(env)
    delivery = await _next(consumer)
    handler = asyncio.create_task(worker.handle(delivery))
    await asyncio.wait_for(started.wait(), 1)
    handler.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handler
    assert delivery.settled
    redelivered = await _next(consumer)
    assert redelivered.envelope == env


def test_worker_requires_consumer() -> None:
    with pytest.raises(NotConfiguredError):
        Worker(App("t"))
