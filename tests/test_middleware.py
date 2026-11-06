from __future__ import annotations

import asyncio
from typing import Any

import pytest
from envelope_helpers import decode_delivery, publish_envelope

from asynk.app import App
from asynk.envelope import Envelope
from asynk.hooks import Next, TaskCall, TaskResult, TaskStatus
from asynk.results import TaskState
from asynk.store.memory import MemoryStore
from asynk.transport import Delivery
from asynk.transport.memory import MemoryConsumer, MemoryProducer, memory_transport
from asynk.worker import Worker


def _setup(**kwargs: Any) -> tuple[App, MemoryProducer, MemoryConsumer, Worker]:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer, store=MemoryStore())
    return app, producer, consumer, Worker(app, **kwargs)


async def _next(consumer: MemoryConsumer) -> Delivery:
    return await asyncio.wait_for(anext(consumer.deliveries()), 1)


async def _handle(worker: Worker, delivery: Delivery) -> TaskResult:
    result = await worker.handle(delivery)
    assert result is not None
    return result


class _SpyDelivery:
    def __init__(self, inner: Delivery) -> None:
        self._inner = inner
        self.calls: list[tuple[str, bool | None]] = []

    @property
    def body(self) -> bytes:
        return self._inner.body

    @property
    def settled(self) -> bool:
        return self._inner.settled

    async def ack(self) -> None:
        self.calls.append(("ack", None))
        await self._inner.ack()

    async def reject(self, *, requeue: bool = False) -> None:
        self.calls.append(("reject", requeue))
        await self._inner.reject(requeue=requeue)


async def test_last_registered_layer_is_outermost_and_unwinds_in_reverse() -> None:
    app, producer, consumer, worker = _setup()
    order: list[str] = []

    def layer(name: str) -> None:
        async def mw(call: TaskCall, next: Next) -> TaskResult:
            order.append(f"{name}-in")
            result = await next(call)
            order.append(f"{name}-out")
            return result

        mw.__name__ = name
        app.middleware(mw)

    for name in "abc":
        layer(name)

    @app.task(name="t")
    async def t() -> int:
        order.append("task")
        return 7

    await publish_envelope(producer, Envelope.create("t", (), {}))
    result = await _handle(worker, await _next(consumer))
    assert order == ["c-in", "b-in", "a-in", "task", "a-out", "b-out", "c-out"]
    assert result.status is TaskStatus.SUCCESS
    assert result.value == 7


async def test_layer_receives_the_envelope_and_unsettled_delivery() -> None:
    app, producer, consumer, worker = _setup()
    seen: list[tuple[str, bool]] = []

    @app.middleware
    async def mw(call: TaskCall, next: Next) -> TaskResult:
        seen.append((call.envelope.task, call.delivery.settled))
        return await next(call)

    @app.task(name="t")
    async def t() -> None: ...

    await publish_envelope(producer, Envelope.create("t", (), {}))
    delivery = await _next(consumer)
    await _handle(worker, delivery)
    assert seen == [("t", False)]
    assert delivery.settled


async def test_veto_records_skipped() -> None:
    app, _, consumer, worker = _setup()
    ran: list[int] = []

    @app.middleware
    async def veto(call: TaskCall, next: Next) -> TaskResult:
        return call.skip()

    @app.task(name="t")
    async def t() -> None:
        ran.append(1)

    handle = await t.delay()
    delivery = await _next(consumer)
    result = await _handle(worker, delivery)
    assert result.status is TaskStatus.SKIPPED
    assert ran == []
    assert await handle.state() is TaskState.SKIPPED
    assert await handle.result(timeout=1) is None
    assert delivery.settled
    with pytest.raises(TimeoutError):
        await _next(consumer)


def _cleanup_layer(app: App, cleaned: list[str]) -> None:
    @app.middleware
    async def mw(call: TaskCall, next: Next) -> TaskResult:
        try:
            return await next(call)
        finally:
            cleaned.append("done")


async def test_layer_finally_runs_when_handle_is_cancelled() -> None:
    app, producer, consumer, worker = _setup()
    cleaned: list[str] = []
    started = asyncio.Event()
    _cleanup_layer(app, cleaned)

    @app.task(name="t")
    async def t() -> None:
        started.set()
        await asyncio.Event().wait()

    env = Envelope.create("t", (), {})
    await publish_envelope(producer, env)
    handling = asyncio.create_task(worker.handle(await _next(consumer)))
    await asyncio.wait_for(started.wait(), 1)
    handling.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handling
    assert cleaned == ["done"]
    assert decode_delivery(await _next(consumer)) == env


async def test_layer_finally_runs_on_shutdown_timeout() -> None:
    app, producer, consumer, worker = _setup(shutdown_timeout=0.1)
    cleaned: list[str] = []
    started = asyncio.Event()
    _cleanup_layer(app, cleaned)

    @app.task(name="t")
    async def t() -> None:
        started.set()
        await asyncio.Event().wait()

    env = Envelope.create("t", (), {})
    await publish_envelope(producer, env)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 1)
    worker.request_shutdown()
    await asyncio.wait_for(run, 1)
    assert cleaned == ["done"]
    remaining = [d async for d in consumer.deliveries()]
    assert [decode_delivery(d) for d in remaining] == [env]


async def test_layer_that_settles_stops_the_default_settlement() -> None:
    app, producer, consumer, worker = _setup()

    @app.middleware
    async def settle(call: TaskCall, next: Next) -> TaskResult:
        result = await next(call)
        await call.delivery.ack()
        return result

    @app.task(name="boom")
    async def boom() -> None:
        raise ValueError("x")

    await publish_envelope(producer, Envelope.create("boom", (), {}))
    delivery = _SpyDelivery(await _next(consumer))
    result = await _handle(worker, delivery)
    assert result.status is TaskStatus.FAILED
    assert delivery.calls == [("ack", None)]


async def test_layer_may_republish_and_ack_a_failure() -> None:
    app, producer, consumer, worker = _setup()

    @app.middleware
    async def retry(call: TaskCall, next: Next) -> TaskResult:
        result = await next(call)
        if result.status is TaskStatus.FAILED:
            retried = call.envelope.retry()
            await producer.publish(retried, app.codec.encode(retried.to_dict()))
            await call.delivery.ack()
        return result

    @app.task(name="boom")
    async def boom() -> None:
        raise ValueError("x")

    await publish_envelope(producer, Envelope.create("boom", (), {}))
    first = await _next(consumer)
    assert (await _handle(worker, first)).status is TaskStatus.FAILED
    assert first.settled
    assert decode_delivery(await _next(consumer)).attempt == 1


async def test_unknown_task_runs_through_the_chain() -> None:
    app, producer, consumer, worker = _setup()
    seen: list[TaskStatus] = []

    @app.middleware
    async def mw(call: TaskCall, next: Next) -> TaskResult:
        result = await next(call)
        seen.append(result.status)
        return result

    env = Envelope.create("nope", (), {})
    await publish_envelope(producer, env)
    result = await _handle(worker, await _next(consumer))
    assert seen == [TaskStatus.UNKNOWN_TASK]
    assert result.status is TaskStatus.UNKNOWN_TASK
    assert await app.get_task(env.id).state() is TaskState.FAILURE


async def test_layer_may_replace_the_outcome() -> None:
    app, _, consumer, worker = _setup()

    @app.middleware
    async def fallback(call: TaskCall, next: Next) -> TaskResult:
        await next(call)
        return TaskResult(call.envelope, TaskStatus.SUCCESS, value="fallback")

    @app.task(name="boom")
    async def boom() -> None:
        raise ValueError("x")

    handle = await boom.delay()
    delivery = await _next(consumer)
    await _handle(worker, delivery)
    assert await handle.state() is TaskState.SUCCESS
    assert await handle.result(timeout=1) == "fallback"
    assert delivery.settled
    with pytest.raises(TimeoutError):
        await _next(consumer)


async def test_layer_registered_after_worker_creation_applies() -> None:
    app, producer, consumer, worker = _setup()
    seen: list[str] = []

    @app.task(name="t")
    async def t() -> None: ...

    @app.middleware
    async def mw(call: TaskCall, next: Next) -> TaskResult:
        seen.append(call.envelope.task)
        return await next(call)

    await publish_envelope(producer, Envelope.create("t", (), {}))
    await _handle(worker, await _next(consumer))
    assert seen == ["t"]
