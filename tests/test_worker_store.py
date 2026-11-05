from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import Any

import pytest

from asynk.app import App
from asynk.envelope import Envelope
from asynk.hooks import TaskStatus
from asynk.results import TaskError, TaskState
from asynk.store.memory import MemoryStore
from asynk.transport import Delivery
from asynk.transport.memory import MemoryConsumer, MemoryProducer, memory_transport
from asynk.worker import Worker


def _setup(store: Any = None) -> tuple[App, MemoryProducer, MemoryConsumer, Worker]:
    producer, consumer = memory_transport()
    app = App(
        "t",
        producer=producer,
        consumer=consumer,
        store=MemoryStore() if store is None else store,
    )
    return app, producer, consumer, Worker(app)


async def _next(consumer: MemoryConsumer) -> Delivery:
    return await asyncio.wait_for(anext(consumer.deliveries()), 1)


async def test_success_stores_value_and_state() -> None:
    app, _, consumer, worker = _setup()

    @app.task(name="add")
    async def add(a: int, b: int) -> int:
        return a + b

    handle = await add.delay(1, 2)
    result = await worker.handle(await _next(consumer))
    assert result.status is TaskStatus.SUCCESS
    assert await asyncio.wait_for(app.get_task(handle.task_id).result(), 1) == 3
    assert await handle.state() is TaskState.SUCCESS


async def test_failure_after_retries_exceeded_stores_error_type_and_message() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="boom", max_retries=0)
    async def boom() -> None:
        raise ValueError("bad input")

    handle = await boom.delay()
    result = await worker.handle(await _next(consumer))
    assert result.status is TaskStatus.RETRIES_EXCEEDED
    assert await handle.state() is TaskState.FAILURE
    with pytest.raises(TaskError) as info:
        await asyncio.wait_for(handle.result(), 1)
    assert info.value.type_name == "ValueError"
    assert info.value.message == "bad input"


async def test_retrying_then_success_keeps_one_id() -> None:
    app, producer, consumer, worker = _setup()
    calls: list[int] = []

    @app.task(name="flaky", max_retries=1)
    async def flaky() -> str:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("once")
        return "ok"

    handle = await flaky.delay()
    first = await _next(consumer)
    result = await worker.handle(first)
    assert result.status is TaskStatus.FAILED
    assert await handle.state() is TaskState.RETRYING
    await producer.publish(first.envelope.retry())
    second = await _next(consumer)
    assert second.envelope.id == handle.task_id
    result = await worker.handle(second)
    assert result.status is TaskStatus.SUCCESS
    assert await handle.state() is TaskState.SUCCESS
    assert await asyncio.wait_for(handle.result(), 1) == "ok"


async def test_skipped_task_result_returns_none() -> None:
    app, _, consumer, worker = _setup()
    from asynk.hooks import SkipTask

    @app.task(name="t1")
    async def t1() -> int:
        return 1

    @app.before_task
    async def veto(envelope: Envelope, delivery: Delivery) -> None:
        raise SkipTask

    handle = await t1.delay()
    result = await worker.handle(await _next(consumer))
    assert result.status is TaskStatus.SKIPPED
    assert await handle.state() is TaskState.SKIPPED
    assert await asyncio.wait_for(handle.result(), 1) is None


async def test_unknown_task_marks_failure() -> None:
    app, producer, consumer, worker = _setup()
    env = Envelope.create("nope", (), {})
    await producer.publish(env)
    result = await worker.handle(await _next(consumer))
    assert result.status is TaskStatus.UNKNOWN_TASK
    handle = app.get_task(env.id)
    assert await handle.state() is TaskState.FAILURE
    with pytest.raises(TaskError) as info:
        await asyncio.wait_for(handle.result(), 1)
    assert info.value.type_name == "UnknownTask"
    assert info.value.message == "nope"


async def test_running_state_visible_while_task_executes() -> None:
    app, _, consumer, worker = _setup()
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task(name="slow")
    async def slow() -> int:
        started.set()
        await release.wait()
        return 5

    handle = await slow.delay()
    assert await handle.state() is TaskState.PENDING
    runner = asyncio.create_task(worker.handle(await _next(consumer)))
    await asyncio.wait_for(started.wait(), 1)
    assert await handle.state() is TaskState.RUNNING
    release.set()
    await asyncio.wait_for(runner, 1)
    assert await handle.state() is TaskState.SUCCESS


async def test_unserializable_return_value_becomes_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, _, consumer, worker = _setup()

    @app.task(name="weird")
    async def weird() -> object:
        return object()

    handle = await weird.delay()
    delivery = await _next(consumer)
    with caplog.at_level(logging.WARNING, logger="asynk"):
        result = await worker.handle(delivery)
    assert result.status is TaskStatus.SUCCESS
    assert delivery.settled
    assert await handle.state() is TaskState.FAILURE
    with pytest.raises(TaskError) as info:
        await asyncio.wait_for(handle.result(), 1)
    assert info.value.type_name == "ResultNotSerializable"
    assert "weird" in info.value.message
    assert "object" in info.value.message
    assert any(r.levelno == logging.WARNING for r in caplog.records)


class _BrokenStore:
    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def get(self, key: str) -> bytes | None:
        raise RuntimeError("get")

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        raise RuntimeError("set")

    async def wait(self, key: str, timeout: float | None = None) -> bytes | None:
        raise RuntimeError("wait")


class _FailingSetStore(MemoryStore):
    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        raise RuntimeError("set")


@pytest.mark.parametrize("store", [_BrokenStore(), _FailingSetStore()])
async def test_store_errors_do_not_fail_the_task(
    store: Any, caplog: pytest.LogCaptureFixture
) -> None:
    app, producer, consumer, worker = _setup(store)

    @app.task(name="add")
    async def add(a: int, b: int) -> int:
        return a + b

    await producer.publish(Envelope.create("add", (2, 3), {}))
    delivery = await _next(consumer)
    with caplog.at_level(logging.ERROR, logger="asynk"):
        result = await worker.handle(delivery)
    assert result.status is TaskStatus.SUCCESS
    assert result.value == 5
    assert delivery.settled
    assert any(r.name == "asynk" for r in caplog.records)


async def test_worker_run_survives_broken_store() -> None:
    app, producer, consumer, worker = _setup(_BrokenStore())
    done = asyncio.Event()

    @app.task(name="t1")
    async def t1() -> None:
        done.set()

    await producer.publish(Envelope.create("t1", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(done.wait(), 1)
    worker.request_shutdown()
    await asyncio.wait_for(run, 1)


class _SpyStore(MemoryStore):
    def __init__(self, calls: list[str]) -> None:
        super().__init__()
        self._calls = calls

    async def start(self) -> None:
        self._calls.append("store.start")

    async def stop(self) -> None:
        self._calls.append("store.stop")


class _SpyConsumer:
    def __init__(self, inner: MemoryConsumer, calls: list[str], fail: bool = False) -> None:
        self._inner = inner
        self._calls = calls
        self._fail = fail

    @property
    def connected(self) -> bool:
        return self._inner.connected

    async def start(self) -> None:
        self._calls.append("consumer.start")
        await self._inner.start()

    async def stop(self) -> None:
        self._calls.append("consumer.stop")
        await self._inner.stop()

    def deliveries(self) -> AsyncIterator[Delivery]:
        if self._fail:
            raise RuntimeError("serve failed")
        return self._inner.deliveries()

    def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]:
        return self._inner.process(delivery)


def _spy_setup(fail: bool = False) -> tuple[list[str], Worker]:
    calls: list[str] = []
    producer, consumer = memory_transport()
    app = App(
        "t",
        producer=producer,
        consumer=_SpyConsumer(consumer, calls, fail),
        store=_SpyStore(calls),
    )

    @app.before_startup
    def user_startup(runner: object) -> None:
        calls.append("user.before_startup")

    @app.after_shutdown
    def user_shutdown(runner: object) -> None:
        calls.append("user.after_shutdown")

    return calls, Worker(app)


async def test_worker_starts_and_stops_store_through_app_hooks() -> None:
    calls, worker = _spy_setup()
    run = asyncio.create_task(worker.run())
    await asyncio.sleep(0.05)
    worker.request_shutdown()
    await asyncio.wait_for(run, 1)
    assert calls == [
        "store.start",
        "user.before_startup",
        "consumer.start",
        "consumer.stop",
        "store.stop",
        "user.after_shutdown",
    ]


async def test_worker_stops_store_when_serving_raises() -> None:
    calls, worker = _spy_setup(fail=True)
    with pytest.raises(RuntimeError, match="serve failed"):
        await asyncio.wait_for(worker.run(), 1)
    assert calls[0] == "store.start"
    assert "store.stop" in calls


async def test_hook_failure_records_retrying_or_failure() -> None:
    app, _, consumer, worker = _setup()

    @app.task(name="once", max_retries=1)
    async def once() -> int:
        return 1

    @app.before_task
    async def broken(envelope: Envelope, delivery: Delivery) -> None:
        raise OSError("hook down")

    handle = await once.delay()
    result = await worker.handle(await _next(consumer))
    assert result.status is TaskStatus.FAILED
    assert await handle.state() is TaskState.RETRYING
    retried = await _next(consumer)
    assert retried.envelope.id == handle.task_id
    result = await worker.handle(retried)
    assert result.status is TaskStatus.RETRIES_EXCEEDED
    assert await handle.state() is TaskState.FAILURE
    with pytest.raises(TaskError) as info:
        await asyncio.wait_for(handle.result(), 1)
    assert (info.value.type_name, info.value.message) == ("OSError", "hook down")


class _BlockingSetStore(MemoryStore):
    """Blocks forever on any write whose value contains `marker`."""

    def __init__(self, marker: bytes) -> None:
        super().__init__()
        self._marker = marker
        self.blocked = asyncio.Event()

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        if self._marker in value:
            self.blocked.set()
            await asyncio.Event().wait()
        return await super().set(key, value, overwrite=overwrite)


@pytest.mark.parametrize("marker", [b"retrying", b"success"])
async def test_cancel_while_recording_requeues_delivery(marker: bytes) -> None:
    store = _BlockingSetStore(marker)
    app, producer, consumer, worker = _setup(store)

    @app.task(name="t1")
    async def t1() -> int:
        if marker == b"retrying":
            raise RuntimeError("fail")
        return 1

    env = Envelope.create("t1", (), {})
    await producer.publish(env)
    delivery = await _next(consumer)
    handler = asyncio.create_task(worker.handle(delivery))
    await asyncio.wait_for(store.blocked.wait(), 1)
    handler.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handler
    assert delivery.settled
    redelivered = await _next(consumer)
    assert redelivered.envelope == env


async def test_worker_run_raises_when_store_start_fails() -> None:
    class FailingStart(MemoryStore):
        async def start(self) -> None:
            raise RuntimeError("start failed")

    _, _, _, worker = _setup(FailingStart())
    with pytest.raises(RuntimeError, match="start failed"):
        await asyncio.wait_for(worker.run(), 1)


async def test_worker_with_null_store_runs_tasks(caplog: pytest.LogCaptureFixture) -> None:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer)
    worker = Worker(app)
    done = asyncio.Event()

    @app.task(name="t1")
    async def t1() -> int:
        done.set()
        return 1

    with caplog.at_level(logging.ERROR, logger="asynk"):
        await t1.delay()
        run = asyncio.create_task(worker.run())
        await asyncio.wait_for(done.wait(), 1)
        worker.request_shutdown()
        await asyncio.wait_for(run, 1)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
