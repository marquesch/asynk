from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any

import pytest
from envelope_helpers import decode_delivery, publish_envelope

from asynk import NotConfiguredError, RetryPolicy, TaskOptions
from asynk.app import App
from asynk.codec.json import JsonCodec
from asynk.envelope import Envelope
from asynk.hooks import Next, TaskCall, TaskResult, TaskStatus
from asynk.transport import Delivery
from asynk.transport.memory import memory_transport
from asynk.worker import Worker


def _setup() -> tuple[App, Any, Any, Worker]:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer)
    return app, producer, consumer, Worker(app)


async def _next(consumer: Any) -> Delivery:
    return await asyncio.wait_for(anext(consumer.deliveries()), 1)


async def _handle(worker: Worker, delivery: Delivery) -> TaskResult:
    result = await worker.handle(delivery)
    assert result is not None
    return result


async def _run(producer: Any, consumer: Any, worker: Worker, env: Envelope) -> TaskResult:
    await publish_envelope(producer, env)
    return await _handle(worker, await _next(consumer))


async def test_success_acks_and_returns_value() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="add")
    async def add(a: int, b: int) -> int:
        return a + b

    await publish_envelope(producer, Envelope.create("add", (1, 2), {}))
    delivery = await _next(consumer)
    result = await _handle(worker, delivery)
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

    await publish_envelope(producer, Envelope.create("boom", (), {}))
    delivery = await _next(consumer)
    result = await _handle(worker, delivery)
    assert result.status is TaskStatus.FAILED
    assert isinstance(result.error, ValueError)
    assert delivery.settled
    with pytest.raises(TimeoutError):
        await _next(consumer)


async def test_retries_exceeded_status() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="limited", options=TaskOptions(retry=RetryPolicy(max_retries=2)))
    async def limited() -> None:
        raise RuntimeError

    @app.task(name="unlimited", options=TaskOptions(retry=RetryPolicy(max_retries=None)))
    async def unlimited() -> None:
        raise RuntimeError

    base = Envelope.create("limited", (), {})
    r = await _run(producer, consumer, worker, base.retry())
    assert r.status is TaskStatus.RETRYING
    assert decode_delivery(await _next(consumer)) == base.retry().retry()
    r = await _run(producer, consumer, worker, base.retry().retry())
    assert r.status is TaskStatus.RETRIES_EXCEEDED
    u = Envelope.create("unlimited", (), {})
    for _ in range(5):
        u = u.retry()
    r = await _run(producer, consumer, worker, u)
    assert r.status is TaskStatus.RETRYING


async def test_cancelled_handle_requeues_delivery_and_reraises() -> None:
    app, producer, consumer, worker = _setup()
    started = asyncio.Event()

    @app.task(name="hang")
    async def hang() -> None:
        started.set()
        await asyncio.Event().wait()

    env = Envelope.create("hang", (), {})
    await publish_envelope(producer, env)
    delivery = await _next(consumer)
    handler = asyncio.create_task(worker.handle(delivery))
    await asyncio.wait_for(started.wait(), 1)
    handler.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handler
    assert delivery.settled
    redelivered = await _next(consumer)
    assert decode_delivery(redelivered) == env


def test_worker_requires_consumer() -> None:
    with pytest.raises(NotConfiguredError):
        Worker(App("t"))


async def test_sync_wrapper_returning_awaitable_is_awaited() -> None:
    import functools

    app, producer, consumer, worker = _setup()
    ran: list[int] = []

    async def inner(a: int) -> int:
        ran.append(a)
        return a * 2

    @functools.wraps(inner)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return inner(*args, **kwargs)

    app.task(name="wrapped")(wrapper)
    result = await _run(producer, consumer, worker, Envelope.create("wrapped", (4,), {}))
    assert result.status is TaskStatus.SUCCESS
    assert result.value == 8
    assert ran == [4]


async def test_failed_task_is_logged_with_exc_info(caplog: pytest.LogCaptureFixture) -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="boom")
    async def boom() -> None:
        raise RuntimeError("kaboom")

    with caplog.at_level(logging.INFO, logger="asynk"):
        await _run(producer, consumer, worker, Envelope.create("boom", (), {}))
    records = [r for r in caplog.records if r.name == "asynk" and "boom" in r.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert records[0].exc_info is not None
    assert isinstance(records[0].exc_info[1], RuntimeError)


async def test_retries_exceeded_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=0)))
    async def boom() -> None:
        raise RuntimeError("kaboom")

    with caplog.at_level(logging.INFO, logger="asynk"):
        await _run(producer, consumer, worker, Envelope.create("boom", (), {}))
    records = [r for r in caplog.records if r.name == "asynk" and "boom" in r.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert records[0].exc_info is not None


async def test_unknown_task_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    _, producer, consumer, worker = _setup()
    with caplog.at_level(logging.INFO, logger="asynk"):
        await _run(producer, consumer, worker, Envelope.create("nope", (), {}))
    records = [r for r in caplog.records if r.name == "asynk" and "nope" in r.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING


async def test_success_is_not_logged_above_debug(caplog: pytest.LogCaptureFixture) -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="ok")
    async def ok() -> None:
        return None

    with caplog.at_level(logging.INFO, logger="asynk"):
        await _run(producer, consumer, worker, Envelope.create("ok", (), {}))
    assert [r for r in caplog.records if r.name == "asynk"] == []


class _ReverseCodec:
    def __init__(self) -> None:
        self._json = JsonCodec()

    def validate(self, value: object) -> None:
        self._json.validate(value)

    def encode(self, value: object) -> bytes:
        return self._json.encode(value)[::-1]

    def decode(self, data: bytes) -> object:
        return self._json.decode(data[::-1])


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


@pytest.mark.parametrize("body", [b"garbage", b"\xff\xfe", b"[]", b'{"task": "t"}', b'{"task": 1}'])
async def test_undecodable_body_is_rejected_without_requeue(
    body: bytes, caplog: pytest.LogCaptureFixture
) -> None:
    app, producer, consumer, worker = _setup()
    seen: list[Envelope] = []

    @app.middleware
    async def spy(call: TaskCall, next: Next) -> TaskResult:
        seen.append(call.envelope)
        return await next(call)

    await producer.publish(Envelope.create("t", (), {}), body)
    delivery = _SpyDelivery(await _next(consumer))
    with caplog.at_level(logging.WARNING, logger="asynk"):
        assert await worker.handle(delivery) is None
    assert delivery.settled
    assert delivery.calls == [("reject", False)]
    assert seen == []
    records = [r for r in caplog.records if r.name == "asynk"]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    with pytest.raises(TimeoutError):
        await _next(consumer)


async def test_worker_survives_an_undecodable_body() -> None:
    app, producer, consumer, worker = _setup()
    done = asyncio.Event()

    @app.task(name="ok")
    async def ok() -> None:
        done.set()

    await producer.publish(Envelope.create("ok", (), {}), b"garbage")
    await publish_envelope(producer, Envelope.create("ok", (), {}))
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(done.wait(), 1)
    worker.request_shutdown()
    await asyncio.wait_for(run, 1)


async def test_worker_decodes_with_the_app_codec() -> None:
    producer, consumer = memory_transport()
    codec = _ReverseCodec()
    app = App("t", producer=producer, consumer=consumer, codec=codec)
    worker = Worker(app)

    @app.task(name="add")
    async def add(a: int, b: int) -> int:
        return a + b

    await publish_envelope(producer, Envelope.create("add", (1, 2), {}), codec)
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.SUCCESS
    assert result.value == 3
