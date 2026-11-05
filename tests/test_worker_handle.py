from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any

import pytest
from envelope_helpers import decode_delivery, publish_envelope

from asynk.app import App, NotConfiguredError
from asynk.codec.json import JsonCodec
from asynk.envelope import Envelope
from asynk.hooks import SkipTask, TaskResult, TaskStatus
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

    await publish_envelope(producer, Envelope.create("boom", (), {}))
    delivery = await _next(consumer)
    result = await _handle(worker, delivery)
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
        await publish_envelope(app.producer, result.envelope.retry())
        await delivery.ack()

    await publish_envelope(producer, Envelope.create("boom", (), {}))
    await worker.handle(await _next(consumer))
    again = await _next(consumer)
    assert decode_delivery(again).attempt == 1


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

    @app.task(name="boom", max_retries=0)
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


async def test_before_task_hooks_run_in_order_before_the_task() -> None:
    app, producer, consumer, worker = _setup()
    order: list[str] = []

    @app.task(name="t")
    async def t() -> None:
        order.append("task")

    @app.before_task
    async def first(envelope: Envelope, delivery: Delivery) -> None:
        assert envelope.task == "t"
        assert not delivery.settled
        order.append("first")

    @app.before_task
    def second(envelope: Envelope, delivery: Delivery) -> None:
        order.append("second")

    result = await _run(producer, consumer, worker, Envelope.create("t", (), {}))
    assert result.status is TaskStatus.SUCCESS
    assert order == ["first", "second", "task"]


async def test_skip_task_vetoes_the_task() -> None:
    app, producer, consumer, worker = _setup()
    ran: list[str] = []
    after: list[TaskStatus] = []

    @app.task(name="t")
    async def t() -> None:
        ran.append("task")

    @app.before_task
    async def veto(envelope: Envelope, delivery: Delivery) -> None:
        raise SkipTask

    @app.before_task
    async def never(envelope: Envelope, delivery: Delivery) -> None:
        ran.append("later hook")

    @app.after_task
    async def seen(result: TaskResult, delivery: Delivery) -> None:
        after.append(result.status)

    await publish_envelope(producer, Envelope.create("t", (), {}))
    delivery = await _next(consumer)
    result = await _handle(worker, delivery)
    assert result.status is TaskStatus.SKIPPED
    assert ran == []
    assert after == [TaskStatus.SKIPPED]
    assert delivery.settled
    with pytest.raises(TimeoutError):
        await _next(consumer)


async def test_unknown_task_skips_before_task_but_runs_after_task() -> None:
    app, producer, consumer, worker = _setup()
    calls: list[str] = []

    @app.before_task
    async def before(envelope: Envelope, delivery: Delivery) -> None:
        calls.append("before")

    @app.after_task
    async def after(result: TaskResult, delivery: Delivery) -> None:
        calls.append("after")

    result = await _run(producer, consumer, worker, Envelope.create("nope", (), {}))
    assert result.status is TaskStatus.UNKNOWN_TASK
    assert calls == ["after"]


async def test_failing_before_task_redelivers_with_attempt_incremented(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, producer, consumer, worker = _setup()
    ran: list[bool] = []

    @app.task(name="t")
    async def t() -> None:
        ran.append(True)

    @app.before_task
    async def broken(envelope: Envelope, delivery: Delivery) -> None:
        raise RuntimeError("hook broke")

    await publish_envelope(producer, Envelope.create("t", (), {}))
    delivery = await _next(consumer)
    with caplog.at_level("ERROR", logger="asynk"):
        result = await _handle(worker, delivery)
    assert result.status is TaskStatus.FAILED
    assert isinstance(result.error, RuntimeError)
    assert ran == []
    assert delivery.settled
    assert any(r.exc_info for r in caplog.records)
    again = await _next(consumer)
    assert decode_delivery(again).id == decode_delivery(delivery).id
    assert decode_delivery(again).attempt == 1


async def test_failing_after_task_redelivers_with_attempt_incremented() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="ok")
    async def ok() -> int:
        return 1

    @app.after_task
    async def broken(result: TaskResult, delivery: Delivery) -> None:
        raise RuntimeError("hook broke")

    result = await _run(producer, consumer, worker, Envelope.create("ok", (), {}))
    assert result.status is TaskStatus.FAILED
    assert decode_delivery(await _next(consumer)).attempt == 1


async def test_hook_failure_stops_redelivering_after_max_retries() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="t", max_retries=1)
    async def t() -> None: ...

    @app.after_task
    async def broken(result: TaskResult, delivery: Delivery) -> None:
        raise RuntimeError("hook broke")

    await publish_envelope(producer, Envelope.create("t", (), {}))
    first = await _handle(worker, await _next(consumer))
    assert first.status is TaskStatus.FAILED
    second_delivery = await _next(consumer)
    assert decode_delivery(second_delivery).attempt == 1
    second = await _handle(worker, second_delivery)
    assert second.status is TaskStatus.RETRIES_EXCEEDED
    assert second_delivery.settled
    with pytest.raises(TimeoutError):
        await _next(consumer)


async def test_hook_failure_without_a_producer_requeues_the_same_attempt() -> None:
    producer, consumer = memory_transport()
    app = App("t", consumer=consumer)
    worker = Worker(app)

    @app.task(name="ok")
    async def ok() -> None: ...

    @app.after_task
    async def broken(result: TaskResult, delivery: Delivery) -> None:
        raise RuntimeError("hook broke")

    await publish_envelope(producer, Envelope.create("ok", (), {}))
    await worker.handle(await _next(consumer))
    assert decode_delivery(await _next(consumer)).attempt == 0


async def test_hook_failure_with_failing_republish_leaves_default_settlement(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class BrokenProducer:
        async def publish(self, envelope: Envelope, body: bytes) -> None:
            raise ConnectionError("broker down")

    producer, consumer = memory_transport()
    app = App("t", producer=BrokenProducer(), consumer=consumer)
    worker = Worker(app)

    @app.task(name="ok")
    async def ok() -> None: ...

    @app.after_task
    async def broken(result: TaskResult, delivery: Delivery) -> None:
        raise RuntimeError("hook broke")

    await publish_envelope(producer, Envelope.create("ok", (), {}))
    delivery = await _next(consumer)
    with caplog.at_level("ERROR", logger="asynk"):
        result = await _handle(worker, delivery)
    assert result.status is TaskStatus.FAILED
    assert delivery.settled
    with pytest.raises(TimeoutError):
        await _next(consumer)


async def test_hook_that_settles_then_raises_is_only_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="ok")
    async def ok() -> None: ...

    @app.after_task
    async def broken(result: TaskResult, delivery: Delivery) -> None:
        await delivery.ack()
        raise RuntimeError("hook broke")

    with caplog.at_level("ERROR", logger="asynk"):
        await _run(producer, consumer, worker, Envelope.create("ok", (), {}))
    assert any(r.exc_info for r in caplog.records)
    with pytest.raises(TimeoutError):
        await _next(consumer)


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
    before: list[Envelope] = []
    after: list[TaskResult] = []
    app.before_task(lambda envelope, delivery: before.append(envelope))
    app.after_task(lambda result, delivery: after.append(result))

    await producer.publish(Envelope.create("t", (), {}), body)
    delivery = _SpyDelivery(await _next(consumer))
    with caplog.at_level(logging.WARNING, logger="asynk"):
        assert await worker.handle(delivery) is None
    assert delivery.settled
    assert delivery.calls == [("reject", False)]
    assert before == [] and after == []
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


async def test_hook_failure_retry_is_published_with_the_app_codec() -> None:
    producer, consumer = memory_transport()
    codec = _ReverseCodec()
    app = App("t", producer=producer, consumer=consumer, codec=codec)
    worker = Worker(app)

    @app.task(name="ok")
    async def ok() -> None: ...

    @app.after_task
    async def broken(result: TaskResult, delivery: Delivery) -> None:
        raise RuntimeError("hook broke")

    env = Envelope.create("ok", (), {})
    await publish_envelope(producer, env, codec)
    await worker.handle(await _next(consumer))
    retried = decode_delivery(await _next(consumer), codec)
    assert retried == env.retry()
