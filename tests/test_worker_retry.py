from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

import pytest
from delivery_helpers import assert_nothing_delivered
from envelope_helpers import decode_delivery, publish_envelope
from timer_helpers import spy_on_delivery_timers

from asynk import Exponential, RetryPolicy, TaskError, TaskOptions
from asynk.app import App
from asynk.envelope import Envelope
from asynk.hooks import Next, TaskCall, TaskResult, TaskStatus
from asynk.results import TaskState, result_key
from asynk.store.memory import MemoryStore
from asynk.transport import Delivery, PublishOptions
from asynk.transport.memory import MemoryConsumer, MemoryProducer, memory_transport
from asynk.worker import Worker


class _RecordingProducer:
    def __init__(self, inner: MemoryProducer) -> None:
        self.inner = inner
        self.published: list[tuple[Envelope, PublishOptions | None]] = []

    async def publish(
        self, envelope: Envelope, body: bytes, options: PublishOptions | None = None
    ) -> str:
        self.published.append((envelope, options))
        return await self.inner.publish(envelope, body, options)


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


def _setup(
    retry: RetryPolicy | None = None,
) -> tuple[App, _RecordingProducer, MemoryConsumer, Worker]:
    inner, consumer = memory_transport()
    producer = _RecordingProducer(inner)
    app = App(
        "t",
        producer=producer,
        consumer=consumer,
        store=MemoryStore(),
        options=TaskOptions(retry=retry),
    )
    return app, producer, consumer, Worker(app)


async def _next(consumer: MemoryConsumer, timeout: float = 1) -> Delivery:
    return await asyncio.wait_for(anext(consumer.deliveries()), timeout)


async def _handle(worker: Worker, delivery: Delivery) -> TaskResult:
    result = await worker.handle(delivery)
    assert result is not None
    return result


def _raise_before_next(app: App, error: Exception) -> None:
    @app.middleware
    async def broken(call: TaskCall, next: Next) -> TaskResult:
        raise error


async def test_failure_with_retries_left_publishes_the_retry_and_acks() -> None:
    app, _, consumer, worker = _setup()

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=2)))
    async def boom() -> None:
        raise ValueError("x")

    handle = await boom.delay()
    delivery = _SpyDelivery(await _next(consumer))
    env = decode_delivery(delivery)
    result = await _handle(worker, delivery)
    assert result.status is TaskStatus.RETRYING
    assert isinstance(result.error, ValueError)
    assert delivery.calls == [("ack", None)]
    assert decode_delivery(await _next(consumer)) == env.retry()
    assert await handle.state() is TaskState.RETRYING
    assert await app.store.get(result_key(env.id)) is None


async def test_retrying_is_recorded_before_the_retry_publish() -> None:
    app, recording, consumer, worker = _setup()
    seen: list[TaskState | None] = []

    class StateReadingProducer:
        async def publish(
            self, envelope: Envelope, body: bytes, options: PublishOptions | None = None
        ) -> str:
            seen.append(await app.get_task(envelope.id).state())
            return await recording.inner.publish(envelope, body, options)

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def boom() -> None:
        raise ValueError("x")

    env = Envelope.create("boom", (), {})
    await publish_envelope(recording.inner, env)
    app.producer = StateReadingProducer()
    await _handle(worker, await _next(consumer))
    assert seen == [TaskState.RETRYING]


async def test_retry_then_success_completes_the_result() -> None:
    app, _, consumer, worker = _setup()
    calls: list[int] = []

    @app.task(name="flaky", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def flaky() -> str:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("once")
        return "ok"

    handle = await flaky.delay()
    first = await _handle(worker, await _next(consumer))
    assert first.status is TaskStatus.RETRYING
    retried = await _next(consumer)
    assert decode_delivery(retried).id == handle.task_id
    second = await _handle(worker, retried)
    assert second.status is TaskStatus.SUCCESS
    assert await handle.result(timeout=1) == "ok"
    assert await handle.state() is TaskState.SUCCESS


async def test_used_up_retries_are_final() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def boom() -> None:
        raise ValueError("bad input")

    env = Envelope.create("boom", (), {}).retry()
    await publish_envelope(producer.inner, env)
    delivery = _SpyDelivery(await _next(consumer))
    result = await _handle(worker, delivery)
    assert result.status is TaskStatus.RETRIES_EXCEEDED
    assert delivery.calls == [("reject", False)]
    handle = app.get_task(env.id)
    assert await handle.state() is TaskState.FAILURE
    with pytest.raises(TaskError) as info:
        await handle.result(timeout=1)
    assert (info.value.type_name, info.value.message) == ("ValueError", "bad input")
    assert producer.published == []
    await assert_nothing_delivered(consumer)


async def test_no_policy_fails_for_good() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="boom")
    async def boom() -> None:
        raise ValueError("x")

    handle = await boom.delay()
    producer.published.clear()
    delivery = _SpyDelivery(await _next(consumer))
    result = await _handle(worker, delivery)
    assert result.status is TaskStatus.FAILED
    assert delivery.calls == [("reject", False)]
    assert await handle.state() is TaskState.FAILURE
    with pytest.raises(TaskError):
        await handle.result(timeout=1)
    assert producer.published == []
    await assert_nothing_delivered(consumer)


async def test_retry_on_mismatch_fails_for_good() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(retry_on=(TimeoutError,))))
    async def boom() -> None:
        raise ValueError("x")

    handle = await boom.delay()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.FAILED
    assert await handle.state() is TaskState.FAILURE
    await assert_nothing_delivered(consumer)


async def test_retry_on_mismatch_on_the_last_attempt_is_failed() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(
        name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=1, retry_on=(TimeoutError,)))
    )
    async def boom() -> None:
        raise ValueError("x")

    await publish_envelope(producer.inner, Envelope.create("boom", (), {}).retry())
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.FAILED


async def test_retry_on_matches_a_subclass() -> None:
    app, _, consumer, worker = _setup()

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(retry_on=(OSError,))))
    async def boom() -> None:
        raise ConnectionError("x")

    await boom.delay()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.RETRYING


async def test_retry_false_overrides_the_app_default() -> None:
    app, _, consumer, worker = _setup(RetryPolicy(max_retries=3))

    @app.task(name="boom", options=TaskOptions(retry=False))
    async def boom() -> None:
        raise ValueError("x")

    handle = await boom.delay()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.FAILED
    assert await handle.state() is TaskState.FAILURE
    await assert_nothing_delivered(consumer)


async def test_app_default_policy_applies() -> None:
    app, _, consumer, worker = _setup(RetryPolicy(max_retries=3))

    @app.task(name="boom")
    async def boom() -> None:
        raise ValueError("x")

    await boom.delay()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.RETRYING


async def test_backoff_is_passed_as_publish_options(monkeypatch: pytest.MonkeyPatch) -> None:
    timers = spy_on_delivery_timers(monkeypatch, run_now=True)
    app, producer, consumer, worker = _setup()

    @app.task(name="constant", options=TaskOptions(retry=RetryPolicy(backoff=2.0)))
    async def constant() -> None:
        raise ValueError("x")

    @app.task(
        name="exponential",
        options=TaskOptions(
            retry=RetryPolicy(max_retries=None, backoff=Exponential(base=1, factor=2, max=60))
        ),
    )
    async def exponential() -> None:
        raise ValueError("x")

    await publish_envelope(producer.inner, Envelope.create("constant", (), {}))
    await _handle(worker, await _next(consumer))
    await _next(consumer)
    env = Envelope.create("exponential", (), {}).retry().retry()
    await publish_envelope(producer.inner, env)
    await _handle(worker, await _next(consumer))
    assert [options for _, options in producer.published] == [
        PublishOptions(delay=2.0),
        PublishOptions(delay=4.0),
    ]
    assert producer.published[1][0] == env.retry()
    assert [delay for delay, _, _ in timers] == [2.0, 4.0]


async def test_no_backoff_publishes_without_options_to_a_two_argument_producer() -> None:
    app, recording, consumer, worker = _setup()
    published: list[Envelope] = []

    class TwoArgumentProducer:
        async def publish(self, envelope: Envelope, body: bytes) -> str:
            published.append(envelope)
            return await recording.inner.publish(envelope, body)

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def boom() -> None:
        raise ValueError("x")

    env = Envelope.create("boom", (), {})
    await publish_envelope(recording.inner, env)
    app.producer = cast(Any, TwoArgumentProducer())
    delivery = _SpyDelivery(await _next(consumer))
    result = await _handle(worker, delivery)
    assert result.status is TaskStatus.RETRYING
    assert delivery.calls == [("ack", None)]
    assert published == [env.retry()]
    assert decode_delivery(await _next(consumer)) == env.retry()


async def _assert_requeued_at_the_same_attempt(
    app: App, consumer: MemoryConsumer, worker: Worker, env: Envelope
) -> None:
    delivery = _SpyDelivery(await _next(consumer))
    result = await worker.handle(delivery)
    assert result is not None
    assert result.status is TaskStatus.RETRYING
    assert delivery.calls == [("reject", True)]
    assert await app.get_task(env.id).state() is TaskState.RETRYING
    assert decode_delivery(await _next(consumer)) == env


async def test_failed_retry_publish_requeues_the_same_attempt(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app, recording, consumer, worker = _setup()

    class BrokenProducer:
        async def publish(
            self, envelope: Envelope, body: bytes, options: PublishOptions | None = None
        ) -> str:
            raise ConnectionError("broker down")

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def boom() -> None:
        raise ValueError("x")

    env = Envelope.create("boom", (), {})
    await publish_envelope(recording.inner, env)
    app.producer = BrokenProducer()
    with caplog.at_level(logging.ERROR, logger="asynk"):
        await _assert_requeued_at_the_same_attempt(app, consumer, worker, env)
    assert any("failed to publish a retry" in r.getMessage() and r.exc_info for r in caplog.records)


async def test_raising_backoff_requeues_the_same_attempt() -> None:
    class BrokenBackoff:
        def delay(self, retry: int) -> float:
            raise RuntimeError("bad backoff")

    app, producer, consumer, worker = _setup()

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(backoff=BrokenBackoff())))
    async def boom() -> None:
        raise ValueError("x")

    env = Envelope.create("boom", (), {})
    await publish_envelope(producer.inner, env)
    await _assert_requeued_at_the_same_attempt(app, consumer, worker, env)
    assert producer.published == []


async def test_no_producer_requeues_the_same_attempt() -> None:
    producer, consumer = memory_transport()
    app = App("t", consumer=consumer, store=MemoryStore(), options=TaskOptions(retry=RetryPolicy()))
    worker = Worker(app)

    @app.task(name="boom")
    async def boom() -> None:
        raise ValueError("x")

    env = Envelope.create("boom", (), {})
    await publish_envelope(producer, env)
    await _assert_requeued_at_the_same_attempt(app, consumer, worker, env)


async def test_middleware_sees_retrying() -> None:
    app, _, consumer, worker = _setup()
    seen: list[TaskStatus] = []

    @app.middleware
    async def spy(call: TaskCall, next: Next) -> TaskResult:
        result = await next(call)
        seen.append(result.status)
        return result

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def boom() -> None:
        raise ValueError("x")

    await boom.delay()
    await _handle(worker, await _next(consumer))
    assert seen == [TaskStatus.RETRYING]


async def test_layer_that_settles_a_retrying_result_suppresses_the_retry() -> None:
    app, producer, consumer, worker = _setup()

    @app.middleware
    async def settle(call: TaskCall, next: Next) -> TaskResult:
        result = await next(call)
        await call.delivery.ack()
        return result

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def boom() -> None:
        raise ValueError("x")

    handle = await boom.delay()
    producer.published.clear()
    delivery = _SpyDelivery(await _next(consumer))
    result = await _handle(worker, delivery)
    assert result.status is TaskStatus.RETRYING
    assert delivery.calls == [("ack", None)]
    assert producer.published == []
    assert await handle.state() is TaskState.RETRYING
    await assert_nothing_delivered(consumer)


async def test_layer_raising_after_a_retrying_task_still_retries() -> None:
    app, _, consumer, worker = _setup()

    @app.middleware
    async def broken(call: TaskCall, next: Next) -> TaskResult:
        await next(call)
        raise RuntimeError("layer broke")

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def boom() -> None:
        raise ValueError("x")

    await boom.delay()
    delivery = await _next(consumer)
    env = decode_delivery(delivery)
    result = await _handle(worker, delivery)
    assert result.status is TaskStatus.RETRYING
    assert isinstance(result.error, ValueError)
    assert decode_delivery(await _next(consumer)) == env.retry()


async def test_layer_raising_before_the_task_without_a_policy_fails_for_good() -> None:
    app, producer, consumer, worker = _setup()

    @app.task(name="ok")
    async def ok() -> None: ...

    _raise_before_next(app, RuntimeError("layer broke"))
    handle = await ok.delay()
    producer.published.clear()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.FAILED
    assert await handle.state() is TaskState.FAILURE
    assert producer.published == []
    await assert_nothing_delivered(consumer)


async def test_layer_raising_before_the_task_retries_through_the_policy() -> None:
    app, _, consumer, worker = _setup()

    @app.task(name="ok", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def ok() -> None: ...

    _raise_before_next(app, RuntimeError("layer broke"))
    handle = await ok.delay()
    first = await _handle(worker, await _next(consumer))
    assert first.status is TaskStatus.RETRYING
    assert await handle.state() is TaskState.RETRYING
    second = await _handle(worker, await _next(consumer))
    assert second.status is TaskStatus.RETRIES_EXCEEDED
    assert await handle.state() is TaskState.FAILURE
    await assert_nothing_delivered(consumer)


async def test_layer_raising_before_the_task_outside_retry_on_fails_for_good() -> None:
    app, _, consumer, worker = _setup()

    @app.task(name="ok", options=TaskOptions(retry=RetryPolicy(retry_on=(ValueError,))))
    async def ok() -> None: ...

    _raise_before_next(app, RuntimeError("layer broke"))
    await ok.delay()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.FAILED
    await assert_nothing_delivered(consumer)


async def test_layer_raising_before_an_unknown_task_uses_the_app_policy() -> None:
    app, producer, consumer, worker = _setup(RetryPolicy(max_retries=1))
    _raise_before_next(app, RuntimeError("layer broke"))
    env = Envelope.create("nope", (), {})
    await publish_envelope(producer.inner, env)
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.RETRYING
    assert decode_delivery(await _next(consumer)) == env.retry()


async def test_unknown_task_is_not_retried() -> None:
    app, producer, consumer, worker = _setup(RetryPolicy(max_retries=1))
    env = Envelope.create("nope", (), {})
    await publish_envelope(producer.inner, env)
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.UNKNOWN_TASK
    assert await app.get_task(env.id).state() is TaskState.FAILURE
    await assert_nothing_delivered(consumer)


async def test_retries_with_backoff_through_the_memory_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timers = spy_on_delivery_timers(monkeypatch, run_now=True)
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer, store=MemoryStore())
    calls: list[int] = []

    @app.task(
        name="flaky",
        options=TaskOptions(
            retry=RetryPolicy(max_retries=3, backoff=Exponential(base=1, factor=2, max=60))
        ),
    )
    async def flaky() -> str:
        calls.append(1)
        if len(calls) <= 2:
            raise RuntimeError("not yet")
        return "ok"

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    handle = await flaky.delay()
    assert await handle.result(timeout=5) == "ok"
    worker.request_shutdown()
    await run
    assert [delay for delay, _, _ in timers] == [1, 2]
    assert len(calls) == 3


async def test_retrying_is_logged_as_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    app, _, consumer, worker = _setup()

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def boom() -> None:
        raise ValueError("x")

    await boom.delay()
    with caplog.at_level(logging.WARNING, logger="asynk"):
        await _handle(worker, await _next(consumer))
    records = [r for r in caplog.records if "retrying" in r.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert records[0].exc_info is not None


async def test_cancel_during_the_retry_publish_requeues_the_original() -> None:
    app, recording, consumer, worker = _setup()
    started = asyncio.Event()

    class BlockingProducer:
        async def publish(
            self, envelope: Envelope, body: bytes, options: PublishOptions | None = None
        ) -> str:
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    @app.task(name="boom", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def boom() -> None:
        raise ValueError("x")

    env = Envelope.create("boom", (), {})
    await publish_envelope(recording.inner, env)
    app.producer = BlockingProducer()
    delivery = _SpyDelivery(await _next(consumer))
    handler = asyncio.create_task(worker.handle(delivery))
    await asyncio.wait_for(started.wait(), 1)
    handler.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handler
    assert delivery.calls == [("reject", True)]
    assert decode_delivery(await _next(consumer)) == env


async def test_sync_task_is_retried() -> None:
    app, _, consumer, worker = _setup()
    calls: list[int] = []

    @app.task(name="flaky", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def flaky() -> str:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("once")
        return "ok"

    handle = await flaky.delay()
    first = await _handle(worker, await _next(consumer))
    assert first.status is TaskStatus.RETRYING
    second = await _handle(worker, await _next(consumer))
    assert second.status is TaskStatus.SUCCESS
    assert second.envelope.attempt == 1
    assert await handle.result(timeout=1) == "ok"
