from __future__ import annotations

import asyncio

import pytest
from envelope_helpers import decode_delivery, publish_envelope

from asynk import RetryPolicy, TaskError, TaskOptions, TaskTimeoutError
from asynk.app import App
from asynk.envelope import Envelope
from asynk.hooks import Next, TaskCall, TaskResult, TaskStatus
from asynk.results import TaskState
from asynk.store.memory import MemoryStore
from asynk.transport import Delivery
from asynk.transport.memory import MemoryConsumer, memory_transport
from asynk.worker import Worker

TINY = 0.01


def _setup(options: TaskOptions | None = None) -> tuple[App, MemoryConsumer, Worker]:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer, store=MemoryStore(), options=options)
    return app, consumer, Worker(app)


async def _next(consumer: MemoryConsumer) -> Delivery:
    return await asyncio.wait_for(anext(consumer.deliveries()), 1)


async def _handle(worker: Worker, delivery: Delivery) -> TaskResult:
    result = await worker.handle(delivery)
    assert result is not None
    return result


async def test_a_task_that_exceeds_its_timeout_fails_with_task_timeout_error() -> None:
    app, consumer, worker = _setup()

    @app.task(options=TaskOptions(timeout=TINY))
    async def hang() -> None:
        await asyncio.Event().wait()

    handle = await hang.delay()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.FAILED
    assert isinstance(result.error, TaskTimeoutError)
    assert await handle.state() is TaskState.FAILURE
    with pytest.raises(TaskError) as info:
        await handle.result(timeout=1)
    assert info.value.type_name == "TaskTimeoutError"


async def test_the_app_default_timeout_applies() -> None:
    app, consumer, worker = _setup(TaskOptions(timeout=TINY))

    @app.task()
    async def hang() -> None:
        await asyncio.Event().wait()

    await hang.delay()
    result = await _handle(worker, await _next(consumer))
    assert isinstance(result.error, TaskTimeoutError)


async def test_a_task_within_its_timeout_succeeds() -> None:
    app, consumer, worker = _setup()

    @app.task(options=TaskOptions(timeout=5))
    async def quick() -> int:
        return 7

    await quick.delay()
    result = await _handle(worker, await _next(consumer))
    assert (result.status, result.value) == (TaskStatus.SUCCESS, 7)


async def test_a_task_without_a_timeout_is_not_limited() -> None:
    app, consumer, worker = _setup()

    @app.task()
    async def slow() -> str:
        await asyncio.sleep(TINY * 5)
        return "done"

    await slow.delay()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.SUCCESS


async def test_a_task_own_timeout_error_is_an_ordinary_failure() -> None:
    app, consumer, worker = _setup()

    @app.task(options=TaskOptions(timeout=5))
    async def http() -> None:
        raise TimeoutError("upstream")

    await http.delay()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.FAILED
    assert type(result.error) is TimeoutError


async def test_a_timeout_is_retried_with_the_default_retry_on() -> None:
    app, consumer, worker = _setup()

    @app.task(options=TaskOptions(timeout=TINY, retry=RetryPolicy(max_retries=1)))
    async def hang() -> None:
        await asyncio.Event().wait()

    handle = await hang.delay()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.RETRYING
    assert isinstance(result.error, TaskTimeoutError)
    assert await handle.state() is TaskState.RETRYING


async def test_a_timeout_is_not_retried_when_retry_on_excludes_it() -> None:
    app, consumer, worker = _setup()
    policy = RetryPolicy(max_retries=3, retry_on=(ConnectionError,))

    @app.task(options=TaskOptions(timeout=TINY, retry=policy))
    async def hang() -> None:
        await asyncio.Event().wait()

    await hang.delay()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.FAILED


async def test_a_timeout_is_retried_when_retry_on_lists_it() -> None:
    app, consumer, worker = _setup()
    policy = RetryPolicy(max_retries=3, retry_on=(TaskTimeoutError,))

    @app.task(options=TaskOptions(timeout=TINY, retry=policy))
    async def hang() -> None:
        await asyncio.Event().wait()

    await hang.delay()
    result = await _handle(worker, await _next(consumer))
    assert result.status is TaskStatus.RETRYING


async def test_every_attempt_gets_a_fresh_timeout_and_the_last_one_exhausts_retries() -> None:
    app, consumer, worker = _setup()
    attempts: list[int] = []

    @app.task(options=TaskOptions(timeout=TINY, retry=RetryPolicy(max_retries=1)))
    async def hang() -> None:
        attempts.append(1)
        await asyncio.Event().wait()

    await hang.delay()
    first = await _handle(worker, await _next(consumer))
    second = await _handle(worker, await _next(consumer))
    assert (first.status, second.status) == (TaskStatus.RETRYING, TaskStatus.RETRIES_EXCEEDED)
    assert isinstance(second.error, TaskTimeoutError)
    assert len(attempts) == 2


async def test_shutdown_cancellation_still_requeues_on_the_same_attempt() -> None:
    app, consumer, worker = _setup()
    started = asyncio.Event()

    @app.task(name="hang", options=TaskOptions(timeout=60))
    async def hang() -> None:
        started.set()
        await asyncio.Event().wait()

    env = Envelope.create("hang", (), {})
    await publish_envelope(app.producer, env)  # type: ignore[arg-type]
    delivery = await _next(consumer)
    handler = asyncio.create_task(worker.handle(delivery))
    await asyncio.wait_for(started.wait(), 1)
    handler.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handler
    assert decode_delivery(await _next(consumer)) == env


async def test_middleware_wraps_the_timed_call_and_sees_the_failed_result() -> None:
    app, consumer, worker = _setup()
    seen: list[TaskResult] = []

    @app.middleware
    async def watch(call: TaskCall, next: Next) -> TaskResult:
        result = await next(call)
        seen.append(result)
        return result

    @app.task(options=TaskOptions(timeout=TINY))
    async def hang() -> None:
        await asyncio.Event().wait()

    await hang.delay()
    await _handle(worker, await _next(consumer))
    assert isinstance(seen[0].error, TaskTimeoutError)
