from __future__ import annotations

import asyncio

import pytest
from timer_helpers import Timers, spy_on_delivery_timers

from asynk.envelope import Envelope
from asynk.transport import Delivery, PublishOptions
from asynk.transport.memory import MemoryConsumer, MemoryDelivery, memory_transport


async def _first(consumer: MemoryConsumer) -> Delivery:
    return await asyncio.wait_for(anext(consumer.deliveries()), 1)


def _env(task: str = "t") -> Envelope:
    return Envelope.create(task, (), {})


async def test_deliveries_are_memory_deliveries() -> None:
    producer, consumer = memory_transport()
    await consumer.start()
    await producer.publish(_env(), b"x")
    assert isinstance(await _first(consumer), MemoryDelivery)


async def test_publish_returns_envelope_id() -> None:
    producer, _ = memory_transport()
    env = _env()
    assert await producer.publish(env, b"x") == env.id


async def test_stop_ends_iteration_when_idle() -> None:
    _, consumer = memory_transport()
    await consumer.start()

    async def drain() -> list[object]:
        return [d async for d in consumer.deliveries()]

    task = asyncio.create_task(drain())
    await asyncio.sleep(0)
    await consumer.stop()
    assert await asyncio.wait_for(task, 1) == []


@pytest.fixture
async def timers(monkeypatch: pytest.MonkeyPatch) -> Timers:
    return spy_on_delivery_timers(monkeypatch)


async def test_publish_with_delay_schedules_the_message(timers: Timers) -> None:
    producer, consumer = memory_transport()
    env = _env()
    assert await producer.publish(env, b"later", PublishOptions(delay=2.5)) == env.id
    await producer.publish(_env(), b"now")
    assert [d for d, _, _ in timers] == [2.5]
    assert (await _first(consumer)).body == b"now"
    _, callback, args = timers[0]
    callback(*args)
    assert (await _first(consumer)).body == b"later"


@pytest.mark.parametrize("options", [None, PublishOptions(), PublishOptions(delay=0)])
async def test_publish_without_a_delay_enqueues_at_once(
    timers: Timers, options: PublishOptions | None
) -> None:
    producer, consumer = memory_transport()
    await producer.publish(_env(), b"x", options)
    assert timers == []
    assert (await _first(consumer)).body == b"x"
