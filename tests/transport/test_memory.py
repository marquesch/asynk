from __future__ import annotations

import asyncio

from asynk.envelope import Envelope
from asynk.transport import Delivery
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


async def test_ack_marks_settled() -> None:
    producer, consumer = memory_transport()
    await producer.publish(_env(), b"x")
    delivery = await _first(consumer)
    assert not delivery.settled
    await delivery.ack()
    assert delivery.settled


async def test_stop_ends_iteration_when_idle() -> None:
    _, consumer = memory_transport()
    await consumer.start()

    async def drain() -> list[object]:
        return [d async for d in consumer.deliveries()]

    task = asyncio.create_task(drain())
    await asyncio.sleep(0)
    await consumer.stop()
    assert await asyncio.wait_for(task, 1) == []
