from __future__ import annotations

import asyncio

import pytest

from asynk.codec import EncodeError
from asynk.envelope import Envelope
from asynk.transport import Delivery
from asynk.transport.memory import MemoryConsumer, memory_transport


async def _first(consumer: MemoryConsumer) -> Delivery:
    return await asyncio.wait_for(anext(consumer.deliveries()), 1)


async def test_publish_then_consume_roundtrip() -> None:
    producer, consumer = memory_transport()
    await consumer.start()
    env = Envelope.create("t", (1, "a"), {"k": 2})
    await producer.publish(env)
    delivery = await _first(consumer)
    assert delivery.envelope == env


async def test_publish_returns_envelope_id() -> None:
    producer, _ = memory_transport()
    env = Envelope.create("t", (), {})
    assert await producer.publish(env) == env.id


async def test_ack_marks_settled() -> None:
    producer, consumer = memory_transport()
    await producer.publish(Envelope.create("t", (), {}))
    delivery = await _first(consumer)
    assert not delivery.settled
    await delivery.ack()
    assert delivery.settled


async def test_double_settle_raises_runtime_error() -> None:
    producer, consumer = memory_transport()
    await producer.publish(Envelope.create("t", (), {}))
    delivery = await _first(consumer)
    await delivery.ack()
    with pytest.raises(RuntimeError):
        await delivery.ack()
    with pytest.raises(RuntimeError):
        await delivery.reject()


async def test_reject_requeue_redelivers() -> None:
    producer, consumer = memory_transport()
    env = Envelope.create("t", (), {})
    await producer.publish(env)
    it = consumer.deliveries()
    first = await asyncio.wait_for(anext(it), 1)
    await first.reject(requeue=True)
    second = await asyncio.wait_for(anext(it), 1)
    assert second.envelope == env


async def test_reject_without_requeue_drops() -> None:
    producer, consumer = memory_transport()
    await producer.publish(Envelope.create("first", (), {}))
    it = consumer.deliveries()
    first = await asyncio.wait_for(anext(it), 1)
    await first.reject(requeue=False)
    await producer.publish(Envelope.create("second", (), {}))
    nxt = await asyncio.wait_for(anext(it), 1)
    assert nxt.envelope.task == "second"


async def test_stop_ends_iteration_when_idle() -> None:
    _, consumer = memory_transport()
    await consumer.start()

    async def drain() -> list[object]:
        return [d async for d in consumer.deliveries()]

    task = asyncio.create_task(drain())
    await asyncio.sleep(0)
    await consumer.stop()
    assert await asyncio.wait_for(task, 1) == []


async def test_connected_flag_follows_start_stop() -> None:
    _, consumer = memory_transport()
    assert not consumer.connected
    await consumer.start()
    assert consumer.connected
    await consumer.stop()
    assert not consumer.connected


async def test_process_acks_on_clean_exit() -> None:
    producer, consumer = memory_transport()
    await producer.publish(Envelope.create("t", (), {}))
    delivery = await _first(consumer)
    async with consumer.process(delivery):
        pass
    assert delivery.settled


async def test_process_rejects_on_exception_and_propagates() -> None:
    producer, consumer = memory_transport()
    await producer.publish(Envelope.create("t", (), {}))
    it = consumer.deliveries()
    delivery = await asyncio.wait_for(anext(it), 1)
    with pytest.raises(ValueError, match="boom"):
        async with consumer.process(delivery):
            raise ValueError("boom")
    assert delivery.settled
    await consumer.stop()
    assert [d async for d in it] == []


async def test_process_leaves_already_settled_delivery_alone() -> None:
    producer, consumer = memory_transport()
    await producer.publish(Envelope.create("t", (), {}))
    delivery = await _first(consumer)
    async with consumer.process(delivery):
        await delivery.ack()
    assert delivery.settled


async def test_malformed_message_is_skipped() -> None:
    producer, consumer = memory_transport()
    producer.publish_raw(b"garbage")
    env = Envelope.create("valid", (), {})
    await producer.publish(env)
    delivery = await _first(consumer)
    assert delivery.envelope == env


def test_producer_validate_delegates_to_codec() -> None:
    producer, _ = memory_transport()
    producer.validate(Envelope.create("t", [1], {}))
    with pytest.raises(EncodeError):
        producer.validate(Envelope.create("t", [object()], {}))
