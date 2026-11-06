from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from asynk.envelope import Envelope
from asynk.transport import Delivery, PublishOptions
from asynk.transport.memory import MemoryConsumer, MemoryDelivery, memory_transport


async def _first(consumer: MemoryConsumer) -> Delivery:
    return await asyncio.wait_for(anext(consumer.deliveries()), 1)


def _env(task: str = "t") -> Envelope:
    return Envelope.create(task, (), {})


async def test_publish_then_consume_moves_the_body_unchanged() -> None:
    producer, consumer = memory_transport()
    await consumer.start()
    await producer.publish(_env(), b"\x00any bytes\n")
    delivery = await _first(consumer)
    assert isinstance(delivery, MemoryDelivery)
    assert delivery.body == b"\x00any bytes\n"


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


async def test_double_settle_raises_runtime_error() -> None:
    producer, consumer = memory_transport()
    await producer.publish(_env(), b"x")
    delivery = await _first(consumer)
    await delivery.ack()
    with pytest.raises(RuntimeError):
        await delivery.ack()
    with pytest.raises(RuntimeError):
        await delivery.reject()


async def test_reject_requeue_re_enqueues_the_same_body() -> None:
    producer, consumer = memory_transport()
    await producer.publish(_env(), b"payload")
    it = consumer.deliveries()
    first = await asyncio.wait_for(anext(it), 1)
    await first.reject(requeue=True)
    second = await asyncio.wait_for(anext(it), 1)
    assert second.body == b"payload"


async def test_reject_without_requeue_drops() -> None:
    producer, consumer = memory_transport()
    await producer.publish(_env("first"), b"first")
    it = consumer.deliveries()
    first = await asyncio.wait_for(anext(it), 1)
    await first.reject(requeue=False)
    await producer.publish(_env("second"), b"second")
    nxt = await asyncio.wait_for(anext(it), 1)
    assert nxt.body == b"second"


async def test_every_body_is_delivered_without_decoding() -> None:
    producer, consumer = memory_transport()
    await producer.publish(_env(), b"garbage")
    await producer.publish(_env(), b"")
    it = consumer.deliveries()
    assert (await asyncio.wait_for(anext(it), 1)).body == b"garbage"
    assert (await asyncio.wait_for(anext(it), 1)).body == b""


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
    await producer.publish(_env(), b"x")
    delivery = await _first(consumer)
    async with consumer.process(delivery):
        pass
    assert delivery.settled


async def test_process_rejects_on_exception_and_propagates() -> None:
    producer, consumer = memory_transport()
    await producer.publish(_env(), b"x")
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
    await producer.publish(_env(), b"x")
    delivery = await _first(consumer)
    async with consumer.process(delivery):
        await delivery.ack()
    assert delivery.settled


_Timers = list[tuple[float, Callable[..., object], tuple[object, ...]]]


@pytest.fixture
async def timers(monkeypatch: pytest.MonkeyPatch) -> _Timers:
    scheduled: _Timers = []
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "call_later", lambda d, cb, *a: scheduled.append((d, cb, a)))
    return scheduled


async def test_publish_with_delay_schedules_the_message(timers: _Timers) -> None:
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
    timers: _Timers, options: PublishOptions | None
) -> None:
    producer, consumer = memory_transport()
    await producer.publish(_env(), b"x", options)
    assert timers == []
    assert (await _first(consumer)).body == b"x"
