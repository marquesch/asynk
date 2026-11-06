from __future__ import annotations

import asyncio

import pytest

from asynk.transport import Consumer

from .conftest import ContractSettings, TransportHandle, envelope, next_delivery


async def test_deliveries_can_be_cancelled_while_idle(make_transport: TransportHandle) -> None:
    consumer = make_transport.consumer
    await consumer.start()
    it = consumer.deliveries()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(anext(it), 0.05)
    await it.aclose()  # type: ignore[attr-defined]
    await consumer.stop()
    assert not consumer.connected


async def test_closing_deliveries_then_stop_works(make_transport: TransportHandle) -> None:
    consumer = make_transport.consumer
    await consumer.start()
    await make_transport.producer.publish(envelope(), b"one")
    it = consumer.deliveries()
    delivery = await next_delivery(it)
    await delivery.ack()
    await it.aclose()  # type: ignore[attr-defined]
    await consumer.stop()
    assert not consumer.connected


async def test_concurrent_consumers_never_share_an_unsettled_message(
    make_transport: TransportHandle,
) -> None:
    other = make_transport.new_consumer()
    first = make_transport.consumer
    await first.start()
    await other.start()
    bodies = [f"body-{i}".encode() for i in range(20)]
    for body in bodies:
        await make_transport.producer.publish(envelope(), body)

    received: list[bytes] = []

    async def drain(consumer: Consumer) -> None:
        # Stop both consumers at the expected total instead of waiting for silence:
        # waiting can outlast a short lease, and an expired lease redelivers an
        # unsettled message. A stopped consumer's iterator ends by itself.
        async for delivery in consumer.deliveries():
            received.append(delivery.body)
            if len(received) >= len(bodies):
                await first.stop()
                await other.stop()

    await asyncio.wait_for(asyncio.gather(drain(first), drain(other)), 10)
    combined = received
    assert len(combined) == len(set(combined))
    assert set(combined) == set(bodies)


async def test_unsettled_messages_are_redelivered_after_stop(
    make_transport: TransportHandle, contract_settings: ContractSettings
) -> None:
    timeout = contract_settings.redelivery_timeout
    if timeout is None:
        pytest.skip("transport does not redeliver after stop()")
    await make_transport.consumer.start()
    await make_transport.producer.publish(envelope(), b"unsettled")
    delivery = await next_delivery(make_transport.consumer.deliveries())
    assert delivery.body == b"unsettled"
    await make_transport.consumer.stop()
    again = make_transport.new_consumer()
    await again.start()
    redelivered = await next_delivery(again.deliveries(), timeout)
    assert redelivered.body == b"unsettled"
    await redelivered.ack()
