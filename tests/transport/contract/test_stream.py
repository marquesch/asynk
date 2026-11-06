from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from asynk.transport import Consumer, Delivery

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

    async def drain(consumer: Consumer) -> list[bytes]:
        seen: list[bytes] = []
        it: AsyncIterator[Delivery] = consumer.deliveries()
        try:
            while True:
                seen.append((await next_delivery(it, 0.2)).body)
        except TimeoutError:
            return seen

    got_first, got_other = await asyncio.gather(drain(first), drain(other))
    combined = got_first + got_other
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
