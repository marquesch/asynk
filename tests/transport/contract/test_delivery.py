from __future__ import annotations

import pytest

from .conftest import TransportHandle, envelope, next_delivery


@pytest.mark.parametrize("body", [b"", b"\x00any bytes\n", b"x" * 1_048_576])
async def test_bytes_round_trip(make_transport: TransportHandle, body: bytes) -> None:
    await make_transport.consumer.start()
    await make_transport.producer.publish(envelope(), body)
    delivery = await next_delivery(make_transport.consumer.deliveries())
    assert delivery.body == body
    assert not delivery.settled
    await delivery.ack()


async def test_acked_message_is_not_redelivered(make_transport: TransportHandle) -> None:
    await make_transport.consumer.start()
    await make_transport.producer.publish(envelope(), b"first")
    it = make_transport.consumer.deliveries()
    await (await next_delivery(it)).ack()
    await make_transport.producer.publish(envelope(), b"sentinel")
    assert (await next_delivery(it)).body == b"sentinel"


async def test_reject_without_requeue_is_not_redelivered(
    make_transport: TransportHandle,
) -> None:
    await make_transport.consumer.start()
    await make_transport.producer.publish(envelope(), b"first")
    it = make_transport.consumer.deliveries()
    await (await next_delivery(it)).reject(requeue=False)
    await make_transport.producer.publish(envelope(), b"sentinel")
    assert (await next_delivery(it)).body == b"sentinel"


async def test_reject_with_requeue_redelivers(make_transport: TransportHandle) -> None:
    await make_transport.consumer.start()
    await make_transport.producer.publish(envelope(), b"again")
    it = make_transport.consumer.deliveries()
    await (await next_delivery(it)).reject(requeue=True)
    assert (await next_delivery(it)).body == b"again"


@pytest.mark.parametrize(
    ("first", "second"),
    [("ack", "ack"), ("ack", "reject"), ("reject", "ack")],
)
async def test_second_settle_raises_runtime_error(
    make_transport: TransportHandle, first: str, second: str
) -> None:
    await make_transport.consumer.start()
    await make_transport.producer.publish(envelope(), b"x")
    delivery = await next_delivery(make_transport.consumer.deliveries())
    await getattr(delivery, first)()
    assert delivery.settled
    with pytest.raises(RuntimeError):
        await getattr(delivery, second)()


async def test_connected_follows_start_and_stop(make_transport: TransportHandle) -> None:
    consumer = make_transport.consumer
    assert not consumer.connected
    await consumer.start()
    assert consumer.connected
    await consumer.stop()
    assert not consumer.connected
