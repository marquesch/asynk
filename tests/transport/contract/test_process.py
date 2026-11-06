from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from asynk import DeliveryFailedError, TaskResult, TaskStatus
from asynk.transport import Delivery

from .conftest import TransportHandle, envelope, next_delivery


async def _delivery(handle: TransportHandle) -> Delivery:
    await handle.consumer.start()
    await handle.producer.publish(envelope(), b"x")
    return await next_delivery(handle.consumer.deliveries())


async def test_process_acks_on_clean_exit(make_transport: TransportHandle) -> None:
    delivery = await _delivery(make_transport)
    async with make_transport.consumer.process(delivery):
        assert not delivery.settled
    assert delivery.settled


def _failed() -> DeliveryFailedError:
    result = TaskResult(envelope(), TaskStatus.FAILED)
    return DeliveryFailedError(result)


@pytest.mark.parametrize("make_exc", [lambda: ValueError("boom"), _failed])
async def test_process_rejects_and_reraises_on_exception(
    make_transport: TransportHandle, make_exc: Callable[[], Exception]
) -> None:
    delivery = await _delivery(make_transport)
    exc = make_exc()
    with pytest.raises(type(exc)) as info:
        async with make_transport.consumer.process(delivery):
            raise exc
    assert info.value is exc
    assert delivery.settled


async def test_process_does_not_suppress_cancelled_error(
    make_transport: TransportHandle,
) -> None:
    delivery = await _delivery(make_transport)
    with pytest.raises(asyncio.CancelledError):
        async with make_transport.consumer.process(delivery):
            raise asyncio.CancelledError
    assert delivery.settled


async def test_process_leaves_settled_delivery_alone(
    make_transport: TransportHandle,
) -> None:
    delivery = await _delivery(make_transport)
    async with make_transport.consumer.process(delivery):
        await delivery.ack()
    assert delivery.settled
