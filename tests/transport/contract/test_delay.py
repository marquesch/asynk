from __future__ import annotations

import asyncio

import pytest

from asynk.transport import PublishOptions

from .conftest import ContractSettings, TransportHandle, envelope, next_delivery


async def test_delayed_publish_is_not_delivered_early(
    make_transport: TransportHandle, contract_settings: ContractSettings
) -> None:
    if not contract_settings.supports_delay:
        pytest.skip("transport does not support delayed publish")
    delay = 0.6
    await make_transport.consumer.start()
    it = make_transport.consumer.deliveries()
    loop = asyncio.get_running_loop()
    start = loop.time()
    await make_transport.producer.publish(envelope(), b"later", PublishOptions(delay=delay))
    delivery = await next_delivery(it, delay + 3)
    assert loop.time() - start >= delay - 0.05
    assert delivery.body == b"later"
    await delivery.ack()
