"""Test helpers that inspect what a memory consumer still has queued."""

from __future__ import annotations

from asynk.transport import Delivery
from asynk.transport.memory import MemoryConsumer


async def drain_deliveries(consumer: MemoryConsumer) -> list[Delivery]:
    """Stops the consumer and returns the deliveries that were still queued.

    `stop` queues an end marker behind everything already queued, so draining
    yields exactly the pending deliveries and then ends, with no waiting. The
    consumer cannot be used afterwards.
    """
    await consumer.stop()
    return [delivery async for delivery in consumer.deliveries()]


async def assert_nothing_delivered(consumer: MemoryConsumer) -> None:
    """Asserts that no delivery is pending, without waiting for one to arrive.

    The consumer cannot be used afterwards.
    """
    pending = await drain_deliveries(consumer)
    assert pending == []
