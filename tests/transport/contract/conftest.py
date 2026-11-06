"""Transport contract suite fixtures.

Matches asynk 0.1.0 (``asynk.__version__`` at the time of writing). To test your
own transport, copy this directory and replace ``REGISTRY``.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, Protocol

import pytest
from redis.asyncio import Redis
from redis_helpers import BlockingFakeRedis
from service_helpers import redis_url

from asynk.envelope import Envelope
from asynk.transport import Consumer, Delivery, Producer
from asynk.transport.memory import MemoryConsumer, MemoryProducer
from asynk.transport.redis import RedisConsumer, RedisProducer


@dataclass(frozen=True)
class ContractSettings:
    """Per-transport knobs for checks that depend on broker behavior."""

    redelivery_timeout: float | None
    """Seconds to wait for a redelivery after `stop()`; None if there is none."""

    supports_delay: bool = True
    """Whether `publish` honors `PublishOptions.delay`."""


class TransportHandle(Protocol):
    """What `make_transport` yields."""

    @property
    def producer(self) -> Producer:
        """The producer publishing to the queue."""
        ...

    @property
    def consumer(self) -> Consumer:
        """The first consumer, not started."""
        ...

    def new_consumer(self) -> Consumer:
        """Returns another, not started, consumer on the same queue."""
        ...

    async def close(self) -> None:
        """Stops every connected consumer and deletes the queue."""
        ...


def envelope() -> Envelope:
    """Returns a throwaway envelope to publish with."""
    return Envelope.create("t", (), {})


async def next_delivery(it: AsyncIterator[Delivery], timeout: float = 2) -> Delivery:
    """Returns the next delivery from `it`, failing after `timeout` seconds."""
    return await asyncio.wait_for(anext(it), timeout)


class _MemoryHandle:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self.producer = MemoryProducer(self._queue)
        self.consumer = MemoryConsumer(self._queue)
        self._consumers: list[Consumer] = [self.consumer]

    def new_consumer(self) -> Consumer:
        consumer = MemoryConsumer(self._queue)
        self._consumers.append(consumer)
        return consumer

    async def close(self) -> None:
        for consumer in self._consumers:
            if consumer.connected:
                await consumer.stop()


class _RedisHandle:
    """A transport on Redis Streams, with a fresh queue per handle."""

    def __init__(self, client: Redis) -> None:
        self._client = client
        self._queue = f"contract-{uuid.uuid4().hex}"
        self.producer = RedisProducer(client=client, queue=self._queue)
        self._consumers: list[RedisConsumer] = []
        self.consumer = self.new_consumer()

    def new_consumer(self) -> RedisConsumer:
        consumer = RedisConsumer(client=self._client, queue=self._queue, block=0.1, lease=0.3)
        self._consumers.append(consumer)
        return consumer

    async def close(self) -> None:
        for consumer in self._consumers:
            if consumer.connected:
                await consumer.stop()
        await self._client.delete(*(f"asynk:{{{self._queue}}}:{kind}" for kind in KINDS))
        await self._client.aclose()


KINDS = ("stream", "delayed", "dead")


def _fake_redis() -> TransportHandle:
    return _RedisHandle(BlockingFakeRedis())


REGISTRY: dict[str, Callable[[], TransportHandle]] = {
    "memory": _MemoryHandle,
    "redis-fake": _fake_redis,
}
_SETTINGS: dict[str, ContractSettings] = {
    "memory": ContractSettings(redelivery_timeout=None),
    "redis-fake": ContractSettings(redelivery_timeout=3),
}

REGISTRY["redis"] = lambda: _RedisHandle(Redis.from_url(redis_url()))
_SETTINGS["redis"] = _SETTINGS["redis-fake"]
# Entries that need a real service: collected always, skipped (or failed, when
# ASYNK_TEST_REQUIRE_SERVICES=1) by the `requires_redis` marker.
_MARKS: dict[str, list[pytest.MarkDecorator]] = {"redis": [pytest.mark.requires_redis]}


@pytest.fixture(params=[pytest.param(n, marks=_MARKS.get(n, [])) for n in sorted(REGISTRY)])
def transport_name(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
def contract_settings(transport_name: str) -> ContractSettings:
    return _SETTINGS[transport_name]


@pytest.fixture
async def make_transport(transport_name: str) -> AsyncIterator[TransportHandle]:
    handle = REGISTRY[transport_name]()
    yield handle
    await handle.close()
