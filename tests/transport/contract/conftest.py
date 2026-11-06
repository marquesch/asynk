"""Transport contract suite fixtures.

Matches asynk 0.1 (``asynk.__version__`` at the time of writing). To test your
own transport, copy this directory and replace ``REGISTRY``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Protocol

import pytest

from asynk.transport import Consumer, Producer
from asynk.transport.memory import MemoryConsumer, MemoryProducer, memory_transport


@dataclass(frozen=True)
class ContractSettings:
    """Per-transport knobs for checks that depend on broker behaviour."""

    redelivery_timeout: float | None
    """Seconds to wait for a redelivery after `stop()`; None if there is none."""


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


class _MemoryHandle:
    def __init__(self) -> None:
        self.producer: MemoryProducer
        self.consumer: MemoryConsumer
        self.producer, self.consumer = memory_transport()
        self._queue = self.consumer._queue
        self.consumers: list[Consumer] = [self.consumer]

    def new_consumer(self) -> Consumer:
        consumer = MemoryConsumer(self._queue)
        self.consumers.append(consumer)
        return consumer


REGISTRY: dict[str, Callable[[], TransportHandle]] = {"memory": _MemoryHandle}

_SETTINGS: dict[str, ContractSettings] = {
    "memory": ContractSettings(redelivery_timeout=None),
}


@pytest.fixture(params=sorted(REGISTRY))
def transport_name(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
def contract_settings(transport_name: str) -> ContractSettings:
    return _SETTINGS[transport_name]


@pytest.fixture
async def make_transport(transport_name: str) -> AsyncIterator[TransportHandle]:
    handle = REGISTRY[transport_name]()
    yield handle
    for consumer in getattr(handle, "consumers", [handle.consumer]):
        if consumer.connected:
            await consumer.stop()
