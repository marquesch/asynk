from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TypeAlias

from asynk.envelope import Envelope
from asynk.transport import Delivery


class _Stop:
    pass


_STOP = _Stop()

_Queue: TypeAlias = "asyncio.Queue[bytes | _Stop]"


class MemoryProducer:
    """Producer that publishes to an in-process queue."""

    def __init__(self, queue: _Queue) -> None:
        """Initializes the producer.

        Args:
            queue: The shared in-process queue.
        """
        self._queue = queue

    async def publish(self, envelope: Envelope, body: bytes) -> str:
        """Enqueues a message.

        Args:
            envelope: The envelope the message carries.
            body: The encoded message to enqueue.

        Returns:
            The envelope id.
        """
        await self._queue.put(body)
        return envelope.id


class MemoryDelivery:
    """Delivery from an in-process queue; rejecting with `requeue` re-enqueues it."""

    def __init__(self, body: bytes, queue: _Queue) -> None:
        """Initializes the delivery.

        Args:
            body: The message bytes.
            queue: The shared in-process queue.
        """
        self._body = body
        self._queue = queue
        self._settled = False

    @property
    def body(self) -> bytes:
        """The message bytes."""
        return self._body

    @property
    def settled(self) -> bool:
        """Whether `ack` or `reject` has been called."""
        return self._settled

    def _settle(self) -> None:
        if self._settled:
            raise RuntimeError("delivery already settled")
        self._settled = True

    async def ack(self) -> None:
        """Settles the delivery."""
        self._settle()

    async def reject(self, *, requeue: bool = False) -> None:
        """Settles the delivery, re-enqueuing it if `requeue` is true.

        Args:
            requeue: Whether to put the message back on the queue.
        """
        self._settle()
        if requeue:
            await self._queue.put(self._body)


class MemoryConsumer:
    """Consumer that reads from an in-process queue.

    `process` acks on success and rejects without requeue on any exception.
    """

    def __init__(self, queue: _Queue) -> None:
        """Initializes the consumer.

        Args:
            queue: The shared in-process queue.
        """
        self._queue = queue
        self._connected = False

    @property
    def connected(self) -> bool:
        """Whether `start` has run and `stop` has not."""
        return self._connected

    async def start(self) -> None:
        """Marks the consumer connected."""
        self._connected = True

    async def stop(self) -> None:
        """Marks the consumer disconnected and ends `deliveries`."""
        self._connected = False
        self._queue.put_nowait(_STOP)

    async def deliveries(self) -> AsyncIterator[Delivery]:
        """Yields a delivery for each queued message until `stop`."""
        while True:
            item = await self._queue.get()
            if isinstance(item, _Stop):
                return
            yield MemoryDelivery(item, self._queue)

    @asynccontextmanager
    async def process(self, delivery: Delivery) -> AsyncIterator[None]:
        """Acks on a clean exit and rejects without requeue on an exception.

        Args:
            delivery: The delivery being handled.

        Yields:
            None, while the delivery is handled.
        """
        try:
            yield
        except BaseException:
            if not delivery.settled:
                await delivery.reject(requeue=False)
            raise
        else:
            if not delivery.settled:
                await delivery.ack()


def memory_transport() -> tuple[MemoryProducer, MemoryConsumer]:
    """Creates a connected in-process producer and consumer pair.

    Returns:
        A `(producer, consumer)` tuple sharing one queue.
    """
    queue: _Queue = asyncio.Queue()
    return MemoryProducer(queue), MemoryConsumer(queue)
