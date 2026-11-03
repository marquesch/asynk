from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TypeAlias

from asynk.codec import Codec, DecodeError
from asynk.codec.json import JsonCodec
from asynk.envelope import Envelope
from asynk.transport import Delivery


class _Stop:
    pass


_STOP = _Stop()

_Queue: TypeAlias = "asyncio.Queue[bytes | _Stop]"


class MemoryProducer:
    """Producer that publishes to an in-process queue."""

    def __init__(self, queue: _Queue, codec: Codec) -> None:
        self._queue = queue
        self._codec = codec

    async def publish(self, envelope: Envelope) -> str:
        """Encodes and enqueues an envelope.

        Args:
            envelope: The envelope to send.

        Returns:
            The envelope id.
        """
        await self._queue.put(self._codec.encode(envelope))
        return envelope.id

    def publish_raw(self, data: bytes) -> None:
        """Enqueues bytes as-is, bypassing the codec (useful for malformed-input tests).

        Args:
            data: The raw message bytes.
        """
        self._queue.put_nowait(data)


class MemoryDelivery:
    """Delivery from an in-process queue; rejecting with `requeue` re-enqueues it."""

    def __init__(self, envelope: Envelope, raw: bytes, queue: _Queue) -> None:
        self._envelope = envelope
        self._raw = raw
        self._queue = queue
        self._settled = False

    @property
    def envelope(self) -> Envelope:
        return self._envelope

    @property
    def settled(self) -> bool:
        return self._settled

    def _settle(self) -> None:
        if self._settled:
            raise RuntimeError("delivery already settled")
        self._settled = True

    async def ack(self) -> None:
        self._settle()

    async def reject(self, *, requeue: bool = False) -> None:
        self._settle()
        if requeue:
            await self._queue.put(self._raw)


class MemoryConsumer:
    """Consumer that reads from an in-process queue.

    Messages that fail to decode are dropped. `process` acks on success and
    rejects without requeue on any exception.
    """

    def __init__(self, queue: _Queue, codec: Codec) -> None:
        self._queue = queue
        self._codec = codec
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    async def start(self) -> None:
        self._connected = True

    async def stop(self) -> None:
        self._connected = False
        self._queue.put_nowait(_STOP)

    async def deliveries(self) -> AsyncIterator[Delivery]:
        while True:
            item = await self._queue.get()
            if isinstance(item, _Stop):
                return
            try:
                envelope = self._codec.decode(item)
            except DecodeError:
                continue
            yield MemoryDelivery(envelope, item, self._queue)

    @asynccontextmanager
    async def process(self, delivery: Delivery) -> AsyncIterator[None]:
        try:
            yield
        except BaseException:
            if not delivery.settled:
                await delivery.reject(requeue=False)
            raise
        else:
            if not delivery.settled:
                await delivery.ack()


def memory_transport(codec: Codec | None = None) -> tuple[MemoryProducer, MemoryConsumer]:
    """Creates a connected in-process producer and consumer pair.

    Args:
        codec: Codec for the queue's messages; defaults to `JsonCodec`.

    Returns:
        A `(producer, consumer)` tuple sharing one queue.
    """
    shared = codec if codec is not None else JsonCodec()
    queue: _Queue = asyncio.Queue()
    return MemoryProducer(queue, shared), MemoryConsumer(queue, shared)
