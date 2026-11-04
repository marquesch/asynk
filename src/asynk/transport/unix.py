from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TypeAlias

from asynk.codec import Codec, DecodeError
from asynk.codec.json import JsonCodec
from asynk.envelope import Envelope
from asynk.transport import Delivery


class _Stop:
    pass


_STOP = _Stop()

_Queue: TypeAlias = "asyncio.Queue[Envelope | _Stop]"


class UnixProducer:
    """Producer that sends each envelope to a unix socket, one connection per message.

    Meant for learning and testing, not production: there is no buffering or
    retry, so publishing fails if no consumer is listening.
    """

    def __init__(self, path: str | os.PathLike[str], codec: Codec | None = None) -> None:
        """Initializes the producer.

        Args:
            path: Path of the socket the consumer listens on.
            codec: Codec for the messages; defaults to `JsonCodec`. It must not
                emit newlines, which delimit messages.
        """
        self._path = os.fspath(path)
        self._codec = codec if codec is not None else JsonCodec()

    async def publish(self, envelope: Envelope) -> str:
        """Encodes an envelope and sends it as one line.

        Args:
            envelope: The envelope to send.

        Returns:
            The envelope id.

        Raises:
            OSError: If no consumer is listening on the socket.
        """
        _, writer = await asyncio.open_unix_connection(self._path)
        try:
            writer.write(self._codec.encode(envelope) + b"\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
        return envelope.id


class UnixDelivery:
    """Delivery from a unix socket; settling only records the outcome.

    Nothing is ever redelivered: rejecting, even with `requeue=True`, loses the message.
    """

    def __init__(self, envelope: Envelope) -> None:
        self._envelope = envelope
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


class UnixConsumer:
    """Consumer that listens on a unix socket and creates the socket file.

    Meant for learning and testing, not production: messages are held in memory
    and are lost if the worker stops, crashes or rejects them.
    """

    def __init__(self, path: str | os.PathLike[str], codec: Codec | None = None) -> None:
        """Initializes the consumer.

        Args:
            path: Path of the socket to create.
            codec: Codec for the messages; defaults to `JsonCodec`.
        """
        self._path = Path(path)
        self._codec = codec if codec is not None else JsonCodec()
        self._queue: _Queue = asyncio.Queue()
        self._server: asyncio.AbstractServer | None = None

    @property
    def connected(self) -> bool:
        return self._server is not None

    async def start(self) -> None:
        """Replaces any stale socket file and starts listening."""
        self._path.unlink(missing_ok=True)
        self._server = await asyncio.start_unix_server(self._on_connection, self._path)

    async def stop(self) -> None:
        """Stops listening, removes the socket file and ends `deliveries`."""
        if self._server is not None:
            self._server.close()
            self._server = None
        self._path.unlink(missing_ok=True)
        self._queue.put_nowait(_STOP)

    async def _on_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            async for line in reader:
                try:
                    self._queue.put_nowait(self._codec.decode(line))
                except DecodeError:
                    continue
        except ValueError:
            pass
        finally:
            writer.close()

    async def deliveries(self) -> AsyncIterator[Delivery]:
        while True:
            item = await self._queue.get()
            if isinstance(item, _Stop):
                return
            yield UnixDelivery(item)

    @asynccontextmanager
    async def process(self, delivery: Delivery) -> AsyncIterator[None]:
        try:
            yield
        except BaseException:
            if not delivery.settled:
                await delivery.reject()
            raise
        else:
            if not delivery.settled:
                await delivery.ack()


def unix_transport(
    path: str | os.PathLike[str], codec: Codec | None = None
) -> tuple[UnixProducer, UnixConsumer]:
    """Creates a producer and consumer pair for one socket path.

    Both ends are usually created in different processes, each calling this
    function with the same path and codec.

    Args:
        path: Path of the socket file.
        codec: Codec for the messages; defaults to `JsonCodec`.

    Returns:
        A `(producer, consumer)` tuple.
    """
    return UnixProducer(path, codec), UnixConsumer(path, codec)
