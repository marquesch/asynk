"""`RedisProducer` and `RedisConsumer`, a transport on Redis Streams."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.exceptions import ConnectionError, ResponseError, TimeoutError

from asynk._redis import RedisConnection
from asynk.envelope import Envelope
from asynk.transport import Delivery, PublishOptions

_BODY = b"body"
_ID_LENGTH = 32


class _Queue:
    """The Redis keys, and the client, of one queue."""

    def __init__(self, url: str | None, client: Redis | None, queue: str) -> None:
        self._connection = RedisConnection(url, client, owner="a Redis transport")
        self.client = self._connection.client
        self.stream = f"asynk:{{{queue}}}:stream"
        self.delayed = f"asynk:{{{queue}}}:delayed"
        self.dead = f"asynk:{{{queue}}}:dead"

    async def close(self) -> None:
        await self._connection.close()


class RedisProducer:
    """Producer that publishes to a Redis stream, optionally after a delay."""

    def __init__(
        self, url: str | None = None, *, client: Redis | None = None, queue: str = "default"
    ) -> None:
        """Initializes the producer.

        Args:
            url: Redis URL, such as `redis://localhost:6379/0`. The producer
                creates its own client; `close` closes it. Give either `url`
                or `client`.
            client: Redis asyncio client, created with `decode_responses=False`.
                The caller owns it and closes it.
            queue: Name of the queue to publish to.

        Raises:
            ValueError: If not exactly one of `url` and `client` is given.
        """
        self._queue = _Queue(url, client, queue)

    async def publish(
        self, envelope: Envelope, body: bytes, options: PublishOptions | None = None
    ) -> str:
        """Adds a message to the queue, held back for `options.delay` seconds if given.

        Args:
            envelope: The envelope the message carries.
            body: The encoded message to send.
            options: The publish options.

        Returns:
            The stream entry id for an immediate message, or the envelope id
            for a delayed one.
        """
        client = self._queue.client
        delay = options.delay if options is not None else None
        if not delay:
            entry_id: Any = await client.xadd(self._queue.stream, {_BODY: body})
            return str(entry_id.decode())
        seconds, microseconds = await client.time()
        due_ms = seconds * 1000 + microseconds // 1000 + round(delay * 1000)
        member = uuid.uuid4().hex.encode() + body
        await client.zadd(self._queue.delayed, {member: due_ms})
        return envelope.id

    async def close(self) -> None:
        """Closes the client if the producer created it."""
        await self._queue.close()


class RedisDelivery:
    """Delivery of a stream entry; settling it removes the entry from the stream."""

    def __init__(self, consumer: RedisConsumer, entry_id: bytes, body: bytes) -> None:
        """Initializes the delivery.

        Args:
            consumer: The consumer that received the entry.
            entry_id: The stream entry id.
            body: The message bytes.
        """
        self._consumer = consumer
        self._entry_id = entry_id
        self._body = body
        self._settled = False

    @property
    def body(self) -> bytes:
        """The message bytes."""
        return self._body

    @property
    def settled(self) -> bool:
        """Whether `ack` or `reject` has completed."""
        return self._settled

    async def ack(self) -> None:
        """Acknowledges the message and removes it from the stream."""
        self._check_unsettled()
        await self._consumer._settle(self._entry_id)
        self._settled = True

    async def reject(self, *, requeue: bool = False) -> None:
        """Rejects the message.

        Args:
            requeue: If True, a fresh copy goes back on the stream; otherwise the
                message moves to the dead-letter stream.
        """
        self._check_unsettled()
        if requeue:
            await self._consumer._settle(self._entry_id, requeue=self._body)
        else:
            await self._consumer._settle(self._entry_id, dead=(self._body, "rejected"))
        self._settled = True

    def _check_unsettled(self) -> None:
        if self._settled:
            raise RuntimeError("delivery already settled")


class RedisConsumer:
    """Consumer that reads a Redis stream through a consumer group.

    `process` acks on success and rejects without requeue on any exception.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        client: Redis | None = None,
        queue: str = "default",
        group: str = "asynk",
        lease: float = 60.0,
        block: float = 1.0,
        max_deliveries: int | None = None,
        dead_maxlen: int = 10_000,
    ) -> None:
        """Initializes the consumer.

        Args:
            url: Redis URL, such as `redis://localhost:6379/0`. The consumer
                creates and closes its own client. Give either `url` or `client`.
            client: Redis asyncio client, created with `decode_responses=False`.
                The caller owns it and closes it.
            queue: Name of the queue to consume.
            group: Name of the consumer group; consumers in one group share the
                queue's messages.
            lease: Seconds a delivered message may stay unsettled before another
                consumer in the group takes it over.
            block: Seconds each read waits for a message; also how often delayed
                messages are moved to the stream and expired leases are taken over.
            max_deliveries: Deliveries after which a message that keeps being
                taken over moves to the dead-letter stream; None sets no limit.
            dead_maxlen: Approximate number of entries the dead-letter stream keeps.

        Raises:
            ValueError: If not exactly one of `url` and `client` is given, or
                `lease`, `block`, `max_deliveries` or `dead_maxlen` is not positive.
        """
        self._queue = _Queue(url, client, queue)
        if lease <= 0 or block <= 0:
            raise ValueError("lease and block must be > 0")
        if max_deliveries is not None and max_deliveries < 1:
            raise ValueError(f"max_deliveries must be >= 1, got {max_deliveries}")
        if dead_maxlen < 1:
            raise ValueError(f"dead_maxlen must be >= 1, got {dead_maxlen}")
        self._group = group
        self._name = uuid.uuid4().hex.encode()
        self._lease_ms = round(lease * 1000)
        self._block_ms = max(1, round(block * 1000))
        self._max_deliveries = max_deliveries
        self._dead_maxlen = dead_maxlen
        self._connected = False
        self._stopped = False

    @property
    def connected(self) -> bool:
        """Whether the last command reached Redis and the consumer is started."""
        return self._connected

    async def start(self) -> None:
        """Connects to Redis and creates the consumer group if it does not exist.

        A new group starts at the beginning of the stream, so messages published
        before it existed are consumed.
        """
        client = self._queue.client
        try:
            await client.xgroup_create(self._queue.stream, self._group, "0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._stopped = False
        self._connected = True

    async def stop(self) -> None:
        """Ends `deliveries` and disconnects.

        Messages still unsettled stay pending for the other consumers of the
        group to take over once their lease expires.
        """
        self._stopped = True
        self._connected = False
        client = self._queue.client
        try:
            for info in await client.xinfo_consumers(self._queue.stream, self._group):
                if info["name"] == self._name and info["pending"] == 0:
                    await client.xgroup_delconsumer(self._queue.stream, self._group, self._name)
        except (ConnectionError, TimeoutError):
            pass
        await self._queue.close()

    async def deliveries(self) -> AsyncIterator[Delivery]:
        """Yields a delivery for each message until `stop`."""
        ready: deque[RedisDelivery] = deque()
        next_upkeep = 0.0
        while not self._stopped:
            try:
                if ready:
                    yield ready.popleft()
                    continue
                if time.monotonic() >= next_upkeep:
                    await self._promote_due()
                    ready.extend(await self._take_over_expired())
                    next_upkeep = time.monotonic() + self._block_ms / 1000
                    continue
                entries: Any = await self._queue.client.xreadgroup(
                    self._group,
                    self._name.decode(),
                    {self._queue.stream: ">"},
                    count=1,
                    block=self._block_ms,
                )
                self._connected = True
                for _, messages in entries:
                    ready.extend(self._delivery(entry_id, fields) for entry_id, fields in messages)
            except (ConnectionError, TimeoutError):
                self._connected = False
                await asyncio.sleep(self._block_ms / 1000)

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

    def _delivery(self, entry_id: bytes, fields: dict[bytes, bytes]) -> RedisDelivery:
        return RedisDelivery(self, entry_id, fields[_BODY])

    async def _settle(
        self,
        entry_id: bytes,
        *,
        requeue: bytes | None = None,
        dead: tuple[bytes, str] | None = None,
    ) -> None:
        """Acks and deletes the entry, in one transaction with a requeue or dead letter."""
        queue = self._queue
        async with queue.client.pipeline(transaction=True) as pipe:
            if requeue is not None:
                pipe.xadd(queue.stream, {_BODY: requeue})
            if dead is not None:
                self._dead_letter(pipe, entry_id, *dead)
            pipe.xack(queue.stream, self._group, entry_id)
            pipe.xdel(queue.stream, entry_id)
            await pipe.execute()
        self._connected = True

    def _dead_letter(self, pipe: Pipeline, entry_id: bytes, body: bytes, reason: str) -> None:
        pipe.xadd(
            self._queue.dead,
            {
                _BODY: body,
                b"reason": reason,
                b"entry_id": entry_id,
                b"at": str(round(time.time() * 1000)),
            },
            maxlen=self._dead_maxlen,
            approximate=True,
        )

    async def _promote_due(self) -> None:
        """Moves the delayed messages that are due onto the stream, atomically."""
        queue = self._queue

        async def move(pipe: Pipeline) -> None:
            seconds, microseconds = await pipe.time()
            now_ms = seconds * 1000 + microseconds // 1000
            due: Any = await pipe.zrangebyscore(queue.delayed, 0, now_ms)
            pipe.multi()  # type: ignore[no-untyped-call]
            for member in due:
                pipe.zrem(queue.delayed, member)
                pipe.xadd(queue.stream, {_BODY: member[_ID_LENGTH:]})

        await queue.client.transaction(move, queue.delayed)

    async def _take_over_expired(self) -> list[RedisDelivery]:
        """Takes over messages unsettled longer than the lease; dead-letters over-delivered ones."""
        queue = self._queue
        client = queue.client
        claim: Any = await client.xautoclaim(
            queue.stream, self._group, self._name.decode(), self._lease_ms, "0-0", count=10
        )
        claimed: list[tuple[bytes, dict[bytes, bytes] | None]] = claim[1]
        taken: list[RedisDelivery] = []
        for entry_id, fields in claimed:
            if fields is None:  # the entry was deleted while pending
                await client.xack(queue.stream, self._group, entry_id)
                continue
            if self._max_deliveries is not None:
                pending: Any = await client.xpending_range(
                    queue.stream, self._group, entry_id, entry_id, 1
                )
                if pending[0]["times_delivered"] > self._max_deliveries:
                    await self._settle(entry_id, dead=(fields[_BODY], "max_deliveries"))
                    continue
            taken.append(self._delivery(entry_id, fields))
        return taken
