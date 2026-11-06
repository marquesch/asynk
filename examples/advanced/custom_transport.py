"""A minimal transport: a producer, a consumer and their deliveries over an asyncio queue.

A real transport talks to a broker in the same places: `publish` sends the body,
`deliveries` receives messages, and `ack` and `reject` settle them.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from asynk import App, Delivery, Envelope, MemoryStore, PublishOptions, Worker

Queue = asyncio.Queue[bytes | None]


class QueueProducer:
    """Sends message bodies to the queue."""

    def __init__(self, queue: Queue) -> None:
        self._queue = queue

    async def publish(
        self, envelope: Envelope, body: bytes, options: PublishOptions | None = None
    ) -> str:
        # The queue cannot delay a message, so `options` is ignored.
        await self._queue.put(body)
        return envelope.id  # the receipt, available as handle.receipt


class QueueDelivery:
    """One received message, settled once."""

    def __init__(self, body: bytes, queue: Queue) -> None:
        self._body = body
        self._queue = queue
        self._settled = False

    @property
    def body(self) -> bytes:
        return self._body

    @property
    def settled(self) -> bool:
        return self._settled

    async def ack(self) -> None:
        self._settled = True

    async def reject(self, *, requeue: bool = False) -> None:
        self._settled = True
        if requeue:
            await self._queue.put(self._body)


class QueueConsumer:
    """Receives messages from the queue and settles each one."""

    def __init__(self, queue: Queue) -> None:
        self._queue = queue
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    async def start(self) -> None:
        self._connected = True

    async def stop(self) -> None:
        self._connected = False
        self._queue.put_nowait(None)

    async def deliveries(self) -> AsyncIterator[Delivery]:
        while (body := await self._queue.get()) is not None:
            yield QueueDelivery(body, self._queue)

    @asynccontextmanager
    async def process(self, delivery: Delivery) -> AsyncIterator[None]:
        try:
            yield
        except BaseException:
            if not delivery.settled:
                await delivery.reject()
            raise  # never suppress, cancellation included
        if not delivery.settled:
            await delivery.ack()


queue: Queue = asyncio.Queue()
app = App(
    "custom-transport",
    producer=QueueProducer(queue),
    consumer=QueueConsumer(queue),
    store=MemoryStore(),
)


@app.task()
async def add(a: int, b: int) -> int:
    return a + b


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())

    handle = await add.delay(2, b=3)
    print("receipt:", handle.receipt == handle.task_id)
    print("2 + 3 =", await handle.result(timeout=5))

    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
