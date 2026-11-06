"""A producer and a worker that share a queue on Redis Streams."""

import asyncio

from asynk import App, MemoryStore, Worker
from asynk.transport.redis import RedisConsumer, RedisProducer

URL = "redis://localhost:6379/0"

app = App(
    "orders",
    producer=RedisProducer(URL, queue="orders"),
    consumer=RedisConsumer(URL, queue="orders", group="orders", max_deliveries=5),
    store=MemoryStore(),
)


@app.task()
async def add(a: int, b: int) -> int:
    return a + b


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())

    handle = await add.delay(1, b=2)
    print("result:", await handle.result(timeout=5))

    worker.request_shutdown()
    await run
    await app.producer.close()  # type: ignore[union-attr]


if __name__ == "__main__":
    asyncio.run(main())
