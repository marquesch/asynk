"""A store on Redis, shared by every process that uses the same prefix."""

import asyncio

from asynk import App, Worker
from asynk.store.redis import RedisStore
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App(
    "orders",
    producer=producer,
    consumer=consumer,
    store=RedisStore("redis://localhost:6379/0", prefix="asynk:orders:"),
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


if __name__ == "__main__":
    asyncio.run(main())
