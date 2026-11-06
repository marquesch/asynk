"""The smallest asynk app: one task, published and run in the same process."""

import asyncio

from asynk import App, MemoryStore, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("first-app", producer=producer, consumer=consumer, store=MemoryStore())


@app.task()
async def add(a: int, b: int) -> int:
    return a + b


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())

    handle = await add.delay(2, b=3)
    print("2 + 3 =", await handle.result(timeout=5))

    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
