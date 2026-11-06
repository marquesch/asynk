"""A blocking call in an async task is offloaded to a thread."""

import asyncio
import time

from asynk import App, MemoryStore, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("blocking-task", producer=producer, consumer=consumer, store=MemoryStore())


@app.task()
async def slow_square(n: int) -> int:
    # time.sleep(0.1) here would block the event loop; to_thread does not.
    await asyncio.to_thread(time.sleep, 0.1)
    return n * n


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())

    handle = await slow_square.delay(4)
    print(await handle.result(timeout=5))

    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
