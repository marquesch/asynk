"""Tasks are async functions and can have default arguments."""

import asyncio
from typing import Any

from asynk import App, MemoryStore, RetryPolicy, TaskHandle, TaskOptions, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("defining-tasks", producer=producer, consumer=consumer, store=MemoryStore())


@app.task()
async def add(a: int, b: int) -> int:
    return a + b


@app.task()
async def shout(text: str) -> str:
    return text.upper()


@app.task(options=TaskOptions(retry=RetryPolicy(max_retries=3)))
async def greet(name: str, greeting: str = "Hello") -> str:
    return f"{greeting}, {name}!"


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())

    handles: list[TaskHandle[Any]] = [
        await add.delay(1, 2),
        await shout.delay("hello"),
        await greet.delay("Ada"),
    ]
    for handle in handles:
        print(await handle.result(timeout=5))

    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
