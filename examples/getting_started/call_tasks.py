"""Calling a task with delay and reading its result from the store."""

import asyncio

from asynk import App, MemoryStore, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("call-tasks", producer=producer, consumer=consumer, store=MemoryStore())


@app.task()
async def greet(name: str, greeting: str = "Hello") -> str:
    return f"{greeting}, {name}!"


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())

    # Positional and keyword arguments, type-checked against the task signature.
    first = await greet.delay("Ada")
    second = await greet.delay("Grace", greeting="Welcome")

    # Each call returns a handle that reads the result from the app's store.
    print(await first.result(timeout=5))
    print(await second.result(timeout=5))
    print((await second.state()).value)

    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
