"""A middleware layer that logs each task before and after it runs."""

import asyncio

from asynk import App, MemoryStore, Next, TaskCall, TaskResult, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("minimal", producer=producer, consumer=consumer, store=MemoryStore())


@app.middleware
async def log_tasks(call: TaskCall, next: Next) -> TaskResult:
    print(f"starting {call.envelope.task} (attempt {call.envelope.attempt})")
    result = await next(call)
    print(f"{call.envelope.task} ended with {result.status.value}")
    return result


@app.task()
async def greet(name: str) -> str:
    return f"hello, {name}"


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    handle = await greet.delay("asynk")
    print("result:", await handle.result(timeout=5))
    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
