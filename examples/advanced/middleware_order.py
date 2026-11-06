"""The last registered middleware layer is the outermost."""

import asyncio

from asynk import App, MemoryStore, Next, TaskCall, TaskResult, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("order", producer=producer, consumer=consumer, store=MemoryStore())

order: list[str] = []


@app.middleware
async def first(call: TaskCall, next: Next) -> TaskResult:
    order.append("first: in")
    result = await next(call)
    order.append("first: out")
    return result


@app.middleware
async def second(call: TaskCall, next: Next) -> TaskResult:
    order.append("second: in")
    result = await next(call)
    order.append("second: out")
    return result


@app.task()
async def work() -> None:
    order.append("task")


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    handle = await work.delay()
    await handle.result(timeout=5)
    worker.request_shutdown()
    await run
    print("\n".join(order))


if __name__ == "__main__":
    asyncio.run(main())
