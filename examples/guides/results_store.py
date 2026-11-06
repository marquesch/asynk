"""Reading task states and results from the app's store."""

import asyncio

from asynk import App, MemoryStore, TaskError, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("results", producer=producer, consumer=consumer, store=MemoryStore())


@app.task()
async def add(a: int, b: int) -> int:
    return a + b


@app.task()
async def divide(a: int, b: int) -> float:
    return a / b


async def main() -> None:
    # Published before any worker runs, so the task is still pending.
    handle = await add.delay(1, b=2)
    print("before the worker:", (await handle.state()).value)

    worker = Worker(app)
    run = asyncio.create_task(worker.run())

    print("result:", await handle.result(timeout=5))
    print("after the worker:", (await handle.state()).value)

    # Another request or process sharing the store only needs the id.
    same = app.get_task(handle.task_id)
    print("from the id:", await same.result(timeout=5))

    failing = await divide.delay(1, 0)
    try:
        await failing.result(timeout=5)
    except TaskError as exc:
        print("failed:", exc.type_name, "-", exc.message)
    print("state:", (await failing.state()).value)

    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
