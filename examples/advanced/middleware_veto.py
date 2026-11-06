"""A middleware layer that vetoes paused tasks with `call.skip()`."""

import asyncio

from asynk import App, MemoryStore, Next, TaskCall, TaskResult, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("veto", producer=producer, consumer=consumer, store=MemoryStore())

paused = {"send_newsletter"}


@app.middleware
async def skip_paused(call: TaskCall, next: Next) -> TaskResult:
    if call.envelope.task in paused:
        return call.skip()  # the task does not run
    return await next(call)


@app.task(name="send_newsletter")
async def send_newsletter() -> str:
    return "sent"


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    handle = await send_newsletter.delay()
    print("result:", await handle.result(timeout=5))
    print("state:", (await handle.state()).value)
    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
