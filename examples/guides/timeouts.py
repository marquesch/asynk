"""Limiting how long one attempt of a task may run."""

import asyncio

from asynk import App, MemoryStore, RetryPolicy, TaskError, TaskOptions, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App(
    "timeouts",
    producer=producer,
    consumer=consumer,
    store=MemoryStore(),
    # The default for every task: each attempt may run for 5 s.
    options=TaskOptions(timeout=5),
)

attempts = 0


# This task's limit replaces the app's, and a timed-out attempt is retried once.
@app.task(options=TaskOptions(timeout=0.05, retry=RetryPolicy(max_retries=1)))
async def call_partner() -> None:
    global attempts
    attempts += 1
    await asyncio.Event().wait()  # never answers


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())

    handle = await call_partner.delay()
    try:
        await handle.result(timeout=5)
    except TaskError as exc:
        print("call_partner failed:", exc.type_name, "after", attempts, "attempts")

    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
