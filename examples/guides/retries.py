"""Retrying failing tasks with a retry policy and exponential backoff."""

import asyncio
from collections import Counter

from asynk import App, Exponential, MemoryStore, RetryPolicy, TaskError, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App(
    "retries",
    producer=producer,
    consumer=consumer,
    store=MemoryStore(),
    # The default for every task: up to 3 retries, waiting 0.01 s, then 0.02 s, ...
    retry=RetryPolicy(max_retries=3, backoff=Exponential(base=0.01, factor=2, max=0.1)),
)

attempts: Counter[str] = Counter()


@app.task()
async def fetch(url: str) -> str:
    attempts["fetch"] += 1
    if attempts["fetch"] <= 2:
        raise ConnectionError(f"cannot reach {url}")
    return "page"


# This task's policy replaces the app's: only timeouts are retried.
@app.task(retry={"max_retries": 5, "retry_on": (TimeoutError,)})
async def charge(amount: int) -> None:
    attempts["charge"] += 1
    raise ValueError("card declined")


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())

    page = await (await fetch.delay("https://example.com")).result(timeout=5)
    print(f"fetch: {page} after {attempts['fetch']} attempts")

    handle = await charge.delay(100)
    try:
        await handle.result(timeout=5)
    except TaskError as exc:
        print("charge failed:", exc.type_name, "-", exc.message)
    print("charge state:", (await handle.state()).value)

    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
