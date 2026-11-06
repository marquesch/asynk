"""Lifecycle hooks shared by every asynk process, branching on the runner's role."""

import asyncio

from asynk import App, MemoryStore, Runner, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("hooks", producer=producer, consumer=consumer, store=MemoryStore())

connections: list[str] = []


@app.before_startup
async def connect(runner: Runner) -> None:
    print(f"{runner.role}: setting up telemetry")
    if runner.role == "worker":
        connections.append("database")  # only workers need the database


@app.after_startup
def started(runner: Runner) -> None:
    print(f"{runner.role}: started")


@app.before_shutdown
def stopping(runner: Runner) -> None:
    print(f"{runner.role}: stopping")


@app.after_shutdown
async def disconnect(runner: Runner) -> None:
    connections.clear()
    print(f"{runner.role}: stopped")


@app.task()
async def query() -> list[str]:
    return list(connections)


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    handle = await query.delay()
    print("connections:", await handle.result(timeout=5))
    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
