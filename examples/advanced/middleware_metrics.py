"""Timing and counting tasks with a middleware layer, using only the standard library."""

import asyncio
import contextlib
import time
from collections import defaultdict

from asynk import App, MemoryStore, Next, TaskCall, TaskError, TaskResult, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("metrics", producer=producer, consumer=consumer, store=MemoryStore())


class TaskMetrics:
    """Counts runs per task and status, and adds up the seconds each task took."""

    def __init__(self) -> None:
        self.counts: defaultdict[tuple[str, str], int] = defaultdict(int)
        self.seconds: defaultdict[str, float] = defaultdict(float)

    def observe(self, task: str, status: str, seconds: float) -> None:
        self.counts[task, status] += 1
        self.seconds[task] += seconds


metrics = TaskMetrics()
paused = {"cleanup"}


@app.middleware
async def skip_paused(call: TaskCall, next: Next) -> TaskResult:
    if call.envelope.task in paused:
        return call.skip()
    return await next(call)


# Registered last, so it is the outermost layer and also sees skipped tasks.
@app.middleware
async def measure(call: TaskCall, next: Next) -> TaskResult:
    started = time.perf_counter()
    result = await next(call)
    metrics.observe(call.envelope.task, result.status.value, time.perf_counter() - started)
    return result


@app.task(name="add")
async def add(a: int, b: int) -> int:
    return a + b


@app.task(name="divide")
async def divide(a: int, b: int) -> float:
    return a / b


@app.task(name="cleanup")
async def cleanup() -> None: ...


async def main() -> TaskMetrics:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    handles = [await add.delay(1, 2), await add.delay(3, 4), await cleanup.delay()]
    for handle in handles:
        await handle.result(timeout=5)
    failing = await divide.delay(1, 0)
    with contextlib.suppress(TaskError):
        await failing.result(timeout=5)
    worker.request_shutdown()
    await run
    for (task, status), count in sorted(metrics.counts.items()):
        print(f"{task} {status}: {count}")
    for task, seconds in sorted(metrics.seconds.items()):
        print(f"{task}: {seconds:.6f}s in total")
    return metrics


if __name__ == "__main__":
    asyncio.run(main())
