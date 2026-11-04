"""Static-only checks, verified by mypy (not collected by pytest)."""

from asynk import Delivery, TaskResult, TaskStatus
from asynk.app import App
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("typing", producer=producer, consumer=consumer)


@app.task()
async def add(a: int, b: int = 0) -> int:
    return a + b


@app.scheduled_task(cron="* * * * *")
async def scheduled(a: int, b: int = 0) -> int:
    return a + b


async def _checks() -> None:
    await add.delay(1, b=2)
    await add.delay("x")  # type: ignore[arg-type]
    await add.delay()  # type: ignore[call-arg]
    await add.delay(1, c=3)  # type: ignore[call-arg]
    await add.with_metadata(a=1).delay(1, b=2)
    await add.with_metadata(a=1).delay("x")  # type: ignore[arg-type]
    await scheduled.delay(1, b=2)
    await scheduled.delay("x")  # type: ignore[arg-type]


@app.after_task
async def retry(result: TaskResult, delivery: Delivery) -> None:
    if result.status is TaskStatus.FAILED:
        await producer.publish(result.envelope.retry())
        await delivery.ack()
