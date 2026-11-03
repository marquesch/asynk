"""Static-only checks, verified by mypy (not collected by pytest)."""

from asynk import Delivery, TaskResult, TaskStatus
from asynk.app import App
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("typing", producer=producer, consumer=consumer)


@app.task()
async def add(a: int, b: int = 0) -> int:
    return a + b


async def _checks() -> None:
    await add.delay(1, b=2)
    await add.delay("x")  # type: ignore[arg-type]
    await add.delay()  # type: ignore[call-arg]
    await add.delay(1, c=3)  # type: ignore[call-arg]


@app.after_task
async def retry(result: TaskResult, delivery: Delivery) -> None:
    if result.status is TaskStatus.FAILED:
        await producer.publish(result.envelope.retry())
        await delivery.ack()
