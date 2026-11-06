from __future__ import annotations

from asynk.app import App
from asynk.transport.memory import memory_transport

_producer, _consumer = memory_transport()
app = App("fixture-scheduler", producer=_producer, consumer=_consumer)
no_producer = App("fixture-no-producer")


@app.scheduled_task(cron="0 0 1 1 *")
async def yearly() -> None:
    return None


@no_producer.scheduled_task(cron="0 0 1 1 *")
async def orphan() -> None:
    return None


_unencodable_producer, _unencodable_consumer = memory_transport()
unencodable = App(
    "fixture-unencodable", producer=_unencodable_producer, consumer=_unencodable_consumer
)


@unencodable.scheduled_task(cron="0 0 1 1 *", args=[object()])
async def bad_args(value: object) -> None:
    return None
