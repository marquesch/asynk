from __future__ import annotations

from asynk.app import App
from asynk.transport.memory import memory_transport

_producer, _consumer = memory_transport()
app = App("fixture-ticker", producer=_producer, consumer=_consumer)
no_producer = App("fixture-no-producer")


@app.tick(cron="0 0 1 1 *")
def yearly() -> None:
    return None


@no_producer.tick(cron="0 0 1 1 *")
def orphan() -> None:
    return None
