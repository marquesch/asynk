from __future__ import annotations

from asynk.app import App
from asynk.transport.memory import memory_transport

_producer, _consumer = memory_transport()
app = App("fixture", producer=_producer, consumer=_consumer)


@app.task()
def noop() -> None:
    return None
