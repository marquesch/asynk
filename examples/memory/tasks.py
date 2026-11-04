"""Tasks for the in-memory transport example.

The memory transport is for demos and tests only: its queue lives inside one
process and vanishes when that process exits. Do not use it in production.
"""

from asynk import App
from asynk.transport.memory import memory_transport

# Producer and consumer share one in-process queue, so whoever publishes and
# whoever runs the worker must be the same process. Messages use JsonCodec.
producer, consumer = memory_transport()
app = App("memory-example", producer=producer, consumer=consumer)


@app.task(max_retries=3)
async def add(a: int, b: int) -> int:
    print(f"{a} + {b} = {a + b}")
    return a + b


# Plain `def` tasks are allowed too; the worker runs them in a thread.
@app.task()
def shout(text: str) -> None:
    print(text.upper())
