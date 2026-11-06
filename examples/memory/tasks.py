"""Tasks for the in-memory transport example."""

from asynk import App, MemoryStore
from asynk.transport.memory import memory_transport

# Producer and consumer share one in-process queue; MemoryStore keeps the task results.
producer, consumer = memory_transport()
app = App("memory-example", producer=producer, consumer=consumer, store=MemoryStore())


@app.task(max_retries=3)
async def add(a: int, b: int) -> int:
    print(f"{a} + {b} = {a + b}")
    return a + b


# Plain `def` tasks are allowed too; the worker runs them in a thread.
@app.task()
def shout(text: str) -> None:
    print(text.upper())


# A scheduled task is an ordinary task that a Scheduler also publishes on a cron
# schedule, with fixed args and kwargs. A Worker still runs it.
@app.scheduled_task(cron="* * * * *", args=["tick"])
async def tick(label: str) -> None:
    print(label)


# `tz` sets the zone the expression is evaluated in (default: the app's `tz`).
@app.scheduled_task(cron="30 8 * * mon-fri", kwargs={"region": "eu"}, tz="UTC")
def morning_report(*, region: str) -> None:
    print(f"morning report for {region}")
