"""Scheduled tasks published by a Scheduler and run by a Worker.

A virtual clock makes a few minutes of schedule pass instantly; a real
scheduler uses the system clock.
"""

import asyncio
from datetime import UTC, datetime, timedelta

from asynk import App, MemoryStore, Scheduler, Worker
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("scheduled", producer=producer, consumer=consumer, store=MemoryStore())

ran: list[str] = []
all_ran = asyncio.Event()


def record(line: str) -> None:
    ran.append(line)
    if len(ran) == 3:
        all_ran.set()


@app.scheduled_task(cron="30 8 * * mon-fri", tz="America/Sao_Paulo")
async def morning_report() -> None:
    record("morning_report")


@app.scheduled_task(cron="*/5 * * * *", args=["eu"], kwargs={"limit": 100})
async def sync(region: str, *, limit: int) -> None:
    record(f"sync {region} limit={limit}")


class VirtualClock:
    """A clock whose `sleep` advances time instead of waiting.

    Attributes:
        finished: Set once the clock reaches its stop time.
    """

    def __init__(self, start: datetime, stop: datetime) -> None:
        self._now = start
        self._stop = stop
        self.finished = asyncio.Event()

    def now(self) -> datetime:
        return self._now

    async def sleep(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)
        if self._now >= self._stop:
            self.finished.set()


async def main() -> None:
    # Monday 11:24:30 to 11:31 UTC: `sync` fires at 11:25 and 11:30, and
    # `morning_report` at 11:30 UTC, which is 08:30 in Sao Paulo.
    clock = VirtualClock(
        start=datetime(2026, 1, 5, 11, 24, 30, tzinfo=UTC),
        stop=datetime(2026, 1, 5, 11, 31, tzinfo=UTC),
    )
    scheduler = Scheduler(app, clock=clock)
    run_scheduler = asyncio.create_task(scheduler.run())
    await clock.finished.wait()
    scheduler.request_shutdown()
    await run_scheduler

    # The scheduler only publishes; a worker runs what it published.
    worker = Worker(app, concurrency=1)
    run_worker = asyncio.create_task(worker.run())
    await all_ran.wait()
    worker.request_shutdown()
    await run_worker

    for line in ran:
        print(line)


if __name__ == "__main__":
    asyncio.run(main())
