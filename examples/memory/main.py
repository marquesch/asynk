"""Publishes tasks, runs a scheduler and a worker in the same process.

Run it with `python examples/memory/main.py`.
"""

import asyncio
from datetime import UTC, datetime, timedelta

from tasks import add, app, shout

from asynk import Scheduler, Worker


class FastClock:
    """A clock that runs ahead of real time, so a cron schedule can be demoed in a blink.

    A `Scheduler` reads time with `now()` and waits with `sleep()`. Here `sleep()`
    only advances a virtual time, which makes minutes of schedule pass in
    milliseconds.
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
        await asyncio.sleep(0.001)  # let the worker run what was just published


async def main() -> None:
    worker = Worker(app)
    run_worker = asyncio.create_task(worker.run())

    # delay() publishes the call and returns a handle.
    handle = await add.delay(2, b=3)
    await shout.delay("hello")

    # The handle reads the app's store: wait for the task and print its return value.
    print("result:", await handle.result(timeout=5))
    print("state:", (await handle.state()).value)

    # The scheduler publishes scheduled tasks as their cron slots come due, and the
    # worker runs them like any other message. A Monday, from 08:28:30 to 08:31:30
    # virtual UTC: three `tick` slots and the 08:30 `morning_report`.
    clock = FastClock(
        start=datetime(2026, 1, 5, 8, 28, 30, tzinfo=UTC),
        stop=datetime(2026, 1, 5, 8, 31, 30, tzinfo=UTC),
    )
    scheduler = Scheduler(app, clock=clock)
    run_scheduler = asyncio.create_task(scheduler.run())
    await clock.finished.wait()
    scheduler.request_shutdown()
    await run_scheduler

    await asyncio.sleep(0.2)

    # Stops fetching new messages and lets running tasks finish.
    worker.request_shutdown()
    await run_worker


if __name__ == "__main__":
    asyncio.run(main())
