"""Publishes tasks and runs a worker in the same process.

Run it with `python examples/memory/main.py`.
"""

import asyncio

from tasks import add, app, shout

from asynk import Worker


async def main() -> None:
    worker = Worker(app)
    run = asyncio.create_task(worker.run())

    # delay() only enqueues; arguments must be JSON-friendly.
    await add.delay(2, b=3)
    await shout.delay("hello")

    await asyncio.sleep(0.2)

    # Stops fetching new messages and lets running tasks finish.
    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
