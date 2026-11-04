"""Sends a task to a worker running in another process.

1. In one terminal, from this directory: `python -m asynk.cli worker app:app`
2. In another terminal, from this directory: `python producer.py`
"""

import asyncio

from app import SOCKET, add


async def main() -> None:
    try:
        await add.delay(2, 3)
    except OSError:
        # Nothing is buffered, so publishing fails while no worker is listening.
        raise SystemExit(f"no worker listening on {SOCKET}; start it first") from None
    print("sent")


if __name__ == "__main__":
    asyncio.run(main())
