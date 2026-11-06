"""A minimal store over a dict, used by an app for task states and results.

A real store keeps the same keys in Redis or a database, so every producer,
worker and scheduler that shares it sees the same records.
"""

import asyncio

from asynk import App, Worker
from asynk.transport.memory import memory_transport


class DictStore:
    """Keeps values in a dict and wakes up waiters when a key is written."""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}
        self._written = asyncio.Condition()

    async def start(self) -> None:
        print("store: connected")  # open a connection pool here

    async def stop(self) -> None:
        print("store: disconnected")  # close it here

    async def get(self, key: str) -> bytes | None:
        return self._data.get(key)

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        if key in self._data and not overwrite:
            return False
        self._data[key] = value
        async with self._written:
            self._written.notify_all()
        return True

    async def wait(self, key: str, timeout: float | None = None) -> bytes | None:
        async with self._written:
            try:
                await asyncio.wait_for(self._written.wait_for(lambda: key in self._data), timeout)
            except TimeoutError:
                return None
        return self._data[key]

    async def delete(self, key: str) -> bool:
        return self._data.pop(key, None) is not None


producer, consumer = memory_transport()
app = App("custom-store", producer=producer, consumer=consumer, store=DictStore())


@app.task()
async def add(a: int, b: int) -> int:
    return a + b


async def main() -> None:
    worker = Worker(app)  # starts the store before it consumes, stops it last
    run = asyncio.create_task(worker.run())

    handle = await add.delay(2, b=3)
    print("2 + 3 =", await handle.result(timeout=5))

    worker.request_shutdown()
    await run


if __name__ == "__main__":
    asyncio.run(main())
