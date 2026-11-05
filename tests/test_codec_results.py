from __future__ import annotations

import asyncio
import pickle

from asynk import App, MemoryStore, Worker
from asynk.codec import DecodeError, EncodeError
from asynk.transport.memory import memory_transport


class PickleCodec:
    """A codec that pickles values."""

    def validate(self, value: object) -> None:
        self.encode(value)

    def encode(self, value: object) -> bytes:
        try:
            return pickle.dumps(value)
        except (pickle.PicklingError, TypeError, AttributeError) as exc:
            raise EncodeError(str(exc)) from exc

    def decode(self, data: bytes) -> object:
        try:
            return pickle.loads(data)  # noqa: S301
        except Exception as exc:
            raise DecodeError(str(exc)) from exc


async def test_one_app_codec_drives_message_bytes_and_stored_records() -> None:
    codec = PickleCodec()
    producer, consumer = memory_transport()
    store = MemoryStore()
    app = App("pickled", producer=producer, consumer=consumer, store=store, codec=codec)
    seen: list[object] = []

    @app.task()
    async def pair(arg: tuple[int, str]) -> tuple[int, str]:
        seen.append(arg)
        return (arg[0] + 1, arg[1])

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    handle = await pair.delay((1, "a"))
    assert await app.get_task(handle.task_id).result(timeout=5) == (2, "a")
    worker.request_shutdown()
    await asyncio.wait_for(run, 5)
    assert seen == [(1, "a")]
    stored = await store.get(f"result:{handle.task_id}")
    assert stored is not None
    assert pickle.loads(stored)["value"] == (2, "a")  # noqa: S301
