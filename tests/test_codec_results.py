from __future__ import annotations

import asyncio
import pickle

from asynk import App, MemoryStore, Worker
from asynk.envelope import Envelope
from asynk.transport.memory import memory_transport


class PickleCodec:
    """A codec that pickles envelopes and values."""

    def validate(self, envelope: Envelope) -> None:
        pickle.dumps(envelope)

    def encode(self, envelope: Envelope) -> bytes:
        return pickle.dumps(envelope)

    def decode(self, data: bytes) -> Envelope:
        envelope = pickle.loads(data)  # noqa: S301
        assert isinstance(envelope, Envelope)
        return envelope

    def encode_value(self, value: object) -> bytes:
        return pickle.dumps(value)

    def decode_value(self, data: bytes) -> object:
        return pickle.loads(data)  # noqa: S301


async def test_results_round_trip_through_a_non_json_codec() -> None:
    codec = PickleCodec()
    producer, consumer = memory_transport(codec)
    app = App("pickled", producer=producer, consumer=consumer, store=MemoryStore(), codec=codec)

    @app.task()
    async def pair() -> tuple[int, str]:
        return (1, "a")

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    handle = await pair.delay()
    assert await app.get_task(handle.task_id).result(timeout=5) == (1, "a")
    worker.request_shutdown()
    await asyncio.wait_for(run, 5)
