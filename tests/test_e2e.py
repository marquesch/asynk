from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import asynk
from asynk import App, Delivery, Envelope, JsonCodec, Worker
from asynk.transport.memory import memory_transport

NAMES = [
    "App",
    "Task",
    "Envelope",
    "Codec",
    "JsonCodec",
    "Producer",
    "Consumer",
    "Delivery",
    "TaskStatus",
    "TaskResult",
    "Worker",
    "NonJsonArgumentError",
    "NotConfiguredError",
    "TaskFailedError",
    "DecodeError",
    "SkipTask",
]


def test_import_surface() -> None:
    assert sorted(asynk.__all__) == sorted(NAMES)
    for name in NAMES:
        assert getattr(asynk, name) is not None


async def test_delay_to_worker_roundtrip() -> None:
    producer, consumer = memory_transport()
    app = App("e2e", producer=producer, consumer=consumer)
    done = asyncio.Event()
    seen: list[tuple[int, int]] = []

    @app.task()
    async def add(a: int, *, b: int) -> None:
        seen.append((a, b))
        done.set()

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    await add.delay(2, b=3)
    await asyncio.wait_for(done.wait(), 5)
    worker.request_shutdown()
    await asyncio.wait_for(run, 5)
    assert seen == [(2, 3)]


class ListProducer:
    def __init__(self, wire: list[str]) -> None:
        self._wire = wire
        self._codec = JsonCodec()

    async def publish(self, envelope: Envelope) -> str:
        self._wire.append(self._codec.encode(envelope).decode())
        return envelope.id


class _ListDelivery:
    def __init__(self, envelope: Envelope) -> None:
        self.envelope_ = envelope
        self.settled_ = False

    @property
    def envelope(self) -> Envelope:
        return self.envelope_

    @property
    def settled(self) -> bool:
        return self.settled_

    async def ack(self) -> None:
        self.settled_ = True

    async def reject(self, *, requeue: bool = False) -> None:
        self.settled_ = True


class ListConsumer:
    def __init__(self, wire: list[str]) -> None:
        self._wire = wire
        self._codec = JsonCodec()
        self._connected = False
        self._stopped = asyncio.Event()

    @property
    def connected(self) -> bool:
        return self._connected

    async def start(self) -> None:
        self._connected = True

    async def stop(self) -> None:
        self._connected = False
        self._stopped.set()

    async def deliveries(self) -> AsyncIterator[Delivery]:
        while not self._stopped.is_set():
            if self._wire:
                yield _ListDelivery(self._codec.decode(self._wire.pop(0).encode()))
            else:
                await asyncio.sleep(0)

    def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]:
        return self._process(delivery)

    @asynccontextmanager
    async def _process(self, delivery: Delivery) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if not delivery.settled:
                await delivery.ack()


async def test_custom_producer_and_consumer_are_decoupled() -> None:
    wire: list[str] = []
    app = App("custom", producer=ListProducer(wire), consumer=ListConsumer(wire))
    done = asyncio.Event()
    loop = asyncio.get_running_loop()
    seen: list[str] = []

    @app.task()
    def greet(name: str) -> None:
        seen.append(name)
        # Sync tasks run in a worker thread; asyncio.Event is not thread-safe.
        loop.call_soon_threadsafe(done.set)

    await greet.delay("ada")
    assert json.loads(wire[0])["task"] == greet.name

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(done.wait(), 5)
    worker.request_shutdown()
    await asyncio.wait_for(run, 5)
    assert seen == ["ada"]
