import logging
import uuid
from typing import Any

import pytest
from envelope_helpers import decode_delivery

from asynk import EncodeError, NotConfiguredError
from asynk.app import App
from asynk.codec.json import JsonCodec
from asynk.envelope import Envelope
from asynk.results import TaskHandle, TaskState, state_key
from asynk.store.memory import MemoryStore
from asynk.task import Task
from asynk.transport import PublishOptions
from asynk.transport.memory import MemoryConsumer, memory_transport


class FakeProducer:
    def __init__(self, error: Exception | None = None) -> None:
        self.published: list[Envelope] = []
        self.bodies: list[bytes] = []
        self.error = error

    async def publish(
        self, envelope: Envelope, body: bytes, options: PublishOptions | None = None
    ) -> str:
        if self.error is not None:
            raise self.error
        self.published.append(envelope)
        self.bodies.append(body)
        return "handle-1"


async def test_delay_publishes_envelope_and_returns_producer_result() -> None:
    producer = FakeProducer()
    app = App("t", producer=producer)

    @app.task()
    async def add(a: int, b: int = 0) -> int:
        return a + b

    handle = await add.delay(1, b=2)

    assert handle.receipt == "handle-1"
    (envelope,) = producer.published
    assert envelope.task == add.name
    assert envelope.args == (1,)
    assert envelope.kwargs == {"b": 2}
    assert envelope.attempt == 0
    assert str(uuid.UUID(envelope.id)) == envelope.id


async def test_delay_without_producer_raises_not_configured() -> None:
    app = App("t")

    @app.task()
    async def noop() -> None: ...

    with pytest.raises(NotConfiguredError):
        await noop.delay()


async def test_delay_rejects_non_json_args_before_publishing() -> None:
    producer, consumer = memory_transport()
    app = App("t", producer=producer)

    @app.task()
    async def take(x: object) -> None: ...

    with pytest.raises(EncodeError):
        await take.delay((1, 2))
    assert consumer._queue.empty()


async def test_delay_propagates_producer_exception() -> None:
    app = App("t", producer=FakeProducer(error=ConnectionError("down")))

    @app.task()
    async def noop() -> None: ...

    with pytest.raises(ConnectionError):
        await noop.delay()


async def _next_envelope(consumer: MemoryConsumer) -> Envelope:
    async for delivery in consumer.deliveries():
        return decode_delivery(delivery)
    raise AssertionError("no delivery")


async def test_with_metadata_delay_sets_metadata() -> None:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer)

    @app.task()
    async def add(a: int, b: int = 0) -> int:
        return a + b

    await add.with_metadata(source="cron").delay(1, b=2)

    envelope = await _next_envelope(consumer)
    assert envelope.metadata == {"source": "cron"}
    assert envelope.args == (1,)
    assert envelope.kwargs == {"b": 2}


async def test_with_metadata_chain_merges_and_overrides() -> None:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer)

    @app.task()
    async def noop() -> None: ...

    await noop.with_metadata(a=1, b=1).with_metadata(b=2).delay()

    assert (await _next_envelope(consumer)).metadata == {"a": 1, "b": 2}


async def test_with_metadata_does_not_mutate_original() -> None:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer)

    @app.task()
    async def noop() -> None: ...

    base = noop.with_metadata(a=1)
    base.with_metadata(a=2, b=3)
    await base.delay()
    await noop.delay()

    assert (await _next_envelope(consumer)).metadata == {"a": 1}
    assert (await _next_envelope(consumer)).metadata == {}


async def test_with_metadata_without_producer_raises_not_configured() -> None:
    app = App("t")

    @app.task()
    async def noop() -> None: ...

    with pytest.raises(NotConfiguredError):
        await noop.with_metadata(a=1).delay()


async def test_with_metadata_rejects_non_json() -> None:
    producer, consumer = memory_transport()
    app = App("t", producer=producer)

    @app.task()
    async def noop() -> None: ...

    with pytest.raises(EncodeError):
        await noop.with_metadata(a=object()).delay()
    assert consumer._queue.empty()


async def test_with_metadata_accepts_self_as_a_key() -> None:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer)

    @app.task()
    async def noop() -> None: ...

    await noop.with_metadata(self=1).with_metadata(self=2, other=3).delay()

    assert (await _next_envelope(consumer)).metadata == {"self": 2, "other": 3}


async def test_delay_returns_handle_with_envelope_id() -> None:
    producer = FakeProducer()
    app = App("t", producer=producer)

    @app.task()
    async def noop() -> None: ...

    handle = await noop.delay()

    assert isinstance(handle, TaskHandle)
    assert handle.task_id == producer.published[0].id


async def test_delay_writes_pending_after_publish() -> None:
    store = MemoryStore()
    seen: list[bytes | None] = []

    class CheckingProducer(FakeProducer):
        async def publish(
            self, envelope: Envelope, body: bytes, options: PublishOptions | None = None
        ) -> str:
            seen.append(await store.get(state_key(envelope.id)))
            return await super().publish(envelope, body)

    app = App("t", producer=CheckingProducer(), store=store)

    @app.task()
    async def noop() -> None: ...

    handle = await noop.delay()

    assert seen == [None]
    assert await handle.state() is TaskState.PENDING


async def test_delay_works_with_default_null_store() -> None:
    producer = FakeProducer()
    app = App("t", producer=producer)

    @app.task()
    async def noop() -> None: ...

    handle = await noop.delay()

    assert handle.receipt == "handle-1"
    assert handle.task_id == producer.published[0].id
    with pytest.raises(NotConfiguredError):
        await handle.state()


async def test_delay_handle_reads_the_app_store() -> None:
    store = MemoryStore()
    app = App("t", producer=FakeProducer(), store=store)

    @app.task()
    async def noop() -> None: ...

    handle = await noop.delay()

    assert await handle.state() is TaskState.PENDING


async def test_delay_publish_failure_propagates() -> None:
    app = App("t", producer=FakeProducer(error=RuntimeError("boom")), store=MemoryStore())

    @app.task()
    async def noop() -> None: ...

    with pytest.raises(RuntimeError, match="boom"):
        await noop.delay()


async def test_delay_continues_when_store_raises(caplog: pytest.LogCaptureFixture) -> None:
    class BrokenStore(MemoryStore):
        async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
            raise OSError("store down")

    producer = FakeProducer()
    app = App("t", producer=producer, store=BrokenStore())

    @app.task()
    async def noop() -> None: ...

    with caplog.at_level(logging.ERROR, logger="asynk"):
        handle = await noop.delay()

    assert len(producer.published) == 1
    assert handle.task_id == producer.published[0].id
    [record] = [r for r in caplog.records if r.name == "asynk"]
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], OSError)


class _ReverseCodec:
    def __init__(self) -> None:
        self._json = JsonCodec()

    def validate(self, value: object) -> None:
        self._json.validate(value)

    def encode(self, value: object) -> bytes:
        return self._json.encode(value)[::-1]

    def decode(self, data: bytes) -> object:
        return self._json.decode(data[::-1])


async def test_delay_encodes_the_envelope_with_the_app_codec() -> None:
    producer = FakeProducer()
    codec = _ReverseCodec()
    app = App("t", producer=producer, codec=codec)

    @app.task()
    async def add(a: int, b: int = 0) -> int:
        return a + b

    await add.with_metadata(m=1).delay(1, b=2)

    (envelope,) = producer.published
    (body,) = producer.bodies
    assert body == codec.encode(envelope.to_dict())
    assert Envelope.from_dict(codec.decode(body)) == envelope


async def test_delay_with_unencodable_argument_writes_no_state() -> None:
    written: list[str] = []

    class RecordingStore(MemoryStore):
        async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
            written.append(key)
            return await super().set(key, value, overwrite=overwrite)

    producer = FakeProducer()
    app = App("t", producer=producer, store=RecordingStore())

    @app.task()
    async def take(x: object) -> None: ...

    with pytest.raises(EncodeError):
        await take.delay(object())
    assert producer.published == []
    assert [key for key in written if key.startswith("state:")] == []


def _fake_publish() -> tuple[list[tuple[Envelope, PublishOptions | None]], Any]:
    calls: list[tuple[Envelope, PublishOptions | None]] = []

    async def publish(envelope: Envelope, options: PublishOptions | None = None) -> TaskHandle[Any]:
        calls.append((envelope, options))
        return TaskHandle(envelope.id, MemoryStore(), JsonCodec(), "r")

    return calls, publish


async def test_task_delay_hands_the_envelope_to_the_injected_publish() -> None:
    calls, publish = _fake_publish()

    async def add(a: int, b: int = 0) -> int:
        return a + b

    task = Task(add, "add", None, publish)
    handle = await task.delay(1, b=2)

    [(envelope, options)] = calls
    assert (envelope.task, envelope.args, envelope.kwargs, envelope.metadata) == (
        "add",
        (1,),
        {"b": 2},
        {},
    )
    assert options is None
    assert handle.task_id == envelope.id
    assert handle.receipt == "r"


async def test_bound_task_delay_carries_metadata_to_the_injected_publish() -> None:
    calls, publish = _fake_publish()

    async def noop() -> None: ...

    task = Task(noop, "noop", None, publish)
    await task.with_metadata(a=1).with_metadata(b=2).delay()

    assert calls[0][0].metadata == {"a": 1, "b": 2}


def test_task_has_no_app_attribute() -> None:
    async def noop() -> None: ...

    assert not hasattr(Task(noop, "noop", None, _fake_publish()[1]), "app")
