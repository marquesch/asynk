import uuid

import pytest

from asynk.app import App, NotConfiguredError
from asynk.codec import EncodeError
from asynk.envelope import Envelope
from asynk.results import TaskHandle, TaskState, decode_status, status_key
from asynk.store.memory import MemoryStore
from asynk.transport.memory import memory_transport


class FakeProducer:
    def __init__(self, error: Exception | None = None) -> None:
        self.published: list[Envelope] = []
        self.error = error

    def validate(self, envelope: Envelope) -> None:
        return None

    async def publish(self, envelope: Envelope) -> str:
        if self.error is not None:
            raise self.error
        self.published.append(envelope)
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


async def _next_envelope(consumer: object) -> Envelope:
    async for delivery in consumer.deliveries():  # type: ignore[attr-defined]
        return delivery.envelope  # type: ignore[no-any-return]
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


async def test_delay_writes_pending_before_publish() -> None:
    store = MemoryStore()
    seen: list[TaskState] = []

    class CheckingProducer(FakeProducer):
        async def publish(self, envelope: Envelope) -> str:
            data = await store.get(status_key(envelope.id))
            assert data is not None
            seen.append(decode_status(data))
            return await super().publish(envelope)

    app = App("t", producer=CheckingProducer(), store=store)

    @app.task()
    async def noop() -> None: ...

    await noop.delay()

    assert seen == [TaskState.PENDING]


async def test_delay_without_store_still_returns_handle() -> None:
    app = App("t", producer=FakeProducer())

    @app.task()
    async def noop() -> None: ...

    handle = await noop.delay()

    assert handle.receipt == "handle-1"


async def test_delay_publish_failure_propagates() -> None:
    app = App("t", producer=FakeProducer(error=RuntimeError("boom")), store=MemoryStore())

    @app.task()
    async def noop() -> None: ...

    with pytest.raises(RuntimeError, match="boom"):
        await noop.delay()


async def test_delay_continues_when_store_raises() -> None:
    class BrokenStore(MemoryStore):
        async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
            raise OSError("store down")

    producer = FakeProducer()
    app = App("t", producer=producer, store=BrokenStore())

    @app.task()
    async def noop() -> None: ...

    handle = await noop.delay()

    assert len(producer.published) == 1
    assert handle.task_id == producer.published[0].id
