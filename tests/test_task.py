import uuid

import pytest

from asynk.app import App, NotConfiguredError
from asynk.envelope import Envelope, NonJsonArgumentError
from asynk.transport.memory import memory_transport


class FakeProducer:
    def __init__(self, error: Exception | None = None) -> None:
        self.published: list[Envelope] = []
        self.error = error

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

    result = await add.delay(1, b=2)

    assert result == "handle-1"
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
    producer = FakeProducer()
    app = App("t", producer=producer)

    @app.task()
    async def take(x: object) -> None: ...

    with pytest.raises(NonJsonArgumentError):
        await take.delay((1, 2))
    assert producer.published == []


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
    producer = FakeProducer()
    app = App("t", producer=producer)

    @app.task()
    async def noop() -> None: ...

    with pytest.raises(NonJsonArgumentError):
        await noop.with_metadata(a=object()).delay()
    assert producer.published == []


async def test_with_metadata_accepts_self_as_a_key() -> None:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer)

    @app.task()
    async def noop() -> None: ...

    await noop.with_metadata(self=1).with_metadata(self=2, other=3).delay()

    assert (await _next_envelope(consumer)).metadata == {"self": 2, "other": 3}
