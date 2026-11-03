import uuid

import pytest

from asynk.app import App, NotConfiguredError
from asynk.envelope import Envelope, NonJsonArgumentError


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
