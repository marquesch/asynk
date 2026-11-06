from __future__ import annotations

import asyncio
import importlib
import subprocess
import sys
from collections.abc import AsyncIterator
from typing import Any

import fakeredis
import pytest
from redis.exceptions import ConnectionError
from redis_helpers import BlockingFakeRedis

import asynk.transport.redis
from asynk import App, Worker
from asynk.envelope import Envelope
from asynk.transport import PublishOptions
from asynk.transport.redis import RedisConsumer, RedisProducer

MESSAGE = "asynk[redis] needs the redis package: pip install 'asynk[redis]'"
STREAM = "asynk:{q}:stream"
DEAD = "asynk:{q}:dead"
DELAYED = "asynk:{q}:delayed"


def _env() -> Envelope:
    return Envelope.create("t", (), {})


@pytest.fixture
async def client() -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    redis = BlockingFakeRedis()
    yield redis
    await redis.aclose()


def _consumer(
    client: fakeredis.FakeAsyncRedis, *, block: float = 0.05, **kwargs: Any
) -> RedisConsumer:
    return RedisConsumer(client=client, queue="q", block=block, **kwargs)


def _producer(client: fakeredis.FakeAsyncRedis) -> RedisProducer:
    return RedisProducer(client=client, queue="q")


async def _dead(client: fakeredis.FakeAsyncRedis) -> Any:
    return await client.xrange(DEAD)


async def _next(consumer: RedisConsumer, timeout: float = 2) -> Any:
    return await asyncio.wait_for(anext(consumer.deliveries()), timeout)


@pytest.mark.slow
def test_core_imports_without_the_client_and_the_guard_names_the_extra() -> None:
    code = (
        "import sys; sys.modules['redis'] = None\n"
        "import asynk, asynk.transport, asynk.transport.memory, asynk.worker\n"
        "try:\n    import asynk.transport.redis\n"
        "except ImportError as exc:\n    print(exc)\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.splitlines() == [MESSAGE]


def test_import_guard_raises_when_the_client_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "redis", None)
    for name in [n for n in sys.modules if n.startswith("asynk.transport.redis")]:
        monkeypatch.delitem(sys.modules, name)
    with pytest.raises(ImportError) as info:
        importlib.import_module("asynk.transport.redis")
    assert str(info.value) == MESSAGE


def test_exactly_one_of_url_and_client_is_required() -> None:
    with pytest.raises(ValueError):
        RedisProducer()
    with pytest.raises(ValueError):
        RedisConsumer("redis://x", client=fakeredis.FakeAsyncRedis())


@pytest.mark.parametrize(
    "kwargs", [{"lease": 0}, {"block": 0}, {"max_deliveries": 0}, {"dead_maxlen": 0}]
)
def test_invalid_settings_raise(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        RedisConsumer(client=fakeredis.FakeAsyncRedis(), **kwargs)


async def test_backlog_published_before_start_is_consumed(
    client: fakeredis.FakeAsyncRedis,
) -> None:
    await _producer(client).publish(_env(), b"early")
    consumer = _consumer(client)
    await consumer.start()
    delivery = await _next(consumer)
    assert delivery.body == b"early"
    await delivery.ack()
    await consumer.stop()


async def test_ack_removes_the_entry_from_the_stream(client: fakeredis.FakeAsyncRedis) -> None:
    consumer = _consumer(client)
    await consumer.start()
    await _producer(client).publish(_env(), b"x")
    await (await _next(consumer)).ack()
    assert await client.xlen(STREAM) == 0
    assert (await client.xpending(STREAM, "asynk"))["pending"] == 0
    await consumer.stop()


async def test_reject_moves_the_message_to_the_dead_stream(
    client: fakeredis.FakeAsyncRedis,
) -> None:
    consumer = _consumer(client)
    await consumer.start()
    await _producer(client).publish(_env(), b"bad")
    delivery = await _next(consumer)
    await delivery.reject()
    [(_, entry)] = await _dead(client)
    assert entry[b"body"] == b"bad"
    assert entry[b"reason"] == b"rejected"
    assert await client.xlen(STREAM) == 0
    await consumer.stop()


async def test_dead_stream_is_trimmed(client: fakeredis.FakeAsyncRedis) -> None:
    consumer = _consumer(client, dead_maxlen=2)
    await consumer.start()
    for i in range(5):
        await _producer(client).publish(_env(), str(i).encode())
        await (await _next(consumer)).reject()
    assert await client.xlen(DEAD) <= 5  # approximate trimming keeps whole blocks
    await consumer.stop()


async def test_requeued_message_is_a_fresh_entry(client: fakeredis.FakeAsyncRedis) -> None:
    consumer = _consumer(client)
    await consumer.start()
    await _producer(client).publish(_env(), b"again")
    first = await _next(consumer)
    await first.reject(requeue=True)
    assert await client.xlen(STREAM) == 1
    assert (await _next(consumer)).body == b"again"
    await consumer.stop()


async def test_expired_lease_is_taken_over_by_another_consumer(
    client: fakeredis.FakeAsyncRedis,
) -> None:
    crashed = _consumer(client, lease=0.02)
    await crashed.start()
    await _producer(client).publish(_env(), b"x")
    await _next(crashed)  # never settled
    other = _consumer(client, lease=0.02)
    await other.start()
    delivery = await _next(other)
    assert delivery.body == b"x"
    await delivery.ack()
    await other.stop()


async def test_message_over_max_deliveries_is_dead_lettered(
    client: fakeredis.FakeAsyncRedis,
) -> None:
    # Nothing here waits for a timeout: cancelling a read that is inside the consumer's
    # upkeep can hang redis-py on Python 3.11, and a short block makes that likely.
    def consumer() -> RedisConsumer:
        return _consumer(client, block=0.01, lease=0.02, max_deliveries=2)

    await _producer(client).publish(_env(), b"poison")
    for _ in range(2):  # each consumer receives it and never settles it
        unsettled = consumer()
        await unsettled.start()
        assert (await _next(unsettled)).body == b"poison"
    await _producer(client).publish(_env(), b"after")
    await asyncio.sleep(0.03)  # the lease of the last delivery expires
    # A third delivery would exceed the limit: the message is dead-lettered, so the next
    # delivery is the one published after it.
    checker = consumer()
    await checker.start()
    after = await _next(checker)
    assert after.body == b"after"
    await after.ack()
    [(_, entry)] = await _dead(client)
    assert entry[b"reason"] == b"max_deliveries"
    assert entry[b"body"] == b"poison"
    assert await client.xlen(STREAM) == 0


async def test_delayed_message_waits_in_the_sorted_set_then_moves(
    client: fakeredis.FakeAsyncRedis,
) -> None:
    receipt = await _producer(client).publish(_env(), b"later", PublishOptions(delay=0.05))
    assert isinstance(receipt, str)
    assert await client.zcard(DELAYED) == 1
    assert await client.xlen(STREAM) == 0
    consumer = _consumer(client)
    await consumer.start()
    delivery = await _next(consumer)
    assert delivery.body == b"later"
    assert await client.zcard(DELAYED) == 0
    await consumer.stop()


async def test_identical_delayed_bodies_are_both_delivered(
    client: fakeredis.FakeAsyncRedis,
) -> None:
    producer = _producer(client)
    for _ in range(2):
        await producer.publish(_env(), b"same", PublishOptions(delay=0.05))
    consumer = _consumer(client)
    await consumer.start()
    assert [(await _next(consumer)).body for _ in range(2)] == [b"same", b"same"]
    await consumer.stop()


async def test_stop_removes_an_idle_consumer_but_not_one_with_pending_messages(
    client: fakeredis.FakeAsyncRedis,
) -> None:
    idle = _consumer(client)
    await idle.start()
    busy = _consumer(client)
    await busy.start()
    await _producer(client).publish(_env(), b"x")
    await _next(busy)
    await idle.stop()
    await busy.stop()
    names = [c["name"] for c in await client.xinfo_consumers(STREAM, "asynk")]
    assert names == [busy._name]


async def test_connected_follows_the_connection(client: fakeredis.FakeAsyncRedis) -> None:
    consumer = _consumer(client, block=0.01)
    await consumer.start()
    failing = True
    failed, recovered = asyncio.Event(), asyncio.Event()

    async def flaky(*args: Any, **kwargs: Any) -> Any:
        if failing:
            failed.set()
            raise ConnectionError("down")
        await asyncio.sleep(0)  # an empty read hands control back, as a blocking one does
        recovered.set()
        return []

    client.xreadgroup = flaky  # type: ignore[method-assign]

    async def deliveries() -> list[Any]:
        return [delivery async for delivery in consumer.deliveries()]

    run = asyncio.create_task(deliveries())
    await failed.wait()
    assert not consumer.connected
    failing = False
    await recovered.wait()
    assert consumer.connected
    await consumer.stop()
    assert await run == []


async def test_worker_runs_a_task_over_the_transport(client: fakeredis.FakeAsyncRedis) -> None:
    consumer = _consumer(client)
    app = App("redis-e2e", producer=_producer(client), consumer=consumer)
    seen: list[int] = []

    @app.task()
    async def add(a: int, b: int) -> None:
        seen.append(a + b)

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    await add.delay(1, b=2)
    for _ in range(100):
        if seen:
            break
        # Real time, on purpose: shutting down at once cancels the consumer inside its upkeep,
        # which can hang redis-py on Python 3.11; this lets it get back to its blocking read.
        await asyncio.sleep(0.05)
    worker.request_shutdown()
    await asyncio.wait_for(run, 5)
    assert seen == [3]
    assert asynk.transport.redis.RedisProducer is RedisProducer
