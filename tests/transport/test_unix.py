from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from asynk import App, Envelope, JsonCodec, Worker
from asynk.transport import Delivery
from asynk.transport.unix import UnixConsumer, UnixDelivery, UnixProducer, unix_transport


async def _first(consumer: UnixConsumer) -> Delivery:
    return await asyncio.wait_for(anext(consumer.deliveries()), 1)


def _env(task: str = "t") -> Envelope:
    return Envelope.create(task, (), {})


async def test_publish_then_consume_moves_the_body_unchanged(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock")
    await consumer.start()
    try:
        env = _env()
        assert await producer.publish(env, b"\x00some bytes") == env.id
        delivery = await _first(consumer)
        assert isinstance(delivery, UnixDelivery)
        assert delivery.body == b"\x00some bytes"
    finally:
        await consumer.stop()


async def test_publish_rejects_a_body_containing_a_newline(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock")
    await consumer.start()
    try:
        with pytest.raises(ValueError, match="newline"):
            await producer.publish(_env(), b"two\nlines")
    finally:
        await consumer.stop()


async def test_publish_without_listener_raises(tmp_path: Path) -> None:
    producer = UnixProducer(tmp_path / "missing.sock")
    with pytest.raises(OSError):
        await producer.publish(_env(), b"x")


async def test_connected_follows_start_and_stop(tmp_path: Path) -> None:
    path = tmp_path / "s.sock"
    _, consumer = unix_transport(path)
    assert not consumer.connected
    await consumer.start()
    assert consumer.connected
    assert path.exists()
    await consumer.stop()
    assert not consumer.connected
    assert not path.exists()


async def test_start_replaces_stale_socket_file(tmp_path: Path) -> None:
    path = tmp_path / "s.sock"
    path.write_text("stale")
    producer, consumer = unix_transport(path)
    await consumer.start()
    try:
        await producer.publish(_env(), b"t")
        assert (await _first(consumer)).body == b"t"
    finally:
        await consumer.stop()


async def test_each_line_becomes_one_delivery_without_the_delimiter(tmp_path: Path) -> None:
    consumer = UnixConsumer(tmp_path / "s.sock")
    await consumer.start()
    try:
        _, writer = await asyncio.open_unix_connection(tmp_path / "s.sock")
        writer.write(b"not json\nsecond\n")
        await writer.drain()
        writer.close()
        it = consumer.deliveries()
        assert (await asyncio.wait_for(anext(it), 1)).body == b"not json"
        assert (await asyncio.wait_for(anext(it), 1)).body == b"second"
    finally:
        await consumer.stop()


async def test_ack_marks_settled_and_double_settle_raises(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock")
    await consumer.start()
    try:
        await producer.publish(_env(), b"x")
        delivery = await _first(consumer)
        assert not delivery.settled
        await delivery.ack()
        assert delivery.settled
        with pytest.raises(RuntimeError):
            await delivery.reject()
    finally:
        await consumer.stop()


async def test_reject_with_requeue_loses_the_message(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock")
    await consumer.start()
    try:
        await producer.publish(_env(), b"first")
        it = consumer.deliveries()
        first = await asyncio.wait_for(anext(it), 1)
        await first.reject(requeue=True)
        await producer.publish(_env(), b"second")
        assert (await asyncio.wait_for(anext(it), 1)).body == b"second"
    finally:
        await consumer.stop()


async def test_process_acks_on_success_and_rejects_on_error(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock")
    await consumer.start()
    try:
        await producer.publish(_env(), b"ok")
        await producer.publish(_env(), b"bad")
        it = consumer.deliveries()
        ok = await asyncio.wait_for(anext(it), 1)
        async with consumer.process(ok):
            pass
        assert ok.settled
        bad = await asyncio.wait_for(anext(it), 1)
        with pytest.raises(ValueError):
            async with consumer.process(bad):
                raise ValueError
        assert bad.settled
    finally:
        await consumer.stop()


async def test_stop_ends_the_delivery_stream(tmp_path: Path) -> None:
    _, consumer = unix_transport(tmp_path / "s.sock")
    await consumer.start()
    waiting = asyncio.ensure_future(anext(consumer.deliveries()))
    await asyncio.sleep(0)
    await consumer.stop()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(waiting, 1)


class _ReverseCodec:
    def __init__(self) -> None:
        self._json = JsonCodec()

    def validate(self, value: object) -> None:
        self._json.validate(value)

    def encode(self, value: object) -> bytes:
        return self._json.encode(value)[::-1]

    def decode(self, data: bytes) -> object:
        return self._json.decode(data[::-1])


async def _run_add_through_socket(app: App, consumer: UnixConsumer) -> list[int]:
    done = asyncio.Event()
    seen: list[int] = []

    @app.task()
    async def add(a: int, b: int) -> None:
        seen.append(a + b)
        done.set()

    worker = Worker(app)
    run = asyncio.create_task(worker.run())
    while not consumer.connected:  # noqa: ASYNC110
        await asyncio.sleep(0.005)
    await add.delay(2, 3)
    await asyncio.wait_for(done.wait(), 5)
    worker.request_shutdown()
    await asyncio.wait_for(run, 5)
    return seen


async def test_worker_runs_a_task_published_through_the_socket(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock")
    app = App("unix", producer=producer, consumer=consumer)
    assert await _run_add_through_socket(app, consumer) == [5]


async def test_app_codec_is_used_on_both_sides_of_the_socket(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock")
    app = App("unix", producer=producer, consumer=consumer, codec=_ReverseCodec())
    assert await _run_add_through_socket(app, consumer) == [5]
