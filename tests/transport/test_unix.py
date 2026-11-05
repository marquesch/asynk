from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from asynk import App, DecodeError, EncodeError, Envelope, JsonCodec, Worker
from asynk.transport import Delivery
from asynk.transport.unix import UnixConsumer, UnixProducer, unix_transport


async def _first(consumer: UnixConsumer) -> Delivery:
    return await asyncio.wait_for(anext(consumer.deliveries()), 1)


async def test_publish_then_consume_roundtrip(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock")
    await consumer.start()
    try:
        env = Envelope.create("t", (1, "a"), {"k": 2})
        assert await producer.publish(env) == env.id
        assert (await _first(consumer)).envelope == env
    finally:
        await consumer.stop()


async def test_publish_without_listener_raises(tmp_path: Path) -> None:
    producer = UnixProducer(tmp_path / "missing.sock")
    with pytest.raises(OSError):
        await producer.publish(Envelope.create("t", (), {}))


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
        await producer.publish(Envelope.create("t", (), {}))
        assert (await _first(consumer)).envelope.task == "t"
    finally:
        await consumer.stop()


async def test_undecodable_message_is_dropped(tmp_path: Path) -> None:
    path = tmp_path / "s.sock"
    producer, consumer = unix_transport(path)
    await consumer.start()
    try:
        _, writer = await asyncio.open_unix_connection(path)
        writer.write(b"not json\n")
        await writer.drain()
        writer.close()
        await producer.publish(Envelope.create("good", (), {}))
        assert (await _first(consumer)).envelope.task == "good"
    finally:
        await consumer.stop()


async def test_ack_marks_settled_and_double_settle_raises(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock")
    await consumer.start()
    try:
        await producer.publish(Envelope.create("t", (), {}))
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
        await producer.publish(Envelope.create("first", (), {}))
        it = consumer.deliveries()
        first = await asyncio.wait_for(anext(it), 1)
        await first.reject(requeue=True)
        await producer.publish(Envelope.create("second", (), {}))
        assert (await asyncio.wait_for(anext(it), 1)).envelope.task == "second"
    finally:
        await consumer.stop()


async def test_process_acks_on_success_and_rejects_on_error(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock")
    await consumer.start()
    try:
        await producer.publish(Envelope.create("ok", (), {}))
        await producer.publish(Envelope.create("bad", (), {}))
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

    def validate(self, envelope: Envelope) -> None:
        self._json.validate(envelope)

    def encode(self, envelope: Envelope) -> bytes:
        return self._json.encode(envelope)[::-1]

    def decode(self, data: bytes) -> Envelope:
        return self._json.decode(data[::-1])

    def encode_value(self, value: object) -> bytes:
        return self._json.encode_value(value)

    def decode_value(self, data: bytes) -> object:
        return self._json.decode_value(data)


async def test_custom_codec_is_used_on_both_sides(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock", _ReverseCodec())
    await consumer.start()
    try:
        env = Envelope.create("t", (1,), {})
        await producer.publish(env)
        assert (await _first(consumer)).envelope == env
    finally:
        await consumer.stop()


async def test_worker_runs_a_task_published_through_the_socket(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock")
    app = App("unix", producer=producer, consumer=consumer)
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
    assert seen == [5]


class _StrictCodec:
    def __init__(self) -> None:
        self._json = JsonCodec()

    def validate(self, envelope: Envelope) -> None:
        self._json.validate(envelope)

    def encode(self, envelope: Envelope) -> bytes:
        return self._json.encode(envelope)

    def decode(self, data: bytes) -> Envelope:
        if data.endswith(b"\n"):
            raise DecodeError("delimiter leaked into the message")
        return self._json.decode(data)

    def encode_value(self, value: object) -> bytes:
        return self._json.encode_value(value)

    def decode_value(self, data: bytes) -> object:
        return self._json.decode_value(data)


async def test_codec_never_sees_the_message_delimiter(tmp_path: Path) -> None:
    producer, consumer = unix_transport(tmp_path / "s.sock", _StrictCodec())
    await consumer.start()
    try:
        await producer.publish(Envelope.create("t", (), {}))
        assert (await _first(consumer)).envelope.task == "t"
    finally:
        await consumer.stop()


def test_producer_validate_delegates_to_codec(tmp_path: Path) -> None:
    producer, _ = unix_transport(tmp_path / "s.sock")
    producer.validate(Envelope.create("t", [1], {}))
    with pytest.raises(EncodeError):
        producer.validate(Envelope.create("t", [object()], {}))
