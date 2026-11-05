import asyncio
import pickle

import pytest

from asynk.codec import Codec, EncodeError
from asynk.codec.json import JsonCodec
from asynk.errors import NotConfiguredError
from asynk.results import (
    TaskError,
    TaskHandle,
    TaskNotFoundError,
    TaskState,
    decode_result,
    decode_state,
    encode_result,
    encode_state,
    result_key,
    state_key,
)
from asynk.store.memory import MemoryStore
from asynk.store.null import NullStore

CODEC = JsonCodec()


class PickleCodec:
    """Minimal codec that stores values with pickle."""

    def validate(self, value: object) -> None:
        return None

    def encode(self, value: object) -> bytes:
        return pickle.dumps(value)

    def decode(self, data: bytes) -> object:
        return pickle.loads(data)


@pytest.fixture
def store() -> MemoryStore:
    return MemoryStore()


def _handle(store: MemoryStore, task_id: str = "x") -> TaskHandle:
    return TaskHandle(task_id, store, CODEC)


def test_keys() -> None:
    assert state_key("x") == "state:x"
    assert result_key("x") == "result:x"


def test_handle_keeps_id_and_receipt(store: MemoryStore) -> None:
    handle = TaskHandle("x", store, CODEC, "r")
    assert handle.task_id == "x"
    assert handle.receipt == "r"
    assert TaskHandle("x", store, CODEC).receipt is None


async def test_state_unknown_for_missing_id(store: MemoryStore) -> None:
    assert await _handle(store).state() is TaskState.UNKNOWN


async def test_state_reads_stored_state(store: MemoryStore) -> None:
    await store.set(state_key("x"), encode_state(CODEC, TaskState.RUNNING))
    assert await _handle(store).state() is TaskState.RUNNING


async def test_result_returns_value(store: MemoryStore) -> None:
    await store.set(state_key("x"), encode_state(CODEC, TaskState.SUCCESS))
    await store.set(result_key("x"), encode_result(CODEC, TaskState.SUCCESS, value={"a": 1}))
    assert await _handle(store).result() == {"a": 1}


async def test_result_waits_until_result_is_written(store: MemoryStore) -> None:
    await store.set(state_key("x"), encode_state(CODEC, TaskState.PENDING))

    async def writer() -> None:
        await asyncio.sleep(0.05)
        await store.set(result_key("x"), encode_result(CODEC, TaskState.SUCCESS, value=7))

    t = asyncio.create_task(writer())
    assert await asyncio.wait_for(_handle(store).result(), 2) == 7
    await t


async def test_result_failure_raises_task_error_with_type_and_message(
    store: MemoryStore,
) -> None:
    await store.set(state_key("x"), encode_state(CODEC, TaskState.FAILURE))
    await store.set(
        result_key("x"), encode_result(CODEC, TaskState.FAILURE, error=("ValueError", "boom"))
    )
    with pytest.raises(TaskError) as exc:
        await _handle(store).result()
    assert exc.value.type_name == "ValueError"
    assert exc.value.message == "boom"


async def test_result_skipped_returns_none(store: MemoryStore) -> None:
    await store.set(state_key("x"), encode_state(CODEC, TaskState.SKIPPED))
    await store.set(result_key("x"), encode_result(CODEC, TaskState.SKIPPED))
    assert await _handle(store).result() is None


async def test_result_unknown_id_raises_task_not_found_immediately(store: MemoryStore) -> None:
    with pytest.raises(TaskNotFoundError):
        await asyncio.wait_for(_handle(store, "nope").result(), 1)


async def test_result_times_out_when_pending(store: MemoryStore) -> None:
    await store.set(state_key("x"), encode_state(CODEC, TaskState.PENDING))
    with pytest.raises(TimeoutError):
        await _handle(store).result(timeout=0.05)


async def test_state_and_result_raise_not_configured_on_null_store() -> None:
    handle = TaskHandle("x", NullStore(), CODEC)
    with pytest.raises(NotConfiguredError):
        await handle.state()
    with pytest.raises(NotConfiguredError):
        await handle.result()


def test_unencodable_value_raises_encode_error() -> None:
    with pytest.raises(EncodeError):
        encode_result(CODEC, TaskState.SUCCESS, value=set())


def test_state_round_trips() -> None:
    for state in TaskState:
        assert decode_state(CODEC, encode_state(CODEC, state)) is state


def test_result_record_round_trips() -> None:
    data = encode_result(CODEC, TaskState.FAILURE, error=("E", "m"))
    assert decode_result(CODEC, data) == (TaskState.FAILURE, None, ("E", "m"))


async def test_records_use_the_given_codec(store: MemoryStore) -> None:
    codec: Codec = PickleCodec()
    await store.set(state_key("x"), encode_state(codec, TaskState.SUCCESS))
    await store.set(result_key("x"), encode_result(codec, TaskState.SUCCESS, value=(1, "a")))
    handle = TaskHandle("x", store, codec)
    assert await handle.state() is TaskState.SUCCESS
    assert await handle.result(timeout=1) == (1, "a")
