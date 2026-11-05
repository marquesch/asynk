import asyncio

import pytest

from asynk.app import App, NotConfiguredError
from asynk.results import (
    TaskError,
    TaskHandle,
    TaskNotFoundError,
    TaskState,
    encode_result,
    encode_status,
    result_key,
    status_key,
)
from asynk.store.memory import MemoryStore


@pytest.fixture
def store() -> MemoryStore:
    return MemoryStore()


@pytest.fixture
def app(store: MemoryStore) -> App:
    return App("t", store=store)


async def test_status_unknown_for_missing_id(app: App) -> None:
    assert await TaskHandle(app, "x").status() is TaskState.UNKNOWN


async def test_status_reads_stored_state(app: App, store: MemoryStore) -> None:
    await store.start()
    await store.set(status_key("x"), encode_status(TaskState.RUNNING))
    assert await TaskHandle(app, "x").status() is TaskState.RUNNING


async def test_result_returns_value(app: App, store: MemoryStore) -> None:
    await store.start()
    await store.set(status_key("x"), encode_status(TaskState.SUCCESS))
    await store.set(result_key("x"), encode_result(TaskState.SUCCESS, value={"a": 1}))
    assert await TaskHandle(app, "x").result() == {"a": 1}


async def test_result_waits_until_result_is_written(app: App, store: MemoryStore) -> None:
    await store.start()
    await store.set(status_key("x"), encode_status(TaskState.PENDING))

    async def writer() -> None:
        await asyncio.sleep(0.05)
        await store.set(result_key("x"), encode_result(TaskState.SUCCESS, value=7))

    t = asyncio.create_task(writer())
    assert await asyncio.wait_for(TaskHandle(app, "x").result(), 2) == 7
    await t


async def test_result_failure_raises_task_error_with_type_and_message(
    app: App, store: MemoryStore
) -> None:
    await store.start()
    await store.set(status_key("x"), encode_status(TaskState.FAILURE))
    await store.set(result_key("x"), encode_result(TaskState.FAILURE, error=("ValueError", "boom")))
    with pytest.raises(TaskError) as exc:
        await TaskHandle(app, "x").result()
    assert exc.value.type_name == "ValueError"
    assert exc.value.message == "boom"


async def test_result_skipped_returns_none(app: App, store: MemoryStore) -> None:
    await store.start()
    await store.set(status_key("x"), encode_status(TaskState.SKIPPED))
    await store.set(result_key("x"), encode_result(TaskState.SKIPPED))
    assert await TaskHandle(app, "x").result() is None


async def test_result_unknown_id_raises_task_not_found_immediately(app: App) -> None:
    with pytest.raises(TaskNotFoundError):
        await asyncio.wait_for(TaskHandle(app, "nope").result(), 1)


async def test_result_times_out_when_pending(app: App, store: MemoryStore) -> None:
    await store.start()
    await store.set(status_key("x"), encode_status(TaskState.PENDING))
    with pytest.raises(TimeoutError):
        await TaskHandle(app, "x").result(timeout=0.05)


async def test_methods_raise_not_configured_without_store() -> None:
    handle = TaskHandle(App("t"), "x")
    with pytest.raises(NotConfiguredError):
        await handle.status()
    with pytest.raises(NotConfiguredError):
        await handle.result()


def test_encode_result_rejects_unserializable_value() -> None:
    with pytest.raises((TypeError, ValueError)):
        encode_result(TaskState.SUCCESS, value=object())
