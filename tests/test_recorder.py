from __future__ import annotations

import logging
from typing import Any

import pytest

from asynk import TaskError
from asynk.codec.json import JsonCodec
from asynk.envelope import Envelope
from asynk.middleware import TaskResult, TaskStatus
from asynk.recorder import ResultRecorder
from asynk.results import TaskHandle, TaskState, result_key
from asynk.store.memory import MemoryStore

CODEC = JsonCodec()


def _envelope() -> Envelope:
    return Envelope.create("add", (1, 2), {})


def _setup() -> tuple[ResultRecorder, MemoryStore]:
    store = MemoryStore()
    return ResultRecorder(store, CODEC), store


class _FailingStore(MemoryStore):
    async def set(self, *args: Any, **kwargs: Any) -> Any:
        raise OSError("store down")


async def test_success_writes_state_and_value() -> None:
    recorder, store = _setup()
    env = _envelope()
    await recorder.record(TaskResult(env, TaskStatus.SUCCESS, value=3))
    handle: TaskHandle[Any] = TaskHandle(env.id, store, CODEC)
    assert await handle.state() is TaskState.SUCCESS
    assert await handle.result() == 3


async def test_unencodable_value_is_recorded_as_a_failure() -> None:
    recorder, store = _setup()
    env = _envelope()
    await recorder.record(TaskResult(env, TaskStatus.SUCCESS, value=object()))
    handle: TaskHandle[Any] = TaskHandle(env.id, store, CODEC)
    assert await handle.state() is TaskState.FAILURE
    with pytest.raises(TaskError) as info:
        await handle.result()
    assert info.value.type_name == "ResultNotSerializable"


async def test_unknown_task_records_failure_with_the_task_name() -> None:
    recorder, store = _setup()
    env = _envelope()
    await recorder.record(TaskResult(env, TaskStatus.UNKNOWN_TASK))
    handle: TaskHandle[Any] = TaskHandle(env.id, store, CODEC)
    assert await handle.state() is TaskState.FAILURE
    with pytest.raises(TaskError) as info:
        await handle.result()
    assert (info.value.type_name, info.value.message) == ("UnknownTask", env.task)


async def test_failing_store_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    recorder = ResultRecorder(_FailingStore(), CODEC)
    env = _envelope()
    with caplog.at_level(logging.ERROR, logger="asynk"):
        await recorder.record(TaskResult(env, TaskStatus.SUCCESS, value=1))
        await recorder.record_state(env.id, TaskState.RUNNING)
    assert [r.levelno for r in caplog.records].count(logging.ERROR) == 2


async def test_record_state_overwrites() -> None:
    recorder, store = _setup()
    await recorder.record_state("x", TaskState.RUNNING)
    await recorder.record_state("x", TaskState.PENDING)
    assert await TaskHandle("x", store, CODEC).state() is TaskState.PENDING


async def test_failed_records_a_failure_with_the_error() -> None:
    recorder, store = _setup()
    env = _envelope()
    await recorder.record(TaskResult(env, TaskStatus.FAILED, error=ValueError("bad")))
    handle: TaskHandle[Any] = TaskHandle(env.id, store, CODEC)
    assert await handle.state() is TaskState.FAILURE
    with pytest.raises(TaskError) as info:
        await handle.result()
    assert (info.value.type_name, info.value.message) == ("ValueError", "bad")


async def test_retrying_records_the_state_only() -> None:
    recorder, store = _setup()
    env = _envelope()
    await recorder.record(TaskResult(env, TaskStatus.RETRYING, error=ValueError("bad")))
    assert await TaskHandle(env.id, store, CODEC).state() is TaskState.RETRYING
    assert await store.get(result_key(env.id)) is None
