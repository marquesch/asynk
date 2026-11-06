"""Task states, result records and `TaskHandle`."""

from __future__ import annotations

import enum
from typing import Any, cast

from asynk.codec import Codec
from asynk.store import Store


class TaskState(enum.Enum):
    """Lifecycle state of a task as recorded in the store.

    Attributes:
        PENDING: Published, not started yet.
        RUNNING: A worker is running it.
        RETRYING: An attempt failed and a retry was published.
        SUCCESS: Finished; `TaskHandle.result` returns the value.
        FAILURE: Failed for good; `TaskHandle.result` raises `TaskError`.
        SKIPPED: A `before_task` hook raised `SkipTask`; `TaskHandle.result` returns None.
        UNKNOWN: The store has no record for this id.
    """

    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    UNKNOWN = "unknown"


class TaskError(Exception):
    """Raised by `TaskHandle.result` when the task failed.

    Attributes:
        type_name: Name of the exception type the task raised.
        message: The exception's message.
    """

    def __init__(self, type_name: str, message: str) -> None:
        super().__init__(f"{type_name}: {message}")
        self.type_name = type_name
        self.message = message


class TaskNotFoundError(LookupError):
    """Raised by `TaskHandle.result` for a task id the store does not know."""


def state_key(task_id: str) -> str:
    """Returns the store key holding a task's state."""
    return f"state:{task_id}"


def result_key(task_id: str) -> str:
    """Returns the store key holding a task's result."""
    return f"result:{task_id}"


def encode_state(codec: Codec, state: TaskState) -> bytes:
    """Encodes a state record with `codec`."""
    return codec.encode({"state": state.value})


def decode_state(codec: Codec, data: bytes) -> TaskState:
    """Decodes a state record written by `encode_state`.

    Args:
        codec: The codec the record was encoded with.
        data: The encoded record.

    Returns:
        The recorded state.

    Raises:
        DecodeError: If `codec` cannot decode `data`.
        KeyError: If the record has no state.
        ValueError: If the recorded state is not a `TaskState` value.
    """
    record = cast("dict[str, Any]", codec.decode(data))
    return TaskState(record["state"])


def encode_result(
    codec: Codec,
    state: TaskState,
    *,
    value: object = None,
    error: tuple[str, str] | None = None,
) -> bytes:
    """Encodes a result record with `codec`.

    Args:
        codec: The codec that encodes the record.
        state: The task's final state.
        value: The task's return value.
        error: The exception's type name and message, for a failed task.

    Returns:
        The encoded record.

    Raises:
        EncodeError: If `codec` cannot encode `value`.
    """
    record: dict[str, Any] = {"state": state.value, "value": value}
    if error is not None:
        record["error"] = {"type": error[0], "message": error[1]}
    return codec.encode(record)


def decode_result(codec: Codec, data: bytes) -> tuple[TaskState, object, tuple[str, str] | None]:
    """Decodes a result record written by `encode_result`.

    Args:
        codec: The codec the record was encoded with.
        data: The encoded record.

    Returns:
        The state, the value, and the error's type name and message (None
        unless the task failed).

    Raises:
        DecodeError: If `codec` cannot decode `data`.
        KeyError: If the record has no state, or its error has no type or message.
        ValueError: If the recorded state is not a `TaskState` value.
    """
    record = cast("dict[str, Any]", codec.decode(data))
    error = record.get("error")
    return (
        TaskState(record["state"]),
        record.get("value"),
        None if error is None else (error["type"], error["message"]),
    )


class TaskHandle:
    """Reference to a published task, used to read its state and result.

    Attributes:
        task_id: The task's id (`Envelope.id`).
        receipt: Whatever the producer returned when publishing, if anything.
    """

    def __init__(self, task_id: str, store: Store, codec: Codec, receipt: Any = None) -> None:
        """Initializes the handle.

        Args:
            task_id: The task's id (`Envelope.id`).
            store: The store holding the task's records.
            codec: The codec the records were encoded with.
            receipt: Whatever the producer returned when publishing, if anything.
        """
        self.task_id = task_id
        self.receipt = receipt
        self._store = store
        self._codec = codec

    async def state(self) -> TaskState:
        """Reads the task's state; `UNKNOWN` if the store has no record of it.

        Raises:
            NotConfiguredError: If no store is configured.
        """
        data = await self._store.get(state_key(self.task_id))
        return TaskState.UNKNOWN if data is None else decode_state(self._codec, data)

    async def result(self, timeout: float | None = None) -> Any:
        """Waits for the task to finish and returns its value.

        Args:
            timeout: Seconds to wait.

        Returns:
            The task's return value, or None if it was skipped.

        Raises:
            NotConfiguredError: If no store is configured.
            TaskNotFoundError: If the store has no record of the task.
            TimeoutError: If no result arrived within `timeout`.
            TaskError: If the task failed.
        """
        if await self._store.get(state_key(self.task_id)) is None:
            raise TaskNotFoundError(self.task_id)
        data = await self._store.wait(result_key(self.task_id), timeout)
        if data is None:
            raise TimeoutError(f"task {self.task_id!r} has no result yet")
        state, value, error = decode_result(self._codec, data)
        if state is TaskState.FAILURE:
            type_name, message = error or ("Exception", "")
            raise TaskError(type_name, message)
        if state is TaskState.SKIPPED:
            return None
        return value
