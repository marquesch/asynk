from __future__ import annotations

import enum
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from asynk.app import App
    from asynk.store import Store


class TaskState(enum.Enum):
    """Lifecycle state of a task as recorded in the store."""

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


def status_key(task_id: str) -> str:
    return f"status:{task_id}"


def result_key(task_id: str) -> str:
    return f"result:{task_id}"


def encode_status(state: TaskState) -> bytes:
    return json.dumps({"state": state.value}).encode()


def decode_status(data: bytes) -> TaskState:
    return TaskState(json.loads(data)["state"])


def encode_result(
    state: TaskState, *, value: object = None, error: tuple[str, str] | None = None
) -> bytes:
    """Encodes a result record.

    Raises:
        TypeError: If `value` is not JSON-serializable.
        ValueError: If `value` is not JSON-serializable (for example a circular reference).
    """
    record: dict[str, Any] = {"state": state.value, "value": value}
    if error is not None:
        record["error"] = {"type": error[0], "message": error[1]}
    return json.dumps(record).encode()


def decode_result(data: bytes) -> tuple[TaskState, object, tuple[str, str] | None]:
    record = json.loads(data)
    error = record.get("error")
    return (
        TaskState(record["state"]),
        record.get("value"),
        None if error is None else (error["type"], error["message"]),
    )


def _require_store(app: App) -> Store:
    from asynk.app import NotConfiguredError

    if app.store is None:
        raise NotConfiguredError(f"app {app.name!r} has no store configured")
    return app.store


async def start_store(app: App) -> Store:
    """Starts the app's store at most once and returns it.

    Raises:
        NotConfiguredError: If the app has no store.
    """
    store = _require_store(app)
    async with app._store_lock():
        if not app._store_started:
            await store.start()
            app._store_started = True
    return store


async def stop_store(app: App) -> None:
    """Stops the app's store if `start_store` started it."""
    if app.store is None:
        return
    async with app._store_lock():
        if app._store_started:
            await app.store.stop()
            app._store_started = False


class TaskHandle:
    """Reference to a published task, used to read its status and result.

    Attributes:
        task_id: The task's id (`Envelope.id`).
        receipt: Whatever the producer returned when publishing, if anything.
    """

    def __init__(self, app: App, task_id: str, receipt: Any = None) -> None:
        self._app = app
        self.task_id = task_id
        self.receipt = receipt

    async def status(self) -> TaskState:
        """Reads the task's state; `UNKNOWN` if the store has no record of it.

        Raises:
            NotConfiguredError: If the app has no store.
        """
        store = await start_store(self._app)
        data = await store.get(status_key(self.task_id))
        return TaskState.UNKNOWN if data is None else decode_status(data)

    async def result(self, timeout: float | None = None) -> Any:
        """Waits for the task to finish and returns its value.

        Args:
            timeout: Seconds to wait, or None to wait indefinitely.

        Returns:
            The task's return value, or None if it was skipped.

        Raises:
            NotConfiguredError: If the app has no store.
            TaskNotFoundError: If the store has no record of the task.
            TimeoutError: If no result arrived within `timeout`.
            TaskError: If the task failed.
        """
        store = await start_store(self._app)
        if await store.get(status_key(self.task_id)) is None:
            raise TaskNotFoundError(self.task_id)
        data = await store.wait(result_key(self.task_id), timeout)
        if data is None:
            raise TimeoutError(f"task {self.task_id!r} has no result yet")
        state, value, error = decode_result(data)
        if state is TaskState.FAILURE:
            type_name, message = error or ("Exception", "")
            raise TaskError(type_name, message)
        if state is TaskState.SKIPPED:
            return None
        return value
