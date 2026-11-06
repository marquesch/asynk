"""The `ResultRecorder`, which writes task states and results to a store."""

from __future__ import annotations

import logging

from asynk.codec import Codec
from asynk.errors import EncodeError
from asynk.hooks import TaskResult, TaskStatus
from asynk.results import TaskState, encode_result, encode_state, result_key, state_key
from asynk.store import Store

logger = logging.getLogger("asynk")

_TERMINAL_STATES = {
    TaskStatus.SUCCESS: TaskState.SUCCESS,
    TaskStatus.SKIPPED: TaskState.SKIPPED,
    TaskStatus.FAILED: TaskState.FAILURE,
    TaskStatus.RETRIES_EXCEEDED: TaskState.FAILURE,
    TaskStatus.UNKNOWN_TASK: TaskState.FAILURE,
}


class ResultRecorder:
    """Writes task states and results to a store, failing open.

    Store errors are logged and never raised, so they cannot affect a task or
    its delivery.
    """

    def __init__(self, store: Store, codec: Codec) -> None:
        """Initializes the recorder.

        Args:
            store: The store to write to.
            codec: The codec to encode records with.
        """
        self._store = store
        self._codec = codec

    async def record_state(self, task_id: str, state: TaskState) -> None:
        """Writes a task's state.

        Args:
            task_id: The task's id.
            state: The state to record.
        """
        try:
            await self._store.set(
                state_key(task_id), encode_state(self._codec, state), overwrite=True
            )
        except Exception:
            logger.exception("failed to record state %s for task id=%s", state.value, task_id)

    async def record(self, result: TaskResult) -> None:
        """Writes the state, and for a finished task the result record, of a task result.

        A `RETRYING` result writes the `RETRYING` state only; every other status
        is final and also writes the result record.

        Args:
            result: The result to record.
        """
        envelope = result.envelope
        if result.status is TaskStatus.RETRYING:
            await self.record_state(envelope.id, TaskState.RETRYING)
            return
        state = _TERMINAL_STATES[result.status]
        try:
            state, record = self._encode_outcome(result, state)
            await self._store.set(
                state_key(envelope.id), encode_state(self._codec, state), overwrite=True
            )
            await self._store.set(result_key(envelope.id), record)
        except Exception:
            logger.exception(
                "failed to record result for task %r (id=%s)", envelope.task, envelope.id
            )

    def _encode_outcome(self, result: TaskResult, state: TaskState) -> tuple[TaskState, bytes]:
        codec = self._codec
        envelope = result.envelope
        if result.status is TaskStatus.SUCCESS:
            try:
                return state, encode_result(codec, state, value=result.value)
            except EncodeError as exc:
                logger.warning(
                    "result of task %r (id=%s) cannot be encoded; recording a failure",
                    envelope.task,
                    envelope.id,
                    exc_info=True,
                )
                error = (
                    "ResultNotSerializable",
                    f"task {envelope.task!r} returned a value of type "
                    f"{type(result.value).__name__!r} that cannot be encoded: {exc}",
                )
                return TaskState.FAILURE, encode_result(codec, TaskState.FAILURE, error=error)
        if result.status is TaskStatus.SKIPPED:
            return state, encode_result(codec, state)
        if result.status is TaskStatus.UNKNOWN_TASK:
            return state, encode_result(codec, state, error=("UnknownTask", envelope.task))
        error_value = result.error
        return state, encode_result(
            codec, state, error=(type(error_value).__name__, str(error_value))
        )
