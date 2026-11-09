"""Task outcome and middleware types."""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeAlias

from asynk.envelope import Envelope
from asynk.results import TaskState
from asynk.transport import Delivery


class TaskStatus(enum.Enum):
    """Outcome of running a task, as seen by middleware and the consumer.

    Attributes:
        SUCCESS: The task returned normally.
        FAILED: The task, or a layer before it ran, raised and is not retried:
            no retry policy applies, the exception is not in its `retry_on`, or
            the layer settled the delivery.
        RETRYING: The task, or a layer before it ran, raised and the retry policy
            has retries left.
        RETRIES_EXCEEDED: The task, or a layer before it ran, raised and the retry
            policy has no retries left.
        UNKNOWN_TASK: The envelope names a task this app has not registered.
        SKIPPED: Middleware skipped the task, so it did not run.
    """

    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    RETRIES_EXCEEDED = "retries_exceeded"
    UNKNOWN_TASK = "unknown_task"
    SKIPPED = "skipped"

    @property
    def state(self) -> TaskState:
        """The `TaskState` recorded for this outcome."""
        return _STATES[self]


@dataclass(frozen=True)
class TaskResult:
    """Result of handling one delivery.

    Attributes:
        envelope: The envelope that was handled.
        status: How the run ended.
        value: The task's return value on success.
        error: The raised exception on failure.
    """

    envelope: Envelope
    status: TaskStatus
    value: Any = None
    error: BaseException | None = None


@dataclass(frozen=True)
class TaskCall:
    """One task about to run, as passed through the middleware chain.

    Attributes:
        envelope: The envelope of the message being handled.
        delivery: The delivery the envelope arrived on.
    """

    envelope: Envelope
    delivery: Delivery

    def skip(self) -> TaskResult:
        """Builds the `SKIPPED` result.

        Return it without calling `next` to veto the task.

        Returns:
            A `TaskResult` with status `TaskStatus.SKIPPED`.
        """
        return TaskResult(self.envelope, TaskStatus.SKIPPED)


Next: TypeAlias = Callable[[TaskCall], Awaitable[TaskResult]]
"""Calls the rest of the middleware chain, ending with the task."""

Middleware: TypeAlias = Callable[[TaskCall, Next], Awaitable[TaskResult]]
"""Async layer around a task: receives the call and `next`, returns the result."""


_STATES = {
    TaskStatus.SUCCESS: TaskState.SUCCESS,
    TaskStatus.SKIPPED: TaskState.SKIPPED,
    TaskStatus.FAILED: TaskState.FAILURE,
    TaskStatus.RETRIES_EXCEEDED: TaskState.FAILURE,
    TaskStatus.UNKNOWN_TASK: TaskState.FAILURE,
    TaskStatus.RETRYING: TaskState.RETRYING,
}
