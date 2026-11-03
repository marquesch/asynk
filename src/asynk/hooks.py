from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeAlias

from asynk.envelope import Envelope
from asynk.transport import Delivery


class TaskStatus(enum.Enum):
    """Outcome of running a task."""

    SUCCESS = "success"
    FAILED = "failed"
    RETRIES_EXCEEDED = "retries_exceeded"
    UNKNOWN_TASK = "unknown_task"


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


AfterTaskHook: TypeAlias = Callable[[TaskResult, Delivery], Awaitable[None]]
"""Async callback invoked after each task run, before the delivery is settled.

It may settle the delivery itself; if it raises, the delivery is requeued.
"""


class TaskFailedError(Exception):
    """Raised inside `Consumer.process` when the task did not succeed.

    Attributes:
        result: The failed task's result.
    """

    def __init__(self, result: TaskResult) -> None:
        super().__init__(f"task {result.envelope.task!r} ended with {result.status.name}")
        self.result = result
