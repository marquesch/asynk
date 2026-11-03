from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from asynk.envelope import Envelope
from asynk.transport import Delivery


class TaskStatus(enum.Enum):
    SUCCESS = "success"
    FAILED = "failed"
    RETRIES_EXCEEDED = "retries_exceeded"
    UNKNOWN_TASK = "unknown_task"


@dataclass(frozen=True)
class TaskResult:
    envelope: Envelope
    status: TaskStatus
    value: Any = None
    error: BaseException | None = None


type AfterTaskHook = Callable[[TaskResult, Delivery], Awaitable[None]]


class TaskFailedError(Exception):
    """Raised inside the consumer's process context to trigger its default rejection."""

    def __init__(self, result: TaskResult) -> None:
        super().__init__(f"task {result.envelope.task!r} ended with {result.status.name}")
        self.result = result
