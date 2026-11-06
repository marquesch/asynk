"""The exceptions asynk defines."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from asynk.hooks import TaskResult


class AsynkError(Exception):
    """Base class of every exception asynk defines."""


class NotConfiguredError(AsynkError):
    """Raised when an app lacks a component required by the operation."""


class EncodeError(AsynkError):
    """Raised when a value holds something the codec cannot encode."""


class DecodeError(AsynkError):
    """Raised when bytes cannot be decoded into a valid value."""


class InvalidEnvelopeError(AsynkError):
    """Raised when data is not a valid envelope."""


class CronError(AsynkError):
    """Raised when a cron expression is invalid."""


class TaskError(AsynkError):
    """Raised by `TaskHandle.result` when the task failed.

    Attributes:
        type_name: Name of the exception type the task raised.
        message: The exception's message.
    """

    def __init__(self, type_name: str, message: str) -> None:
        super().__init__(f"{type_name}: {message}")
        self.type_name = type_name
        self.message = message


class TaskNotFoundError(AsynkError):
    """Raised by `TaskHandle.result` for a task id the store does not know."""


class TaskTimeoutError(AsynkError):
    """Raised inside a worker when a task attempt exceeds its `timeout`."""


class DeliveryFailedError(AsynkError):
    """Raised in `Consumer.process` for a failed, unknown or retries-exceeded task.

    Attributes:
        result: The failed task's result.
    """

    def __init__(self, result: TaskResult) -> None:
        super().__init__(f"task {result.envelope.task!r} ended with {result.status.name}")
        self.result = result
