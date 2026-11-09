"""Retry policies, backoff strategies and the retry decision."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from asynk._validate import check_number
from asynk.middleware import TaskStatus
from asynk.retry.backoff import Backoff, Exponential

__all__ = ["Backoff", "Exponential", "RetryPolicy", "retry_delay", "retry_status"]


@dataclass(frozen=True)
class RetryPolicy:
    """How a failing task is retried.

    Attributes:
        max_retries: How many times a failed task is retried; `None` retries
            without limit and `0` never retries.
        backoff: Seconds to wait before each retry, or a `Backoff` strategy;
            `None` retries immediately.
        retry_on: Exception types that trigger a retry; any other error fails
            the task at once.

    Raises:
        TypeError: If a field has the wrong type.
        ValueError: If a field is out of range, or `retry_on` is empty.
    """

    max_retries: int | None = 1
    backoff: float | Backoff | None = None
    retry_on: tuple[type[Exception], ...] = (Exception,)

    def __post_init__(self) -> None:
        _check_max_retries(self.max_retries)
        _check_backoff(self.backoff)
        object.__setattr__(self, "retry_on", _checked_retry_on(self.retry_on))


def _check_max_retries(retries: object) -> None:
    if retries is None:
        return
    if isinstance(retries, bool) or not isinstance(retries, int):
        raise TypeError(f"max_retries must be an int or None, got {retries!r}")
    if retries < 0:
        raise ValueError(f"max_retries must be at least 0, got {retries!r}")


def _check_backoff(backoff: object) -> None:
    if backoff is None or callable(getattr(backoff, "delay", None)):
        return
    if isinstance(backoff, bool) or not isinstance(backoff, int | float):
        raise TypeError(f"backoff must be a number, a Backoff or None, got {backoff!r}")
    check_number("backoff", backoff)


def _checked_retry_on(retry_on: Any) -> tuple[type[Exception], ...]:
    try:
        kinds = tuple(retry_on)
    except TypeError:
        raise TypeError(f"retry_on must be a tuple of exception types, got {retry_on!r}") from None
    if not kinds:
        raise ValueError("retry_on must not be empty")
    for kind in kinds:
        if not (isinstance(kind, type) and issubclass(kind, Exception)):
            raise TypeError(f"retry_on must contain Exception subclasses, got {kind!r}")
    return kinds


def retry_status(policy: RetryPolicy | None, attempt: int, error: BaseException) -> TaskStatus:
    """Decides what a failure leads to.

    Args:
        policy: The policy in force, or `None` for none.
        attempt: The attempt number of the failed delivery, counted from 0.
        error: The exception that was raised.

    Returns:
        `FAILED` without a policy or when the error is not in `retry_on`,
        `RETRIES_EXCEEDED` when the retries are used up, else `RETRYING`.
    """
    if policy is None or not isinstance(error, policy.retry_on):
        return TaskStatus.FAILED
    if policy.max_retries is not None and attempt >= policy.max_retries:
        return TaskStatus.RETRIES_EXCEEDED
    return TaskStatus.RETRYING


def retry_delay(policy: RetryPolicy | None, attempt: int) -> float | None:
    """Computes the wait before the retry that follows a failed attempt.

    Args:
        policy: The policy in force, or `None` for none.
        attempt: The attempt number of the failed delivery, counted from 0.

    Returns:
        The delay in seconds, or `None` when the retry is immediate.
    """
    if policy is None or policy.backoff is None:
        return None
    backoff = policy.backoff
    delay = (
        float(backoff) if isinstance(backoff, int | float) else float(backoff.delay(attempt + 1))
    )
    return delay if delay > 0 else None
