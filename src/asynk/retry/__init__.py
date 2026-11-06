"""Retry policies, backoff strategies and the retry decision."""

from __future__ import annotations

import math
from dataclasses import dataclass

from asynk.hooks import TaskStatus
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
        retries = self.max_retries
        if retries is not None:
            if isinstance(retries, bool) or not isinstance(retries, int):
                raise TypeError(f"max_retries must be an int or None, got {retries!r}")
            if retries < 0:
                raise ValueError(f"max_retries must be at least 0, got {retries!r}")

        backoff = self.backoff
        if isinstance(backoff, int | float) and not isinstance(backoff, bool):
            if not math.isfinite(backoff) or backoff < 0:
                raise ValueError(
                    f"backoff must be a finite number of seconds >= 0, got {backoff!r}"
                )
        elif backoff is not None and (
            isinstance(backoff, bool) or not callable(getattr(backoff, "delay", None))
        ):
            raise TypeError(f"backoff must be a number, a Backoff or None, got {backoff!r}")

        try:
            retry_on = tuple(self.retry_on)
        except TypeError:
            raise TypeError(
                f"retry_on must be a tuple of exception types, got {self.retry_on!r}"
            ) from None
        if not retry_on:
            raise ValueError("retry_on must not be empty")
        for kind in retry_on:
            if not (isinstance(kind, type) and issubclass(kind, Exception)):
                raise TypeError(f"retry_on must contain Exception subclasses, got {kind!r}")
        object.__setattr__(self, "retry_on", retry_on)


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
