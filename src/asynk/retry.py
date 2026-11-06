"""Retry policies, backoff strategies and the retry decision."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping
from dataclasses import dataclass, fields
from numbers import Real
from typing import Any, Protocol

from asynk.hooks import TaskStatus


class Backoff(Protocol):
    """A strategy that decides how long to wait before a retry."""

    def delay(self, retry: int) -> float:
        """Returns the seconds to wait before a retry.

        Args:
            retry: The retry number, counted from 1.

        Returns:
            The delay in seconds; a value of zero or less means no delay.
        """
        ...


@dataclass(frozen=True)
class Exponential:
    """Exponential backoff: the delay grows by `factor` on each retry.

    The first retry waits `base`, the next `base * factor`, and so on, never
    more than `max`.

    Attributes:
        base: Seconds to wait before the first retry; must be positive.
        factor: Multiplier applied for each further retry; at least 1.
        max: Upper bound of the delay in seconds; at least `base`.
        jitter: When true, the delay is drawn uniformly between zero and the
            capped delay.

    Raises:
        ValueError: If `base`, `factor` or `max` is out of range.
    """

    base: float
    factor: float
    max: float
    jitter: bool = False

    def __post_init__(self) -> None:
        if not self.base > 0:
            raise ValueError(f"base must be positive, got {self.base!r}")
        if not self.factor >= 1:
            raise ValueError(f"factor must be at least 1, got {self.factor!r}")
        if not self.max >= self.base:
            raise ValueError(f"max must be at least base ({self.base!r}), got {self.max!r}")

    def delay(self, retry: int) -> float:
        """Returns the seconds to wait before a retry.

        Args:
            retry: The retry number, counted from 1.

        Returns:
            The delay in seconds, at most `max`.
        """
        exponent = retry - 1
        if exponent > 0 and exponent * math.log(self.factor) >= math.log(self.max / self.base):
            capped = self.max
        else:
            try:
                capped = min(self.max, self.base * self.factor**exponent)
            except OverflowError:
                capped = self.max
        return random.uniform(0, capped) if self.jitter else capped


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
        ValueError: If a field is out of range, or `retry_on` is empty (use
            `retry=False` on the task to turn retries off).
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
        if isinstance(backoff, Real) and not isinstance(backoff, bool):
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
            raise ValueError("retry_on must not be empty; pass retry=False to turn retries off")
        for kind in retry_on:
            if not (isinstance(kind, type) and issubclass(kind, Exception)):
                raise TypeError(f"retry_on must contain Exception subclasses, got {kind!r}")
        object.__setattr__(self, "retry_on", retry_on)


def as_retry_policy(value: RetryPolicy | Mapping[str, Any]) -> RetryPolicy:
    """Converts a retry setting to a `RetryPolicy`.

    Args:
        value: A `RetryPolicy`, returned unchanged, or a mapping of its fields.

    Returns:
        The policy.

    Raises:
        TypeError: If `value` is neither, or the mapping has unknown keys.
    """
    if isinstance(value, RetryPolicy):
        return value
    if not isinstance(value, Mapping):
        raise TypeError(f"retry must be a RetryPolicy or a mapping, got {value!r}")
    known = {f.name for f in fields(RetryPolicy)}
    unknown = sorted(str(key) for key in value if key not in known)
    if unknown:
        names = ", ".join(repr(key) for key in unknown)
        raise TypeError(f"unknown RetryPolicy field(s): {names}")
    return RetryPolicy(**value)


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
    delay = float(backoff) if isinstance(backoff, int | float) else backoff.delay(attempt + 1)
    return delay if delay > 0 else None
