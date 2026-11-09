"""Per-task options."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

from asynk._validate import check_number
from asynk.retry import RetryPolicy


@dataclass(frozen=True)
class TaskOptions:
    """Options of a task, or the defaults of every task of an app.

    Attributes:
        retry: The retry policy; `False` disables retries and `None` leaves
            the option unset.
        timeout: Seconds one attempt of the task may run before it is cancelled and
            fails with `TaskTimeoutError`; `False` means no limit and `None` leaves
            the option unset.

    Raises:
        TypeError: If `retry` is not a `RetryPolicy`, `False` or `None`, or `timeout`
            is not a number, `False` or `None`.
        ValueError: If `timeout` is not a positive, finite number.
    """

    retry: RetryPolicy | Literal[False] | None = None
    timeout: float | Literal[False] | None = None

    def __post_init__(self) -> None:
        if (
            self.retry is not None
            and self.retry is not False
            and not isinstance(self.retry, RetryPolicy)
        ):
            raise TypeError(f"retry must be a RetryPolicy, False or None, got {self.retry!r}")
        timeout = self.timeout
        if timeout is None or timeout is False:
            return
        check_number("timeout", timeout, exclusive=True)

    @property
    def policy(self) -> RetryPolicy | None:
        """The retry policy in force; None when retries are off or unset."""
        return None if self.retry is False else self.retry

    @property
    def time_limit(self) -> float | None:
        """The per-attempt time limit in force; None when there is none or it is unset."""
        return None if self.timeout is False else self.timeout

    def with_defaults(self, defaults: TaskOptions) -> TaskOptions:
        """Fills the options left unset from `defaults`.

        Args:
            defaults: The options to fall back on.

        Returns:
            New options; neither `self` nor `defaults` is changed.
        """
        return replace(
            self,
            retry=defaults.retry if self.retry is None else self.retry,
            timeout=defaults.timeout if self.timeout is None else self.timeout,
        )
