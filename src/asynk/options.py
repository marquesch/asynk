"""Per-task options."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

from asynk.retry import RetryPolicy


@dataclass(frozen=True)
class TaskOptions:
    """Options of a task, or the defaults of every task of an app.

    Attributes:
        retry: The retry policy; `False` disables retries and `None` leaves
            the option unset.

    Raises:
        TypeError: If `retry` is not a `RetryPolicy`, `False` or `None`.
    """

    retry: RetryPolicy | Literal[False] | None = None

    def __post_init__(self) -> None:
        if (
            self.retry is not None
            and self.retry is not False
            and not isinstance(self.retry, RetryPolicy)
        ):
            raise TypeError(f"retry must be a RetryPolicy, False or None, got {self.retry!r}")

    @property
    def policy(self) -> RetryPolicy | None:
        """The retry policy in force; None when retries are off or unset."""
        return None if self.retry is False else self.retry

    def with_defaults(self, defaults: TaskOptions) -> TaskOptions:
        """Fills the options left unset from `defaults`.

        Args:
            defaults: The options to fall back on.

        Returns:
            New options; neither `self` nor `defaults` is changed.
        """
        return replace(self, retry=defaults.retry if self.retry is None else self.retry)
