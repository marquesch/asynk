"""Per-task options."""

from __future__ import annotations

from dataclasses import dataclass
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


def resolve_retry(*layers: TaskOptions | None) -> RetryPolicy | None:
    """Resolves the retry policy in force from layers of options.

    Args:
        *layers: Options from the most to the least specific; those that leave
            `retry` unset are skipped.

    Returns:
        The policy of the first layer that sets `retry`, or None when it is
        `False` or no layer sets it.
    """
    for layer in layers:
        if layer is not None and layer.retry is not None:
            return None if layer.retry is False else layer.retry
    return None
