"""Scheduled tasks and their cron schedules."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, tzinfo
from typing import ParamSpec, TypeVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from asynk.cron import CronExpression
from asynk.retry import RetryPolicy
from asynk.task import Publish, Task

P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class Schedule:
    """When and with what arguments a `ScheduledTask` is published.

    Attributes:
        cron: The parsed cron expression.
        expression: The cron expression as written.
        tz: Timezone in which the expression is evaluated.
        args: Positional arguments for each published call.
        kwargs: Keyword arguments for each published call.
    """

    cron: CronExpression
    expression: str
    tz: tzinfo
    args: tuple[object, ...]
    kwargs: dict[str, object]

    @classmethod
    def build(
        cls,
        expression: str,
        *,
        tz: tzinfo,
        args: Sequence[object] = (),
        kwargs: Mapping[str, object] | None = None,
    ) -> Schedule:
        """Parses a cron expression into a schedule.

        Args:
            expression: Five-field cron expression.
            tz: Timezone in which the expression is evaluated.
            args: Positional arguments for each published call.
            kwargs: Keyword arguments for each published call.

        Returns:
            The schedule.

        Raises:
            CronError: If the cron expression is invalid.
        """
        cron = CronExpression.parse(expression)
        return cls(cron, expression, tz, tuple(args), dict(kwargs or {}))

    @property
    def tz_name(self) -> str:
        """A stable human-readable name of `tz`: the IANA key, or "UTC"."""
        return getattr(self.tz, "key", None) or str(self.tz)


class ScheduledTask(Task[P, R]):
    """A task that a `Scheduler` publishes on a cron schedule.

    Attributes:
        schedule: The scheduled task's schedule.
    """

    def __init__(
        self,
        func: Callable[P, Awaitable[R]],
        name: str,
        retry: RetryPolicy | None,
        publish: Publish,
        *,
        schedule: Schedule,
        timeout: float | None = None,
    ) -> None:
        super().__init__(func, name, retry, publish, timeout=timeout)
        self.schedule = schedule


def resolve_timezone(key: str, owner: str) -> tzinfo:
    """Loads a timezone by IANA name.

    Args:
        key: The timezone name.
        owner: Description of what asked for it, used in the error message.

    Returns:
        The timezone.

    Raises:
        ValueError: If the timezone is unknown or malformed.
    """
    if key == "UTC":
        return UTC
    try:
        return ZoneInfo(key)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"{owner}: unknown timezone {key!r}") from exc
