from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from asynk.cron import CronExpression
from asynk.envelope import JSONValue
from asynk.task import Task

if TYPE_CHECKING:
    from asynk.app import App

P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class Schedule:
    """When and with what arguments a `Tick` is published.

    Attributes:
        cron: The parsed cron expression.
        expression: The cron expression as written.
        tz: Timezone in which the expression is evaluated.
        args: Positional arguments for each published call.
        kwargs: Keyword arguments for each published call.
        catch_up: Whether missed occurrences are published after downtime.
    """

    cron: CronExpression
    expression: str
    tz: ZoneInfo
    args: tuple[JSONValue, ...]
    kwargs: dict[str, JSONValue]
    catch_up: bool


class Tick(Task[P, R]):
    """A task that a `Ticker` publishes on a cron schedule.

    Attributes:
        schedule: The tick's schedule.
    """

    def __init__(
        self,
        app: App,
        func: Callable[P, R],
        name: str,
        max_retries: int | None,
        options: dict[str, Any],
        schedule: Schedule,
    ) -> None:
        super().__init__(app, func, name, max_retries, options)
        self.schedule = schedule


def resolve_timezone(key: str, owner: str) -> ZoneInfo:
    """Loads a timezone by IANA name.

    Args:
        key: The timezone name.
        owner: Description of what asked for it, used in the error message.

    Returns:
        The timezone.

    Raises:
        ValueError: If the timezone is unknown or malformed.
    """
    try:
        return ZoneInfo(key)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"{owner}: unknown timezone {key!r}") from exc
