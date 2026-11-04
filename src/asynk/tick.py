from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timezone, tzinfo
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from asynk.cron import CronExpression
from asynk.envelope import JSONValue, ensure_json
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
        catch_up: Whether slots missed while the ticker process is running (for
            example during a stall) are published late; slots missed while it
            was down are not recovered.
    """

    cron: CronExpression
    expression: str
    tz: tzinfo
    args: tuple[JSONValue, ...]
    kwargs: dict[str, JSONValue]
    catch_up: bool

    @classmethod
    def build(
        cls,
        expression: str,
        *,
        tz: tzinfo,
        args: Sequence[object] = (),
        kwargs: Mapping[str, object] | None = None,
        catch_up: bool = False,
    ) -> Schedule:
        """Parses a cron expression and checks the arguments into a schedule.

        Args:
            expression: Five-field cron expression.
            tz: Timezone in which the expression is evaluated.
            args: Positional arguments for each published call.
            kwargs: Keyword arguments for each published call.
            catch_up: Whether slots missed while the ticker is running are published late.

        Returns:
            The schedule.

        Raises:
            CronError: If the cron expression is invalid.
            NonJsonArgumentError: If `args` or `kwargs` are not JSON-friendly.
        """
        cron = CronExpression.parse(expression)
        call_args = tuple(args)
        call_kwargs = dict(kwargs or {})
        ensure_json(list(call_args), "args")
        ensure_json(call_kwargs, "kwargs")
        return cls(
            cron,
            expression,
            tz,
            cast("tuple[JSONValue, ...]", call_args),
            cast("dict[str, JSONValue]", call_kwargs),
            catch_up,
        )

    @property
    def tz_name(self) -> str:
        """A stable human-readable name of `tz`: the IANA key, or "UTC"."""
        return getattr(self.tz, "key", None) or str(self.tz)


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


def resolve_timezone(key: str, owner: str) -> tzinfo:
    """Loads a timezone by IANA name.

    "UTC" resolves to `datetime.timezone.utc` without touching the tz database;
    any other name needs one (install `tzdata` where the system has none).

    Args:
        key: The timezone name.
        owner: Description of what asked for it, used in the error message.

    Returns:
        The timezone.

    Raises:
        ValueError: If the timezone is unknown or malformed.
    """
    if key == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(key)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"{owner}: unknown timezone {key!r}") from exc
