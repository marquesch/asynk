"""Five-field cron expressions and their next firing time.

Fields, separated by any whitespace:

    minute        0-59
    hour          0-23
    day of month  1-31
    month         1-12 or JAN-DEC
    day of week   0-7 or SUN-SAT (0 and 7 are both Sunday)

Each field is a comma-separated list of items. An item is `*`, a value `n`, a
range `a-b`, or a stepped `*/s` or `a-b/s`. Names are case-insensitive and
may be used as values and range ends in the month and day-of-week fields.

Day matching follows classic Vixie cron. A day field is *restricted* when its
text does not start with `*` (so `*` and `*/2` are unrestricted, `1-31` is
restricted). When both day of month and day of week are restricted, a day
matches if either field matches. Otherwise a day must match both fields (an
unrestricted field still applies its own values: `*/2` means odd days).

Slots are local wall-clock times in the tzinfo of the `datetime` given to
`CronExpression.next_after`. A wall time that does not exist (a DST
spring-forward gap) is skipped. A wall time that occurs twice (a DST fall-back)
fires once, at its first occurrence (`fold=0`).
"""

# This module should change for almost no reason. A PR that changes it is a
# poor decision unless there is a clear reason for it. It is small and boring on
# purpose: pure functions over `datetime`, no I/O, no clock, no asyncio and no
# imports from the rest of asynk.

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta

_MONTH_NAMES = {
    name: i
    for i, name in enumerate(
        ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"), 1
    )
}
_WEEKDAY_NAMES = {"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}

# Longest day of month in any year, February counted as 29.
_MAX_DAYS = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

_MAX_NUMBER_DIGITS = 4

# Any month/day combination accepted by `parse` falls on every weekday within
# 400 years (one Gregorian cycle), so `next_after` finds a slot well before this.
_SEARCH_YEARS = 401


class CronError(ValueError):
    """Raised when a cron expression is invalid."""


@dataclass(frozen=True)
class _Field:
    name: str
    low: int
    high: int
    names: dict[str, int] = field(default_factory=dict)


_MINUTE = _Field("minute", 0, 59)
_HOUR = _Field("hour", 0, 23)
_DAY = _Field("day of month", 1, 31)
_MONTH = _Field("month", 1, 12, _MONTH_NAMES)
# 7 is Sunday again; `parse` folds it to 0.
_WEEKDAY = _Field("day of week", 0, 7, _WEEKDAY_NAMES)


@dataclass(frozen=True)
class CronExpression:
    """A parsed five-field cron expression.

    Build one with `CronExpression.parse`. Two expressions are equal when they
    fire at the same slots, regardless of how they were written.

    Attributes:
        minutes: Matching minutes, 0-59.
        hours: Matching hours, 0-23.
        days: Matching days of month, 1-31.
        months: Matching months, 1-12.
        weekdays: Matching days of week, 0-6 with 0 as Sunday.
        days_restricted: Whether the day-of-month text does not start with `*`.
        weekdays_restricted: Whether the day-of-week text does not start with `*`.
        expression: The text that was parsed.
    """

    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]
    days_restricted: bool
    weekdays_restricted: bool
    expression: str = field(default="", compare=False)

    @classmethod
    def parse(cls, expression: str) -> CronExpression:
        """Parses a five-field cron expression.

        Args:
            expression: Minute, hour, day of month, month and day of week,
                separated by whitespace.

        Returns:
            The parsed expression.

        Raises:
            CronError: If the expression does not have five fields, a field is
                malformed or out of range, or its day of month and month can
                never occur together (such as 31 February) while that day of
                month must match.
        """
        parts = expression.split()
        if len(parts) != 5:
            raise CronError(f"{expression!r}: expected 5 fields, got {len(parts)}")
        try:
            minutes = _parse_field(parts[0], _MINUTE)
            hours = _parse_field(parts[1], _HOUR)
            days = _parse_field(parts[2], _DAY)
            months = _parse_field(parts[3], _MONTH)
            weekdays = frozenset(d % 7 for d in _parse_field(parts[4], _WEEKDAY))
        except CronError as exc:
            raise CronError(f"{expression!r}: {exc}") from None
        cron = cls(
            minutes=minutes,
            hours=hours,
            days=days,
            months=months,
            weekdays=weekdays,
            days_restricted=not parts[2].startswith("*"),
            weekdays_restricted=not parts[4].startswith("*"),
            expression=expression,
        )
        or_days = cron.days_restricted and cron.weekdays_restricted
        if not or_days and not any(d <= _MAX_DAYS[m - 1] for m in months for d in days):
            raise CronError(f"{expression!r}: day of month never occurs in the given months")
        return cron

    def next_after(self, after: datetime) -> datetime:
        """Returns the first slot strictly later than `after`.

        Slots are whole minutes of wall-clock time in `after`'s tzinfo; see the
        module docstring for how DST gaps and repeated hours are handled.

        Args:
            after: A timezone-aware datetime. Seconds and microseconds are
                allowed; the result is always a whole minute.

        Returns:
            An aware datetime in `after`'s tzinfo, strictly later than `after`
            as an instant.

        Raises:
            ValueError: If `after` is naive.
        """
        tz = after.tzinfo
        if tz is None or after.utcoffset() is None:
            raise ValueError(f"after must be timezone-aware, got {after!r}")
        after_utc = after.astimezone(UTC)
        # Normalize to the wall time that `tz` itself gives this instant, so a
        # non-normalized `after` (such as fold=1 inside a DST gap) starts at the
        # right wall minute. Existing fold=0 wall times map to instants in order,
        # so candidates earlier than this minute are earlier instants too.
        after = after_utc.astimezone(tz)
        start = after.replace(tzinfo=None, second=0, microsecond=0, fold=0)
        start += timedelta(minutes=1)
        hours = sorted(self.hours)
        minutes = sorted(self.minutes)
        day = start.date()
        end = date(min(day.year + _SEARCH_YEARS, date.max.year), 12, 31)
        while day <= end:
            if day.month not in self.months:
                day = _first_of_next_month(day)
                continue
            if self._day_matches(day):
                for hour in hours:
                    for minute in minutes:
                        wall = datetime.combine(day, time(hour, minute))
                        if wall < start:
                            continue
                        candidate = wall.replace(tzinfo=tz)
                        if not _exists(candidate):
                            continue
                        if candidate.astimezone(UTC) <= after_utc:
                            continue
                        return candidate
            day += timedelta(days=1)
        # Safety net only: `parse` rules out expressions with no slot in 400 years.
        raise ValueError(f"{self.expression!r}: no slot after {after.isoformat()}")

    def _day_matches(self, day: date) -> bool:
        in_days = day.day in self.days
        in_weekdays = day.isoweekday() % 7 in self.weekdays
        if self.days_restricted and self.weekdays_restricted:
            return in_days or in_weekdays
        return in_days and in_weekdays


def _parse_field(text: str, spec: _Field) -> frozenset[int]:
    values: set[int] = set()
    for item in text.split(","):
        values.update(_parse_item(item, spec))
    return frozenset(values)


def _parse_item(item: str, spec: _Field) -> range:
    if not item:
        raise CronError(f"{spec.name}: empty list item")
    base, slash, step_text = item.partition("/")
    step = 1
    if slash:
        step = _parse_number(step_text, spec.name + " step")
        if step == 0:
            raise CronError(f"{spec.name}: step must be at least 1")
    if base == "*":
        return range(spec.low, spec.high + 1, step)
    low_text, dash, high_text = base.partition("-")
    low = _parse_value(low_text, spec)
    if not dash:
        if slash:
            raise CronError(f"{spec.name}: a step needs '*' or a range, got {item!r}")
        return range(low, low + 1)
    high = _parse_value(high_text, spec)
    if low > high:
        raise CronError(f"{spec.name}: reversed range {item!r}")
    return range(low, high + 1, step)


def _parse_value(text: str, spec: _Field) -> int:
    named = spec.names.get(text.lower())
    value = named if named is not None else _parse_number(text, spec.name)
    if not spec.low <= value <= spec.high:
        raise CronError(f"{spec.name}: {value} is outside {spec.low}-{spec.high}")
    return value


def _parse_number(text: str, what: str) -> int:
    if not (text.isascii() and text.isdigit()):
        raise CronError(f"{what}: expected a number, got {text!r}")
    # No field needs more than two digits; the cap also keeps `int` from raising
    # its own ValueError on very long strings.
    if len(text) > _MAX_NUMBER_DIGITS:
        raise CronError(f"{what}: number {text[:10]!r}... is too long")
    return int(text)


def _first_of_next_month(day: date) -> date:
    if day.month == 12:
        return date(day.year + 1, 1, 1)
    return date(day.year, day.month + 1, 1)


def _exists(local: datetime) -> bool:
    # A wall time in a DST gap does not survive a round trip through UTC.
    tz = local.tzinfo
    back = local.astimezone(UTC).astimezone(tz)
    return back.replace(tzinfo=None) == local.replace(tzinfo=None)
