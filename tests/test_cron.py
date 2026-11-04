from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from asynk.cron import CronError, CronExpression

UTC = timezone.utc
NY = ZoneInfo("America/New_York")
SAO_PAULO = ZoneInfo("America/Sao_Paulo")
KOLKATA = ZoneInfo("Asia/Kolkata")

ALL_MINUTES = frozenset(range(60))
ALL_HOURS = frozenset(range(24))
ALL_DAYS = frozenset(range(1, 32))
ALL_MONTHS = frozenset(range(1, 13))
ALL_WEEKDAYS = frozenset(range(7))


def utc(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
    microsecond: int = 0,
) -> datetime:
    return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=UTC)


def next_after(expression: str, after: datetime) -> datetime:
    return CronExpression.parse(expression).next_after(after)


def fires(expression: str, after: datetime, count: int) -> list[datetime]:
    cron = CronExpression.parse(expression)
    result = []
    for _ in range(count):
        after = cron.next_after(after)
        result.append(after)
    return result


# --- parse: valid -----------------------------------------------------------


@pytest.mark.parametrize(
    ("expression", "minutes", "hours", "days", "months", "weekdays"),
    [
        ("* * * * *", ALL_MINUTES, ALL_HOURS, ALL_DAYS, ALL_MONTHS, ALL_WEEKDAYS),
        ("*/15 * * * *", {0, 15, 30, 45}, ALL_HOURS, ALL_DAYS, ALL_MONTHS, ALL_WEEKDAYS),
        ("0 8 * * 1-5", {0}, {8}, ALL_DAYS, ALL_MONTHS, {1, 2, 3, 4, 5}),
        (
            "0,30 9-17 * * mon-fri",
            {0, 30},
            set(range(9, 18)),
            ALL_DAYS,
            ALL_MONTHS,
            {1, 2, 3, 4, 5},
        ),
        ("0 0 1 jan *", {0}, {0}, {1}, {1}, ALL_WEEKDAYS),
        ("1-10/3 * * * *", {1, 4, 7, 10}, ALL_HOURS, ALL_DAYS, ALL_MONTHS, ALL_WEEKDAYS),
        ("0 0 * JAN,Dec SUN,Sat", {0}, {0}, ALL_DAYS, {1, 12}, {0, 6}),
        ("0 0 * * 5-7", {0}, {0}, ALL_DAYS, ALL_MONTHS, {5, 6, 0}),
        ("0 0 * * */2", {0}, {0}, ALL_DAYS, ALL_MONTHS, {0, 2, 4, 6}),
        ("0 0 */10 */5 *", {0}, {0}, {1, 11, 21, 31}, {1, 6, 11}, ALL_WEEKDAYS),
        ("59 23 31 12 6", {59}, {23}, {31}, {12}, {6}),
        (
            "1,2-4,10-20/5 * * * *",
            {1, 2, 3, 4, 10, 15, 20},
            ALL_HOURS,
            ALL_DAYS,
            ALL_MONTHS,
            ALL_WEEKDAYS,
        ),
        ("*/100 * * * *", {0}, ALL_HOURS, ALL_DAYS, ALL_MONTHS, ALL_WEEKDAYS),
    ],
)
def test_parse_valid(
    expression: str,
    minutes: set[int],
    hours: set[int],
    days: set[int],
    months: set[int],
    weekdays: set[int],
) -> None:
    cron = CronExpression.parse(expression)
    assert cron.minutes == minutes
    assert cron.hours == hours
    assert cron.days == days
    assert cron.months == months
    assert cron.weekdays == weekdays


def test_seven_is_sunday() -> None:
    assert CronExpression.parse("0 0 * * 7") == CronExpression.parse("0 0 * * 0")


def test_names_are_case_insensitive() -> None:
    assert CronExpression.parse("0 0 * JAN-MAR MON-FRI") == CronExpression.parse(
        "0 0 * jan-mar mon-fri"
    )
    assert CronExpression.parse("0 0 * Jan-Mar Mon-Fri") == CronExpression.parse("0 0 * 1-3 1-5")


@pytest.mark.parametrize(
    "expression",
    ["0\t8  *   *\t\t1-5", "  0 8 * * 1-5  ", "0 8 * * 1-5\n"],
)
def test_parse_accepts_any_whitespace_between_fields(expression: str) -> None:
    assert CronExpression.parse(expression) == CronExpression.parse("0 8 * * 1-5")


@pytest.mark.parametrize(
    ("expression", "dom_restricted", "dow_restricted"),
    [
        ("* * * * *", False, False),
        ("* * */2 * *", False, False),
        ("* * * * */2", False, False),
        ("* * 13 * *", True, False),
        ("* * 1-31 * *", True, False),
        ("* * * * 5", False, True),
        ("* * * * 0-7", False, True),
        ("* * 13 * 5", True, True),
    ],
)
def test_day_fields_restricted_unless_text_starts_with_star(
    expression: str, dom_restricted: bool, dow_restricted: bool
) -> None:
    cron = CronExpression.parse(expression)
    assert cron.days_restricted is dom_restricted
    assert cron.weekdays_restricted is dow_restricted


# --- parse: invalid ---------------------------------------------------------


@pytest.mark.parametrize(
    "expression",
    [
        "",
        "   ",
        "* * * *",
        "* * * * * *",
        "60 * * * *",
        "-1 * * * *",
        "* 24 * * *",
        "* * 0 * *",
        "* * 32 * *",
        "* * * 0 *",
        "* * * 13 *",
        "* * * * 8",
        "*/0 * * * *",
        "1-10/0 * * * *",
        "5-1 * * * *",
        "1,,2 * * * *",
        ",1 * * * *",
        "1, * * * *",
        "1- * * * *",
        "-5 * * * *",
        "1-2-3 * * * *",
        "*/ * * * *",
        "*/2/2 * * * *",
        "*/-1 * * * *",
        "5/15 * * * *",
        "*-5 * * * *",
        "** * * * *",
        "L * * * *",
        "? * * * *",
        "a * * * *",
        "1.5 * * * *",
        "+1 * * * *",
        "١ * * * *",
        "* * * * mon/2",
        "* * * * fri-mon",
        "* * * mon * ",
        "* * * * jan",
        "jan * * * *",
        "* * * monday *",
        "@daily",
        "0 0 31 2 *",
        "0 0 30 feb *",
        "0 0 31 4,6,9,11 *",
        "0 0 30,31 2 *",
    ],
)
def test_parse_invalid(expression: str) -> None:
    with pytest.raises(CronError):
        CronExpression.parse(expression)


def test_cron_error_is_value_error_and_names_the_expression() -> None:
    with pytest.raises(ValueError, match="60 \\* \\* \\* \\*"):
        CronExpression.parse("60 * * * *")


@pytest.mark.parametrize(
    "expression",
    ["0 0 29 2 *", "0 0 31 1-2 *", "0 0 30,31 2,4 *", "0 0 31 2 1", "0 0 30 feb mon"],
)
def test_parse_accepts_day_month_combinations_that_occur(expression: str) -> None:
    CronExpression.parse(expression)


# --- next_after (UTC) -------------------------------------------------------


@pytest.mark.parametrize(
    ("expression", "after", "expected"),
    [
        # strictly after: on a slot returns the following slot
        ("* * * * *", utc(2026, 1, 1, 12, 0), utc(2026, 1, 1, 12, 1)),
        ("*/15 * * * *", utc(2026, 1, 1, 12, 15), utc(2026, 1, 1, 12, 30)),
        ("0 8 * * *", utc(2026, 1, 1, 8, 0), utc(2026, 1, 2, 8, 0)),
        # seconds and microseconds are ignored in favour of the next whole minute
        ("* * * * *", utc(2026, 1, 1, 12, 0, 0, 1), utc(2026, 1, 1, 12, 1)),
        ("* * * * *", utc(2026, 1, 1, 12, 0, 59, 999999), utc(2026, 1, 1, 12, 1)),
        ("*/15 * * * *", utc(2026, 1, 1, 12, 14, 59), utc(2026, 1, 1, 12, 15)),
        # rollover minute -> hour -> day -> month -> year
        ("* * * * *", utc(2026, 1, 1, 12, 59), utc(2026, 1, 1, 13, 0)),
        ("* * * * *", utc(2026, 1, 1, 23, 59), utc(2026, 1, 2, 0, 0)),
        ("* * * * *", utc(2026, 1, 31, 23, 59), utc(2026, 2, 1, 0, 0)),
        ("* * * * *", utc(2026, 12, 31, 23, 59), utc(2027, 1, 1, 0, 0)),
        ("30 * * * *", utc(2026, 1, 1, 12, 31), utc(2026, 1, 1, 13, 30)),
        ("0 9 * * *", utc(2026, 1, 1, 9, 1), utc(2026, 1, 2, 9, 0)),
        ("0 0 1 * *", utc(2026, 1, 1, 0, 0), utc(2026, 2, 1, 0, 0)),
        ("0 0 1 1 *", utc(2026, 1, 1, 0, 0), utc(2027, 1, 1, 0, 0)),
        ("0 0 1 jan *", utc(2026, 6, 15, 10, 0), utc(2027, 1, 1, 0, 0)),
        # leap day
        ("0 0 29 2 *", utc(2025, 1, 1, 0, 0), utc(2028, 2, 29, 0, 0)),
        ("0 0 29 2 *", utc(2028, 2, 29, 0, 0), utc(2032, 2, 29, 0, 0)),
        # day 31 skips 30-day months
        ("0 0 31 * *", utc(2026, 3, 31, 0, 0), utc(2026, 5, 31, 0, 0)),
        ("0 0 31 * *", utc(2026, 7, 31, 0, 0), utc(2026, 8, 31, 0, 0)),
        ("0 0 31 * *", utc(2026, 1, 31, 0, 0), utc(2026, 3, 31, 0, 0)),
        # weekday ranges (2026-01-02 is a Friday)
        ("0 8 * * 1-5", utc(2026, 1, 2, 8, 0), utc(2026, 1, 5, 8, 0)),
        ("0 8 * * mon-fri", utc(2026, 1, 3, 12, 0), utc(2026, 1, 5, 8, 0)),
        ("0 8 * * sat,sun", utc(2026, 1, 2, 12, 0), utc(2026, 1, 3, 8, 0)),
        ("0 8 * * 7", utc(2026, 1, 2, 12, 0), utc(2026, 1, 4, 8, 0)),
        ("0 8 * * 0", utc(2026, 1, 2, 12, 0), utc(2026, 1, 4, 8, 0)),
        ("0 8 * * 5-7", utc(2026, 1, 3, 12, 0), utc(2026, 1, 4, 8, 0)),
        # month restriction combined with weekday
        ("0 0 * feb mon", utc(2026, 1, 1, 0, 0), utc(2026, 2, 2, 0, 0)),
    ],
)
def test_next_after_utc(expression: str, after: datetime, expected: datetime) -> None:
    result = next_after(expression, after)
    assert result == expected
    assert result.tzinfo is UTC
    assert result.second == 0
    assert result.microsecond == 0


def test_vixie_or_when_both_day_fields_restricted() -> None:
    # 2026-02-13 is a Friday; 2026-03-13 is a Friday too; 2026-02-06, -20, -27 are Fridays.
    assert fires("0 0 13 * 5", utc(2026, 1, 31, 0, 0), 6) == [
        utc(2026, 2, 6),
        utc(2026, 2, 13),
        utc(2026, 2, 20),
        utc(2026, 2, 27),
        utc(2026, 3, 6),
        utc(2026, 3, 13),
    ]
    # 2026-04-13 is a Monday: the 13th fires even though it is not a Friday.
    assert utc(2026, 4, 13) in fires("0 0 13 * 5", utc(2026, 4, 1), 3)


def test_vixie_or_with_impossible_day_of_month() -> None:
    # 31 February never occurs, but the weekday side still matches.
    assert fires("0 0 31 2 1", utc(2026, 1, 1), 2) == [utc(2026, 2, 2), utc(2026, 2, 9)]


def test_only_fridays_when_day_of_month_is_star() -> None:
    result = fires("0 0 * * 5", utc(2026, 1, 1), 10)
    assert all(d.weekday() == 4 for d in result)
    assert result[0] == utc(2026, 1, 2)
    assert all(b - a == timedelta(days=7) for a, b in zip(result, result[1:], strict=False))


def test_only_the_13th_when_day_of_week_is_star() -> None:
    result = fires("0 0 13 * *", utc(2026, 1, 1), 12)
    assert result == [utc(2026, m, 13) for m in range(1, 13)]


def test_star_step_day_of_month_is_unrestricted_and_ands_with_weekday() -> None:
    # */2 starts with '*', so it is unrestricted: odd days that are also Fridays.
    result = fires("0 0 */2 * 5", utc(2026, 1, 1), 6)
    assert result == [
        utc(2026, 1, 9),
        utc(2026, 1, 23),
        utc(2026, 2, 13),
        utc(2026, 2, 27),
        utc(2026, 3, 13),
        utc(2026, 3, 27),
    ]
    assert all(d.weekday() == 4 and d.day % 2 == 1 for d in result)


def test_star_step_day_of_week_is_unrestricted_and_ands_with_day_of_month() -> None:
    # 13th that falls on Sun/Tue/Thu/Sat. 2026: Jan 13 Tue, Jun 13 Sat, Aug 13 Thu.
    assert fires("0 0 13 * */2", utc(2026, 1, 1), 3) == [
        utc(2026, 1, 13),
        utc(2026, 6, 13),
        utc(2026, 8, 13),
    ]


def test_explicit_full_range_day_of_month_is_restricted() -> None:
    # 1-31 does not start with '*', so it ORs with the weekday: every day matches.
    assert fires("0 0 1-31 * 5", utc(2026, 1, 1), 3) == [
        utc(2026, 1, 2),
        utc(2026, 1, 3),
        utc(2026, 1, 4),
    ]


def test_next_after_keeps_fixed_offset_tzinfo() -> None:
    tz = timezone(timedelta(hours=5, minutes=45))
    after = datetime(2026, 1, 1, 8, 59, 30, tzinfo=tz)
    result = next_after("0 9 * * *", after)
    assert result == datetime(2026, 1, 1, 9, 0, tzinfo=tz)
    assert result.tzinfo is tz


def test_next_after_naive_raises_value_error() -> None:
    with pytest.raises(ValueError):
        next_after("* * * * *", datetime(2026, 1, 1, 12, 0))


# --- next_after (time zones) ------------------------------------------------


def test_sao_paulo_weekdays_from_friday_evening() -> None:
    # 2026-01-02 is a Friday.
    after = datetime(2026, 1, 2, 19, 0, tzinfo=SAO_PAULO)
    result = next_after("30 8 * * 1-5", after)
    assert result == datetime(2026, 1, 5, 8, 30, tzinfo=SAO_PAULO)
    assert result.tzinfo is SAO_PAULO
    assert result.utcoffset() == timedelta(hours=-3)
    assert result.isoformat() == "2026-01-05T08:30:00-03:00"


def test_kolkata_nine_am_is_0330_utc() -> None:
    after = datetime(2026, 1, 1, 0, 0, tzinfo=KOLKATA)
    result = next_after("0 9 * * *", after)
    assert result == datetime(2026, 1, 1, 9, 0, tzinfo=KOLKATA)
    assert result.astimezone(UTC) == utc(2026, 1, 1, 3, 30)


def test_after_in_another_zone_is_compared_as_an_instant() -> None:
    # The caller converts to the tick's zone; the result follows `after`'s tzinfo.
    after = utc(2026, 1, 1, 3, 30).astimezone(KOLKATA)
    assert next_after("0 9 * * *", after) == datetime(2026, 1, 2, 9, 0, tzinfo=KOLKATA)


def test_spring_forward_gap_has_no_slot_that_day() -> None:
    # 2026-03-08 02:00 EST jumps to 03:00 EDT: 02:30 does not exist.
    after = datetime(2026, 3, 8, 0, 0, tzinfo=NY)
    result = next_after("30 2 * * *", after)
    assert result == datetime(2026, 3, 9, 2, 30, tzinfo=NY)
    assert result.utcoffset() == timedelta(hours=-4)


def test_spring_forward_every_minute_skips_the_gap() -> None:
    after = datetime(2026, 3, 8, 1, 59, tzinfo=NY)
    result = next_after("* * * * *", after)
    assert (result.hour, result.minute) == (3, 0)
    assert result.astimezone(UTC) - after.astimezone(UTC) == timedelta(minutes=1)


def test_fall_back_fires_once_at_first_occurrence() -> None:
    # 2026-11-01 02:00 EDT falls back to 01:00 EST: 01:30 happens twice.
    result = fires("30 1 * * *", datetime(2026, 11, 1, 0, 0, tzinfo=NY), 2)
    assert result[0] == datetime(2026, 11, 1, 1, 30, fold=0, tzinfo=NY)
    assert result[0].fold == 0
    assert result[0].utcoffset() == timedelta(hours=-4)
    assert result[0].astimezone(UTC) == utc(2026, 11, 1, 5, 30)
    assert result[1] == datetime(2026, 11, 2, 1, 30, tzinfo=NY)


def test_fall_back_from_second_occurrence_goes_to_next_day() -> None:
    after = datetime(2026, 11, 1, 1, 30, fold=1, tzinfo=NY)
    assert after.astimezone(UTC) == utc(2026, 11, 1, 6, 30)
    result = next_after("30 1 * * *", after)
    assert result == datetime(2026, 11, 2, 1, 30, tzinfo=NY)
    assert result.astimezone(UTC) > after.astimezone(UTC)


def test_fall_back_half_hourly_never_returns_instant_at_or_before_after() -> None:
    cron = CronExpression.parse("*/30 * * * *")
    start = utc(2026, 11, 1, 3, 0)  # 2026-10-31 23:00 EDT
    for step in range(5 * 60 // 5):
        after = (start + timedelta(minutes=5 * step)).astimezone(NY)
        result = cron.next_after(after)
        assert result.astimezone(UTC) > after.astimezone(UTC), after
        assert result.minute in (0, 30)
        assert result.tzinfo is NY


def test_fall_back_half_hourly_sequence() -> None:
    result = fires("*/30 * * * *", datetime(2026, 11, 1, 0, 30, tzinfo=NY), 5)
    assert [d.astimezone(UTC) for d in result] == [
        utc(2026, 11, 1, 5, 0),  # 01:00 EDT
        utc(2026, 11, 1, 5, 30),  # 01:30 EDT
        # the repeated 01:00 and 01:30 EST do not fire again
        utc(2026, 11, 1, 7, 0),  # 02:00 EST
        utc(2026, 11, 1, 7, 30),  # 02:30 EST
        utc(2026, 11, 1, 8, 0),  # 03:00 EST
    ]


def test_repeated_next_after_strictly_increases_across_spring_forward() -> None:
    cron = CronExpression.parse("*/7 * * * *")
    after = datetime(2026, 3, 7, 23, 0, tzinfo=NY)
    for _ in range(200):
        nxt = cron.next_after(after)
        assert nxt.astimezone(UTC) > after.astimezone(UTC)
        after = nxt
