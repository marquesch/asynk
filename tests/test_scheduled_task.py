from dataclasses import fields
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pytest

from asynk import CronError, ScheduledTask, Task, TaskOptions
from asynk.app import App
from asynk.envelope import Envelope
from asynk.retry import RetryPolicy
from asynk.scheduled_task import Schedule
from asynk.transport import PublishOptions


class FakeProducer:
    def __init__(self) -> None:
        self.published: list[Envelope] = []

    async def publish(
        self, envelope: Envelope, body: bytes, options: PublishOptions | None = None
    ) -> str:
        self.published.append(envelope)
        return "handle"


async def test_scheduled_task_is_registered_as_task() -> None:
    producer = FakeProducer()
    app = App("t", producer=producer)

    @app.scheduled_task(cron="* * * * *")
    async def add(a: int, b: int = 0) -> int:
        return a + b

    assert isinstance(add, ScheduledTask)
    assert isinstance(add, Task)
    assert app.tasks[add.name] is add
    assert await add(1, b=2) == 3
    assert (await add.delay(1, b=2)).receipt == "handle"
    assert producer.published[0].task == add.name


def test_scheduled_task_default_name_matches_task_rules() -> None:
    app = App("t")

    @app.task()
    async def plain() -> None: ...

    @app.scheduled_task(cron="* * * * *")
    async def scheduled() -> None: ...

    prefix = plain.name.removesuffix("plain")
    assert scheduled.name == f"{prefix}scheduled"

    @app.scheduled_task(cron="* * * * *", name="custom")
    async def other() -> None: ...

    assert other.name == "custom"


def test_duplicate_name_with_task_raises_value_error() -> None:
    app = App("t")

    @app.task(name="x")
    async def a() -> None: ...

    with pytest.raises(ValueError, match="'x'"):

        @app.scheduled_task(cron="* * * * *", name="x")
        async def b() -> None: ...

    @app.scheduled_task(cron="* * * * *", name="y")
    async def c() -> None: ...

    with pytest.raises(ValueError, match="'y'"):

        @app.task(name="y")
        async def d() -> None: ...


def test_app_tz_default_is_utc() -> None:
    assert App("t").tz is UTC


def test_scheduled_task_uses_app_tz_by_default() -> None:
    app = App("t", tz="Europe/Paris")

    @app.scheduled_task(cron="0 8 * * *")
    async def f() -> None: ...

    assert f.schedule.tz == ZoneInfo("Europe/Paris")


def test_scheduled_task_tz_overrides_app_tz() -> None:
    app = App("t", tz="Europe/Paris")

    @app.scheduled_task(cron="0 8 * * *", tz="Asia/Tokyo")
    async def f() -> None: ...

    assert f.schedule.tz == ZoneInfo("Asia/Tokyo")


def test_schedule_fields() -> None:
    app = App("t")

    @app.scheduled_task(cron="*/5 * * * *", args=[1, "a"], kwargs={"k": [1]})
    async def f(*args: object, **kwargs: object) -> None: ...

    s = f.schedule
    assert s.expression == "*/5 * * * *"
    assert s.cron.parse("*/5 * * * *") == s.cron
    assert s.args == (1, "a")
    assert s.kwargs == {"k": [1]}
    assert list(app.scheduled_tasks) == [f]


def test_schedule_defaults() -> None:
    app = App("t")

    @app.scheduled_task(cron="* * * * *")
    async def f() -> None: ...

    assert f.schedule.args == ()
    assert f.schedule.kwargs == {}


def test_schedule_has_no_catch_up() -> None:
    assert "catch_up" not in {f.name for f in fields(Schedule)}
    with pytest.raises(TypeError):
        Schedule.build("* * * * *", tz=UTC, catch_up=True)  # type: ignore[call-arg]


def test_invalid_cron_raises_at_decoration() -> None:
    app = App("t")
    with pytest.raises(CronError):
        app.scheduled_task(cron="nope")
    assert app.tasks == {}


def test_unknown_app_tz_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Not/AZone"):
        App("myapp", tz="Not/AZone")


def test_unknown_scheduled_task_tz_raises_value_error_naming_scheduled_task() -> None:
    app = App("t")
    with pytest.raises(ValueError, match="mytask"):

        @app.scheduled_task(cron="* * * * *", tz="Not/AZone", name="mytask")
        async def f() -> None: ...

    with pytest.raises(ValueError, match="unnamed"):

        @app.scheduled_task(cron="* * * * *", tz="Not/AZone")
        async def unnamed() -> None: ...

    assert app.tasks == {}


def test_scheduled_tasks_lists_only_scheduled_tasks_in_order() -> None:
    app = App("t")

    @app.scheduled_task(cron="* * * * *")
    async def first() -> None: ...

    @app.task()
    async def plain() -> None: ...

    @app.scheduled_task(cron="* * * * *")
    async def second() -> None: ...

    assert list(app.scheduled_tasks) == [first, second]


def test_scheduled_task_retry_policy() -> None:
    app = App("t")

    @app.scheduled_task(cron="* * * * *", options=TaskOptions(retry=RetryPolicy(max_retries=3)))
    async def f() -> None: ...

    @app.scheduled_task(cron="* * * * *")
    async def g() -> None: ...

    assert f.retry == RetryPolicy(max_retries=3)
    assert g.retry is None
    assert len(app.scheduled_tasks) == 2


@pytest.fixture
def no_tzdb(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(key: str) -> ZoneInfo:
        raise ZoneInfoNotFoundError(f"No time zone found with key {key}")

    monkeypatch.setattr("asynk.scheduled_task.ZoneInfo", missing)


@pytest.mark.usefixtures("no_tzdb")
def test_default_utc_does_not_need_tz_database() -> None:
    app = App("x")

    @app.scheduled_task(cron="* * * * *")
    async def f() -> None: ...

    assert app.tz is UTC
    assert f.schedule.tz is UTC
    now = datetime(2026, 1, 1, 12, 0, 30, tzinfo=UTC)
    assert f.schedule.cron.next_after(now.astimezone(f.schedule.tz)) == datetime(
        2026, 1, 1, 12, 1, tzinfo=UTC
    )


@pytest.mark.usefixtures("no_tzdb")
def test_explicit_utc_does_not_need_tz_database() -> None:
    app = App("x", tz="UTC")

    @app.scheduled_task(cron="* * * * *", tz="UTC", name="n")
    async def f() -> None: ...

    assert f.schedule.tz is UTC


@pytest.mark.usefixtures("no_tzdb")
def test_non_utc_still_needs_tz_database() -> None:
    with pytest.raises(ValueError, match="app 'x': unknown timezone 'Europe/Paris'"):
        App("x", tz="Europe/Paris")
    app = App("x")
    with pytest.raises(ValueError, match="scheduled task 'n': unknown timezone 'Europe/Paris'"):
        app.scheduled_task(cron="* * * * *", tz="Europe/Paris", name="n")


async def _noop() -> None: ...


def test_scheduled_task_takes_its_schedule_by_keyword() -> None:
    schedule = Schedule.build("* * * * *", tz=UTC)
    publish: Any = None
    with pytest.raises(TypeError):
        ScheduledTask(_noop, "n", None, publish, schedule)  # type: ignore[call-arg]
    assert ScheduledTask(_noop, "n", None, publish, schedule=schedule).schedule is schedule
