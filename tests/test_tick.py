from zoneinfo import ZoneInfo

import pytest

from asynk import Task, Tick
from asynk.app import App
from asynk.cron import CronError
from asynk.envelope import Envelope, NonJsonArgumentError


class FakeProducer:
    def __init__(self) -> None:
        self.published: list[Envelope] = []

    async def publish(self, envelope: Envelope) -> str:
        self.published.append(envelope)
        return "handle"


async def test_tick_is_registered_as_task() -> None:
    producer = FakeProducer()
    app = App("t", producer=producer)

    @app.tick(cron="* * * * *")
    def add(a: int, b: int = 0) -> int:
        return a + b

    assert isinstance(add, Tick)
    assert isinstance(add, Task)
    assert app.tasks[add.name] is add
    assert add(1, b=2) == 3
    assert await add.delay(1, b=2) == "handle"
    assert producer.published[0].task == add.name


def test_tick_default_name_matches_task_rules() -> None:
    app = App("t")

    @app.task()
    def plain() -> None: ...

    @app.tick(cron="* * * * *")
    def ticked() -> None: ...

    prefix = plain.name.removesuffix("plain")
    assert ticked.name == f"{prefix}ticked"

    @app.tick(cron="* * * * *", name="custom")
    def other() -> None: ...

    assert other.name == "custom"


def test_duplicate_name_with_task_raises_value_error() -> None:
    app = App("t")

    @app.task(name="x")
    def a() -> None: ...

    with pytest.raises(ValueError, match="'x'"):

        @app.tick(cron="* * * * *", name="x")
        def b() -> None: ...

    @app.tick(cron="* * * * *", name="y")
    def c() -> None: ...

    with pytest.raises(ValueError, match="'y'"):

        @app.task(name="y")
        def d() -> None: ...


def test_app_tz_default_is_utc() -> None:
    assert App("t").tz == ZoneInfo("UTC")


def test_tick_uses_app_tz_by_default() -> None:
    app = App("t", tz="Europe/Paris")

    @app.tick(cron="0 8 * * *")
    def f() -> None: ...

    assert f.schedule.tz == ZoneInfo("Europe/Paris")


def test_tick_tz_overrides_app_tz() -> None:
    app = App("t", tz="Europe/Paris")

    @app.tick(cron="0 8 * * *", tz="Asia/Tokyo")
    def f() -> None: ...

    assert f.schedule.tz == ZoneInfo("Asia/Tokyo")


def test_schedule_fields() -> None:
    app = App("t")

    @app.tick(cron="*/5 * * * *", args=[1, "a"], kwargs={"k": [1]}, catch_up=True)
    def f(*args: object, **kwargs: object) -> None: ...

    s = f.schedule
    assert s.expression == "*/5 * * * *"
    assert s.cron.parse("*/5 * * * *") == s.cron
    assert s.args == (1, "a")
    assert s.kwargs == {"k": [1]}
    assert s.catch_up is True
    assert list(app.ticks) == [f]


def test_schedule_defaults() -> None:
    app = App("t")

    @app.tick(cron="* * * * *")
    def f() -> None: ...

    assert f.schedule.args == ()
    assert f.schedule.kwargs == {}
    assert f.schedule.catch_up is False


def test_invalid_cron_raises_at_decoration() -> None:
    app = App("t")
    with pytest.raises(CronError):
        app.tick(cron="nope")
    assert app.tasks == {}


def test_unknown_app_tz_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Not/AZone"):
        App("myapp", tz="Not/AZone")


def test_unknown_tick_tz_raises_value_error_naming_tick() -> None:
    app = App("t")
    with pytest.raises(ValueError, match="mytick"):

        @app.tick(cron="* * * * *", tz="Not/AZone", name="mytick")
        def f() -> None: ...

    with pytest.raises(ValueError, match="unnamed"):

        @app.tick(cron="* * * * *", tz="Not/AZone")
        def unnamed() -> None: ...

    assert app.tasks == {}


def test_non_json_args_raise() -> None:
    app = App("t")
    with pytest.raises(NonJsonArgumentError):
        app.tick(cron="* * * * *", args=[object()])
    with pytest.raises(NonJsonArgumentError):
        app.tick(cron="* * * * *", kwargs={"k": object()})


def test_ticks_lists_only_ticks_in_order() -> None:
    app = App("t")

    @app.tick(cron="* * * * *")
    def first() -> None: ...

    @app.task()
    def plain() -> None: ...

    @app.tick(cron="* * * * *")
    def second() -> None: ...

    assert list(app.ticks) == [first, second]


def test_max_retries_and_options_kept() -> None:
    app = App("t")

    @app.tick(cron="* * * * *", max_retries=3, queue="q")
    def f() -> None: ...

    assert f.max_retries == 3
    assert f.options == {"queue": "q"}
