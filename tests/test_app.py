import inspect

import pytest

from asynk.app import App


def test_task_registers_with_default_name() -> None:
    app = App("t")

    @app.task()
    def fn() -> None: ...

    expected = f"{fn.func.__module__}.{fn.func.__qualname__}"
    assert fn.name == expected
    assert app.get_task(expected) is fn
    assert app.tasks[expected] is fn


def test_explicit_name_and_options() -> None:
    app = App("t")

    @app.task(name="x", max_retries=3, queue="q")
    def fn() -> None: ...

    assert fn.name == "x"
    assert fn.max_retries == 3
    assert fn.options == {"queue": "q"}


def test_duplicate_name_raises_value_error() -> None:
    app = App("t")

    @app.task(name="dup")
    def a() -> None: ...

    with pytest.raises(ValueError, match="dup"):

        @app.task(name="dup")
        def b() -> None: ...


def test_unknown_task_raises_key_error() -> None:
    with pytest.raises(KeyError):
        App("t").get_task("nope")


async def test_direct_call_runs_function() -> None:
    app = App("t")

    @app.task()
    def add(a: int, b: int) -> int:
        return a + b

    @app.task()
    async def aadd(a: int, b: int) -> int:
        return a + b

    assert add(1, 2) == 3
    coro = aadd(1, 2)
    assert inspect.iscoroutine(coro)
    assert await coro == 3


def test_is_async_flag() -> None:
    app = App("t")

    @app.task()
    def s() -> None: ...

    @app.task()
    async def a() -> None: ...

    assert s.is_async is False
    assert a.is_async is True


def test_app_without_producer_or_consumer_is_valid() -> None:
    app = App("t")
    assert app.name == "t"
    assert app.producer is None
    assert app.consumer is None
