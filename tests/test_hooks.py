from __future__ import annotations

import functools
from typing import Any

import pytest

from asynk.app import App
from asynk.hooks import SkipTask, TaskStatus

EVENTS = [
    "before_startup",
    "after_startup",
    "before_shutdown",
    "after_shutdown",
    "before_task",
    "after_task",
]


@pytest.mark.parametrize("event", EVENTS)
def test_decorator_registers_and_returns_the_function(event: str) -> None:
    app = App("t")

    async def hook(*args: Any) -> None: ...

    own = list(getattr(app.hooks, event))
    assert getattr(app, event)(hook) is hook
    assert list(getattr(app.hooks, event)) == [*own, hook]


@pytest.mark.parametrize("event", EVENTS)
def test_hooks_run_in_registration_order(event: str) -> None:
    app = App("t")

    def first(*args: Any) -> None: ...

    def second(*args: Any) -> None: ...

    def third(*args: Any) -> None: ...

    own = list(getattr(app.hooks, event))
    for hook in (first, second, third):
        getattr(app, event)(hook)
    assert list(getattr(app.hooks, event)) == [*own, first, second, third]


@pytest.mark.parametrize("event", EVENTS)
def test_registering_the_same_function_twice_keeps_one(event: str) -> None:
    app = App("t")

    def hook(*args: Any) -> None: ...

    own = list(getattr(app.hooks, event))
    getattr(app, event)(hook)
    getattr(app, event)(hook)
    assert list(getattr(app.hooks, event)) == [*own, hook]


def test_hooks_made_by_a_factory_all_register() -> None:
    app = App("t")

    def make() -> Any:
        def hook(*args: Any) -> None: ...

        return hook

    first, second = make(), make()
    app.before_task(first)
    app.before_task(second)
    assert list(app.hooks.before_task) == [first, second]


def test_distinct_lambdas_all_register() -> None:
    app = App("t")
    first: Any = lambda *args: None  # noqa: E731
    second: Any = lambda *args: None  # noqa: E731
    app.before_task(first)
    app.before_task(second)
    assert list(app.hooks.before_task) == [first, second]


class _Recorder:
    def hook(self, *args: Any) -> None: ...

    def __call__(self, *args: Any) -> None: ...


def test_bound_methods_of_different_instances_all_register() -> None:
    app = App("t")
    a, b = _Recorder(), _Recorder()
    app.before_task(a.hook)
    app.before_task(b.hook)
    assert list(app.hooks.before_task) == [a.hook, b.hook]


def test_the_same_bound_method_accessed_twice_registers_once() -> None:
    app = App("t")
    a = _Recorder()
    assert a.hook is not a.hook
    app.before_task(a.hook)
    app.before_task(a.hook)
    assert list(app.hooks.before_task) == [a.hook]


def test_partials_register() -> None:
    app = App("t")

    def base(*args: Any) -> None: ...

    first, second = functools.partial(base, 1), functools.partial(base, 2)
    app.before_task(first)
    app.before_task(second)
    app.before_task(first)
    assert list(app.hooks.before_task) == [first, second]


def test_callable_instances_register() -> None:
    app = App("t")
    first, second = _Recorder(), _Recorder()
    app.before_task(first)
    app.before_task(second)
    app.before_task(first)
    assert list(app.hooks.before_task) == [first, second]


def test_same_function_may_be_registered_for_different_events() -> None:
    app = App("t")

    def hook(*args: Any) -> None: ...

    app.before_task(hook)
    app.after_task(hook)
    assert list(app.hooks.before_task) == [hook]
    assert list(app.hooks.after_task) == [hook]


def test_skip_task_is_an_exception_and_skipped_is_a_status() -> None:
    assert issubclass(SkipTask, Exception)
    assert TaskStatus.SKIPPED.value == "skipped"
