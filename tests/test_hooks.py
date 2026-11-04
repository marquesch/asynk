from __future__ import annotations

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

    assert getattr(app, event)(hook) is hook
    assert list(getattr(app.hooks, event)) == [hook]


@pytest.mark.parametrize("event", EVENTS)
def test_hooks_run_in_registration_order(event: str) -> None:
    app = App("t")

    def first(*args: Any) -> None: ...

    def second(*args: Any) -> None: ...

    def third(*args: Any) -> None: ...

    for hook in (first, second, third):
        getattr(app, event)(hook)
    assert list(getattr(app.hooks, event)) == [first, second, third]


@pytest.mark.parametrize("event", EVENTS)
def test_registering_the_same_function_twice_keeps_one(event: str) -> None:
    app = App("t")

    def hook(*args: Any) -> None: ...

    getattr(app, event)(hook)
    getattr(app, event)(hook)
    assert list(getattr(app.hooks, event)) == [hook]


def test_a_later_function_with_the_same_key_is_ignored() -> None:
    app = App("t")

    def make() -> Any:
        def hook(*args: Any) -> None: ...

        return hook

    first, second = make(), make()
    app.before_task(first)
    app.before_task(second)
    assert list(app.hooks.before_task) == [first]


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
