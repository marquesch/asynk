from __future__ import annotations

import dataclasses
import functools
from typing import Any, cast

import pytest

from asynk.app import App
from asynk.envelope import Envelope
from asynk.hooks import TaskCall, TaskResult, TaskStatus
from asynk.transport import Delivery

EVENTS = [
    "before_startup",
    "after_startup",
    "before_shutdown",
    "after_shutdown",
    "middleware",
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
    app.after_startup(first)
    app.after_startup(second)
    assert list(app.hooks.after_startup) == [first, second]


def test_distinct_lambdas_all_register() -> None:
    app = App("t")
    first: Any = lambda *args: None  # noqa: E731
    second: Any = lambda *args: None  # noqa: E731
    app.after_startup(first)
    app.after_startup(second)
    assert list(app.hooks.after_startup) == [first, second]


class _Recorder:
    def hook(self, *args: Any) -> None: ...

    def __call__(self, *args: Any) -> None: ...


def test_bound_methods_of_different_instances_all_register() -> None:
    app = App("t")
    a, b = _Recorder(), _Recorder()
    app.after_startup(a.hook)
    app.after_startup(b.hook)
    assert list(app.hooks.after_startup) == [a.hook, b.hook]


def test_the_same_bound_method_accessed_twice_registers_once() -> None:
    app = App("t")
    a = _Recorder()
    assert a.hook is not a.hook
    app.after_startup(a.hook)
    app.after_startup(a.hook)
    assert list(app.hooks.after_startup) == [a.hook]


def test_partials_register() -> None:
    app = App("t")

    def base(*args: Any) -> None: ...

    first, second = functools.partial(base, 1), functools.partial(base, 2)
    app.after_startup(first)
    app.after_startup(second)
    app.after_startup(first)
    assert list(app.hooks.after_startup) == [first, second]


def test_callable_instances_register() -> None:
    app = App("t")
    first, second = _Recorder(), _Recorder()
    app.after_startup(first)
    app.after_startup(second)
    app.after_startup(first)
    assert list(app.hooks.after_startup) == [first, second]


def test_same_function_may_be_registered_for_different_events() -> None:
    app = App("t")

    async def hook(*args: Any) -> Any: ...

    app.before_shutdown(hook)
    app.middleware(hook)
    assert list(app.hooks.before_shutdown) == [hook]
    assert list(app.hooks.middleware) == [hook]


def test_skipped_is_a_status() -> None:
    assert TaskStatus.SKIPPED.value == "skipped"


def test_task_call_skip_builds_a_skipped_result() -> None:
    envelope = Envelope.create("t", (), {})
    call = TaskCall(envelope, cast(Delivery, object()))
    assert call.skip() == TaskResult(envelope, TaskStatus.SKIPPED)


def test_task_call_is_frozen() -> None:
    call = TaskCall(Envelope.create("t", (), {}), cast(Delivery, object()))
    with pytest.raises(dataclasses.FrozenInstanceError):
        call.envelope = Envelope.create("u", (), {})  # type: ignore[misc]
