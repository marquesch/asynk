import inspect
from typing import Any

import pytest

from asynk import TaskOptions
from asynk.app import App
from asynk.codec.json import JsonCodec
from asynk.results import TaskHandle, TaskState, encode_state, state_key
from asynk.retry import RetryPolicy
from asynk.store.memory import MemoryStore
from asynk.store.null import NullStore


def test_task_registers_with_default_name() -> None:
    app = App("t")

    @app.task()
    def fn() -> None: ...

    expected = f"t.{fn.func.__qualname__}"
    assert fn.name == expected
    assert app.registered_task(expected) is fn
    assert app.tasks[expected] is fn


def test_default_name_ignores_module() -> None:
    app = App("t")

    def fn() -> None: ...

    fn.__module__ = "__main__"
    task = app.task()(fn)

    assert task.name == f"t.{fn.__qualname__}"


def test_explicit_name() -> None:
    app = App("t")

    @app.task(name="x")
    def fn() -> None: ...

    assert fn.name == "x"


def test_app_has_no_task_options_by_default() -> None:
    assert App("a").options == TaskOptions()


def test_app_keeps_its_options() -> None:
    options = TaskOptions(retry=RetryPolicy(max_retries=2))
    assert App("a", options=options).options is options


def test_task_inherits_the_app_default() -> None:
    app = App("a", options=TaskOptions(retry=RetryPolicy(max_retries=5)))

    @app.task()
    async def t() -> None: ...

    assert t.retry is app.options.retry


def test_task_policy_replaces_the_app_default_entirely() -> None:
    app = App("a", options=TaskOptions(retry=RetryPolicy(max_retries=5, backoff=3.0)))

    @app.task(options=TaskOptions(retry=RetryPolicy(backoff=1.0)))
    async def t() -> None: ...

    assert t.retry == RetryPolicy(max_retries=1, backoff=1.0)


def test_task_options_without_retry_use_the_app_default() -> None:
    app = App("a", options=TaskOptions(retry=RetryPolicy(max_retries=5)))

    @app.task(options=TaskOptions())
    async def t() -> None: ...

    assert t.retry is app.options.retry


def test_retry_false_disables_retries() -> None:
    app = App("a", options=TaskOptions(retry=RetryPolicy(max_retries=5)))

    @app.task(options=TaskOptions(retry=False))
    async def t() -> None: ...

    assert t.retry is None


def test_retry_false_at_app_level_means_no_retries() -> None:
    app = App("a", options=TaskOptions(retry=False))

    @app.task()
    async def t() -> None: ...

    assert t.retry is None


@pytest.mark.parametrize("bad", [{"options": {"retry": 3}}, {"options": TaskOptions}])
def test_invalid_options_are_rejected(bad: Any) -> None:
    with pytest.raises(TypeError):
        App("a", **bad)
    app = App("a")
    with pytest.raises(TypeError):
        app.task(options=bad["options"])


def test_max_retries_and_extra_options_are_rejected() -> None:
    app = App("a")

    with pytest.raises(TypeError):
        app.task(max_retries=3)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        app.task(retry=RetryPolicy())  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        app.task(queue="q")  # type: ignore[call-arg]


def test_duplicate_name_raises_value_error() -> None:
    app = App("t")

    @app.task(name="dup")
    def a() -> None: ...

    with pytest.raises(ValueError, match="dup"):

        @app.task(name="dup")
        def b() -> None: ...


def test_unknown_task_raises_key_error() -> None:
    with pytest.raises(KeyError):
        App("t").registered_task("nope")


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


def test_store_defaults_to_null_store() -> None:
    assert isinstance(App("t").store, NullStore)


def test_store_is_kept() -> None:
    store = MemoryStore()
    assert App("t", store=store).store is store


def test_codec_defaults_to_json_codec() -> None:
    assert isinstance(App("t").codec, JsonCodec)


def test_codec_is_kept() -> None:
    codec = JsonCodec()
    assert App("t", codec=codec).codec is codec


def test_store_is_not_a_lifecycle_hook() -> None:
    app = App("t", store=MemoryStore())
    assert list(app.hooks.before_startup) == []
    assert list(app.hooks.after_shutdown) == []


async def test_get_task_returns_handle_bound_to_store_and_codec() -> None:
    store = MemoryStore()
    app = App("t", store=store)
    await store.set(state_key("abc"), encode_state(app.codec, TaskState.RUNNING))

    handle = app.get_task("abc")

    assert isinstance(handle, TaskHandle)
    assert handle.task_id == "abc"
    assert handle.receipt is None
    assert await handle.state() is TaskState.RUNNING
