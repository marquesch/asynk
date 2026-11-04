from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from types import MappingProxyType
from typing import Any, ParamSpec, TypeVar, cast

from asynk.cron import CronExpression
from asynk.envelope import JSONValue, ensure_json
from asynk.hooks import (
    AfterTaskHook,
    BeforeTaskHook,
    Hooks,
    LifecycleHook,
)
from asynk.task import Task
from asynk.tick import Schedule, Tick, resolve_timezone
from asynk.transport import Consumer, Producer

P = ParamSpec("P")
R = TypeVar("R")


class NotConfiguredError(RuntimeError):
    """Raised when an app lacks a component required by the operation."""


class App:
    """Registry of tasks plus the producer and consumer that move their messages.

    Attributes:
        name: Name of the app.
        producer: Used by `Task.delay`; None if the app only consumes.
        consumer: Used by `Worker`; None if the app only produces.
        tz: Default timezone for ticks.
        hooks: The hooks registered through the `before_*` and `after_*` decorators.
    """

    def __init__(
        self,
        name: str,
        *,
        producer: Producer | None = None,
        consumer: Consumer | None = None,
        tz: str = "UTC",
    ) -> None:
        self.name = name
        self.tz = resolve_timezone(tz, f"app {name!r}")
        self.producer = producer
        self.consumer = consumer
        self._tasks: dict[str, Task[..., Any]] = {}
        self._ticks: list[Tick[..., Any]] = []
        self.hooks = Hooks()

    @property
    def tasks(self) -> Mapping[str, Task[..., Any]]:
        return MappingProxyType(self._tasks)

    @property
    def ticks(self) -> Sequence[Tick[..., Any]]:
        """The registered ticks, in registration order."""
        return tuple(self._ticks)

    def task(
        self,
        *,
        name: str | None = None,
        max_retries: int | None = None,
        **options: Any,
    ) -> Callable[[Callable[P, R]], Task[P, R]]:
        """Decorator that registers a function as a task.

        Args:
            name: Task name; defaults to the function's module and qualified name.
            max_retries: Attempts after which failures stop being retried; None for no limit.
            **options: Extra options stored on the task.

        Returns:
            A decorator producing a `Task`.

        Raises:
            ValueError: If the name is already registered.
        """

        def decorator(func: Callable[P, R]) -> Task[P, R]:
            task_name = self._claim_name(name, func)
            task = Task(self, func, task_name, max_retries, options)
            self._tasks[task_name] = task
            return task

        return decorator

    def tick(
        self,
        *,
        cron: str,
        tz: str | None = None,
        args: Sequence[object] = (),
        kwargs: Mapping[str, object] | None = None,
        catch_up: bool = False,
        name: str | None = None,
        max_retries: int | None = None,
        **options: Any,
    ) -> Callable[[Callable[P, R]], Tick[P, R]]:
        """Decorator that registers a function as a task published on a cron schedule.

        The cron expression, arguments and (when `name` is given) timezone are
        checked immediately; the timezone is otherwise checked when decorating.

        Args:
            cron: Five-field cron expression.
            tz: IANA timezone for the expression; defaults to the app's.
            args: Positional arguments for each published call.
            kwargs: Keyword arguments for each published call.
            catch_up: Whether to publish occurrences missed during downtime.
            name: Task name; defaults to the function's module and qualified name.
            max_retries: Attempts after which failures stop being retried; None for no limit.
            **options: Extra options stored on the task.

        Returns:
            A decorator producing a `Tick`.

        Raises:
            CronError: If the cron expression is invalid.
            ValueError: If the timezone is unknown, or the name is already registered.
            NonJsonArgumentError: If `args` or `kwargs` are not JSON-friendly.
        """
        expression = CronExpression.parse(cron)
        call_args = tuple(args)
        call_kwargs = dict(kwargs or {})
        ensure_json(list(call_args), "args")
        ensure_json(call_kwargs, "kwargs")
        if tz is not None and name is not None:
            resolve_timezone(tz, f"tick {name!r}")

        def decorator(func: Callable[P, R]) -> Tick[P, R]:
            tick_name = self._claim_name(name, func)
            zone = self.tz if tz is None else resolve_timezone(tz, f"tick {tick_name!r}")
            schedule = Schedule(
                expression,
                cron,
                zone,
                cast("tuple[JSONValue, ...]", call_args),
                cast("dict[str, JSONValue]", call_kwargs),
                catch_up,
            )
            tick = Tick(self, func, tick_name, max_retries, options, schedule)
            self._tasks[tick_name] = tick
            self._ticks.append(tick)
            return tick

        return decorator

    def _claim_name(self, name: str | None, func: Callable[..., Any]) -> str:
        task_name = name or f"{func.__module__}.{func.__qualname__}"
        if task_name in self._tasks:
            raise ValueError(f"task {task_name!r} is already registered")
        return task_name

    def get_task(self, name: str) -> Task[..., Any]:
        """Looks up a registered task.

        Args:
            name: The task name.

        Returns:
            The task.

        Raises:
            KeyError: If no task has that name.
        """
        return self._tasks[name]

    def before_startup(self, hook: LifecycleHook) -> LifecycleHook:
        """Registers a hook run before the worker starts its consumer.

        Hooks may be sync or async and receive the `Worker`. Registering the same
        function twice has no effect. A hook that raises stops `Worker.run`.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.before_startup.add(hook)

    def after_startup(self, hook: LifecycleHook) -> LifecycleHook:
        """Registers a hook run once the consumer has started.

        Same rules as `before_startup`.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.after_startup.add(hook)

    def before_shutdown(self, hook: LifecycleHook) -> LifecycleHook:
        """Registers a hook run when the worker stops fetching, before it drains tasks.

        Same rules as `before_startup`. If a hook raises, the worker still drains
        and cleans up, then `Worker.run` raises.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.before_shutdown.add(hook)

    def after_shutdown(self, hook: LifecycleHook) -> LifecycleHook:
        """Registers a hook run after tasks are drained and the consumer has stopped.

        Same rules as `before_startup`.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.after_shutdown.add(hook)

    def before_task(self, hook: BeforeTaskHook) -> BeforeTaskHook:
        """Registers a hook run before each known task.

        Hooks may be sync or async and receive the `Envelope` and `Delivery`.
        Raise `SkipTask` to veto the task. Any other exception is treated as a
        hook failure: the message is redelivered with its attempt count
        incremented, up to the task's `max_retries`. Registering the same function
        twice has no effect.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.before_task.add(hook)

    def after_task(self, hook: AfterTaskHook) -> AfterTaskHook:
        """Registers a hook run after each task, before its delivery is settled.

        Hooks may be sync or async and receive the `TaskResult` and `Delivery`, and
        may settle the delivery. They also run for unknown and skipped tasks.
        Failures are handled as for `before_task`.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.after_task.add(hook)
