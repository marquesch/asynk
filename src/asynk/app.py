from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

from asynk.errors import NotConfiguredError as NotConfiguredError
from asynk.hooks import (
    AfterTaskHook,
    BeforeTaskHook,
    Hooks,
    LifecycleHook,
)
from asynk.scheduled_task import Schedule, ScheduledTask, resolve_timezone
from asynk.store import Store
from asynk.task import Task
from asynk.transport import Consumer, Producer

if TYPE_CHECKING:
    from asynk.results import TaskHandle

P = ParamSpec("P")
R = TypeVar("R")


class App:
    """Registry of tasks plus the producer and consumer that move their messages.

    Attributes:
        name: Name of the app.
        producer: Used by `Task.delay`; None if the app only consumes.
        consumer: Used by `Worker`; None if the app only produces.
        store: Optional key-value store for task results and other state; None if unused.
            It is started lazily, once, and shared by task handles, the worker and
            the scheduler, so use one app and store from one event loop. Reusing one
            `App` across separate `asyncio.run` calls is not supported when the
            store is shared.
        tz: Default timezone for scheduled tasks.
        hooks: The hooks registered through the `before_*` and `after_*` decorators.
    """

    def __init__(
        self,
        name: str,
        *,
        producer: Producer | None = None,
        consumer: Consumer | None = None,
        store: Store | None = None,
        tz: str = "UTC",
    ) -> None:
        self.name = name
        self.tz = resolve_timezone(tz, f"app {name!r}")
        self.producer = producer
        self.consumer = consumer
        self.store = store
        self._store_started = False
        self._store_lock_obj: asyncio.Lock | None = None
        self._tasks: dict[str, Task[..., Any]] = {}
        self._scheduled_tasks: list[ScheduledTask[..., Any]] = []
        self.hooks = Hooks()

    def _store_lock(self) -> asyncio.Lock:
        if self._store_lock_obj is None:
            self._store_lock_obj = asyncio.Lock()
        return self._store_lock_obj

    @property
    def tasks(self) -> Mapping[str, Task[..., Any]]:
        return MappingProxyType(self._tasks)

    @property
    def scheduled_tasks(self) -> Sequence[ScheduledTask[..., Any]]:
        """The registered scheduled tasks, in registration order."""
        return tuple(self._scheduled_tasks)

    def task(
        self,
        *,
        name: str | None = None,
        max_retries: int | None = None,
        **options: Any,
    ) -> Callable[[Callable[P, R]], Task[P, R]]:
        """Decorator that registers a function as a task.

        Args:
            name: Task name; defaults to the app name and the function's qualified name.
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

    def scheduled_task(
        self,
        *,
        cron: str,
        tz: str | None = None,
        args: Sequence[object] = (),
        kwargs: Mapping[str, object] | None = None,
        name: str | None = None,
        max_retries: int | None = None,
        **options: Any,
    ) -> Callable[[Callable[P, R]], ScheduledTask[P, R]]:
        """Decorator that registers a function as a task published on a cron schedule.

        The cron expression and (when `name` is given) timezone are
        checked immediately; the timezone is otherwise checked when decorating. The
        arguments are checked by the producer's codec when a `Scheduler` is created.

        Args:
            cron: Five-field cron expression.
            tz: IANA timezone for the expression; defaults to the app's.
            args: Positional arguments for each published call.
            kwargs: Keyword arguments for each published call.
            name: Task name; defaults to the app name and the function's qualified name.
            max_retries: Attempts after which failures stop being retried; None for no limit.
            **options: Extra options stored on the task.

        Returns:
            A decorator producing a `ScheduledTask`.

        Raises:
            CronError: If the cron expression is invalid.
            ValueError: If the timezone is unknown, or the name is already registered.
        """
        # Built now so a bad expression fails at the call.
        schedule = Schedule.build(cron, tz=self.tz, args=args, kwargs=kwargs)
        if tz is not None and name is not None:
            resolve_timezone(tz, f"scheduled task {name!r}")

        def decorator(func: Callable[P, R]) -> ScheduledTask[P, R]:
            task_name = self._claim_name(name, func)
            zone = self.tz if tz is None else resolve_timezone(tz, f"scheduled task {task_name!r}")
            task = ScheduledTask(
                self, func, task_name, max_retries, options, replace(schedule, tz=zone)
            )
            self._tasks[task_name] = task
            self._scheduled_tasks.append(task)
            return task

        return decorator

    def _claim_name(self, name: str | None, func: Callable[..., Any]) -> str:
        task_name = name or f"{self.name}.{func.__qualname__}"
        if task_name in self._tasks:
            raise ValueError(f"task {task_name!r} is already registered")
        return task_name

    def registered_task(self, name: str) -> Task[..., Any]:
        """Looks up a registered task.

        Args:
            name: The task name.

        Returns:
            The task.

        Raises:
            KeyError: If no task has that name.
        """
        return self._tasks[name]

    def get_task(self, task_id: str) -> TaskHandle:
        """Returns a handle for reading the status and result of a published task.

        No store lookup happens here; the handle reads the store when used.

        Args:
            task_id: The task's id (`Envelope.id`).

        Returns:
            A `TaskHandle`.
        """
        from asynk.results import TaskHandle

        return TaskHandle(self, task_id)

    def before_startup(self, hook: LifecycleHook) -> LifecycleHook:
        """Registers a hook run before a worker starts its consumer or a scheduler its loop.

        Lifecycle hooks run in every asynk process, worker and scheduler alike.
        They may be sync or async and receive the `Runner` (the `Worker` or
        `Scheduler`); `runner.role` is "worker" or "scheduler". Registering the
        same function twice has no effect. A hook that raises stops `Worker.run`
        or `Scheduler.run`.

        Example:
            Initialize telemetry in every process, but open a database pool only
            in workers::

                @app.before_startup
                async def startup(runner: Runner) -> None:
                    init_sentry()
                    if runner.role == "worker":
                        await database.connect()

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.before_startup.add(hook)

    def after_startup(self, hook: LifecycleHook) -> LifecycleHook:
        """Registers a hook run once the consumer (worker) or the scheduler loop has started.

        Same rules as `before_startup`.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.after_startup.add(hook)

    def before_shutdown(self, hook: LifecycleHook) -> LifecycleHook:
        """Registers a hook run when a worker stops fetching (before draining) or a scheduler stops.

        Same rules as `before_startup`. If a hook raises, the worker still drains
        and cleans up, then `Worker.run` raises; the scheduler cleans up and
        `Scheduler.run` raises.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.before_shutdown.add(hook)

    def after_shutdown(self, hook: LifecycleHook) -> LifecycleHook:
        """Registers a hook run last: after a worker drained and stopped, or a scheduler stopped.

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
