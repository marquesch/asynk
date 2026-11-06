"""The `App`: tasks, transport, store, codec and hooks."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from types import MappingProxyType
from typing import Any, Literal, ParamSpec, TypeVar

from asynk.codec import Codec
from asynk.codec.json import JsonCodec
from asynk.hooks import (
    Hooks,
    LifecycleHook,
    Middleware,
)
from asynk.results import TaskHandle
from asynk.retry import RetryPolicy, as_retry_policy
from asynk.scheduled_task import Schedule, ScheduledTask, resolve_timezone
from asynk.store import Store
from asynk.store.null import NullStore
from asynk.task import Task
from asynk.transport import Consumer, Producer

P = ParamSpec("P")
R = TypeVar("R")


class App:
    """Registry of tasks plus the producer and consumer that move their messages.

    Attributes:
        name: Name of the app.
        producer: Used by `Task.delay`; None if the app only consumes.
        consumer: Used by `Worker`; None if the app only produces.
        store: Key-value store for task states, task results and scheduler claim keys.
        codec: Codec that encodes messages, task states and results.
        tz: Default timezone for scheduled tasks.
        retry: Default retry policy of the app's tasks; None for no retries.
        hooks: The hooks and middleware registered through the `before_*`, `after_*` and
            `middleware` decorators.
    """

    def __init__(
        self,
        name: str,
        *,
        producer: Producer | None = None,
        consumer: Consumer | None = None,
        store: Store | None = None,
        codec: Codec | None = None,
        tz: str = "UTC",
        retry: RetryPolicy | Mapping[str, Any] | None = None,
    ) -> None:
        """Initializes the app.

        Args:
            name: Name of the app.
            producer: Used by `Task.delay`; None if the app only consumes.
            consumer: Used by `Worker`; None if the app only produces.
            store: Key-value store for task states, task results and scheduler
                claim keys; defaults to a `NullStore`.
            codec: Codec that encodes messages, task states and results;
                defaults to a `JsonCodec`.
            tz: Default timezone for scheduled tasks.
            retry: Default retry policy for the app's tasks, as a `RetryPolicy` or a
                dict of its fields; None for no retries.

        Raises:
            TypeError: If `retry` has an unknown field or a value of the wrong type.
            ValueError: If the timezone is unknown, or a `retry` value is out of range.
        """
        self.name = name
        self.tz = resolve_timezone(tz, f"app {name!r}")
        self.retry = None if retry is None else as_retry_policy(retry)
        self.producer = producer
        self.consumer = consumer
        self.store: Store = store if store is not None else NullStore()
        self.codec: Codec = codec if codec is not None else JsonCodec()
        self._tasks: dict[str, Task[..., Any]] = {}
        self._scheduled_tasks: list[ScheduledTask[..., Any]] = []
        self.hooks = Hooks()

    @property
    def tasks(self) -> Mapping[str, Task[..., Any]]:
        """The registered tasks, by name, as a read-only mapping."""
        return MappingProxyType(self._tasks)

    @property
    def scheduled_tasks(self) -> Sequence[ScheduledTask[..., Any]]:
        """The registered scheduled tasks, in registration order."""
        return tuple(self._scheduled_tasks)

    def task(
        self,
        *,
        name: str | None = None,
        retry: RetryPolicy | Mapping[str, Any] | Literal[False] | None = None,
    ) -> Callable[[Callable[P, R]], Task[P, R]]:
        """Decorator that registers a function as a task.

        Args:
            name: Task name; defaults to the app name and the function's qualified name.
            retry: Retry policy, as a `RetryPolicy` or a dict of its fields; replaces the
                app's default. None uses the app's default, False disables retries.

        Returns:
            A decorator producing a `Task`.

        Raises:
            TypeError: If `retry` has an unknown field or a value of the wrong type.
            ValueError: If the name is already registered, or a `retry` value is out
                of range.
        """
        policy = self._task_policy(retry)

        def decorator(func: Callable[P, R]) -> Task[P, R]:
            task_name = self._claim_name(name, func)
            task = Task(self, func, task_name, policy)
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
        retry: RetryPolicy | Mapping[str, Any] | Literal[False] | None = None,
    ) -> Callable[[Callable[P, R]], ScheduledTask[P, R]]:
        """Decorator that registers a function as a task published on a cron schedule.

        Args:
            cron: Five-field cron expression.
            tz: IANA timezone for the expression; defaults to the app's.
            args: Positional arguments for each published call.
            kwargs: Keyword arguments for each published call.
            name: Task name; defaults to the app name and the function's qualified name.
            retry: Retry policy, as a `RetryPolicy` or a dict of its fields; replaces the
                app's default. None uses the app's default, False disables retries.

        Returns:
            A decorator producing a `ScheduledTask`.

        Raises:
            CronError: If the cron expression is invalid.
            TypeError: If `retry` has an unknown field or a value of the wrong type.
            ValueError: If the timezone is unknown, the name is already registered, or
                a `retry` value is out of range.
        """
        # Built now so a bad expression fails at the call.
        schedule = Schedule.build(cron, tz=self.tz, args=args, kwargs=kwargs)
        policy = self._task_policy(retry)
        if tz is not None and name is not None:
            resolve_timezone(tz, f"scheduled task {name!r}")

        def decorator(func: Callable[P, R]) -> ScheduledTask[P, R]:
            task_name = self._claim_name(name, func)
            zone = self.tz if tz is None else resolve_timezone(tz, f"scheduled task {task_name!r}")
            task = ScheduledTask(self, func, task_name, policy, replace(schedule, tz=zone))
            self._tasks[task_name] = task
            self._scheduled_tasks.append(task)
            return task

        return decorator

    def _task_policy(
        self, retry: RetryPolicy | Mapping[str, Any] | Literal[False] | None
    ) -> RetryPolicy | None:
        if retry is None:
            return self.retry
        if retry is False:
            return None
        return as_retry_policy(retry)

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
        """Returns a handle for reading the state and result of a published task.

        Args:
            task_id: The task's id (`Envelope.id`).

        Returns:
            A `TaskHandle` reading this app's store with this app's codec.
        """
        return TaskHandle(task_id, self.store, self.codec)

    def before_startup(self, hook: LifecycleHook) -> LifecycleHook:
        """Registers a hook run after the store starts and before serving begins.

        It runs before a worker's consumer or a scheduler's loop starts. Lifecycle
        hooks may be sync or async and receive the `Runner` they run in, a `Worker`
        or a `Scheduler`; `runner.role` tells them apart. Registering the same hook
        twice has no effect.

        Example:
            Initialize telemetry in every process, but open a database pool only
            in workers:

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
        """Registers a hook run once a worker or scheduler has started.

        Same rules as `before_startup`.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.after_startup.add(hook)

    def before_shutdown(self, hook: LifecycleHook) -> LifecycleHook:
        """Registers a hook run when a worker or scheduler begins to shut down.

        A worker runs it when it stops fetching, before draining. Same rules as
        `before_startup`.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.before_shutdown.add(hook)

    def after_shutdown(self, hook: LifecycleHook) -> LifecycleHook:
        """Registers a hook run after a worker or scheduler has stopped, before the store stops.

        Same rules as `before_startup`.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        return self.hooks.after_shutdown.add(hook)

    def middleware(self, layer: Middleware) -> Middleware:
        """Registers a middleware layer around each task.

        A layer is an async function `(call: TaskCall, next: Next) -> TaskResult`.
        It receives the call and `next`, which runs the rest of the chain and the
        task, and returns a `TaskResult`: the one from `next` or another one, such
        as `call.skip()`, the `SKIPPED` result that vetoes the task. The last
        registered layer is the outermost. Registering the same layer twice has no
        effect.

        Args:
            layer: The middleware layer to register.

        Returns:
            The layer, unchanged.

        Example:
            Log how long each task takes:

                @app.middleware
                async def timed(call: TaskCall, next: Next) -> TaskResult:
                    started = time.monotonic()
                    result = await next(call)
                    logger.info("%s took %.3fs", call.envelope.task, time.monotonic() - started)
                    return result
        """
        return self.hooks.middleware.add(layer)
