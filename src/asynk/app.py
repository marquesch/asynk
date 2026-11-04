from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any, ParamSpec, TypeVar

from asynk.hooks import (
    AfterTaskHook,
    BeforeTaskHook,
    Hooks,
    LifecycleHook,
)
from asynk.task import Task
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
        hooks: The hooks registered through the `before_*` and `after_*` decorators.
    """

    def __init__(
        self,
        name: str,
        *,
        producer: Producer | None = None,
        consumer: Consumer | None = None,
    ) -> None:
        self.name = name
        self.producer = producer
        self.consumer = consumer
        self._tasks: dict[str, Task[..., Any]] = {}
        self.hooks = Hooks()

    @property
    def tasks(self) -> Mapping[str, Task[..., Any]]:
        return MappingProxyType(self._tasks)

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
            task_name = name or f"{func.__module__}.{func.__qualname__}"
            if task_name in self._tasks:
                raise ValueError(f"task {task_name!r} is already registered")
            task = Task(self, func, task_name, max_retries, options)
            self._tasks[task_name] = task
            return task

        return decorator

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
