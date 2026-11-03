from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

from asynk.task import Task
from asynk.transport import Consumer, Producer

if TYPE_CHECKING:
    from asynk.hooks import AfterTaskHook


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
        after_task_hook: Hook set through `after_task`.
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
        self.after_task_hook: AfterTaskHook | None = None

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

    def after_task(self, hook: AfterTaskHook) -> AfterTaskHook:
        """Registers the hook run after every task; usable as a decorator.

        Args:
            hook: The async hook, replacing any previous one.

        Returns:
            The hook, unchanged.
        """
        self.after_task_hook = hook
        return hook
