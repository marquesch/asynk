from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any

from asynk.task import Task
from asynk.transport import Consumer, Producer


class NotConfiguredError(RuntimeError):
    """Raised when an app lacks a component required by the operation."""


class App:
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

    @property
    def tasks(self) -> Mapping[str, Task[..., Any]]:
        return MappingProxyType(self._tasks)

    def task[**P, R](
        self,
        *,
        name: str | None = None,
        max_retries: int | None = None,
        **options: Any,
    ) -> Callable[[Callable[P, R]], Task[P, R]]:
        def decorator(func: Callable[P, R]) -> Task[P, R]:
            task_name = name or f"{func.__module__}.{func.__qualname__}"
            if task_name in self._tasks:
                raise ValueError(f"task {task_name!r} is already registered")
            task = Task(self, func, task_name, max_retries, options)
            self._tasks[task_name] = task
            return task

        return decorator

    def get_task(self, name: str) -> Task[..., Any]:
        return self._tasks[name]
