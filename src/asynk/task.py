from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar

from asynk.envelope import Envelope

if TYPE_CHECKING:
    from asynk.app import App


P = ParamSpec("P")
R = TypeVar("R")


class Task(Generic[P, R]):
    """A registered function that can be called directly or enqueued with `delay`.

    Attributes:
        app: The app the task is registered with.
        func: The wrapped function, sync or async.
        name: Unique name used to route messages to this task.
        max_retries: Attempts after which failures stop being retried; None for no limit.
        options: Extra options passed to the `App.task` decorator.
    """

    def __init__(
        self,
        app: App,
        func: Callable[P, R],
        name: str,
        max_retries: int | None,
        options: dict[str, Any],
    ) -> None:
        self.app = app
        self.func = func
        self.name = name
        self.max_retries = max_retries
        self.options = options

    @property
    def is_async(self) -> bool:
        """Whether the wrapped function is a coroutine function."""
        return inspect.iscoroutinefunction(self.func)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Runs the wrapped function in-process."""
        return self.func(*args, **kwargs)

    async def delay(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        """Publishes a call to this task for a worker to run.

        Args:
            *args: Positional arguments for the task.
            **kwargs: Keyword arguments for the task.

        Returns:
            The producer's receipt.

        Raises:
            NotConfiguredError: If the app has no producer.
            NonJsonArgumentError: If any argument is not JSON-friendly.
        """
        from asynk.app import NotConfiguredError

        producer = self.app.producer
        if producer is None:
            raise NotConfiguredError(f"app {self.app.name!r} has no producer configured")
        envelope = Envelope.create(self.name, args, kwargs)
        return await producer.publish(envelope)
