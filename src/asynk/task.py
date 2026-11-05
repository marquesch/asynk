from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar

from asynk.envelope import Envelope

if TYPE_CHECKING:
    from asynk.app import App
    from asynk.results import TaskHandle

logger = logging.getLogger("asynk")


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

    def with_metadata(self, /, **metadata: object) -> BoundTask[P, R]:
        """Returns a publisher that attaches metadata to the envelopes it sends.

        Args:
            **metadata: Data carried alongside the call.

        Returns:
            A `BoundTask` carrying the metadata; this task is left unchanged.
        """
        return BoundTask(self, dict(metadata))

    async def delay(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle:
        """Publishes a call to this task for a worker to run.

        Args:
            *args: Positional arguments for the task.
            **kwargs: Keyword arguments for the task.

        Returns:
            A `TaskHandle` whose `task_id` is the envelope id and whose `receipt` is
            the producer's receipt.

        Raises:
            NotConfiguredError: If the app has no producer.
            EncodeError: If the producer's codec cannot encode an argument.
        """
        return await self.with_metadata().delay(*args, **kwargs)


class BoundTask(Generic[P, R]):
    """An immutable view of a task that publishes with fixed metadata."""

    def __init__(self, task: Task[P, R], metadata: dict[str, object]) -> None:
        self._task = task
        self._metadata = metadata

    def with_metadata(self, /, **metadata: object) -> BoundTask[P, R]:
        """Returns a new `BoundTask` with more metadata; later keys override earlier ones.

        Args:
            **metadata: More data carried alongside the call.

        Returns:
            A new `BoundTask`; this one is left unchanged.
        """
        return BoundTask(self._task, {**self._metadata, **metadata})

    async def delay(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle:
        """Publishes a call to the task with this object's metadata.

        Args:
            *args: Positional arguments for the task.
            **kwargs: Keyword arguments for the task.

        Returns:
            A `TaskHandle` whose `task_id` is the envelope id and whose `receipt` is
            the producer's receipt.

        Raises:
            NotConfiguredError: If the app has no producer.
            EncodeError: If the producer's codec cannot encode an argument or a
                metadata value.
        """
        from asynk.app import NotConfiguredError
        from asynk.results import (
            TaskHandle,
            TaskState,
            encode_status,
            start_store,
            status_key,
        )

        app = self._task.app
        producer = app.producer
        if producer is None:
            raise NotConfiguredError(f"app {app.name!r} has no producer configured")
        envelope = Envelope.create(self._task.name, args, kwargs, self._metadata)
        if app.store is not None:
            try:
                store = await start_store(app)
                await store.set(status_key(envelope.id), encode_status(TaskState.PENDING))
            except Exception:
                logger.exception("could not record pending state for task %s", envelope.id)
        receipt = await producer.publish(envelope)
        return TaskHandle(app, envelope.id, receipt)
