"""`Task` and `BoundTask`, which run and publish task calls."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, Generic, ParamSpec, TypeAlias, TypeVar

from asynk.envelope import Envelope
from asynk.results import TaskHandle
from asynk.retry import RetryPolicy
from asynk.transport import PublishOptions

P = ParamSpec("P")
R = TypeVar("R")

Publish: TypeAlias = Callable[[Envelope, PublishOptions | None], Awaitable[TaskHandle[Any]]]
"""Publishes an envelope for a worker to run and returns its handle; `App.publish`."""


class Task(Generic[P, R]):
    """A registered async function that can be called directly or enqueued with `delay`.

    Attributes:
        func: The wrapped `async def` function.
        name: Unique name used to route messages to this task.
        retry: The task's retry policy; None for no retries.
        timeout: Seconds one attempt may run; None for no limit.
    """

    def __init__(
        self,
        func: Callable[P, Awaitable[R]],
        name: str,
        retry: RetryPolicy | None,
        publish: Publish,
        *,
        timeout: float | None = None,
    ) -> None:
        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"task {getattr(func, '__qualname__', func)!r} must be an async def function "
                "(wrap blocking work with asyncio.to_thread inside the task)"
            )
        self._publish = publish
        self.func = func
        self.name = name
        self.retry = retry
        self.timeout = timeout

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
        """Calls the wrapped function in-process; await the result."""
        return self.func(*args, **kwargs)

    def with_metadata(self, /, **metadata: object) -> BoundTask[P, R]:
        """Returns a publisher that attaches metadata to the envelopes it sends.

        Args:
            **metadata: Data carried alongside the call.

        Returns:
            A `BoundTask` carrying the metadata; this task is left unchanged.
        """
        return BoundTask(self.name, self._publish, dict(metadata))

    async def delay(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle[R]:
        """Publishes a call to this task for a worker to run.

        Args:
            *args: Positional arguments for the task.
            **kwargs: Keyword arguments for the task.

        Returns:
            A `TaskHandle` whose `task_id` is the envelope id and whose `receipt` is
            the producer's receipt.

        Raises:
            NotConfiguredError: If the app has no producer.
            EncodeError: If the app's codec cannot encode an argument.
        """
        return await self.with_metadata().delay(*args, **kwargs)


class BoundTask(Generic[P, R]):
    """An immutable view of a task that publishes with fixed metadata."""

    def __init__(self, name: str, publish: Publish, metadata: dict[str, object]) -> None:
        self._name = name
        self._publish = publish
        self._metadata = metadata

    def with_metadata(self, /, **metadata: object) -> BoundTask[P, R]:
        """Returns a new `BoundTask` with more metadata; later keys override earlier ones.

        Args:
            **metadata: More data carried alongside the call.

        Returns:
            A new `BoundTask`; this one is left unchanged.
        """
        return BoundTask(self._name, self._publish, {**self._metadata, **metadata})

    async def delay(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle[R]:
        """Publishes a call to the task with this object's metadata.

        Args:
            *args: Positional arguments for the task.
            **kwargs: Keyword arguments for the task.

        Returns:
            A `TaskHandle` whose `task_id` is the envelope id and whose `receipt` is
            the producer's receipt.

        Raises:
            NotConfiguredError: If the app has no producer.
            EncodeError: If the app's codec cannot encode an argument or a
                metadata value.
        """
        envelope = Envelope.create(self._name, args, kwargs, self._metadata)
        return await self._publish(envelope, None)
