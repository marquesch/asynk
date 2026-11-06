"""Lifecycle hooks, task middleware types, and task results."""

from __future__ import annotations

import enum
import inspect
import logging
from collections.abc import Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeAlias, TypeVar

from asynk.envelope import Envelope
from asynk.transport import Delivery

if TYPE_CHECKING:
    from asynk.runner import Runner

logger = logging.getLogger("asynk")


class TaskStatus(enum.Enum):
    """Outcome of running a task, as seen by middleware and the consumer.

    Attributes:
        SUCCESS: The task returned normally.
        FAILED: The task raised and its delivery was requeued or republished for a retry.
        RETRIES_EXCEEDED: The task raised on its last allowed attempt; the delivery is rejected.
        UNKNOWN_TASK: The envelope names a task this app has not registered.
        SKIPPED: Middleware skipped the task, so it did not run.
    """

    SUCCESS = "success"
    FAILED = "failed"
    RETRIES_EXCEEDED = "retries_exceeded"
    UNKNOWN_TASK = "unknown_task"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class TaskResult:
    """Result of handling one delivery.

    Attributes:
        envelope: The envelope that was handled.
        status: How the run ended.
        value: The task's return value on success.
        error: The raised exception on failure.
    """

    envelope: Envelope
    status: TaskStatus
    value: Any = None
    error: BaseException | None = None


@dataclass(frozen=True)
class TaskCall:
    """One task about to run, as passed through the middleware chain.

    Attributes:
        envelope: The envelope of the message being handled.
        delivery: The delivery the envelope arrived on.
    """

    envelope: Envelope
    delivery: Delivery

    def skip(self) -> TaskResult:
        """Builds the `SKIPPED` result.

        Return it without calling `next` to veto the task.

        Returns:
            A `TaskResult` with status `TaskStatus.SKIPPED`.
        """
        return TaskResult(self.envelope, TaskStatus.SKIPPED)


Next: TypeAlias = Callable[[TaskCall], Awaitable[TaskResult]]
"""Calls the rest of the middleware chain, ending with the task."""

Middleware: TypeAlias = Callable[[TaskCall, Next], Awaitable[TaskResult]]
"""Async layer around a task: receives the call and `next`, returns the result."""

LifecycleHook: TypeAlias = Callable[["Runner"], Awaitable[None] | None]
"""Hook for the lifecycle events of a worker or scheduler; sync or async, receives the runner."""

H = TypeVar("H", bound=Callable[..., Any])


class HookRegistry(Generic[H]):
    """Ordered collection of hooks for one event.

    Registering the same hook again, or an equal one such as the same bound method,
    is a no-op. Iterating yields the hooks in registration order.
    """

    def __init__(self) -> None:
        self._hooks: list[H] = []

    def add(self, hook: H) -> H:
        """Registers a hook unless it is already registered.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        if not any(hook is existing or hook == existing for existing in self._hooks):
            self._hooks.append(hook)
        return hook

    def __iter__(self) -> Iterator[H]:
        """Iterates over the hooks in registration order."""
        return iter(list(self._hooks))


@dataclass
class Hooks:
    """The hooks registered on an app, one registry per event.

    Attributes:
        before_startup: Hooks run before a worker or scheduler starts.
        after_startup: Hooks run once a worker or scheduler has started.
        before_shutdown: Hooks run when a worker or scheduler begins to shut down.
        after_shutdown: Hooks run after a worker or scheduler has stopped.
        middleware: Layers run around each task.
    """

    before_startup: HookRegistry[LifecycleHook] = field(default_factory=HookRegistry)
    after_startup: HookRegistry[LifecycleHook] = field(default_factory=HookRegistry)
    before_shutdown: HookRegistry[LifecycleHook] = field(default_factory=HookRegistry)
    after_shutdown: HookRegistry[LifecycleHook] = field(default_factory=HookRegistry)
    middleware: HookRegistry[Middleware] = field(default_factory=HookRegistry)


async def run_hooks(hooks: Iterable[Callable[..., Any]], *args: Any) -> None:
    """Calls hooks in order, awaiting the async ones; the first exception propagates.

    Args:
        hooks: The hooks to call.
        *args: Arguments passed to every hook.
    """
    for hook in hooks:
        result = hook(*args)
        if inspect.isawaitable(result):
            await result


async def run_all_hooks(hooks: Iterable[Callable[..., Any]], *args: Any) -> None:
    """Calls every hook in order, even after one raises.

    Each failure is logged. Only `Exception` is collected; cancellation propagates at once.

    Args:
        hooks: The hooks to call.
        *args: Arguments passed to every hook.

    Raises:
        Exception: The failure itself, when exactly one hook raised.
        ExceptionGroup: Every failure in hook order, when several hooks raised.
    """
    errors: list[Exception] = []
    for hook in hooks:
        try:
            result = hook(*args)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            logger.exception("hook %s failed", getattr(hook, "__qualname__", repr(hook)))
            errors.append(exc)
    if len(errors) == 1:
        raise errors[0]
    if errors:
        raise ExceptionGroup(f"{len(errors)} hooks failed", errors)
