from __future__ import annotations

import enum
import inspect
from collections.abc import Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeAlias, TypeVar

from asynk.envelope import Envelope
from asynk.transport import Delivery

if TYPE_CHECKING:
    from asynk.runner import Runner


class TaskStatus(enum.Enum):
    """Outcome of running a task, as seen by hooks and the consumer.

    Worker-internal. The public, handle-side equivalent is
    `asynk.results.TaskState`, which `TaskHandle.status` returns.
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


LifecycleHook: TypeAlias = Callable[["Runner"], Awaitable[None] | None]
"""Hook for the lifecycle events of a worker or scheduler; sync or async, receives the runner."""

BeforeTaskHook: TypeAlias = Callable[[Envelope, Delivery], Awaitable[None] | None]
"""Hook run before a task. Raise `SkipTask` to veto the task."""

AfterTaskHook: TypeAlias = Callable[[TaskResult, Delivery], Awaitable[None] | None]
"""Hook run after a task, before the delivery is settled; it may settle the delivery."""

H = TypeVar("H", bound=Callable[..., Any])


class SkipTask(Exception):
    """Raised by a `before_task` hook to veto the task.

    The task does not run and its delivery is acknowledged. `after_task` hooks
    still run, with a `TaskStatus.SKIPPED` result.
    """


class HookRegistry(Generic[H]):
    """Ordered, idempotent collection of hooks for one event.

    Hooks are keyed by `module.qualname`, so registering the same function again
    (for example because a module is imported twice) is a no-op. Iterating yields
    the hooks in registration order.
    """

    def __init__(self) -> None:
        self._hooks: dict[str, H] = {}

    def add(self, hook: H) -> H:
        """Registers a hook unless one with the same key is already registered.

        Args:
            hook: The hook to register.

        Returns:
            The hook, unchanged.
        """
        key = f"{hook.__module__}.{hook.__qualname__}"
        self._hooks.setdefault(key, hook)
        return hook

    def __iter__(self) -> Iterator[H]:
        return iter(list(self._hooks.values()))


@dataclass
class Hooks:
    """The hooks registered on an app, one registry per event."""

    before_startup: HookRegistry[LifecycleHook] = field(default_factory=HookRegistry)
    after_startup: HookRegistry[LifecycleHook] = field(default_factory=HookRegistry)
    before_shutdown: HookRegistry[LifecycleHook] = field(default_factory=HookRegistry)
    after_shutdown: HookRegistry[LifecycleHook] = field(default_factory=HookRegistry)
    before_task: HookRegistry[BeforeTaskHook] = field(default_factory=HookRegistry)
    after_task: HookRegistry[AfterTaskHook] = field(default_factory=HookRegistry)


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


class TaskFailedError(Exception):
    """Raised inside `Consumer.process` when the task did not succeed.

    Worker-internal. The public, handle-side equivalent is
    `asynk.results.TaskError`, which `TaskHandle.result` raises.

    Attributes:
        result: The failed task's result.
    """

    def __init__(self, result: TaskResult) -> None:
        super().__init__(f"task {result.envelope.task!r} ended with {result.status.name}")
        self.result = result
