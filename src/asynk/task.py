from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from asynk.envelope import Envelope

if TYPE_CHECKING:
    from asynk.app import App


class Task[**P, R]:
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
        return inspect.iscoroutinefunction(self.func)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.func(*args, **kwargs)

    async def delay(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        from asynk.app import NotConfiguredError

        producer = self.app.producer
        if producer is None:
            raise NotConfiguredError(f"app {self.app.name!r} has no producer configured")
        envelope = Envelope.create(self.name, args, kwargs)
        return await producer.publish(envelope)
