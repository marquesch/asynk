"""`Runner`, the base of `Worker` and `Scheduler`."""

from __future__ import annotations

import abc
import logging
from typing import TYPE_CHECKING, ClassVar, Literal, TypeAlias

from asynk.hooks import run_all_hooks, run_hooks

if TYPE_CHECKING:
    from asynk.app import App

logger = logging.getLogger("asynk")

Role: TypeAlias = Literal["worker", "scheduler"]
"""Kind of asynk process: "worker" or "scheduler"."""


class Runner(abc.ABC):
    """An asynk process: a `Worker` or a `Scheduler`.

    Lifecycle hooks receive the runner they run in, so one hook can serve every
    asynk process and branch on `role` where it must differ.

    Attributes:
        app: The app being served.
        role: Which kind of process this is, "worker" or "scheduler".
    """

    app: App
    role: ClassVar[Role]

    @abc.abstractmethod
    def request_shutdown(self) -> None:
        """Asks `run` to stop; idempotent and safe in signal handlers."""

    async def run(self) -> None:
        """Serves the app until shutdown is requested.

        The app's store is started before the startup hooks and stopped after the
        shutdown hooks; see `App.before_startup` and its siblings.
        """
        store = self.app.store
        await store.start()
        try:
            await run_hooks(self.app.hooks.before_startup, self)
            await self._start()
            try:
                await self._serve()
            finally:
                await run_all_hooks(self.app.hooks.after_shutdown, self)
        except BaseException:
            try:
                await store.stop()
            except Exception:
                logger.exception("failed to stop the store")
            raise
        await store.stop()

    async def _start(self) -> None:  # noqa: B027
        """Starts the runner's own resources; runs after the `before_startup` hooks."""

    @abc.abstractmethod
    async def _serve(self) -> None:
        """Runs the main loop; `after_startup`, `before_shutdown` and cleanup run inside it."""
