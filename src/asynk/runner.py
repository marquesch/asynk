"""`Runner`, the base of `Worker` and `Scheduler`."""

from __future__ import annotations

import abc
import asyncio
import logging
from collections.abc import Awaitable
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeAlias, TypeVar

from asynk.hooks import run_all_hooks, run_hooks
from asynk.probes import ProbeFiles
from asynk.signals import shutdown_signals

if TYPE_CHECKING:
    from asynk.app import App

T = TypeVar("T")

logger = logging.getLogger("asynk")

Role: TypeAlias = Literal["worker", "scheduler"]
"""Kind of asynk process: "worker" or "scheduler"."""


class Runner(abc.ABC):
    """An asynk process: a `Worker` or a `Scheduler`.

    Lifecycle hooks receive the runner they run in, so one hook can serve every
    asynk process and branch on `role` where it must differ.

    Subclasses implement `_serve` and may override `_start` and
    `_on_shutdown_requested`.

    Attributes:
        app: The app being served.
        probe_dir: Directory for probe files; None disables them.
        heartbeat_interval: Seconds between probe file updates.
        role: Which kind of process this is, "worker" or "scheduler".
    """

    role: ClassVar[Role]

    def __init__(
        self,
        app: App,
        *,
        probe_dir: Path | None = None,
        heartbeat_interval: float = 5.0,
    ) -> None:
        """Initializes the runner.

        Args:
            app: The app to serve.
            probe_dir: Directory for probe files; None disables them.
            heartbeat_interval: Seconds between probe file updates.

        Raises:
            ValueError: If `heartbeat_interval` <= 0.
        """
        if heartbeat_interval <= 0:
            raise ValueError(f"heartbeat_interval must be > 0, got {heartbeat_interval}")
        self.app = app
        self.probe_dir = probe_dir
        self.heartbeat_interval = heartbeat_interval
        self._probes = ProbeFiles(probe_dir) if probe_dir is not None else None
        self._shutdown = asyncio.Event()

    def request_shutdown(self) -> None:
        """Asks `run` to stop; idempotent and safe in signal handlers."""
        if not self._shutdown.is_set():
            logger.info("%s shutdown requested", self.role)
            self._shutdown.set()
            self._on_shutdown_requested()

    async def run_with_signals(self) -> None:
        """Runs like `run`, with SIGTERM and SIGINT requesting a graceful shutdown."""
        with shutdown_signals(self.request_shutdown):
            await self.run()

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

    def _on_shutdown_requested(self) -> None:  # noqa: B027
        """Runs once, synchronously, when shutdown is first requested."""

    def _beat(self, *, ready: bool) -> None:
        """Refreshes the probe files, logging instead of raising on `OSError`."""
        if self._probes is None:
            return
        try:
            self._probes.beat(ready=ready)
        except OSError:
            logger.exception("failed to write probe files")

    def _clear_probes(self) -> None:
        """Removes the probe files, logging instead of raising on `OSError`."""
        if self._probes is None:
            return
        try:
            self._probes.clear()
        except OSError:
            logger.exception("failed to clear probe files")

    async def _race(self, aw: Awaitable[T]) -> tuple[bool, T | None]:
        """Awaits `aw` unless shutdown is requested first, then cancels it.

        Args:
            aw: The awaitable to run.

        Returns:
            `(True, value)` if `aw` finished, `(False, None)` if shutdown won or
            `aw` cancelled itself.

        Raises:
            Exception: Whatever `aw` raised.
        """
        fut = asyncio.ensure_future(aw)
        stop: asyncio.Future[Any] = asyncio.ensure_future(self._shutdown.wait())
        try:
            await asyncio.wait({fut, stop}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            unfinished = {f for f in (fut, stop) if not f.done()}
            for f in unfinished:
                f.cancel()
            if unfinished:
                await asyncio.wait(unfinished)
        if fut.cancelled():
            return False, None
        return True, fut.result()

    @abc.abstractmethod
    async def _serve(self) -> None:
        """Runs the main loop; `after_startup`, `before_shutdown` and cleanup run inside it."""
