"""The `Scheduler`, which publishes scheduled tasks on their cron slots."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar, Protocol

from asynk.app import App
from asynk.envelope import Envelope
from asynk.errors import NotConfiguredError
from asynk.hooks import run_hooks
from asynk.probes import ProbeFiles
from asynk.runner import Role, Runner
from asynk.scheduled_task import ScheduledTask
from asynk.signals import shutdown_signals

logger = logging.getLogger("asynk")

_MAX_GRACE = 60.0
_EPSILON = timedelta(microseconds=1)


class Clock(Protocol):
    """Source of wall-clock time and of (monotonic) sleeping for a `Scheduler`."""

    def now(self) -> datetime:
        """Returns the current wall-clock time as an aware datetime."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Waits for `seconds` of monotonic time.

        Args:
            seconds: How long to wait.
        """
        ...


class SystemClock:
    """The real clock: UTC wall time and `asyncio.sleep`."""

    def now(self) -> datetime:
        """Returns the current time in UTC."""
        return datetime.now(UTC)

    async def sleep(self, seconds: float) -> None:
        """Waits for `seconds` using `asyncio.sleep`.

        Args:
            seconds: How long to wait.
        """
        await asyncio.sleep(seconds)


class Scheduler(Runner):
    """Publishes an app's scheduled tasks when their cron slots come due.

    `run` publishes due slots until shutdown is requested. Each scheduled task
    starts at its first slot after the current time.

    Each publish carries the metadata `idempotency_key`, `"<task name>:<slot>"`,
    and `scheduled_at`, the slot as a UTC ISO-8601 instant.

    Attributes:
        app: The app whose scheduled tasks are published.
        clock: Source of time and sleeping.
        probe_dir: Directory for the liveness probe file; None disables it.
        heartbeat_interval: Seconds between probe file updates.
        grace: Seconds a slot may be late and still fire.
        role: Always "scheduler".
    """

    role: ClassVar[Role] = "scheduler"

    def __init__(
        self,
        app: App,
        *,
        clock: Clock | None = None,
        probe_dir: Path | None = None,
        heartbeat_interval: float = 5.0,
        grace: float = 30.0,
    ) -> None:
        """Initializes the scheduler.

        Args:
            app: The app to publish for; it must have a producer and scheduled tasks.
            clock: Source of time and sleeping; defaults to `SystemClock`.
            probe_dir: Directory for the liveness probe file; None disables it.
            heartbeat_interval: Seconds between probe file updates.
            grace: Seconds a slot may be late and still fire.

        Raises:
            NotConfiguredError: If the app has no producer.
            EncodeError: If the app's codec cannot encode a scheduled task's arguments.
            ValueError: If the app has no scheduled tasks, `heartbeat_interval` <= 0, or
                `grace` is not in [0, 60).
        """
        if app.producer is None:
            raise NotConfiguredError(f"app {app.name!r} has no producer configured")
        if not app.scheduled_tasks:
            raise ValueError(f"app {app.name!r} has no scheduled tasks")
        if heartbeat_interval <= 0:
            raise ValueError(f"heartbeat_interval must be > 0, got {heartbeat_interval}")
        if not 0 <= grace < _MAX_GRACE:
            raise ValueError(f"grace must be in [0, {_MAX_GRACE:g}), got {grace}")
        for task in app.scheduled_tasks:
            app.codec.validate(
                Envelope.create(task.name, task.schedule.args, task.schedule.kwargs).to_dict()
            )
        self.app = app
        self.clock: Clock = clock if clock is not None else SystemClock()
        self.probe_dir = probe_dir
        self.heartbeat_interval = heartbeat_interval
        self.grace = grace
        self._shutdown = asyncio.Event()
        self._probes = ProbeFiles(probe_dir) if probe_dir is not None else None

    def request_shutdown(self) -> None:
        """Asks `run` to stop; idempotent and safe in signal handlers."""
        if not self._shutdown.is_set():
            logger.info("scheduler shutdown requested")
            self._shutdown.set()

    async def run_with_signals(self) -> None:
        """Runs like `run`, with SIGTERM and SIGINT requesting a shutdown."""
        with shutdown_signals(self.request_shutdown):
            await self.run()

    async def _serve(self) -> None:
        self._beat()
        shutdown = asyncio.ensure_future(self._shutdown.wait())
        try:
            now = self._now()
            pending: dict[ScheduledTask[..., Any], datetime] = {
                task: task.schedule.cron.next_after(now.astimezone(task.schedule.tz))
                for task in self.app.scheduled_tasks
            }
            logger.info("scheduler started with %d scheduled task(s)", len(pending))
            await run_hooks(self.app.hooks.after_startup, self)
            while not self._shutdown.is_set():
                now = self._now()
                await self._handle_due(pending, now, shutdown)
                earliest = min(pending.values())
                delay = min(max((earliest - now).total_seconds(), 0.0), self.heartbeat_interval)
                await self._sleep(delay, shutdown)
                self._beat()
            await run_hooks(self.app.hooks.before_shutdown, self)
        finally:
            shutdown.cancel()
            await asyncio.gather(shutdown, return_exceptions=True)
            if self._probes is not None:
                try:
                    self._probes.clear()
                except OSError:
                    logger.exception("failed to clear probe files")

    async def _handle_due(
        self,
        pending: dict[ScheduledTask[..., Any], datetime],
        now: datetime,
        shutdown: asyncio.Future[Any],
    ) -> None:
        grace = timedelta(seconds=self.grace)
        while True:
            # Oldest slot first; ties keep registration order.
            task = min(pending, key=pending.__getitem__)
            slot = pending[task]
            if slot > now:
                return
            if now - slot <= grace:
                if not await self._fire(task, slot, shutdown):
                    return
                pending[task] = task.schedule.cron.next_after(slot)
                continue
            # `now` is UTC and `slot` is zoned, so `max` compares instants.
            resume_after = max(slot, now - grace - _EPSILON)
            pending[task] = task.schedule.cron.next_after(resume_after.astimezone(task.schedule.tz))
            logger.warning(
                "scheduled task %r: skipping missed slot(s) from %s; next slot %s",
                task.name,
                _utc_iso(slot),
                _utc_iso(pending[task]),
            )

    async def _fire(
        self, task: ScheduledTask[..., Any], slot: datetime, shutdown: asyncio.Future[Any]
    ) -> bool:
        """Claims and publishes one slot.

        The claim is released unless the publish succeeded.

        Returns:
            False if shutdown cancelled the publish; True once the slot is handled
            (published, claimed by another scheduler, or failed).
        """
        scheduled_at = _utc_iso(slot)
        idempotency_key = f"{task.name}:{scheduled_at}"
        claim_key = f"claim:{idempotency_key}"
        claim_written = False
        try:
            claimed = await self.app.store.set(claim_key, b"1")
            claim_written = claimed
        except Exception:
            # Fail open: a store outage must not stop the schedule.
            logger.exception(
                "failed to claim scheduled task %r for slot %s; publishing anyway",
                task.name,
                scheduled_at,
            )
            claimed = True
        if not claimed:
            logger.info(
                "scheduled task %r for slot %s already claimed; skipping",
                task.name,
                scheduled_at,
            )
            return True
        schedule = task.schedule
        publish = task.with_metadata(
            idempotency_key=idempotency_key,
            scheduled_at=scheduled_at,
        ).delay(*schedule.args, **schedule.kwargs)
        published = False
        try:
            try:
                finished = await self._race(publish, shutdown)
            except Exception:
                logger.exception(
                    "failed to publish scheduled task %r for slot %s", task.name, scheduled_at
                )
                return True
            if finished:
                published = True
                logger.debug("published scheduled task %r for slot %s", task.name, scheduled_at)
                return True
            if not shutdown.done():
                # The publish cancelled itself: a failed publish, so the slot advances.
                logger.error(
                    "publish of scheduled task %r for slot %s was cancelled; slot skipped",
                    task.name,
                    scheduled_at,
                )
                return True
            logger.warning(
                "shutdown cancelled the publish of scheduled task %r for slot %s; "
                "slot not published",
                task.name,
                scheduled_at,
            )
            return False
        finally:
            # Also runs when the whole `run()` task is cancelled from outside.
            if claim_written and not published:
                await self._release_claim(claim_key, task, scheduled_at)

    async def _release_claim(
        self, claim_key: str, task: ScheduledTask[..., Any], scheduled_at: str
    ) -> None:
        try:
            await self.app.store.delete(claim_key)
        except Exception:
            # Fail open, like the claim: a store outage must not stop the schedule.
            logger.exception(
                "failed to release the claim of scheduled task %r for slot %s",
                task.name,
                scheduled_at,
            )

    async def _sleep(self, seconds: float, shutdown: asyncio.Future[Any]) -> None:
        await self._race(self.clock.sleep(seconds), shutdown)

    @staticmethod
    async def _race(aw: Awaitable[Any], shutdown: asyncio.Future[Any]) -> bool:
        """Awaits `aw` unless shutdown comes first, then cancels it.

        Returns:
            True if `aw` finished (its exception, if any, is re-raised); False if
            shutdown cancelled it.
        """
        fut = asyncio.ensure_future(aw)
        try:
            await asyncio.wait({fut, shutdown}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            if not fut.done():
                fut.cancel()
                await asyncio.wait({fut})
        if fut.cancelled():
            return False
        fut.result()
        return True

    def _now(self) -> datetime:
        # Normalized to UTC so comparisons and subtractions with slots (which
        # carry each scheduled task's zone) are between instants, never wall times.
        return self.clock.now().astimezone(UTC)

    def _beat(self) -> None:
        if self._probes is None:
            return
        try:
            self._probes.beat(ready=False)
        except OSError:
            logger.exception("failed to write probe files")


def _utc_iso(slot: datetime) -> str:
    return slot.astimezone(UTC).isoformat()
