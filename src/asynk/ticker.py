from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from asynk.app import App, NotConfiguredError
from asynk.probes import ProbeFiles
from asynk.signals import shutdown_signals
from asynk.tick import Tick

logger = logging.getLogger("asynk")

_MAX_GRACE = 60.0
_EPSILON = timedelta(microseconds=1)


class Clock(Protocol):
    """Source of wall-clock time and of (monotonic) sleeping for a `Ticker`."""

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
    """The real clock: UTC wall time, and the event loop's monotonic sleep."""

    def now(self) -> datetime:
        """Returns the current time in UTC."""
        return datetime.now(timezone.utc)

    async def sleep(self, seconds: float) -> None:
        """Waits for `seconds` using `asyncio.sleep`.

        Args:
            seconds: How long to wait.
        """
        await asyncio.sleep(seconds)


class Ticker:
    """Publishes an app's ticks when their cron slots come due.

    Fire times are decided with the wall clock, re-read after every wake, so a
    slot is never published twice by one ticker even if the clock steps back.
    Each publish carries an `idempotency_key` of `"<task name>:<slot>"` and a
    `scheduled_at`, the slot as a UTC ISO-8601 instant.

    Attributes:
        app: The app whose ticks are published.
        clock: Source of time and sleeping.
        probe_dir: Directory for the liveness probe file; None disables it.
        heartbeat_interval: Seconds between probe file updates. The main loop
            writes the probe, so a loop stuck in a hung publish stops beating
            and its liveness probe goes stale.
        grace: Seconds a slot may be late and still fire; older slots are
            skipped unless the tick has `catch_up`.
        max_sleep: Longest single sleep, in seconds, before the clock is re-read;
            sleeps are also capped at `heartbeat_interval`.
    """

    def __init__(
        self,
        app: App,
        *,
        clock: Clock | None = None,
        probe_dir: Path | None = None,
        heartbeat_interval: float = 5.0,
        grace: float = 30.0,
        max_sleep: float = 30.0,
    ) -> None:
        """Initializes the ticker.

        Args:
            app: The app to publish for; it must have a producer and ticks.
            clock: Source of time and sleeping; defaults to `SystemClock`.
            probe_dir: Directory for the liveness probe file; None disables it.
            heartbeat_interval: Seconds between probe file updates.
            grace: Seconds a slot may be late and still fire.
            max_sleep: Longest single sleep, in seconds, before the clock is re-read;
                sleeps are also capped at `heartbeat_interval`.

        Raises:
            NotConfiguredError: If the app has no producer.
            ValueError: If the app has no ticks, `heartbeat_interval` <= 0,
                `max_sleep` <= 0, or `grace` is not in [0, 60).
        """
        if app.producer is None:
            raise NotConfiguredError(f"app {app.name!r} has no producer configured")
        if not app.ticks:
            raise ValueError(f"app {app.name!r} has no ticks")
        if heartbeat_interval <= 0:
            raise ValueError(f"heartbeat_interval must be > 0, got {heartbeat_interval}")
        if max_sleep <= 0:
            raise ValueError(f"max_sleep must be > 0, got {max_sleep}")
        if not 0 <= grace < _MAX_GRACE:
            raise ValueError(f"grace must be in [0, {_MAX_GRACE:g}), got {grace}")
        self.app = app
        self.clock: Clock = clock if clock is not None else SystemClock()
        self.probe_dir = probe_dir
        self.heartbeat_interval = heartbeat_interval
        self.grace = grace
        self.max_sleep = max_sleep
        self._shutdown = asyncio.Event()
        self._probes = ProbeFiles(probe_dir) if probe_dir is not None else None
        self._last_beat = 0.0

    def request_shutdown(self) -> None:
        """Asks `run` to stop; idempotent and safe in signal handlers."""
        if not self._shutdown.is_set():
            logger.info("ticker shutdown requested")
            self._shutdown.set()

    async def run_with_signals(self) -> None:
        """Runs like `run`, with SIGTERM and SIGINT requesting a shutdown."""
        with shutdown_signals(self.request_shutdown):
            await self.run()

    async def run(self) -> None:
        """Publishes due slots until shutdown is requested.

        Nothing fires at startup: each tick's first slot is the first one after
        the current time. Shutdown interrupts the sleep and does not wait for
        slots that are not yet due.
        """
        self._beat()
        shutdown = asyncio.ensure_future(self._shutdown.wait())
        try:
            now = self._now()
            pending: dict[Tick[..., Any], datetime] = {
                tick: tick.schedule.cron.next_after(now.astimezone(tick.schedule.tz))
                for tick in self.app.ticks
            }
            logger.info("ticker started with %d tick(s)", len(pending))
            cap = min(self.max_sleep, self.heartbeat_interval)
            while not self._shutdown.is_set():
                now = self._now()
                await self._handle_due(pending, now, shutdown)
                if self._shutdown.is_set():
                    break
                earliest = min(pending.values())
                delay = min(max((earliest - now).total_seconds(), 0.0), cap)
                await self._sleep(delay, shutdown)
                self._beat()
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
        pending: dict[Tick[..., Any], datetime],
        now: datetime,
        shutdown: asyncio.Future[Any],
    ) -> None:
        grace = timedelta(seconds=self.grace)
        while not self._shutdown.is_set():
            # Oldest slot first; ties keep registration order.
            tick = min(pending, key=pending.__getitem__)
            slot = pending[tick]
            if slot > now:
                return
            if now - slot <= grace or tick.schedule.catch_up:
                if not await self._fire(tick, slot, shutdown):
                    return
                pending[tick] = tick.schedule.cron.next_after(slot)
                # Keep liveness fresh through a long catch-up burst.
                if time.monotonic() - self._last_beat >= self.heartbeat_interval:
                    self._beat()
                continue
            # `now` is UTC and `slot` is zoned, so `max` compares instants.
            resume_after = max(slot, now - grace - _EPSILON)
            pending[tick] = tick.schedule.cron.next_after(resume_after.astimezone(tick.schedule.tz))
            logger.warning(
                "tick %r: skipping missed slot(s) from %s; next slot %s",
                tick.name,
                _utc_iso(slot),
                _utc_iso(pending[tick]),
            )

    async def _fire(
        self, tick: Tick[..., Any], slot: datetime, shutdown: asyncio.Future[Any]
    ) -> bool:
        """Publishes one slot; returns False if shutdown cancelled the publish."""
        scheduled_at = _utc_iso(slot)
        schedule = tick.schedule
        publish = tick.with_metadata(
            idempotency_key=f"{tick.name}:{scheduled_at}",
            scheduled_at=scheduled_at,
        ).delay(*schedule.args, **schedule.kwargs)
        try:
            finished = await self._race(publish, shutdown)
        except Exception:
            logger.exception("failed to publish tick %r for slot %s", tick.name, scheduled_at)
            return True
        if not finished and not shutdown.done():
            # The publish cancelled itself; treat it like a failed publish.
            logger.error("publish of tick %r for slot %s was cancelled", tick.name, scheduled_at)
            return True
        if not finished:
            logger.warning(
                "shutdown cancelled the publish of tick %r for slot %s; slot not published",
                tick.name,
                scheduled_at,
            )
            return False
        logger.debug("published tick %r for slot %s", tick.name, scheduled_at)
        return True

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
        # carry each tick's zone) are between instants, never wall times.
        return self.clock.now().astimezone(timezone.utc)

    def _beat(self) -> None:
        if self._probes is None:
            return
        self._last_beat = time.monotonic()
        try:
            self._probes.beat(ready=False)
        except OSError:
            logger.exception("failed to write probe files")


def _utc_iso(slot: datetime) -> str:
    return slot.astimezone(timezone.utc).isoformat()
