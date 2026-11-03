from __future__ import annotations

import asyncio
import inspect
import logging
import signal
from collections.abc import Awaitable
from pathlib import Path
from typing import Any

from asynk.app import App, NotConfiguredError
from asynk.hooks import TaskFailedError, TaskResult, TaskStatus
from asynk.probes import ProbeFiles
from asynk.transport import Consumer, Delivery

_SHUTDOWN_SIGNALS = (signal.SIGTERM, signal.SIGINT)

logger = logging.getLogger("asynk")


class Worker:
    def __init__(
        self,
        app: App,
        *,
        concurrency: int = 10,
        shutdown_timeout: float = 30.0,
        probe_dir: Path | None = None,
        heartbeat_interval: float = 5.0,
    ) -> None:
        if app.consumer is None:
            raise NotConfiguredError(f"app {app.name!r} has no consumer configured")
        self.app = app
        self.consumer: Consumer = app.consumer
        self.concurrency = concurrency
        self.shutdown_timeout = shutdown_timeout
        self.probe_dir = probe_dir
        self.heartbeat_interval = heartbeat_interval
        self._shutdown = asyncio.Event()
        self._probes = ProbeFiles(probe_dir) if probe_dir is not None else None
        self._probes_live = False

    def request_shutdown(self) -> None:
        """Ask the run loop to stop fetching and drain; idempotent, signal-handler safe."""
        if not self._shutdown.is_set():
            logger.info("worker shutdown requested")
            self._shutdown.set()
            if self._probes is not None and self._probes_live:
                # Stop advertising readiness right away, not at the next heartbeat.
                try:
                    self._probes.remove_readiness()
                except OSError:
                    logger.exception("failed to remove readiness probe")

    async def _heartbeat(self, probes: ProbeFiles) -> None:
        while True:
            ready = self.consumer.connected and not self._shutdown.is_set()
            try:
                probes.beat(ready=ready)
            except OSError:
                logger.exception("failed to write probe files")
            await asyncio.sleep(self.heartbeat_interval)

    async def run_with_signals(self) -> None:
        """Run until SIGTERM/SIGINT; the previous signal handlers are restored on exit."""
        loop = asyncio.get_running_loop()
        previous = {sig: signal.getsignal(sig) for sig in _SHUTDOWN_SIGNALS}
        for sig in _SHUTDOWN_SIGNALS:
            loop.add_signal_handler(sig, self.request_shutdown)
        try:
            await self.run()
        finally:
            for sig, handler in previous.items():
                loop.remove_signal_handler(sig)
                signal.signal(sig, handler)

    async def run(self) -> None:
        """Consume deliveries until shutdown is requested, then drain and stop the consumer."""
        await self.consumer.start()
        semaphore = asyncio.Semaphore(self.concurrency)
        in_flight: set[asyncio.Task[None]] = set()
        deliveries = aiter(self.consumer.deliveries())
        shutdown = asyncio.ensure_future(self._shutdown.wait())
        heartbeat: asyncio.Task[None] | None = None
        if self._probes is not None:
            self._probes_live = True
            heartbeat = asyncio.create_task(self._heartbeat(self._probes))
        try:
            while not self._shutdown.is_set():
                if not await self._race(semaphore.acquire(), shutdown):
                    break
                try:
                    delivery = await self._race(anext(deliveries), shutdown)
                except StopAsyncIteration:
                    delivery = None
                if delivery is None:
                    semaphore.release()
                    break
                task = asyncio.create_task(self._handle_and_release(delivery, semaphore))
                in_flight.add(task)
                task.add_done_callback(in_flight.discard)
            await self._drain(in_flight)
        except BaseException:
            # Cancelled, or the transport failed: don't orphan handlers on a stopped consumer.
            # Their cancel path requeues unsettled deliveries.
            await self._cancel(in_flight)
            raise
        finally:
            shutdown.cancel()
            try:
                aclose = getattr(deliveries, "aclose", None)
                if aclose is not None:
                    await aclose()
                await self.consumer.stop()
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
                if self._probes is not None:
                    self._probes_live = False
                    try:
                        self._probes.clear()
                    except OSError:
                        logger.exception("failed to clear probe files")

    async def _race[T](self, aw: Awaitable[T], shutdown: asyncio.Future[Any]) -> T | None:
        """Await `aw` unless shutdown wins; a result that arrived anyway is still returned."""
        fut = asyncio.ensure_future(aw)
        try:
            await asyncio.wait({fut, shutdown}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            if not fut.done():
                fut.cancel()
                # Wait for the cancellation to land without swallowing our own.
                await asyncio.wait({fut})
        if fut.cancelled():
            return None
        return fut.result()

    async def _handle_and_release(self, delivery: Delivery, semaphore: asyncio.Semaphore) -> None:
        try:
            await self.handle(delivery)
        except Exception:
            logger.exception("unexpected error handling task %r", delivery.envelope.task)
        finally:
            semaphore.release()

    async def _drain(self, in_flight: set[asyncio.Task[None]]) -> None:
        if not in_flight:
            return
        _, pending = await asyncio.wait(set(in_flight), timeout=self.shutdown_timeout)
        if pending:
            logger.warning("cancelling %d task(s) still running at shutdown", len(pending))
            await self._cancel(pending)

    @staticmethod
    async def _cancel(tasks: set[asyncio.Task[None]]) -> None:
        tasks = set(tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def handle(self, delivery: Delivery) -> TaskResult:
        """Process exactly one delivery; task and hook errors never propagate."""
        result: TaskResult | None = None
        try:
            async with self.consumer.process(delivery):
                try:
                    result = await self._execute(delivery)
                    self._log_result(result)
                    await self._run_hook(result, delivery)
                except asyncio.CancelledError:
                    # Settle before the context manager sees the cancellation, so a
                    # shutdown-cancelled task is redelivered rather than dropped.
                    if not delivery.settled:
                        await delivery.reject(requeue=True)
                    raise
                if not delivery.settled and result.status is not TaskStatus.SUCCESS:
                    raise TaskFailedError(result)
        except TaskFailedError as exc:
            return exc.result
        assert result is not None
        return result

    @staticmethod
    def _log_result(result: TaskResult) -> None:
        name = result.envelope.task
        match result.status:
            case TaskStatus.FAILED:
                logger.error(
                    "task %r (id=%s, attempt=%d) failed",
                    name,
                    result.envelope.id,
                    result.envelope.attempt,
                    exc_info=result.error,
                )
            case TaskStatus.RETRIES_EXCEEDED:
                logger.warning(
                    "task %r (id=%s, attempt=%d) exceeded its retries",
                    name,
                    result.envelope.id,
                    result.envelope.attempt,
                    exc_info=result.error,
                )
            case TaskStatus.UNKNOWN_TASK:
                logger.warning("unknown task %r (id=%s)", name, result.envelope.id)
            case TaskStatus.SUCCESS:
                logger.debug("task %r (id=%s) succeeded", name, result.envelope.id)

    async def _execute(self, delivery: Delivery) -> TaskResult:
        envelope = delivery.envelope
        try:
            task = self.app.get_task(envelope.task)
        except KeyError:
            return TaskResult(envelope, TaskStatus.UNKNOWN_TASK)
        try:
            value: Any
            if task.is_async:
                value = await task.func(*envelope.args, **envelope.kwargs)
            else:
                value = await asyncio.to_thread(task.func, *envelope.args, **envelope.kwargs)
                if inspect.isawaitable(value):
                    # e.g. a sync wrapper or functools.partial around an async function
                    value = await value
        except Exception as exc:
            exceeded = task.max_retries is not None and envelope.attempt >= task.max_retries
            status = TaskStatus.RETRIES_EXCEEDED if exceeded else TaskStatus.FAILED
            return TaskResult(envelope, status, error=exc)
        return TaskResult(envelope, TaskStatus.SUCCESS, value=value)

    async def _run_hook(self, result: TaskResult, delivery: Delivery) -> None:
        hook = self.app.after_task_hook
        if hook is None:
            return
        try:
            await hook(result, delivery)
        except Exception:
            logger.exception("after_task hook failed for task %r", result.envelope.task)
            if not delivery.settled:
                await delivery.reject(requeue=True)
