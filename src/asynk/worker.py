"""The `Worker`, which consumes deliveries and runs tasks."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from pathlib import Path
from typing import ClassVar

from asynk.app import App
from asynk.envelope import Envelope
from asynk.errors import (
    DecodeError,
    InvalidEnvelopeError,
    NotConfiguredError,
    TaskTimeoutError,
)
from asynk.hooks import (
    Middleware,
    Next,
    TaskCall,
    TaskResult,
    TaskStatus,
    run_hooks,
)
from asynk.recorder import ResultRecorder
from asynk.results import TaskState
from asynk.retry import RetryPolicy, retry_delay, retry_status
from asynk.runner import Role, Runner
from asynk.transport import Consumer, Delivery, PublishOptions

logger = logging.getLogger("asynk")


def _link(layer: Middleware, inner: Next) -> Next:
    return lambda call: layer(call, inner)


class Worker(Runner):
    """Runs an app's tasks, consuming deliveries with bounded concurrency.

    `run` serves until shutdown is requested or deliveries end. In-flight tasks
    are given `shutdown_timeout` seconds to finish; stragglers are cancelled and
    their deliveries requeued.

    Attributes:
        app: The app to serve.
        consumer: The app's consumer.
        concurrency: Maximum tasks running at once.
        shutdown_timeout: Seconds to let running tasks finish after shutdown is requested.
        probe_dir: Directory for probe files; None disables them.
        heartbeat_interval: Seconds between probe file updates.
        role: Always "worker".
    """

    role: ClassVar[Role] = "worker"

    def __init__(
        self,
        app: App,
        *,
        concurrency: int = 10,
        shutdown_timeout: float = 10.0,
        probe_dir: Path | None = None,
        heartbeat_interval: float = 5.0,
    ) -> None:
        """Initializes the worker.

        Args:
            app: The app to serve; it must have a consumer.
            concurrency: Maximum tasks running at once.
            shutdown_timeout: Seconds to let running tasks finish after shutdown is requested.
            probe_dir: Directory for probe files; None disables them.
            heartbeat_interval: Seconds between probe file updates.

        Raises:
            ValueError: If `concurrency` < 1, `heartbeat_interval` <= 0 or
                `shutdown_timeout` < 0.
            NotConfiguredError: If the app has no consumer.
        """
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")
        if shutdown_timeout < 0:
            raise ValueError(f"shutdown_timeout must be >= 0, got {shutdown_timeout}")
        if app.consumer is None:
            raise NotConfiguredError(f"app {app.name!r} has no consumer configured")
        super().__init__(app, probe_dir=probe_dir, heartbeat_interval=heartbeat_interval)
        self.consumer: Consumer = app.consumer
        self.concurrency = concurrency
        self.shutdown_timeout = shutdown_timeout
        self._recorder = ResultRecorder(app.store, app.codec)
        self._probes_live = False

    def _on_shutdown_requested(self) -> None:
        if self._probes is not None and self._probes_live:
            try:
                self._probes.remove_readiness()
            except OSError:
                logger.exception("failed to remove readiness probe")

    async def _heartbeat(self) -> None:
        while True:
            self._beat(ready=self.consumer.connected and not self._shutdown.is_set())
            await asyncio.sleep(self.heartbeat_interval)

    async def _start(self) -> None:
        await self.consumer.start()

    async def _serve(self) -> None:
        semaphore = asyncio.Semaphore(self.concurrency)
        in_flight: set[asyncio.Task[None]] = set()
        deliveries = aiter(self.consumer.deliveries())
        heartbeat: asyncio.Task[None] | None = None
        if self._probes is not None:
            self._probes_live = True
            heartbeat = asyncio.create_task(self._heartbeat())
        try:
            await run_hooks(self.app.hooks.after_startup, self)
            await self._consume(deliveries, semaphore, in_flight)
            try:
                await run_hooks(self.app.hooks.before_shutdown, self)
            finally:
                await self._drain(in_flight)
        except BaseException:
            await self._cancel(in_flight)
            raise
        finally:
            try:
                aclose = getattr(deliveries, "aclose", None)
                if aclose is not None:
                    await aclose()
                await self.consumer.stop()
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
                self._probes_live = False
                self._clear_probes()

    async def _consume(
        self,
        deliveries: AsyncIterator[Delivery],
        semaphore: asyncio.Semaphore,
        in_flight: set[asyncio.Task[None]],
    ) -> None:
        while not self._shutdown.is_set():
            delivery = await self._next_delivery(deliveries, semaphore)
            if delivery is None:
                return
            task = asyncio.create_task(self._handle_and_release(delivery, semaphore))
            in_flight.add(task)
            task.add_done_callback(in_flight.discard)

    async def _next_delivery(
        self, deliveries: AsyncIterator[Delivery], semaphore: asyncio.Semaphore
    ) -> Delivery | None:
        """Returns the next delivery holding a concurrency slot, or None if the loop should end."""
        acquired, _ = await self._race(semaphore.acquire())
        if not acquired:
            return None
        try:
            fetched, delivery = await self._race(anext(deliveries))
        except StopAsyncIteration:
            fetched, delivery = False, None
        if not fetched:
            semaphore.release()
        return delivery

    async def _handle_and_release(self, delivery: Delivery, semaphore: asyncio.Semaphore) -> None:
        self.app.telemetry.task_started()
        try:
            await self.handle(delivery)
        except Exception:
            logger.exception("unexpected error handling a delivery")
            if not delivery.settled:
                try:
                    await delivery.reject()
                except Exception:
                    logger.exception("failed to reject a delivery after an unexpected error")
        finally:
            self.app.telemetry.task_finished()
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

    async def handle(self, delivery: Delivery) -> TaskResult | None:
        """Handles one delivery: runs its task and records the outcome.

        Args:
            delivery: The delivery to handle.

        Returns:
            The task's result, or None if the body is not a valid envelope.
        """
        try:
            envelope = Envelope.from_dict(self.app.codec.decode(delivery.body))
        except (DecodeError, InvalidEnvelopeError) as exc:
            logger.warning("rejecting an undecodable or invalid message: %s", exc)
            await delivery.reject()
            return None
        try:
            call = TaskCall(envelope, delivery)

            outcome: TaskResult | None = None

            async def run_task(call: TaskCall) -> TaskResult:
                nonlocal outcome
                outcome = await self._execute(call)
                return outcome

            chain: Next = run_task
            for layer in self.app.hooks.middleware:
                chain = _link(layer, chain)
            if self.app.telemetry.layer is not None:
                chain = _link(self.app.telemetry.layer, chain)
            try:
                result = await chain(call)
                self._log_result(result)
            except Exception as exc:
                if outcome is None:
                    result = self._on_middleware_failure(envelope, delivery, exc)
                else:
                    logger.error(
                        "middleware failed after task %r (id=%s) ran",
                        envelope.task,
                        envelope.id,
                        exc_info=exc,
                    )
                    result = outcome
                    self._log_result(result)
            await self._recorder.record(result)
            await self._settle(delivery, envelope, result)
        except asyncio.CancelledError:
            if not delivery.settled:
                await delivery.reject(requeue=True)
            raise
        return result

    async def _settle(self, delivery: Delivery, envelope: Envelope, result: TaskResult) -> None:
        if delivery.settled:  # a middleware layer may have settled it
            return
        match result.status:
            case TaskStatus.SUCCESS | TaskStatus.SKIPPED:
                await delivery.ack()
            case TaskStatus.RETRYING:
                await self._publish_retry(envelope, delivery)
            case _:
                await delivery.reject()

    def _policy(self, task_name: str) -> RetryPolicy | None:
        task = self.app.tasks.get(task_name)
        return task.retry if task is not None else self.app.options.policy

    def _on_middleware_failure(
        self, envelope: Envelope, delivery: Delivery, exc: Exception
    ) -> TaskResult:
        logger.error(
            "middleware failed for task %r (id=%s, attempt=%d)",
            envelope.task,
            envelope.id,
            envelope.attempt,
            exc_info=exc,
        )
        if delivery.settled:
            status = TaskStatus.FAILED
        else:
            status = retry_status(self._policy(envelope.task), envelope.attempt, exc)
        return TaskResult(envelope, status, error=exc)

    async def _publish_retry(self, envelope: Envelope, delivery: Delivery) -> None:
        if self.app.producer is None:
            logger.error(
                "cannot publish a retry of task %r (id=%s): the app has no producer; requeueing",
                envelope.task,
                envelope.id,
            )
            await delivery.reject(requeue=True)
            return
        try:
            retry = envelope.retry()
            delay = retry_delay(self._policy(envelope.task), envelope.attempt)
            options = None if delay is None else PublishOptions(delay=delay)
            await self.app.send(retry, options)
        except Exception:
            logger.exception(
                "failed to publish a retry of task %r (id=%s); requeueing",
                envelope.task,
                envelope.id,
            )
            await delivery.reject(requeue=True)
            return
        self.app.telemetry.retry_published(envelope)
        await delivery.ack()
        logger.info(
            "published retry of task %r (id=%s, attempt=%d, delay=%s)",
            envelope.task,
            envelope.id,
            retry.attempt,
            delay,
        )

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
            case TaskStatus.RETRYING:
                logger.warning(
                    "task %r (id=%s, attempt=%d) failed; retrying",
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
            case TaskStatus.SKIPPED:
                logger.info("task %r (id=%s) skipped by middleware", name, result.envelope.id)
            case TaskStatus.SUCCESS:
                logger.debug("task %r (id=%s) succeeded", name, result.envelope.id)

    async def _execute(self, call: TaskCall) -> TaskResult:
        envelope = call.envelope
        try:
            task = self.app.registered_task(envelope.task)
        except KeyError:
            return TaskResult(envelope, TaskStatus.UNKNOWN_TASK)
        await self._recorder.record_state(envelope.id, TaskState.RUNNING)
        try:
            async with asyncio.timeout(task.timeout) as limit:
                value = await task.func(*envelope.args, **envelope.kwargs)
        except Exception as exc:
            if isinstance(exc, TimeoutError) and limit.expired():
                exc = TaskTimeoutError(f"task {envelope.task!r} exceeded {task.timeout} s")
            return TaskResult(envelope, retry_status(task.retry, envelope.attempt, exc), error=exc)
        return TaskResult(envelope, TaskStatus.SUCCESS, value=value)
