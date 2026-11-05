from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable
from pathlib import Path
from typing import Any, ClassVar, TypeVar

from asynk.app import App, NotConfiguredError
from asynk.hooks import SkipTask, TaskFailedError, TaskResult, TaskStatus, run_hooks
from asynk.probes import ProbeFiles
from asynk.results import (
    TaskState,
    encode_result,
    encode_status,
    result_key,
    start_store,
    status_key,
    stop_store,
)
from asynk.runner import Role, Runner
from asynk.signals import shutdown_signals
from asynk.transport import Consumer, Delivery

T = TypeVar("T")

_CLEAN_STATUSES = (TaskStatus.SUCCESS, TaskStatus.SKIPPED)

_TERMINAL_STATES = {
    TaskStatus.SUCCESS: TaskState.SUCCESS,
    TaskStatus.SKIPPED: TaskState.SKIPPED,
    TaskStatus.UNKNOWN_TASK: TaskState.FAILURE,
    TaskStatus.RETRIES_EXCEEDED: TaskState.FAILURE,
}

logger = logging.getLogger("asynk")


class Worker(Runner):
    """Runs an app's tasks, consuming deliveries with bounded concurrency.

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
        shutdown_timeout: float = 30.0,
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
        if heartbeat_interval <= 0:
            raise ValueError(f"heartbeat_interval must be > 0, got {heartbeat_interval}")
        if shutdown_timeout < 0:
            raise ValueError(f"shutdown_timeout must be >= 0, got {shutdown_timeout}")
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
        """Asks `run` to stop fetching and drain; idempotent and safe in signal handlers."""
        if not self._shutdown.is_set():
            logger.info("worker shutdown requested")
            self._shutdown.set()
            if self._probes is not None and self._probes_live:
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
        """Runs like `run`, with SIGTERM and SIGINT requesting a graceful shutdown."""
        with shutdown_signals(self.request_shutdown):
            await self.run()

    async def run(self) -> None:
        """Consumes and runs tasks until shutdown is requested or deliveries end.

        In-flight tasks are given `shutdown_timeout` seconds to finish; stragglers
        are cancelled and their deliveries requeued.

        Startup and shutdown hooks run around the consumer; see `App.before_startup`
        and its siblings. A hook that raises makes this method raise.

        If the app has a store, it is started before the consumer and stopped
        after it; store errors are logged and never stop the worker.
        """
        await run_hooks(self.app.hooks.before_startup, self)
        await self._start_store()
        try:
            await self.consumer.start()
            try:
                await self._serve()
            finally:
                await run_hooks(self.app.hooks.after_shutdown, self)
        finally:
            await self._stop_store()

    async def _start_store(self) -> None:
        if self.app.store is None:
            return
        try:
            await start_store(self.app)
        except Exception:
            logger.exception("failed to start the store")

    async def _stop_store(self) -> None:
        try:
            await stop_store(self.app)
        except Exception:
            logger.exception("failed to stop the store")

    async def _serve(self) -> None:
        semaphore = asyncio.Semaphore(self.concurrency)
        in_flight: set[asyncio.Task[None]] = set()
        deliveries = aiter(self.consumer.deliveries())
        shutdown = asyncio.ensure_future(self._shutdown.wait())
        heartbeat: asyncio.Task[None] | None = None
        if self._probes is not None:
            self._probes_live = True
            heartbeat = asyncio.create_task(self._heartbeat(self._probes))
        try:
            await run_hooks(self.app.hooks.after_startup, self)
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
            try:
                await run_hooks(self.app.hooks.before_shutdown, self)
            finally:
                await self._drain(in_flight)
        except BaseException:
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

    async def _race(self, aw: Awaitable[T], shutdown: asyncio.Future[Any]) -> T | None:
        fut = asyncio.ensure_future(aw)
        try:
            await asyncio.wait({fut, shutdown}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            if not fut.done():
                fut.cancel()
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
        """Runs the task for one delivery inside the consumer's `process` context.

        `before_task` and `after_task` hooks run around the task. If a hook raises,
        the error is logged and the message is redelivered with `attempt`
        incremented (up to the task's `max_retries`), and the returned result has
        status `FAILED`.

        If the app has a store, the task's state and result are recorded there;
        store errors are logged and never affect the task or its delivery. Store
        calls have no timeout, so a store that hangs rather than raising delays the
        task's start (the `RUNNING` write); use a store with its own client timeouts.

        Args:
            delivery: The delivery to handle.

        Returns:
            The task's result.
        """
        result: TaskResult | None = None
        try:
            async with self.consumer.process(delivery):
                try:
                    try:
                        result = await self._execute(delivery)
                        self._log_result(result)
                        await run_hooks(self.app.hooks.after_task, result, delivery)
                    except Exception as exc:
                        result = await self._on_hook_failure(delivery, exc)
                    await self._record(result)
                except asyncio.CancelledError:
                    if not delivery.settled:
                        await delivery.reject(requeue=True)
                    raise
                if not delivery.settled and result.status not in _CLEAN_STATUSES:
                    raise TaskFailedError(result)
        except TaskFailedError as exc:
            return exc.result
        assert result is not None
        return result

    async def _on_hook_failure(self, delivery: Delivery, exc: Exception) -> TaskResult:
        envelope = delivery.envelope
        logger.error(
            "hook failed for task %r (id=%s, attempt=%d)",
            envelope.task,
            envelope.id,
            envelope.attempt,
            exc_info=exc,
        )
        status = TaskStatus.FAILED
        if not delivery.settled:
            task = self.app.tasks.get(envelope.task)
            max_retries = task.max_retries if task is not None else None
            if max_retries is not None and envelope.attempt >= max_retries:
                status = TaskStatus.RETRIES_EXCEEDED
                await delivery.reject()
            elif self.app.producer is None:
                await delivery.reject(requeue=True)
            else:
                try:
                    await self.app.producer.publish(envelope.retry())
                except Exception:
                    logger.exception(
                        "failed to redeliver task %r (id=%s)", envelope.task, envelope.id
                    )
                else:
                    await delivery.ack()
        return TaskResult(envelope, status, error=exc)

    async def _record_state(self, task_id: str, state: TaskState) -> None:
        if self.app.store is None:
            return
        try:
            store = await start_store(self.app)
            await store.set(status_key(task_id), encode_status(state), overwrite=True)
        except Exception:
            logger.exception("failed to record state %s for task id=%s", state.value, task_id)

    async def _record(self, result: TaskResult) -> None:
        if self.app.store is None:
            return
        envelope = result.envelope
        state = _TERMINAL_STATES.get(result.status)
        if state is None:
            await self._record_state(envelope.id, TaskState.RETRYING)
            return
        try:
            record = self._encode_outcome(result, state)
            if record is None:
                state = TaskState.FAILURE
                record = encode_result(
                    state,
                    error=(
                        "ResultNotSerializable",
                        f"task {envelope.task!r} returned a value of type "
                        f"{type(result.value).__name__!r} that cannot be stored as JSON",
                    ),
                )
            store = await start_store(self.app)
            await store.set(status_key(envelope.id), encode_status(state), overwrite=True)
            await store.set(result_key(envelope.id), record)
        except Exception:
            logger.exception(
                "failed to record result for task %r (id=%s)", envelope.task, envelope.id
            )

    @staticmethod
    def _encode_outcome(result: TaskResult, state: TaskState) -> bytes | None:
        if result.status is TaskStatus.SUCCESS:
            try:
                return encode_result(state, value=result.value)
            except (TypeError, ValueError):
                logger.warning(
                    "result of task %r (id=%s) is not JSON-serializable; recording a failure",
                    result.envelope.task,
                    result.envelope.id,
                    exc_info=True,
                )
                return None
        if result.status is TaskStatus.SKIPPED:
            return encode_result(state)
        if result.status is TaskStatus.UNKNOWN_TASK:
            return encode_result(state, error=("UnknownTask", result.envelope.task))
        error = result.error
        return encode_result(state, error=(type(error).__name__, str(error)))

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
            case TaskStatus.SKIPPED:
                logger.info("task %r (id=%s) skipped by a hook", name, result.envelope.id)
            case TaskStatus.SUCCESS:
                logger.debug("task %r (id=%s) succeeded", name, result.envelope.id)

    async def _execute(self, delivery: Delivery) -> TaskResult:
        envelope = delivery.envelope
        try:
            task = self.app.registered_task(envelope.task)
        except KeyError:
            return TaskResult(envelope, TaskStatus.UNKNOWN_TASK)
        try:
            await run_hooks(self.app.hooks.before_task, envelope, delivery)
        except SkipTask:
            return TaskResult(envelope, TaskStatus.SKIPPED)
        await self._record_state(envelope.id, TaskState.RUNNING)
        try:
            value: Any
            if task.is_async:
                value = await task.func(*envelope.args, **envelope.kwargs)
            else:
                value = await asyncio.to_thread(task.func, *envelope.args, **envelope.kwargs)
                if inspect.isawaitable(value):
                    value = await value
        except Exception as exc:
            exceeded = task.max_retries is not None and envelope.attempt >= task.max_retries
            status = TaskStatus.RETRIES_EXCEEDED if exceeded else TaskStatus.FAILED
            return TaskResult(envelope, status, error=exc)
        return TaskResult(envelope, TaskStatus.SUCCESS, value=value)
