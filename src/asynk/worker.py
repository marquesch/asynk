from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from asynk.app import App, NotConfiguredError
from asynk.hooks import TaskFailedError, TaskResult, TaskStatus
from asynk.transport import Consumer, Delivery

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

    async def handle(self, delivery: Delivery) -> TaskResult:
        """Process exactly one delivery; task and hook errors never propagate."""
        result: TaskResult | None = None
        try:
            async with self.consumer.process(delivery):
                result = await self._execute(delivery)
                await self._run_hook(result, delivery)
                if not delivery.settled and result.status is not TaskStatus.SUCCESS:
                    raise TaskFailedError(result)
        except TaskFailedError as exc:
            return exc.result
        assert result is not None
        return result

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
