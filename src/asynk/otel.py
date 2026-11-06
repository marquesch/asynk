"""OpenTelemetry tracing and metrics for an app."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from functools import partial
from typing import Any, TypeVar

from asynk._extras import require_extra

require_extra("opentelemetry", "otel")

from opentelemetry import context, metrics, propagate, trace  # noqa: E402
from opentelemetry.metrics import MeterProvider  # noqa: E402
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, TracerProvider  # noqa: E402

import asynk  # noqa: E402
from asynk.envelope import Envelope  # noqa: E402
from asynk.hooks import Next, TaskCall, TaskResult, TaskStatus  # noqa: E402

logger = logging.getLogger("asynk")

_SYSTEM = "asynk"
_ERROR_STATUSES = (TaskStatus.FAILED, TaskStatus.RETRYING, TaskStatus.RETRIES_EXCEEDED)

T = TypeVar("T")


def _error_type(exc: BaseException) -> str:
    cls = type(exc)
    return (
        cls.__qualname__ if cls.__module__ == "builtins" else f"{cls.__module__}.{cls.__qualname__}"
    )


class OtelTelemetry:
    """Traces and measures an app's publishing and tasks with OpenTelemetry.

    Publishing writes the active trace context into the envelope metadata. A worker
    runs each task under a `CONSUMER` span whose parent is that context, and records
    the metrics `messaging.process.duration`, `messaging.client.sent.messages`,
    `messaging.client.consumed.messages`, `asynk.worker.active_tasks` and
    `asynk.task.retries`.

    Any error in this class is logged once and turns all of its telemetry off for the
    process; tasks and publishing carry on.

    Attributes:
        layer: The middleware a worker runs outside every layer of the app.
    """

    def __init__(
        self,
        *,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
    ) -> None:
        """Initializes the component.

        Args:
            tracer_provider: Provider of the tracer; defaults to the global one.
            meter_provider: Provider of the meter; defaults to the global one.
        """
        self._enabled = True
        self._tracer = trace.get_tracer("asynk", asynk.__version__, tracer_provider=tracer_provider)
        meter = metrics.get_meter("asynk", asynk.__version__, meter_provider=meter_provider)
        self._duration = meter.create_histogram(
            "messaging.process.duration",
            unit="s",
            description="Duration of processing a message.",
        )
        self._sent = meter.create_counter(
            "messaging.client.sent.messages",
            unit="{message}",
            description="Messages published.",
        )
        self._consumed = meter.create_counter(
            "messaging.client.consumed.messages",
            unit="{message}",
            description="Messages a worker took for processing.",
        )
        self._active = meter.create_up_down_counter(
            "asynk.worker.active_tasks",
            unit="{task}",
            description="Deliveries a worker is handling.",
        )
        self._retries = meter.create_counter(
            "asynk.task.retries",
            unit="{retry}",
            description="Retries a worker published.",
        )
        self.layer = self._layer

    def _guard(self, action: Callable[[], T]) -> T | None:
        """Runs `action` while telemetry is on; on an error, logs it once and turns it off."""
        if not self._enabled:
            return None
        try:
            return action()
        except Exception:
            self._enabled = False
            logger.exception("OpenTelemetry failed; its telemetry is off for this process")
            return None

    def inject(self, metadata: dict[str, object]) -> None:
        """Adds the active trace context to the metadata; keys already there are kept.

        Args:
            metadata: The metadata of the envelope; changed in place.
        """

        def action() -> None:
            carrier: dict[str, str] = {}
            propagate.inject(carrier)
            for key, value in carrier.items():
                metadata.setdefault(key, value)

        self._guard(action)

    def message_published(self, envelope: Envelope) -> None:
        """Counts a published message.

        Args:
            envelope: The envelope that was sent.
        """
        self._guard(
            lambda: self._sent.add(1, {**_base("publish"), "asynk.task.name": envelope.task})
        )

    def task_started(self) -> None:
        """Counts a delivery as active."""
        self._guard(lambda: self._active.add(1))

    def task_finished(self) -> None:
        """Stops counting a delivery as active."""
        self._guard(lambda: self._active.add(-1))

    def retry_published(self, envelope: Envelope) -> None:
        """Counts a published retry.

        Args:
            envelope: The envelope of the attempt that is being retried.
        """
        self._guard(lambda: self._retries.add(1, {"asynk.task.name": envelope.task}))

    async def _layer(self, call: TaskCall, next: Next) -> TaskResult:
        started = self._guard(lambda: self._begin(call))
        if started is None:
            return await next(call)
        span, token, clock = started
        try:
            result = await next(call)
        except BaseException as exc:
            self._guard(partial(self._end_raised, call, span, clock, exc))
            raise
        else:
            self._guard(partial(self._end, call, span, clock, result))
            return result
        finally:
            if token is not None:
                self._guard(lambda: context.detach(token))

    def _begin(self, call: TaskCall) -> tuple[Span, Any, float]:
        envelope = call.envelope
        carrier = {k: v for k, v in envelope.metadata.items() if isinstance(v, str)}
        parent = propagate.extract(carrier)
        span = self._tracer.start_span(
            f"process {envelope.task}",
            context=parent,
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system.name": _SYSTEM,
                "messaging.operation.name": "process",
                "messaging.operation.type": "process",
                "messaging.message.id": envelope.id,
                "asynk.task.name": envelope.task,
                "asynk.task.attempt": envelope.attempt,
            },
        )
        token = context.attach(trace.set_span_in_context(span, parent))
        self._consumed.add(1, {**_base("process"), "asynk.task.name": envelope.task})
        return span, token, time.perf_counter()

    def _end(self, call: TaskCall, span: Span, clock: float, result: TaskResult) -> None:
        attributes = {
            **_base("process"),
            "asynk.task.name": call.envelope.task,
            "asynk.task.status": result.status.value,
        }
        try:
            span.set_attribute("asynk.task.status", result.status.value)
            if result.status in _ERROR_STATUSES:
                error = result.error
                if error is not None:
                    span.record_exception(error)
                    attributes["error.type"] = _error_type(error)
                    span.set_attribute("error.type", attributes["error.type"])
                span.set_status(Status(StatusCode.ERROR))
        finally:
            span.end()
        self._duration.record(time.perf_counter() - clock, attributes)

    def _end_raised(self, call: TaskCall, span: Span, clock: float, exc: BaseException) -> None:
        error_type = _error_type(exc)
        attributes = {
            **_base("process"),
            "asynk.task.name": call.envelope.task,
            "error.type": error_type,
        }
        try:
            span.set_attribute("error.type", error_type)
            if isinstance(exc, Exception):
                span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR))
        finally:
            span.end()
        self._duration.record(time.perf_counter() - clock, attributes)


def _base(operation: str) -> dict[str, str]:
    return {"messaging.system.name": _SYSTEM, "messaging.operation.name": operation}
