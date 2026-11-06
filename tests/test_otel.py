from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from opentelemetry import context, trace
from opentelemetry.trace import SpanContext, SpanKind, StatusCode, TraceFlags
from opentelemetry.trace.span import NonRecordingSpan, TraceState
from otel_helpers import (
    TRACE_ID,
    TRACEPARENT,
    FakeMeterProvider,
    FakeSpan,
    FakeTracerProvider,
    remote_parent,
)

import asynk
from asynk import App, Envelope, NullTelemetry, RetryPolicy, TaskOptions, Worker
from asynk.hooks import Next, TaskCall, TaskResult, TaskStatus
from asynk.otel import OtelTelemetry
from asynk.store.memory import MemoryStore
from asynk.telemetry import select_telemetry
from asynk.transport import Delivery
from asynk.transport.memory import memory_transport


class _Setup:
    def __init__(self) -> None:
        self.tracers = FakeTracerProvider()
        self.meters = FakeMeterProvider()
        producer, consumer = memory_transport()
        self.app = App("t", producer=producer, consumer=consumer, store=MemoryStore())
        self.app.telemetry = OtelTelemetry(
            tracer_provider=self.tracers,  # type: ignore[arg-type]
            meter_provider=self.meters,  # type: ignore[arg-type]
        )
        self.worker = Worker(self.app)

    @property
    def spans(self) -> list[FakeSpan]:
        return self.tracers.spans

    def metric(self, name: str) -> Any:
        return self.meters.instruments[name]

    async def delivery(self) -> Delivery:
        assert self.app.consumer is not None
        return await asyncio.wait_for(anext(self.app.consumer.deliveries()), 1)

    async def run(self, envelope: Envelope) -> TaskResult:
        """Publishes inside a remote parent, then handles the delivery."""
        token = context.attach(trace.set_span_in_context(remote_parent()))
        try:
            await self.app.publish(envelope)
        finally:
            context.detach(token)
        result = await self.worker.handle(await self.delivery())
        assert result is not None
        return result


def _envelope(task: str = "work", **metadata: object) -> Envelope:
    return Envelope.create(task, (), {}, metadata)


# --- publish side ---------------------------------------------------------------------


def test_without_a_current_span_the_metadata_is_untouched() -> None:
    telemetry = OtelTelemetry()
    metadata: dict[str, object] = {"mine": 1}
    telemetry.inject(metadata)
    assert metadata == {"mine": 1}


def test_inject_writes_the_traceparent_of_the_active_span() -> None:
    telemetry = OtelTelemetry()
    metadata: dict[str, object] = {}
    token = context.attach(trace.set_span_in_context(remote_parent()))
    try:
        telemetry.inject(metadata)
    finally:
        context.detach(token)
    assert metadata == {"traceparent": TRACEPARENT}


def test_inject_writes_the_tracestate_when_present() -> None:
    span = NonRecordingSpan(
        SpanContext(
            TRACE_ID,
            7,
            is_remote=False,
            trace_flags=TraceFlags(1),
            trace_state=TraceState([("k", "v")]),
        )
    )
    metadata: dict[str, object] = {}
    token = context.attach(trace.set_span_in_context(span))
    try:
        OtelTelemetry().inject(metadata)
    finally:
        context.detach(token)
    assert metadata["tracestate"] == "k=v"


def test_inject_keeps_explicit_metadata() -> None:
    metadata: dict[str, object] = {"traceparent": "mine"}
    token = context.attach(trace.set_span_in_context(remote_parent()))
    try:
        OtelTelemetry().inject(metadata)
    finally:
        context.detach(token)
    assert metadata == {"traceparent": "mine"}


async def test_publish_counts_the_message() -> None:
    s = _Setup()
    await s.app.publish(_envelope())
    sent = s.metric("messaging.client.sent.messages")
    assert sent.unit == "{message}"
    assert sent.points == [
        (
            1,
            {
                "messaging.system.name": "asynk",
                "messaging.operation.name": "publish",
                "asynk.task.name": "work",
            },
        )
    ]


async def test_send_neither_injects_nor_counts() -> None:
    s = _Setup()
    token = context.attach(trace.set_span_in_context(remote_parent()))
    try:
        envelope = _envelope()
        await s.app.send(envelope)
    finally:
        context.detach(token)
    assert envelope.metadata == {}
    assert s.metric("messaging.client.sent.messages").points == []


# --- worker side ----------------------------------------------------------------------


async def test_instrumentation_scope_is_asynk_with_its_version() -> None:
    s = _Setup()
    assert s.tracers.tracers[0].scope == ("asynk", asynk.__version__)
    assert s.meters.meters[0].scope == ("asynk", asynk.__version__)


async def test_a_task_runs_under_a_consumer_span_child_of_the_published_context() -> None:
    s = _Setup()

    @s.app.task(name="work")
    async def work() -> int:
        return 1

    envelope = _envelope()
    result = await s.run(envelope)
    assert result.status is TaskStatus.SUCCESS
    (span,) = s.spans
    assert span.name == "process work"
    assert span.kind is SpanKind.CONSUMER
    assert span.parent_span_context().trace_id == TRACE_ID
    assert span.parent_span_context().is_remote
    assert span.attributes == {
        "messaging.system.name": "asynk",
        "messaging.operation.name": "process",
        "messaging.operation.type": "process",
        "messaging.message.id": envelope.id,
        "asynk.task.name": "work",
        "asynk.task.attempt": 0,
        "asynk.task.status": "success",
    }
    assert span.status is None
    assert span.ended == 1
    assert span.exceptions == []


async def test_the_task_sees_the_span_as_current_and_it_is_detached_afterwards() -> None:
    s = _Setup()
    seen: list[Any] = []

    @s.app.task(name="work")
    async def work() -> None:
        seen.append(trace.get_current_span())

    await s.run(_envelope())
    assert seen == [s.spans[0]]
    assert trace.get_current_span() is not s.spans[0]


async def test_a_task_published_without_context_starts_a_new_trace() -> None:
    s = _Setup()

    @s.app.task(name="work")
    async def work() -> None: ...

    assert s.app.producer is not None
    await s.app.send(_envelope())
    await s.worker.handle(await s.delivery())
    assert not s.spans[0].parent_span_context().is_valid


async def test_a_metadata_value_that_is_not_text_is_ignored() -> None:
    s = _Setup()

    @s.app.task(name="work")
    async def work() -> None: ...

    await s.app.send(_envelope(traceparent=5))
    result = await s.worker.handle(await s.delivery())
    assert result is not None and result.status is TaskStatus.SUCCESS
    assert len(s.spans) == 1
    # Telemetry is still on afterwards.
    await s.app.send(_envelope())
    await s.worker.handle(await s.delivery())
    assert len(s.spans) == 2


async def test_a_failing_task_records_the_exception_and_sets_error() -> None:
    s = _Setup()
    boom = ValueError("boom")

    @s.app.task(name="work", options=TaskOptions(retry=False))
    async def work() -> None:
        raise boom

    result = await s.run(_envelope())
    assert result.status is TaskStatus.FAILED
    (span,) = s.spans
    assert span.exceptions == [boom]
    assert span.status is not None and span.status.status_code is StatusCode.ERROR
    assert span.attributes["asynk.task.status"] == "failed"
    assert span.attributes["error.type"] == "ValueError"
    assert span.ended == 1


async def test_a_retry_keeps_the_context_and_counts() -> None:
    s = _Setup()

    @s.app.task(name="work", options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    async def work() -> None:
        raise RuntimeError("x")

    result = await s.run(_envelope())
    assert result.status is TaskStatus.RETRYING
    assert s.spans[0].status is not None
    assert s.spans[0].attributes["asynk.task.status"] == "retrying"
    retried = Envelope.from_dict(s.app.codec.decode((await s.delivery()).body))
    assert retried.attempt == 1
    assert retried.metadata == {"traceparent": TRACEPARENT}
    assert s.metric("asynk.task.retries").points == [(1, {"asynk.task.name": "work"})]


async def test_retries_exceeded_is_an_error() -> None:
    s = _Setup()

    @s.app.task(name="work", options=TaskOptions(retry=RetryPolicy(max_retries=0)))
    async def work() -> None:
        raise RuntimeError("x")

    result = await s.run(_envelope())
    assert result.status is TaskStatus.RETRIES_EXCEEDED
    assert s.spans[0].status is not None
    assert s.spans[0].status.status_code is StatusCode.ERROR
    assert s.spans[0].attributes["asynk.task.status"] == "retries_exceeded"


async def test_the_span_is_outermost_and_a_veto_is_not_an_error() -> None:
    s = _Setup()
    inside: list[Any] = []

    @s.app.task(name="work")
    async def work() -> None:
        raise AssertionError("must not run")

    @s.app.middleware
    async def veto(call: TaskCall, next: Next) -> TaskResult:
        inside.append(trace.get_current_span())
        return call.skip()

    result = await s.run(_envelope())
    assert result.status is TaskStatus.SKIPPED
    (span,) = s.spans
    assert inside == [span]
    assert span.status is None
    assert span.exceptions == []
    assert span.attributes["asynk.task.status"] == "skipped"
    assert "error.type" not in span.attributes


async def test_a_raising_user_layer_is_inside_the_span() -> None:
    s = _Setup()
    error = RuntimeError("layer")

    @s.app.task(name="work", options=TaskOptions(retry=False))
    async def work() -> None: ...

    @s.app.middleware
    async def broken(call: TaskCall, next: Next) -> TaskResult:
        raise error

    result = await s.run(_envelope())
    assert result.status is TaskStatus.FAILED
    (span,) = s.spans
    assert span.exceptions == [error]
    assert span.status is not None and span.status.status_code is StatusCode.ERROR
    assert span.ended == 1


async def test_a_cancelled_task_ends_its_span_with_an_error() -> None:
    s = _Setup()
    started = asyncio.Event()

    @s.app.task(name="work")
    async def work() -> None:
        started.set()
        await asyncio.sleep(10)

    await s.app.send(_envelope())
    handling = asyncio.create_task(s.worker.handle(await s.delivery()))
    await started.wait()
    handling.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handling
    (span,) = s.spans
    assert span.ended == 1
    assert span.status is not None and span.status.status_code is StatusCode.ERROR
    assert span.exceptions == []
    assert span.attributes["error.type"] == "asyncio.exceptions.CancelledError"


async def test_unknown_task_is_recorded_without_an_error() -> None:
    s = _Setup()
    result = await s.run(_envelope("nope"))
    assert result.status is TaskStatus.UNKNOWN_TASK
    assert s.spans[0].attributes["asynk.task.status"] == "unknown_task"
    assert s.spans[0].status is None


# --- metrics --------------------------------------------------------------------------


async def test_processing_is_measured_and_consumption_counted() -> None:
    s = _Setup()

    @s.app.task(name="work")
    async def work() -> None: ...

    await s.run(_envelope())
    duration = s.metric("messaging.process.duration")
    assert duration.unit == "s"
    ((seconds, attributes),) = duration.points
    assert seconds >= 0
    assert attributes == {
        "messaging.system.name": "asynk",
        "messaging.operation.name": "process",
        "asynk.task.name": "work",
        "asynk.task.status": "success",
    }
    consumed = s.metric("messaging.client.consumed.messages")
    assert consumed.unit == "{message}"
    assert [amount for amount, _ in consumed.points] == [1]


async def test_a_failure_adds_the_error_type_to_the_duration() -> None:
    s = _Setup()

    @s.app.task(name="work", options=TaskOptions(retry=False))
    async def work() -> None:
        raise KeyError("k")

    await s.run(_envelope())
    ((_, attributes),) = s.metric("messaging.process.duration").points
    assert attributes["error.type"] == "KeyError"
    assert attributes["asynk.task.status"] == "failed"


async def test_active_tasks_goes_up_and_down_also_on_errors_and_cancellation() -> None:
    s = _Setup()
    active = s.metric("asynk.worker.active_tasks")
    assert active.kind == "up_down_counter"

    class _Sem:
        def release(self) -> None: ...

    async def explode(delivery: Delivery) -> None:
        raise RuntimeError("x")

    async def hang(delivery: Delivery) -> None:
        await asyncio.sleep(10)

    sem: Any = _Sem()
    s.worker.handle = explode  # type: ignore[method-assign]
    await s.worker._handle_and_release(None, sem)  # type: ignore[arg-type]
    assert active.points == [(1, {}), (-1, {})]

    s.worker.handle = hang  # type: ignore[method-assign]
    running = asyncio.create_task(s.worker._handle_and_release(None, sem))  # type: ignore[arg-type]
    await asyncio.sleep(0)
    assert active.total == 1
    running.cancel()
    await asyncio.gather(running, return_exceptions=True)
    assert active.total == 0


# --- fail open ------------------------------------------------------------------------


class _Raising:
    def __init__(self) -> None:
        self.calls = 0

    def add(self, *args: Any, **kwargs: Any) -> None:
        self.calls += 1
        raise RuntimeError("telemetry down")

    record = add


async def test_a_failing_telemetry_call_is_logged_once_and_never_affects_a_task(
    caplog: pytest.LogCaptureFixture,
) -> None:
    s = _Setup()
    telemetry = s.app.telemetry
    assert isinstance(telemetry, OtelTelemetry)
    broken = _Raising()
    telemetry._consumed = broken  # type: ignore[assignment]
    telemetry._active = broken  # type: ignore[assignment]

    @s.app.task(name="work")
    async def work() -> int:
        return 3

    with caplog.at_level(logging.ERROR, logger="asynk"):
        first = await s.run(_envelope())
        second = await s.run(_envelope())
    assert first.status is second.status is TaskStatus.SUCCESS
    assert first.value == 3
    assert len([r for r in caplog.records if "OpenTelemetry failed" in r.message]) == 1
    # Once disabled, no more telemetry calls are made.
    assert broken.calls == 1
    assert len(s.spans) == 1


async def test_a_failing_tracer_does_not_affect_publishing_or_tasks() -> None:
    s = _Setup()
    telemetry = s.app.telemetry
    assert isinstance(telemetry, OtelTelemetry)

    class _BadTracer:
        def start_span(self, *a: Any, **k: Any) -> None:
            raise RuntimeError("tracer down")

    telemetry._tracer = _BadTracer()  # type: ignore[assignment]

    @s.app.task(name="work")
    async def work() -> int:
        return 9

    result = await s.run(_envelope())
    assert result.status is TaskStatus.SUCCESS and result.value == 9


async def test_a_failing_injection_does_not_fail_publishing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s = _Setup()

    def boom(carrier: Any) -> None:
        raise RuntimeError("propagator down")

    monkeypatch.setattr("asynk.otel.propagate.inject", boom)
    handle = await s.app.publish(_envelope())
    assert handle.task_id


async def test_the_default_providers_are_inert() -> None:
    producer, consumer = memory_transport()
    app = App("t", producer=producer, consumer=consumer)
    assert isinstance(app.telemetry, OtelTelemetry)

    @app.task(name="work")
    async def work() -> int:
        return 5

    envelope = _envelope()
    await app.publish(envelope)
    assert envelope.metadata == {}
    assert app.consumer is not None
    result = await Worker(app).handle(await anext(app.consumer.deliveries()))
    assert result is not None and result.value == 5


# --- selection ------------------------------------------------------------------------


def test_the_app_picks_the_opentelemetry_component_when_the_package_is_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OTEL_SDK_DISABLED", raising=False)
    assert isinstance(select_telemetry(), OtelTelemetry)


@pytest.mark.parametrize("value", ["true", "True", " TRUE "])
def test_otel_sdk_disabled_turns_it_off(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("OTEL_SDK_DISABLED", value)
    assert type(select_telemetry()) is NullTelemetry


def test_other_values_of_otel_sdk_disabled_leave_it_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_SDK_DISABLED", "false")
    assert isinstance(select_telemetry(), OtelTelemetry)


def test_a_component_that_cannot_be_built_falls_back_to_the_null_object(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(self: OtelTelemetry) -> None:
        raise RuntimeError("no")

    monkeypatch.delenv("OTEL_SDK_DISABLED", raising=False)
    monkeypatch.setattr(OtelTelemetry, "__init__", broken)
    with caplog.at_level(logging.ERROR, logger="asynk"):
        assert type(select_telemetry()) is NullTelemetry
    assert "telemetry is off" in caplog.text


def test_importing_the_module_without_the_package_names_the_extra() -> None:
    import subprocess
    import sys

    code = "import sys; sys.modules['opentelemetry'] = None\nimport asynk.otel\n"
    out = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True)
    assert "pip install 'asynk[otel]'" in out.stderr
