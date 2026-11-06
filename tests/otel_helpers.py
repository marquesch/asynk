"""Hand-written recording fakes of the OpenTelemetry API's providers, so tests need no SDK."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

TRACE_ID = 0x0AF7651916CD43DD8448EB211C80319C
SPAN_ID = 0xB7AD6B7169203331
TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


def remote_parent() -> NonRecordingSpan:
    """A span with a valid, sampled context, to publish from."""
    return NonRecordingSpan(
        SpanContext(TRACE_ID, SPAN_ID, is_remote=False, trace_flags=TraceFlags(1))
    )


@dataclass(eq=False)
class FakeSpan(trace.Span):
    name: str
    kind: Any
    parent: Context | None
    attributes: dict[str, Any]
    exceptions: list[BaseException] = field(default_factory=list)
    status: Any = None
    ended: int = 0

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def record_exception(self, exception: BaseException, *args: Any, **kwargs: Any) -> None:
        self.exceptions.append(exception)

    def set_status(self, status: Any, description: str | None = None) -> None:
        self.status = status

    def end(self, end_time: int | None = None) -> None:
        self.ended += 1

    def parent_span_context(self) -> SpanContext:
        return trace.get_current_span(self.parent).get_span_context()

    def get_span_context(self) -> SpanContext:
        return SpanContext(TRACE_ID, 1, is_remote=False)

    def is_recording(self) -> bool:
        return True

    def set_attributes(self, attributes: Any) -> None:
        self.attributes.update(attributes)

    def add_event(self, *args: Any, **kwargs: Any) -> None: ...

    def update_name(self, name: str) -> None:
        self.name = name


class FakeTracer:
    def __init__(self, scope: tuple[str, str | None]) -> None:
        self.scope = scope
        self.spans: list[FakeSpan] = []

    def start_span(
        self,
        name: str,
        context: Context | None = None,
        kind: Any = None,
        attributes: Any = None,
        **_: Any,
    ) -> FakeSpan:
        span = FakeSpan(name, kind, context, dict(attributes or {}))
        self.spans.append(span)
        return span


class FakeTracerProvider:
    def __init__(self) -> None:
        self.tracers: list[FakeTracer] = []

    def get_tracer(self, name: str, version: str | None = None, *_: Any) -> FakeTracer:
        tracer = FakeTracer((name, version))
        self.tracers.append(tracer)
        return tracer

    @property
    def spans(self) -> list[FakeSpan]:
        return [span for tracer in self.tracers for span in tracer.spans]


@dataclass
class FakeInstrument:
    name: str
    kind: str
    unit: str
    points: list[tuple[float, dict[str, Any]]] = field(default_factory=list)

    def add(self, amount: float, attributes: dict[str, Any] | None = None, **_: Any) -> None:
        self.points.append((amount, dict(attributes or {})))

    def record(self, amount: float, attributes: dict[str, Any] | None = None, **_: Any) -> None:
        self.points.append((amount, dict(attributes or {})))

    @property
    def total(self) -> float:
        return sum(amount for amount, _ in self.points)


class FakeMeter:
    def __init__(self, scope: tuple[str, str | None]) -> None:
        self.scope = scope
        self.instruments: dict[str, FakeInstrument] = {}

    def _make(self, kind: str, name: str, unit: str = "", **_: Any) -> FakeInstrument:
        instrument = FakeInstrument(name, kind, unit)
        self.instruments[name] = instrument
        return instrument

    def create_histogram(self, name: str, unit: str = "", **kw: Any) -> FakeInstrument:
        return self._make("histogram", name, unit, **kw)

    def create_counter(self, name: str, unit: str = "", **kw: Any) -> FakeInstrument:
        return self._make("counter", name, unit, **kw)

    def create_up_down_counter(self, name: str, unit: str = "", **kw: Any) -> FakeInstrument:
        return self._make("up_down_counter", name, unit, **kw)


class FakeMeterProvider:
    def __init__(self) -> None:
        self.meters: list[FakeMeter] = []

    def get_meter(self, name: str, version: str | None = None, *_: Any) -> FakeMeter:
        meter = FakeMeter((name, version))
        self.meters.append(meter)
        return meter

    @property
    def instruments(self) -> dict[str, FakeInstrument]:
        return {n: i for meter in self.meters for n, i in meter.instruments.items()}
