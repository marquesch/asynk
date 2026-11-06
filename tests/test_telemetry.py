from __future__ import annotations

import subprocess
import sys
from typing import Any

import pytest

from asynk import App, Envelope, NullTelemetry, RetryPolicy, TaskOptions, Telemetry, Worker
from asynk.hooks import TaskStatus
from asynk.transport.memory import memory_transport


class _SpyTelemetry(NullTelemetry):
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def inject(self, metadata: dict[str, object]) -> None:
        self.calls.append(("inject", dict(metadata)))
        metadata.setdefault("injected", "yes")

    def message_published(self, envelope: Envelope) -> None:
        self.calls.append(("published", envelope.id))

    def task_started(self) -> None:
        self.calls.append(("started", None))

    def task_finished(self) -> None:
        self.calls.append(("finished", None))

    def retry_published(self, envelope: Envelope) -> None:
        self.calls.append(("retry", envelope.id))


def _app(**kwargs: Any) -> App:
    producer, consumer = memory_transport()
    return App("t", producer=producer, consumer=consumer, **kwargs)


def test_null_telemetry_does_nothing() -> None:
    telemetry: Telemetry = NullTelemetry()
    envelope = Envelope.create("t", (), {})
    telemetry.inject(envelope.metadata)
    telemetry.message_published(envelope)
    telemetry.task_started()
    telemetry.task_finished()
    telemetry.retry_published(envelope)
    assert telemetry.layer is None
    assert envelope.metadata == {}


def test_app_selects_a_telemetry_component() -> None:
    assert isinstance(_app().telemetry, Telemetry)


def test_app_without_the_package_uses_the_null_object() -> None:
    code = (
        "import sys; sys.modules['opentelemetry'] = None\n"
        "import asynk\n"
        "app = asynk.App('t')\n"
        "assert type(app.telemetry) is asynk.NullTelemetry\n"
    )
    out = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr


@pytest.mark.slow
def test_importing_asynk_does_not_import_opentelemetry() -> None:
    code = "import sys, asynk\nassert 'opentelemetry' not in sys.modules\n"
    out = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr


async def test_publish_injects_before_encoding_and_reports_the_send() -> None:
    app = _app()
    spy = app.telemetry = _SpyTelemetry()
    envelope = Envelope.create("t", (), {}, {"mine": 1})
    await app.publish(envelope)
    assert spy.calls == [("inject", {"mine": 1}), ("published", envelope.id)]
    assert envelope.metadata == {"mine": 1, "injected": "yes"}
    assert app.consumer is not None
    delivery = await anext(app.consumer.deliveries())
    assert Envelope.from_dict(app.codec.decode(delivery.body)).metadata["injected"] == "yes"


async def test_send_does_not_inject() -> None:
    app = _app()
    spy = app.telemetry = _SpyTelemetry()
    await app.send(Envelope.create("t", (), {}))
    assert spy.calls == []


async def test_worker_reports_started_and_finished_even_when_handling_fails() -> None:
    app = _app()
    spy = app.telemetry = _SpyTelemetry()
    worker = Worker(app)

    async def boom(delivery: Any) -> None:
        raise RuntimeError("x")

    worker.handle = boom  # type: ignore[method-assign]
    await worker._handle_and_release(object(), _Sem())  # type: ignore[arg-type]
    assert [c[0] for c in spy.calls] == ["started", "finished"]


class _Sem:
    def release(self) -> None: ...


async def test_worker_reports_a_published_retry() -> None:
    app = _app(options=TaskOptions(retry=RetryPolicy(max_retries=1)))
    spy = app.telemetry = _SpyTelemetry()

    @app.task(name="t")
    async def t() -> None:
        raise RuntimeError("x")

    worker = Worker(app)
    envelope = Envelope.create("t", (), {})
    await app.send(envelope)
    assert app.consumer is not None
    delivery = await anext(app.consumer.deliveries())
    result = await worker.handle(delivery)
    assert result is not None and result.status is TaskStatus.RETRYING
    assert ("retry", envelope.id) in spy.calls
