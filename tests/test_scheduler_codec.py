from __future__ import annotations

import pickle

import pytest

from asynk.app import App
from asynk.codec import EncodeError
from asynk.scheduler import Scheduler
from asynk.transport.memory import memory_transport


class _PickleCodec:
    def validate(self, value: object) -> None:
        pickle.dumps(value)

    def encode(self, value: object) -> bytes:
        return pickle.dumps(value)

    def decode(self, data: bytes) -> object:
        return pickle.loads(data)  # noqa: S301 - round trip within the test


class _RejectEverythingCodec(_PickleCodec):
    def validate(self, value: object) -> None:
        raise EncodeError("nothing is encodable")


def _app(codec: object) -> App:
    producer, _ = memory_transport()
    return App("t", producer=producer, codec=codec)  # type: ignore[arg-type]


def test_scheduler_validates_arguments_with_the_app_codec_rejecting() -> None:
    app = _app(_RejectEverythingCodec())

    @app.scheduled_task(cron="* * * * *", args=[1])
    def job(n: int) -> None: ...

    with pytest.raises(EncodeError):
        Scheduler(app)


def test_scheduler_validates_arguments_with_the_app_codec_accepting() -> None:
    app = _app(_PickleCodec())

    @app.scheduled_task(cron="* * * * *", args=[(1, 2)])
    def job(pair: tuple[int, int]) -> None: ...

    Scheduler(app)


def test_default_json_codec_rejects_the_same_tuple_argument() -> None:
    producer, _ = memory_transport()
    app = App("t", producer=producer)

    @app.scheduled_task(cron="* * * * *", args=[(1, 2)])
    def job(pair: tuple[int, int]) -> None: ...

    with pytest.raises(EncodeError):
        Scheduler(app)
