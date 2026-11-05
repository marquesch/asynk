import dataclasses
import uuid

import pytest

from asynk.envelope import Envelope


def test_create_generates_uuid4_and_attempt_zero() -> None:
    e = Envelope.create("t", [1], {"a": 2})
    assert uuid.UUID(e.id).version == 4
    assert e.attempt == 0
    assert e.args == (1,)
    assert e.kwargs == {"a": 2}


def test_ids_are_unique() -> None:
    assert Envelope.create("t", [], {}).id != Envelope.create("t", [], {}).id


def test_retry_increments_attempt_and_keeps_id() -> None:
    e = Envelope.create("t", [1], {"a": 2})
    r = e.retry()
    assert r.id == e.id
    assert r.attempt == 1
    assert r.args == e.args
    assert r.kwargs == e.kwargs
    assert r.task == e.task


def test_envelope_is_frozen() -> None:
    e = Envelope.create("t", [], {})
    with pytest.raises(dataclasses.FrozenInstanceError):
        e.attempt = 1  # type: ignore[misc]


def test_create_does_not_validate_values() -> None:
    marker = object()
    e = Envelope.create("t", [marker], {"k": marker}, {"m": marker})
    assert e.args == (marker,)
    assert e.kwargs == {"k": marker}
    assert e.metadata == {"m": marker}


def test_create_with_metadata() -> None:
    assert Envelope.create("t", (), {}, {"k": 1}).metadata == {"k": 1}
