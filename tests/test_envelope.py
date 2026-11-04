import dataclasses
import uuid

import pytest

from asynk.envelope import Envelope, NonJsonArgumentError, ensure_json


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


@pytest.mark.parametrize("value", [None, True, 1, 1.5, "x", [1, {"a": [None]}]])
def test_ensure_json_accepts(value: object) -> None:
    ensure_json(value)


@pytest.mark.parametrize(
    "value",
    [
        (1, 2),
        float("nan"),
        float("inf"),
        {1: "a"},
        {"a": {1, 2}},
        object(),
        [1, [2, (3,)]],
    ],
)
def test_ensure_json_rejects(value: object) -> None:
    with pytest.raises(NonJsonArgumentError):
        ensure_json(value)


def test_error_message_contains_path() -> None:
    with pytest.raises(NonJsonArgumentError) as exc:
        Envelope.create("t", [], {"items": [1, 2, (3,)]})
    assert "kwargs.items[2]" in str(exc.value)


def test_create_with_metadata() -> None:
    assert Envelope.create("t", (), {}, {"k": 1}).metadata == {"k": 1}


def test_create_rejects_non_json_metadata() -> None:
    with pytest.raises(NonJsonArgumentError):
        Envelope.create("t", (), {}, {"k": object()})
