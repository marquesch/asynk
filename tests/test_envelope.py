import ast
import dataclasses
import inspect
import uuid

import pytest

from asynk import AsynkError, DecodeError, InvalidEnvelopeError
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


def _valid() -> dict[str, object]:
    return {"id": "i", "task": "t", "args": [], "kwargs": {}, "attempt": 0, "metadata": {}}


def test_to_dict_from_dict_round_trip() -> None:
    e = Envelope(
        task="t",
        args=(1, [2, {"a": None}], "x"),
        kwargs={"k": {"n": [1.5, True]}},
        id="abc",
        attempt=2,
        metadata={"k": "v"},
    )
    assert Envelope.from_dict(e.to_dict()) == e


def test_to_dict_shape() -> None:
    e = Envelope("t", (1, 2), {"k": 1}, "abc", attempt=3, metadata={"m": 2})
    assert e.to_dict() == {
        "id": "abc",
        "task": "t",
        "args": [1, 2],
        "kwargs": {"k": 1},
        "attempt": 3,
        "metadata": {"m": 2},
    }


def test_to_dict_copies_containers() -> None:
    e = Envelope.create("t", [], {"k": 1}, {"m": 2})
    d = e.to_dict()
    d["kwargs"]["x"] = 1  # type: ignore[index]
    d["metadata"]["x"] = 1  # type: ignore[index]
    assert e.kwargs == {"k": 1}
    assert e.metadata == {"m": 2}


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("task", 1, "'task' must be a string"),
        ("task", None, "'task' must be a string"),
        ("id", 5, "'id' must be a string"),
        ("args", {}, "'args' must be a list"),
        ("args", (1,), "'args' must be a list"),
        ("kwargs", [], "'kwargs' must be an object"),
        ("attempt", True, "'attempt' must be a non-negative integer"),
        ("attempt", -1, "'attempt' must be a non-negative integer"),
        ("attempt", 1.0, "'attempt' must be a non-negative integer"),
        ("metadata", None, "'metadata' must be an object"),
    ],
)
def test_from_dict_rejects_malformed_fields(key: str, value: object, message: str) -> None:
    data = _valid()
    data[key] = value
    with pytest.raises(InvalidEnvelopeError, match=message):
        Envelope.from_dict(data)


@pytest.mark.parametrize("key", ["task", "id", "args", "kwargs", "attempt", "metadata"])
def test_from_dict_rejects_missing_fields(key: str) -> None:
    data = _valid()
    del data[key]
    with pytest.raises(InvalidEnvelopeError, match=f"'{key}' must be"):
        Envelope.from_dict(data)


@pytest.mark.parametrize("data", [[], "x", None, 1, b"{}"])
def test_from_dict_rejects_non_mapping(data: object) -> None:
    with pytest.raises(InvalidEnvelopeError, match="envelope must be a mapping"):
        Envelope.from_dict(data)


def test_from_dict_builds_tuple_args() -> None:
    e = Envelope.from_dict({**_valid(), "args": [1, 2]})
    assert e.args == (1, 2)


def test_invalid_envelope_error_is_an_asynk_error_but_not_a_decode_error() -> None:
    assert issubclass(InvalidEnvelopeError, AsynkError)
    assert not issubclass(InvalidEnvelopeError, (ValueError, DecodeError))


def test_envelope_module_does_not_import_the_codec() -> None:
    import asynk.envelope as module

    tree = ast.parse(inspect.getsource(module))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    assert not [name for name in imported if name.startswith("asynk.codec")]
