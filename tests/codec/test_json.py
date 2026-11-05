import json

import pytest

from asynk.codec import Codec, DecodeError, EncodeError
from asynk.codec.json import JsonCodec
from asynk.envelope import Envelope


def make() -> Envelope:
    return Envelope(
        task="t",
        args=(1, [2, {"a": None}], "x"),
        kwargs={"k": {"n": [1.5, True]}},
        id="abc",
        attempt=2,
        metadata={"k": "v"},
    )


def test_roundtrip() -> None:
    e = make()
    assert JsonCodec().decode(JsonCodec().encode(e)) == e


def test_encoded_is_utf8_json_object() -> None:
    data = JsonCodec().encode(make())
    obj = json.loads(data.decode("utf-8"))
    assert isinstance(obj, dict)
    assert {"id", "task", "args", "kwargs", "attempt", "metadata"} <= obj.keys()


@pytest.mark.parametrize(
    "data",
    [
        b"not json",
        b"[]",
        b'{"task": "t"}',
        b"\xff\xfe",
        b'{"task":1,"args":[],"kwargs":{},"id":"i","attempt":0,"metadata":{}}',
        b'{"task":"t","args":{},"kwargs":{},"id":"i","attempt":0,"metadata":{}}',
        b'{"task":"t","args":[],"kwargs":[],"id":"i","attempt":0,"metadata":{}}',
        b'{"task":"t","args":[],"kwargs":{},"id":"i","attempt":true,"metadata":{}}',
        b'{"task":"t","args":[],"kwargs":{},"id":"i","attempt":-1,"metadata":{}}',
        b'{"task":"t","args":[],"kwargs":{},"id":"i","attempt":0,"metadata":null}',
        b'{"task":"t","args":[NaN],"kwargs":{},"id":"i","attempt":0,"metadata":{}}',
    ],
)
def test_decode_rejects_garbage(data: bytes) -> None:
    with pytest.raises(DecodeError):
        JsonCodec().decode(data)


def test_decode_error_is_value_error() -> None:
    assert issubclass(DecodeError, ValueError)


def test_json_codec_satisfies_protocol() -> None:
    c: Codec = JsonCodec()
    assert c.decode(c.encode(make())) == make()


@pytest.mark.parametrize("depth", [1100, 2000, 5000])
def test_decode_deeply_nested_raises_decode_error(depth: int) -> None:
    nested = "[" * depth + "]" * depth
    data = (
        '{"task":"t","args":[' + nested + '],"kwargs":{},"id":"i","attempt":0,"metadata":{}}'
    ).encode()
    with pytest.raises(DecodeError):
        JsonCodec().decode(data)


@pytest.mark.parametrize(
    "data",
    [
        b'{"task":"t","args":[],"kwargs":{},"id":5,"attempt":0,"metadata":{}}',
        b'{"task":"t","args":[],"kwargs":{},"id":"i","attempt":0,"metadata":{"a":NaN}}',
    ],
)
def test_decode_rejects_bad_id_and_nan_metadata(data: bytes) -> None:
    with pytest.raises(DecodeError):
        JsonCodec().decode(data)


@pytest.mark.parametrize("value", [None, True, 1, 1.5, "x", [1, {"a": [None]}]])
def test_validate_accepts_json_values(value: object) -> None:
    JsonCodec().validate(Envelope.create("t", [value], {"k": value}, {"m": value}))


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
def test_validate_rejects_non_json_values(value: object) -> None:
    codec = JsonCodec()
    for env in (
        Envelope.create("t", [value], {}),
        Envelope.create("t", [], {"k": value}),
        Envelope.create("t", [], {}, {"m": value}),
    ):
        with pytest.raises(EncodeError):
            codec.validate(env)
        with pytest.raises(EncodeError):
            codec.encode(env)


def test_validate_error_message_contains_path() -> None:
    env = Envelope.create("t", [], {"items": [1, 2, (3,)]})
    with pytest.raises(EncodeError) as exc:
        JsonCodec().validate(env)
    assert "kwargs.items[2]" in str(exc.value)


def test_validate_rejects_non_str_kwarg_and_metadata_keys() -> None:
    codec = JsonCodec()
    bad_kwargs = Envelope("t", (), {1: "a"}, "id")  # type: ignore[dict-item]
    bad_meta = Envelope("t", (), {}, "id", metadata={1: "a"})  # type: ignore[dict-item]
    for env in (bad_kwargs, bad_meta):
        with pytest.raises(EncodeError):
            codec.validate(env)
