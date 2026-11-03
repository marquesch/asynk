import json

import pytest

from asynk.codec import Codec, DecodeError, JsonCodec
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
