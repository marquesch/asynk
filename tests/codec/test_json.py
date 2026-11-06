import contextlib
import json
from unittest.mock import patch

import pytest

from asynk import AsynkError, DecodeError, EncodeError
from asynk.codec import Codec
from asynk.codec.json import JsonCodec


def test_encode_decode_roundtrips_nested_plain_data() -> None:
    codec = JsonCodec()
    value: dict[str, object] = {"a": [1, 2.5, None, True, "x", {"b": [[], {}]}], "c": "d"}
    assert codec.decode(codec.encode(value)) == value


@pytest.mark.parametrize("value", [None, True, 1, 1.5, "x", [1, {"a": [None]}]])
def test_encode_decode_roundtrips_scalars_and_containers(value: object) -> None:
    codec = JsonCodec()
    assert codec.decode(codec.encode(value)) == value


def test_encoded_is_compact_utf8_json() -> None:
    data = JsonCodec().encode({"a": [1, "é"]})
    assert json.loads(data.decode("utf-8")) == {"a": [1, "é"]}
    assert b" " not in data


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
def test_validate_and_encode_reject_non_json_values(value: object) -> None:
    codec = JsonCodec()
    with pytest.raises(EncodeError):
        codec.validate(value)
    with pytest.raises(EncodeError):
        codec.encode(value)


def test_encode_rejects_tuple() -> None:
    with pytest.raises(EncodeError):
        JsonCodec().encode((1, 2))


def test_encode_rejects_non_str_key() -> None:
    with pytest.raises(EncodeError):
        JsonCodec().encode({1: "a"})


def test_encode_rejects_nan() -> None:
    with pytest.raises(EncodeError):
        JsonCodec().encode(float("nan"))


def test_validate_error_message_contains_path() -> None:
    with pytest.raises(EncodeError) as exc:
        JsonCodec().validate({"kwargs": {"items": [1, 2, (3,)]}})
    assert "kwargs.items[2]" in str(exc.value)


def test_validate_raises_without_encoding() -> None:
    with patch("asynk.codec.json.json.dumps") as dumps:
        with pytest.raises(EncodeError):
            JsonCodec().validate([object()])
        JsonCodec().validate([1])
    dumps.assert_not_called()


@pytest.mark.parametrize("depth", [1100, 5000])
def test_encode_deeply_nested_raises_encode_error(depth: int) -> None:
    value: list[object] = []
    for _ in range(depth):
        value = [value]
    with pytest.raises(EncodeError, match="nested too deeply"):
        JsonCodec().encode(value)


@pytest.mark.parametrize("data", [b"{not json", b"not json", b"\xff\xfe", b""])
def test_decode_rejects_invalid_json(data: bytes) -> None:
    with pytest.raises(DecodeError):
        JsonCodec().decode(data)


@pytest.mark.parametrize("data", [b"NaN", b"[Infinity]", b'{"a":-Infinity}'])
def test_decode_rejects_nan_constants(data: bytes) -> None:
    with pytest.raises(DecodeError):
        JsonCodec().decode(data)


@pytest.mark.parametrize("depth", [1100, 2000, 5000, 100_000])
def test_decode_deeply_nested_returns_or_raises_decode_error(depth: int) -> None:
    # Whether the stdlib parser accepts a given depth depends on the Python version.
    with contextlib.suppress(DecodeError):
        JsonCodec().decode(("[" * depth + "]" * depth).encode())


def test_decode_returns_non_mapping_values() -> None:
    assert JsonCodec().decode(b"[]") == []


def test_errors_are_asynk_errors() -> None:
    assert issubclass(DecodeError, AsynkError)
    assert issubclass(EncodeError, AsynkError)
    assert not issubclass(DecodeError, ValueError)
    assert not issubclass(EncodeError, ValueError)


def test_json_codec_satisfies_protocol() -> None:
    c: Codec = JsonCodec()
    assert c.decode(c.encode({"a": 1})) == {"a": 1}


def test_codec_package_does_not_import_envelope() -> None:
    import inspect

    import asynk.codec as package

    assert "asynk.envelope" not in inspect.getsource(package)
