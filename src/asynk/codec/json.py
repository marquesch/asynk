"""`JsonCodec`, the default codec."""

from __future__ import annotations

import json
import math
from typing import Any

from asynk.errors import DecodeError, EncodeError


def _ensure_json(value: object, path: str) -> None:
    """Raises `EncodeError` if `value` is not JSON-friendly; `path` locates it."""
    if value is None or isinstance(value, bool | int | str):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise EncodeError(f"{path}: non-finite float {value!r} is not JSON-friendly")
        return
    if isinstance(value, list):
        for i, item in enumerate(value):
            _ensure_json(item, f"{path}[{i}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise EncodeError(f"{path}: dict key {key!r} is not a str")
            _ensure_json(item, f"{path}.{key}")
        return
    raise EncodeError(f"{path}: {type(value).__name__} is not JSON-friendly")


def _reject_constant(name: str) -> Any:
    raise DecodeError(f"invalid JSON constant {name}")


class JsonCodec:
    """Codec that encodes values as compact UTF-8 JSON.

    Accepts None, bool, int, float, str, lists and str-keyed dicts. NaN and
    infinite floats are rejected in both directions.
    """

    def validate(self, value: object) -> None:
        """Checks that a value is JSON-friendly.

        Args:
            value: The value to check.

        Raises:
            EncodeError: If `value` holds something JSON cannot represent, or a
                dict with non-str keys.
        """
        try:
            _ensure_json(value, "value")
        except RecursionError as exc:
            raise EncodeError("value is nested too deeply") from exc

    def encode(self, value: object) -> bytes:
        """Serializes a JSON-friendly value to JSON bytes.

        Args:
            value: The value to serialize.

        Returns:
            The UTF-8 encoded JSON document.

        Raises:
            EncodeError: If `value` is not JSON-friendly.
        """
        self.validate(value)
        return json.dumps(value, allow_nan=False, separators=(",", ":")).encode("utf-8")

    def decode(self, data: bytes) -> object:
        """Deserializes JSON bytes into a value.

        Args:
            data: A UTF-8 encoded JSON document.

        Returns:
            The decoded value.

        Raises:
            DecodeError: If `data` is not a valid JSON document.
        """
        try:
            return json.loads(data.decode("utf-8"), parse_constant=_reject_constant)
        except (ValueError, RecursionError) as exc:
            raise DecodeError(f"invalid JSON: {exc}") from exc
