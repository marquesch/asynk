from __future__ import annotations

import json
import math
from typing import Any

from asynk.codec import DecodeError, EncodeError
from asynk.envelope import Envelope


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
    """Codec that encodes envelopes as compact UTF-8 JSON.

    Accepts None, bool, int, float, str, lists and str-keyed dicts. NaN and
    infinite floats are rejected in both directions.
    """

    def validate(self, envelope: Envelope) -> None:
        """Checks that an envelope's values are JSON-friendly.

        Args:
            envelope: The envelope to check.

        Raises:
            EncodeError: If `args`, `kwargs` or `metadata` hold a value JSON cannot
                represent, or a dict with non-str keys.
        """
        try:
            for i, arg in enumerate(envelope.args):
                _ensure_json(arg, f"args[{i}]")
            for name, containers in (("kwargs", envelope.kwargs), ("metadata", envelope.metadata)):
                for key, val in containers.items():
                    if not isinstance(key, str):
                        raise EncodeError(f"{name}: key {key!r} is not a str")
                    _ensure_json(val, f"{name}.{key}")
        except RecursionError as exc:
            raise EncodeError("value is nested too deeply") from exc

    def encode(self, envelope: Envelope) -> bytes:
        """Serializes an envelope to JSON bytes.

        Args:
            envelope: The envelope to serialize.

        Returns:
            The UTF-8 encoded JSON document.

        Raises:
            EncodeError: If the envelope holds a value JSON cannot represent.
        """
        self.validate(envelope)
        payload = {
            "id": envelope.id,
            "task": envelope.task,
            "args": list(envelope.args),
            "kwargs": envelope.kwargs,
            "attempt": envelope.attempt,
            "metadata": envelope.metadata,
        }
        return json.dumps(payload, allow_nan=False, separators=(",", ":")).encode("utf-8")

    def encode_value(self, value: object) -> bytes:
        """Serializes a JSON-friendly value to JSON bytes.

        Args:
            value: The value to serialize.

        Returns:
            The UTF-8 encoded JSON document.

        Raises:
            EncodeError: If `value` is not JSON-friendly.
        """
        try:
            _ensure_json(value, "value")
        except RecursionError as exc:
            raise EncodeError("value is nested too deeply") from exc
        return json.dumps(value, allow_nan=False, separators=(",", ":")).encode("utf-8")

    def decode_value(self, data: bytes) -> object:
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

    def decode(self, data: bytes) -> Envelope:
        """Deserializes JSON bytes into an envelope.

        Args:
            data: A UTF-8 encoded JSON document.

        Returns:
            The decoded envelope.

        Raises:
            DecodeError: If `data` is not valid JSON or lacks a well-formed envelope.
        """
        obj = self.decode_value(data)
        if not isinstance(obj, dict):
            raise DecodeError("envelope must be a JSON object")

        task = obj.get("task")
        id_ = obj.get("id")
        args = obj.get("args")
        kwargs = obj.get("kwargs")
        attempt = obj.get("attempt")
        metadata = obj.get("metadata")

        if not isinstance(task, str):
            raise DecodeError("'task' must be a string")
        if not isinstance(id_, str):
            raise DecodeError("'id' must be a string")
        if not isinstance(args, list):
            raise DecodeError("'args' must be a list")
        if not isinstance(kwargs, dict):
            raise DecodeError("'kwargs' must be an object")
        if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 0:
            raise DecodeError("'attempt' must be a non-negative integer")
        if not isinstance(metadata, dict):
            raise DecodeError("'metadata' must be an object")

        envelope = Envelope(
            task=task,
            args=tuple(args),
            kwargs=kwargs,
            id=id_,
            attempt=attempt,
            metadata=metadata,
        )
        try:
            self.validate(envelope)
        except EncodeError as exc:
            raise DecodeError(str(exc)) from exc
        return envelope
