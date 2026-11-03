from __future__ import annotations

import json
from typing import Any

from asynk.codec import DecodeError
from asynk.envelope import Envelope, JSONValue, NonJsonArgumentError, ensure_json


def _reject_constant(name: str) -> Any:
    raise DecodeError(f"invalid JSON constant {name}")


class JsonCodec:
    """Codec that encodes envelopes as compact UTF-8 JSON.

    NaN and infinite floats are rejected in both directions.
    """

    def encode(self, envelope: Envelope) -> bytes:
        """Serializes an envelope to JSON bytes.

        Args:
            envelope: The envelope to serialize.

        Returns:
            The UTF-8 encoded JSON document.
        """
        payload = {
            "id": envelope.id,
            "task": envelope.task,
            "args": list(envelope.args),
            "kwargs": envelope.kwargs,
            "attempt": envelope.attempt,
            "metadata": envelope.metadata,
        }
        return json.dumps(payload, allow_nan=False, separators=(",", ":")).encode("utf-8")

    def decode(self, data: bytes) -> Envelope:
        """Deserializes JSON bytes into an envelope.

        Args:
            data: A UTF-8 encoded JSON document.

        Returns:
            The decoded envelope.

        Raises:
            DecodeError: If `data` is not valid JSON or lacks a well-formed envelope.
        """
        try:
            obj = json.loads(data.decode("utf-8"), parse_constant=_reject_constant)
        except (ValueError, RecursionError) as exc:
            raise DecodeError(f"invalid JSON: {exc}") from exc
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

        try:
            ensure_json(args, "args")
            ensure_json(kwargs, "kwargs")
            ensure_json(metadata, "metadata")
        except (NonJsonArgumentError, RecursionError) as exc:
            raise DecodeError(str(exc)) from exc

        typed_args: tuple[JSONValue, ...] = tuple(args)
        return Envelope(
            task=task,
            args=typed_args,
            kwargs=kwargs,
            id=id_,
            attempt=attempt,
            metadata=metadata,
        )
