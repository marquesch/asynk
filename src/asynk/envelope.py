from __future__ import annotations

import math
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TypeAlias

JSONValue: TypeAlias = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]


class NonJsonArgumentError(TypeError):
    """Raised when a task argument is not JSON-friendly."""


def ensure_json(value: object, path: str = "value") -> None:
    """Checks that a value is JSON-friendly.

    Args:
        value: The value to check, recursively.
        path: Name used to locate the offending value in the error message.

    Raises:
        NonJsonArgumentError: If `value` contains a non-finite float, a dict with
            non-str keys, or a type JSON cannot represent.
    """
    if value is None or isinstance(value, bool | int | str):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise NonJsonArgumentError(f"{path}: non-finite float {value!r} is not JSON-friendly")
        return
    if isinstance(value, list):
        for i, item in enumerate(value):
            ensure_json(item, f"{path}[{i}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise NonJsonArgumentError(f"{path}: dict key {key!r} is not a str")
            ensure_json(item, f"{path}.{key}")
        return
    raise NonJsonArgumentError(f"{path}: {type(value).__name__} is not JSON-friendly")


@dataclass(frozen=True, slots=True)
class Envelope:
    """The message exchanged between producers and consumers.

    Attributes:
        task: Registered name of the task to run.
        args: Positional arguments for the task.
        kwargs: Keyword arguments for the task.
        id: Unique message id.
        attempt: Zero-based delivery attempt counter.
        metadata: Free-form JSON data carried alongside the task call.
    """

    task: str
    args: tuple[JSONValue, ...]
    kwargs: dict[str, JSONValue]
    id: str
    attempt: int = 0
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        task: str,
        args: Sequence[object],
        kwargs: Mapping[str, object],
        metadata: Mapping[str, object] | None = None,
    ) -> Envelope:
        """Builds a new envelope with a fresh id, validating the arguments.

        Args:
            task: Registered name of the task to run.
            args: Positional arguments for the task.
            kwargs: Keyword arguments for the task.
            metadata: Optional JSON data carried alongside the call.

        Returns:
            A new envelope at attempt 0.

        Raises:
            NonJsonArgumentError: If any argument is not JSON-friendly.
        """
        for i, arg in enumerate(args):
            ensure_json(arg, f"args[{i}]")
        for key, val in kwargs.items():
            if not isinstance(key, str):
                raise NonJsonArgumentError(f"kwargs: key {key!r} is not a str")
            ensure_json(val, f"kwargs.{key}")
        meta = dict(metadata) if metadata is not None else {}
        for key, val in meta.items():
            if not isinstance(key, str):
                raise NonJsonArgumentError(f"metadata: key {key!r} is not a str")
            ensure_json(val, f"metadata.{key}")
        return cls(
            task=task,
            args=tuple(args),  # type: ignore[arg-type]
            kwargs=dict(kwargs),  # type: ignore[arg-type]
            id=str(uuid.uuid4()),
            attempt=0,
            metadata=meta,  # type: ignore[arg-type]
        )

    def retry(self) -> Envelope:
        """Returns a copy of this envelope with `attempt` incremented."""
        return replace(self, attempt=self.attempt + 1)
