from __future__ import annotations

import math
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TypeAlias

JSONValue: TypeAlias = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]


class NonJsonArgumentError(TypeError):
    pass


def ensure_json(value: object, path: str = "value") -> None:
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
    task: str
    args: tuple[JSONValue, ...]
    kwargs: dict[str, JSONValue]
    id: str
    attempt: int = 0
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @classmethod
    def create(cls, task: str, args: Sequence[object], kwargs: Mapping[str, object]) -> Envelope:
        for i, arg in enumerate(args):
            ensure_json(arg, f"args[{i}]")
        for key, val in kwargs.items():
            if not isinstance(key, str):
                raise NonJsonArgumentError(f"kwargs: key {key!r} is not a str")
            ensure_json(val, f"kwargs.{key}")
        return cls(
            task=task,
            args=tuple(args),  # type: ignore[arg-type]
            kwargs=dict(kwargs),  # type: ignore[arg-type]
            id=str(uuid.uuid4()),
            attempt=0,
        )

    def retry(self) -> Envelope:
        return replace(self, attempt=self.attempt + 1)
