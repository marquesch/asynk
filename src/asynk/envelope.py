"""The `Envelope` exchanged between producers and consumers."""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace


class InvalidEnvelopeError(ValueError):
    """Raised when data is not a valid envelope."""


@dataclass(frozen=True, slots=True)
class Envelope:
    """The message exchanged between producers and consumers.

    Attributes:
        task: Registered name of the task to run.
        args: Positional arguments for the task.
        kwargs: Keyword arguments for the task.
        id: Unique message id.
        attempt: Zero-based delivery attempt counter.
        metadata: Free-form data carried alongside the task call.
    """

    task: str
    args: tuple[object, ...]
    kwargs: dict[str, object]
    id: str
    attempt: int = 0
    metadata: dict[str, object] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        task: str,
        args: Sequence[object],
        kwargs: Mapping[str, object],
        metadata: Mapping[str, object] | None = None,
    ) -> Envelope:
        """Builds a new envelope with a fresh id.

        Args:
            task: Registered name of the task to run.
            args: Positional arguments for the task.
            kwargs: Keyword arguments for the task.
            metadata: Optional data carried alongside the call.

        Returns:
            A new envelope at attempt 0.
        """
        return cls(
            task=task,
            args=tuple(args),
            kwargs=dict(kwargs),
            id=str(uuid.uuid4()),
            attempt=0,
            metadata=dict(metadata) if metadata is not None else {},
        )

    @classmethod
    def from_dict(cls, data: object) -> Envelope:
        """Builds an envelope from the mapping produced by `to_dict`.

        Args:
            data: A mapping with the keys `id`, `task`, `args`, `kwargs`,
                `attempt` and `metadata`.

        Returns:
            The envelope the mapping describes.

        Raises:
            InvalidEnvelopeError: If `data` is not a mapping or a key is missing or has
                the wrong type.
        """
        if not isinstance(data, Mapping):
            raise InvalidEnvelopeError("envelope must be a mapping")

        task = data.get("task")
        id_ = data.get("id")
        args = data.get("args")
        kwargs = data.get("kwargs")
        attempt = data.get("attempt")
        metadata = data.get("metadata")

        if not isinstance(task, str):
            raise InvalidEnvelopeError("'task' must be a string")
        if not isinstance(id_, str):
            raise InvalidEnvelopeError("'id' must be a string")
        if not isinstance(args, list):
            raise InvalidEnvelopeError("'args' must be a list")
        if not isinstance(kwargs, dict):
            raise InvalidEnvelopeError("'kwargs' must be an object")
        if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 0:
            raise InvalidEnvelopeError("'attempt' must be a non-negative integer")
        if not isinstance(metadata, dict):
            raise InvalidEnvelopeError("'metadata' must be an object")

        return cls(
            task=task,
            args=tuple(args),
            kwargs=dict(kwargs),
            id=id_,
            attempt=attempt,
            metadata=dict(metadata),
        )

    def to_dict(self) -> dict[str, object]:
        """Returns the envelope as a mapping of plain values.

        Returns:
            A dict with the keys `id`, `task`, `args` (a list), `kwargs`,
            `attempt` and `metadata`, accepted by `from_dict`.
        """
        return {
            "id": self.id,
            "task": self.task,
            "args": list(self.args),
            "kwargs": dict(self.kwargs),
            "attempt": self.attempt,
            "metadata": dict(self.metadata),
        }

    def retry(self) -> Envelope:
        """Returns a copy of this envelope with `attempt` incremented."""
        return replace(self, attempt=self.attempt + 1)
