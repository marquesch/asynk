from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace


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

        Values are not validated; the codec checks what it can encode.

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

    def retry(self) -> Envelope:
        """Returns a copy of this envelope with `attempt` incremented."""
        return replace(self, attempt=self.attempt + 1)
