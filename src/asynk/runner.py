from __future__ import annotations

import abc
from typing import TYPE_CHECKING, ClassVar, Literal, TypeAlias

if TYPE_CHECKING:
    from asynk.app import App

Role: TypeAlias = Literal["worker", "scheduler"]
"""Kind of asynk process: "worker" or "scheduler"."""


class Runner(abc.ABC):
    """An asynk process: a `Worker` or a `Scheduler`.

    Lifecycle hooks receive the runner they run in, so one hook can serve every
    asynk process and branch on `role` where it must differ.

    Attributes:
        app: The app being served.
        role: Which kind of process this is, "worker" or "scheduler".
    """

    app: App
    role: ClassVar[Role]

    @abc.abstractmethod
    def request_shutdown(self) -> None:
        """Asks `run` to stop; idempotent and safe in signal handlers."""
