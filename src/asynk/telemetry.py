"""The `Telemetry` interface, the `NullTelemetry` null object and how an app picks one."""

from __future__ import annotations

import importlib
import logging
import os
from typing import Protocol, runtime_checkable

from asynk.envelope import Envelope
from asynk.hooks import Middleware

logger = logging.getLogger("asynk")


@runtime_checkable
class Telemetry(Protocol):
    """Tracing and metrics hooks an app calls at fixed points.

    An implementation must not raise: telemetry never breaks publishing or tasks.

    Attributes:
        layer: Middleware a worker runs outside every layer of the app, or None.
    """

    layer: Middleware | None

    def inject(self, metadata: dict[str, object]) -> None:
        """Adds trace context to the metadata of an envelope about to be published.

        Keys already in `metadata` are kept.

        Args:
            metadata: The metadata of the envelope; changed in place.
        """
        ...

    def message_published(self, envelope: Envelope) -> None:
        """Records that an envelope was published.

        Args:
            envelope: The envelope that was sent.
        """
        ...

    def task_started(self) -> None:
        """Records that a worker began handling a delivery."""
        ...

    def task_finished(self) -> None:
        """Records that a worker finished handling a delivery, however it ended."""
        ...

    def retry_published(self, envelope: Envelope) -> None:
        """Records that a worker published a retry.

        Args:
            envelope: The envelope of the attempt that is being retried.
        """
        ...


class NullTelemetry:
    """Telemetry that does nothing; the one an app uses when OpenTelemetry is off.

    Attributes:
        layer: Always None.
    """

    layer: Middleware | None = None

    def inject(self, metadata: dict[str, object]) -> None:
        """Does nothing."""

    def message_published(self, envelope: Envelope) -> None:
        """Does nothing."""

    def task_started(self) -> None:
        """Does nothing."""

    def task_finished(self) -> None:
        """Does nothing."""

    def retry_published(self, envelope: Envelope) -> None:
        """Does nothing."""


def select_telemetry() -> Telemetry:
    """Picks the telemetry component for a new app.

    Returns:
        An `OtelTelemetry` when `opentelemetry-api` is installed and the
        `OTEL_SDK_DISABLED` environment variable is not `true`; a `NullTelemetry`
        otherwise, or if the OpenTelemetry one cannot be built.
    """
    if os.environ.get("OTEL_SDK_DISABLED", "").strip().lower() == "true":
        return NullTelemetry()
    try:
        # By name: the core must not import OpenTelemetry names, and `asynk.otel` imports it.
        telemetry: Telemetry = importlib.import_module("asynk.otel").OtelTelemetry()
        return telemetry
    except ImportError:
        return NullTelemetry()
    except Exception:
        logger.exception("could not set up OpenTelemetry; telemetry is off")
        return NullTelemetry()
