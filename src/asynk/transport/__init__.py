"""The `Producer`, `Consumer` and `Delivery` transport protocols."""

from __future__ import annotations

import math
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Protocol

from asynk.envelope import Envelope


@dataclass(frozen=True)
class PublishOptions:
    """Instructions for publishing one message.

    Attributes:
        delay: Seconds the broker holds the message before delivering it; None
            delivers it now.
    """

    delay: float | None = None

    def __post_init__(self) -> None:
        """Validates the options.

        Raises:
            TypeError: If `delay` is not a number.
            ValueError: If `delay` is negative, NaN or infinite.
        """
        delay = self.delay
        if delay is None:
            return
        if isinstance(delay, bool) or not isinstance(delay, (int, float)):
            raise TypeError(f"delay must be a number of seconds, got {delay!r}")
        if not math.isfinite(delay) or delay < 0:
            raise ValueError(f"delay must be a finite number >= 0, got {delay!r}")


class Producer(Protocol):
    """Sends encoded messages to a broker."""

    async def publish(
        self, envelope: Envelope, body: bytes, options: PublishOptions | None = None
    ) -> Any:
        """Publishes one message.

        Args:
            envelope: The envelope the message carries, for routing and broker
                metadata such as its id.
            body: The encoded message to send.
            options: Publish options such as a delay; a producer applies the
                ones its broker supports.

        Returns:
            An implementation-defined receipt (for example a broker message id).
        """
        ...


class Delivery(Protocol):
    """A received message, settled exactly once by `ack` or `reject`."""

    @property
    def body(self) -> bytes:
        """The message bytes, as given to `Producer.publish`."""
        ...

    @property
    def settled(self) -> bool:
        """Whether `ack` or `reject` has completed."""
        ...

    async def ack(self) -> None:
        """Acknowledges the message so the broker does not redeliver it."""
        ...

    async def reject(self, *, requeue: bool = False) -> None:
        """Rejects the message.

        Args:
            requeue: If True, the broker redelivers the message.
        """
        ...


class Consumer(Protocol):
    """Receives deliveries from a broker and decides how each is settled.

    The worker calls `start`, iterates `deliveries`, closes the iterator, then
    calls `stop`. Each delivery is handled inside `process(delivery)`. When a
    task failed, was unknown or exceeded its retries and the delivery is still
    unsettled, the worker raises `DeliveryFailedError` inside that block. On exit,
    `process` settles a delivery that is still unsettled (usually ack on a
    clean exit, reject on an exception) and re-raises the exception.
    """

    @property
    def connected(self) -> bool:
        """Whether the broker connection is healthy; drives the readiness probe."""
        ...

    async def start(self) -> None:
        """Connects to the broker; called once before `deliveries`."""
        ...

    async def stop(self) -> None:
        """Disconnects from the broker; called once after the iterator is closed."""
        ...

    def deliveries(self) -> AsyncIterator[Delivery]:
        """Returns the stream of incoming deliveries, one per received message."""
        ...

    def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]:
        """Wraps the handling of one delivery; see the class docstring for the contract.

        Args:
            delivery: The delivery being handled.

        Returns:
            An async context manager entered around the handling.
        """
        ...
