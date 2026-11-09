"""The `Producer`, `Consumer` and `Delivery` transport protocols."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Protocol

from asynk._validate import check_number
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
        if self.delay is not None:
            check_number("delay", self.delay)


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
    """A received message, settled exactly once by `ack` or `reject`.

    A second `ack` or `reject` raises `RuntimeError`.
    """

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
    """Receives deliveries from a broker.

    The worker calls `start`, iterates `deliveries`, closes the iterator, then
    calls `stop`. It settles every delivery itself with `Delivery.ack` or
    `Delivery.reject`.
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
