from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

from asynk.envelope import Envelope


class Producer(Protocol):
    """Sends envelopes to a broker."""

    async def publish(self, envelope: Envelope) -> Any:
        """Publishes one envelope.

        Args:
            envelope: The envelope to send.

        Returns:
            An implementation-defined receipt (for example a broker message id).
        """
        ...


class Delivery(Protocol):
    """A received message, settled exactly once by `ack` or `reject`."""

    @property
    def envelope(self) -> Envelope:
        """The decoded envelope carried by this delivery."""
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

    Implementer contract:

    * `process` is entered once per delivery. If the task failed, was unknown, or
      exceeded its retries, the worker raises `TaskFailedError` (with
      `.result.status`) inside the block; any other exception is unexpected. On
      a clean exit the context manager must settle an unsettled delivery
      (usually ack). On an exception it must settle an unsettled delivery
      (usually reject) and must not suppress the exception, notably
      `asyncio.CancelledError`.
    * When a task is cancelled at the shutdown timeout, the worker rejects its
      unsettled delivery with `requeue=True` before the cancellation reaches
      `process`, so the message is redelivered rather than dropped.
    * `deliveries` may be cancelled at any await point during shutdown. If the
      returned iterator has an `aclose()` method the worker calls it. When the
      iterator ends, the worker drains in-flight tasks and `Worker.run` returns.
    * `stop` is called after the iterator is closed. Deliveries still unsettled
      then should be redelivered by the broker.
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
        """Returns the stream of incoming deliveries."""
        ...

    def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]:
        """Wraps the handling of one delivery; see the class docstring for the contract.

        Args:
            delivery: The delivery being handled.

        Returns:
            An async context manager entered around the handling.
        """
        ...
