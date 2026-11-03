from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

from asynk.envelope import Envelope


class Producer(Protocol):
    """Sends envelopes to a broker."""

    async def publish(self, envelope: Envelope) -> Any:
        """Publish one envelope; the return value (e.g. a broker receipt) is opaque to asynk."""
        ...


class Delivery(Protocol):
    """One received message, settled exactly once by ack or reject."""

    @property
    def envelope(self) -> Envelope:
        """The decoded envelope carried by this delivery."""
        ...

    @property
    def settled(self) -> bool:
        """True once `ack` or `reject` has completed."""
        ...

    async def ack(self) -> None:
        """Acknowledge the message; the broker will not redeliver it."""
        ...

    async def reject(self, *, requeue: bool = False) -> None:
        """Reject the message; with `requeue=True` the broker redelivers it."""
        ...


class Consumer(Protocol):
    """Receives deliveries from a broker and decides how each one is settled.

    Contract for implementers:

    * `process(delivery)` is entered once per delivery. If the task failed, was
      unknown, or exceeded its retries, the worker raises `TaskFailedError`
      (carrying `.result.status`) inside the block; any other exception means
      something unexpected happened. On a clean exit with an unsettled delivery
      the context manager must settle it (usually ack). On an exception it must
      settle an unsettled delivery (usually reject) and must NOT suppress the
      exception, in particular `asyncio.CancelledError`.
    * On cancellation (shutdown timeout) the worker itself rejects an unsettled
      delivery with `requeue=True` before the cancellation reaches the context
      manager, so the message is redelivered rather than dropped.
    * `deliveries()` may be cancelled at any await point during shutdown. If the
      returned iterator has an `aclose()` method the worker calls it. When the
      iterator ends, the worker drains in-flight tasks and `run()` returns.
    * `stop()` is called after the iterator is closed. Deliveries that are still
      unsettled at that point should be redelivered by the broker.
    """

    @property
    def connected(self) -> bool:
        """Whether the broker connection is healthy; drives the readiness probe."""
        ...

    async def start(self) -> None:
        """Connect; called once before `deliveries()`."""
        ...

    async def stop(self) -> None:
        """Disconnect; called once after the delivery iterator is closed."""
        ...

    def deliveries(self) -> AsyncIterator[Delivery]:
        """Return the stream of incoming deliveries."""
        ...

    def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]:
        """Wrap the handling of one delivery; see the class docstring for the contract."""
        ...
