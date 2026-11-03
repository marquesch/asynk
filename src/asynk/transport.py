from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

from asynk.envelope import Envelope


class Producer(Protocol):
    async def publish(self, envelope: Envelope) -> Any: ...


class Delivery(Protocol):
    @property
    def envelope(self) -> Envelope: ...

    @property
    def settled(self) -> bool: ...

    async def ack(self) -> None: ...

    async def reject(self, *, requeue: bool = False) -> None: ...


class Consumer(Protocol):
    @property
    def connected(self) -> bool: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def deliveries(self) -> AsyncIterator[Delivery]: ...

    def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]: ...
