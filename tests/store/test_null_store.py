from __future__ import annotations

import pytest

from asynk.errors import NotConfiguredError
from asynk.store.null import NullStore


async def test_set_always_reports_written() -> None:
    store = NullStore()
    assert await store.set("k", b"v") is True
    assert await store.set("k", b"v") is True


async def test_delete_reports_nothing_removed() -> None:
    assert await NullStore().delete("k") is False


async def test_get_raises_not_configured() -> None:
    with pytest.raises(NotConfiguredError, match="no store configured"):
        await NullStore().get("k")


async def test_wait_raises_not_configured() -> None:
    with pytest.raises(NotConfiguredError, match="no store configured"):
        await NullStore().wait("k", timeout=0.1)


async def test_start_and_stop_are_noops() -> None:
    store = NullStore()
    await store.start()
    await store.stop()
