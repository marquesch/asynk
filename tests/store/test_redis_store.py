from __future__ import annotations

import asyncio
import importlib
import sys
import time
from pathlib import Path
from typing import Any

import fakeredis
import pytest
import redis.asyncio
from import_guard_helpers import core_without_the_client

import asynk
import asynk.store.redis
from asynk.app import App
from asynk.hooks import TaskStatus
from asynk.results import TaskState
from asynk.store import Store
from asynk.store.redis import RedisStore
from asynk.transport.memory import memory_transport
from asynk.worker import Worker

MESSAGE = "asynk[redis] needs the redis package: pip install 'asynk[redis]'"


class ClosingFake(fakeredis.FakeAsyncRedis):
    closed = 0

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        self.closed += 1
        await super().aclose(close_connection_pool)


@pytest.fixture
def server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


def _store(server: fakeredis.FakeServer, **kwargs: Any) -> RedisStore:
    return RedisStore(client=fakeredis.FakeAsyncRedis(server=server), **kwargs)


@pytest.mark.slow
def test_core_imports_without_the_client_and_the_guard_names_the_extra() -> None:
    result = core_without_the_client()
    assert result["loaded"] == []
    assert result["asynk.store.redis"] == [MESSAGE, "ModuleNotFoundError"]


def test_import_guard_raises_when_the_client_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "redis", None)
    for name in [
        n for n in sys.modules if n == "asynk.store.redis" or n.startswith("asynk.store.redis.")
    ]:
        monkeypatch.delitem(sys.modules, name)
    with pytest.raises(ImportError) as info:
        importlib.import_module("asynk.store.redis")
    assert str(info.value) == MESSAGE


def test_import_guard_reraises_a_failing_dependency_of_the_client(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    package = tmp_path / "redis"
    package.mkdir()
    (package / "__init__.py").write_text("import not_a_real_redis_dependency\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    for name in [
        n
        for n in sys.modules
        if n in ("redis", "asynk.store.redis") or n.startswith(("redis.", "asynk.store.redis."))
    ]:
        monkeypatch.delitem(sys.modules, name)
    with pytest.raises(ImportError) as info:
        importlib.import_module("asynk.store.redis")
    assert info.value.name == "not_a_real_redis_dependency"
    assert str(info.value) != MESSAGE


@pytest.mark.slow
def test_core_modules_do_not_import_the_client() -> None:
    assert core_without_the_client()["loaded"] == []


def test_package_exports_the_store() -> None:
    import asynk.store.redis as package
    import asynk.store.redis.store as module

    assert package.__all__ == ["RedisStore"]
    assert package.RedisStore is module.RedisStore
    assert not hasattr(asynk, "RedisStore")
    assert "RedisStore" not in asynk.__all__


def test_url_and_client_are_exclusive(server: fakeredis.FakeServer) -> None:
    with pytest.raises(ValueError, match="exactly one of url and client"):
        RedisStore("redis://localhost:6379/0", client=fakeredis.FakeAsyncRedis(server=server))
    with pytest.raises(ValueError, match="exactly one of url and client"):
        RedisStore()


@pytest.mark.parametrize("ttl", [0.0, -1.0])
def test_ttl_must_be_positive(server: fakeredis.FakeServer, ttl: float) -> None:
    with pytest.raises(ValueError, match="ttl must be > 0"):
        _store(server, ttl=ttl)


@pytest.mark.parametrize("interval", [0.0, -1.0])
def test_max_poll_interval_must_be_positive(server: fakeredis.FakeServer, interval: float) -> None:
    with pytest.raises(ValueError, match="max_poll_interval must be > 0"):
        _store(server, max_poll_interval=interval)


async def test_stop_leaves_an_injected_client_open(server: fakeredis.FakeServer) -> None:
    client = fakeredis.FakeAsyncRedis(server=server)
    store = RedisStore(client=client)
    await store.start()
    await store.stop()
    await store.stop()
    assert await client.ping() is True


async def test_stop_closes_an_owned_client_each_time_it_is_used(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = ClosingFake()
    calls: list[dict[str, Any]] = []

    def from_url(cls: Any, /, url: str, **kwargs: Any) -> ClosingFake:
        calls.append({"url": url, **kwargs})
        return fake

    monkeypatch.setattr(redis.asyncio.Redis, "from_url", classmethod(from_url))
    store = RedisStore("redis://example:6379/0")
    assert calls == [
        {"url": "redis://example:6379/0", "socket_timeout": 5.0, "socket_connect_timeout": 5.0}
    ]
    await store.start()
    assert fake.closed == 0
    await store.stop()
    assert fake.closed == 1
    await store.start()
    await store.stop()
    assert fake.closed == 2


async def test_get_missing_returns_none(server: fakeredis.FakeServer) -> None:
    assert await _store(server).get("k") is None


async def test_set_writes_and_returns_true(server: fakeredis.FakeServer) -> None:
    store = _store(server)
    assert await store.set("k", b"v") is True
    assert await store.get("k") == b"v"


async def test_set_without_overwrite_keeps_existing_and_returns_false(
    server: fakeredis.FakeServer,
) -> None:
    store = _store(server)
    await store.set("k", b"one")
    assert await store.set("k", b"two") is False
    assert await store.get("k") == b"one"


async def test_set_overwrite_replaces_and_returns_true(server: fakeredis.FakeServer) -> None:
    store = _store(server)
    await store.set("k", b"one")
    assert await store.set("k", b"two", overwrite=True) is True
    assert await store.get("k") == b"two"


async def test_delete_existing_key_removes_it_and_returns_true(
    server: fakeredis.FakeServer,
) -> None:
    store = _store(server)
    await store.set("k", b"v")
    assert await store.delete("k") is True
    assert await store.get("k") is None
    assert await store.set("k", b"v") is True


async def test_delete_missing_key_returns_false(server: fakeredis.FakeServer) -> None:
    assert await _store(server).delete("k") is False


async def test_keys_are_prefixed(server: fakeredis.FakeServer) -> None:
    await _store(server).set("k", b"v")
    assert await fakeredis.FakeAsyncRedis(server=server).get("asynk:k") == b"v"


async def test_prefixes_isolate_stores(server: fakeredis.FakeServer) -> None:
    a, b = _store(server, prefix="asynk:a:"), _store(server, prefix="asynk:b:")
    assert await a.set("k", b"1") is True
    assert await b.set("k", b"2") is True
    assert await a.get("k") == b"1"
    assert await b.get("k") == b"2"


@pytest.mark.parametrize("overwrite", [False, True])
async def test_set_applies_the_ttl_in_milliseconds(
    server: fakeredis.FakeServer, overwrite: bool
) -> None:
    await _store(server, ttl=10.0).set("k", b"v", overwrite=overwrite)
    assert 9000 < await fakeredis.FakeAsyncRedis(server=server).pttl("asynk:k") <= 10000


async def test_overwrite_resets_the_ttl(server: fakeredis.FakeServer) -> None:
    store = _store(server, ttl=10.0)
    raw = fakeredis.FakeAsyncRedis(server=server)
    await store.set("k", b"v")
    await raw.pexpire("asynk:k", 500)
    assert await raw.pttl("asynk:k") <= 500
    await store.set("k", b"w", overwrite=True)
    assert await raw.pttl("asynk:k") > 9900


async def test_concurrent_claims_from_two_stores_have_one_winner(
    server: fakeredis.FakeServer,
) -> None:
    a, b = _store(server), _store(server)
    results = await asyncio.gather(*(s.set("claim:x", b"1") for s in [a, b] * 10))
    assert results.count(True) == 1


async def test_client_decoding_responses_is_rejected_on_get(
    server: fakeredis.FakeServer,
) -> None:
    store = RedisStore(client=fakeredis.FakeAsyncRedis(server=server, decode_responses=True))
    await store.set("k", b"v")
    with pytest.raises(TypeError, match="decode_responses=False"):
        await store.get("k")


async def test_unreachable_server_raises_redis_error() -> None:
    store = RedisStore("redis://127.0.0.1:1/0")
    with pytest.raises(redis.exceptions.ConnectionError):
        await asyncio.wait_for(store.get("k"), 5)
    await store.stop()


async def test_worker_with_unreachable_redis_still_succeeds() -> None:
    producer, consumer = memory_transport()
    store = RedisStore("redis://127.0.0.1:1/0")
    app = App("t", producer=producer, consumer=consumer, store=store)

    @app.task(name="add")
    async def add(a: int, b: int) -> int:
        return a + b

    await add.delay(1, 2)
    delivery: Any = await asyncio.wait_for(anext(consumer.deliveries()), 5)
    result = await asyncio.wait_for(Worker(app).handle(delivery), 10)
    assert result is not None
    assert result.status is TaskStatus.SUCCESS
    assert delivery.settled
    await store.stop()


@pytest.mark.parametrize("call", ["set", "delete"])
async def test_unreachable_server_raises_redis_error_on_writes(call: str) -> None:
    store = RedisStore("redis://127.0.0.1:1/0")
    op = store.set("k", b"v") if call == "set" else store.delete("k")
    with pytest.raises(redis.exceptions.ConnectionError):
        await asyncio.wait_for(op, 5)
    await store.stop()


async def test_expired_key_is_not_returned_and_can_be_set_again(
    server: fakeredis.FakeServer,
) -> None:
    store = _store(server, ttl=0.02)
    await store.set("k", b"old")
    await asyncio.sleep(0.04)
    assert await store.get("k") is None
    assert await store.set("k", b"new") is True


async def test_delete_expired_key_returns_false(server: fakeredis.FakeServer) -> None:
    store = _store(server, ttl=0.02)
    await store.set("k", b"v")
    await asyncio.sleep(0.04)
    assert await store.delete("k") is False


async def test_wait_returns_existing_value(server: fakeredis.FakeServer) -> None:
    store = _store(server)
    await store.set("k", b"v")
    assert await store.wait("k") == b"v"


async def test_wait_returns_value_set_later_by_another_store(
    server: fakeredis.FakeServer,
) -> None:
    reader, writer = _store(server, max_poll_interval=0.01), _store(server)
    waiter = asyncio.create_task(reader.wait("k"))
    await asyncio.sleep(0.03)
    await writer.set("k", b"v")
    assert await asyncio.wait_for(waiter, 1) == b"v"


async def test_wait_times_out_with_none(server: fakeredis.FakeServer) -> None:
    assert await _store(server).wait("k", timeout=0.05) is None


async def test_wait_never_sleeps_past_the_timeout(server: fakeredis.FakeServer) -> None:
    store = _store(server, max_poll_interval=10.0)
    start = time.monotonic()
    assert await store.wait("k", timeout=0.05) is None
    assert time.monotonic() - start < 0.5


async def test_wait_with_zero_timeout_reads_once(server: fakeredis.FakeServer) -> None:
    store = _store(server)
    assert await store.wait("k", timeout=0) is None
    await store.set("k", b"v")
    assert await store.wait("k", timeout=0) == b"v"


async def test_wait_ignores_expired_value(server: fakeredis.FakeServer) -> None:
    store = _store(server, ttl=0.02)
    await store.set("k", b"v")
    await asyncio.sleep(0.04)
    assert await store.wait("k", timeout=0.05) is None


async def test_wait_cancellation_propagates(server: fakeredis.FakeServer) -> None:
    waiter = asyncio.create_task(_store(server).wait("k"))
    await asyncio.sleep(0.01)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter


async def test_handle_reads_a_result_written_through_another_store(
    server: fakeredis.FakeServer,
) -> None:
    producer, consumer = memory_transport()
    app_a = App(
        "t", producer=producer, consumer=consumer, store=_store(server, max_poll_interval=0.01)
    )
    app_b = App(
        "t", producer=producer, consumer=consumer, store=_store(server, max_poll_interval=0.01)
    )

    @app_a.task(name="add")
    async def add(a: int, b: int) -> int:
        return a + b

    handle = await add.delay(1, 2)
    delivery: Any = await asyncio.wait_for(anext(consumer.deliveries()), 5)
    await Worker(app_a).handle(delivery)
    reader = app_b.get_task(handle.task_id)
    assert await reader.result(timeout=1) == 3
    assert await reader.state() is TaskState.SUCCESS


def test_redis_store_satisfies_the_store_protocol(server: fakeredis.FakeServer) -> None:
    store: Store = _store(server)
    assert store is not None
