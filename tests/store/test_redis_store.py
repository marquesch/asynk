from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path
from typing import Any

import fakeredis
import pytest
import redis.asyncio

import asynk
import asynk.store.redis
from asynk.store.redis import RedisStore

MESSAGE = "RedisStore needs the redis extra: pip install 'asynk[redis]'"


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


def test_core_imports_without_the_client_and_the_guard_names_the_extra() -> None:
    code = (
        "import sys; sys.modules['redis'] = None\n"
        "import asynk, asynk.store, asynk.store.memory, asynk.worker, asynk.scheduler\n"
        "assert not [m for m in sys.modules if m.startswith('redis') and sys.modules[m]]\n"
        "try:\n    import asynk.store.redis\n"
        "except ImportError as exc:\n    print(exc)\n    print(type(exc.__cause__).__name__)\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.splitlines() == [MESSAGE, "ModuleNotFoundError"]


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


def test_core_modules_do_not_import_the_client() -> None:
    code = (
        "import sys\n"
        "import asynk, asynk.cli, asynk.store, asynk.worker, asynk.scheduler\n"
        "import asynk.transport.memory\n"
        "print(sorted(m for m in sys.modules if m == 'redis' or m.startswith('redis.')))\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "[]"


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


async def test_stop_closes_an_owned_client_once(monkeypatch: pytest.MonkeyPatch) -> None:
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
    await store.stop()
    assert fake.closed == 1
