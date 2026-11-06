from __future__ import annotations

import fakeredis
import pytest

from asynk._redis import RedisConnection


class _Spy(fakeredis.FakeAsyncRedis):
    closed = False

    async def aclose(self, *args: object, **kwargs: object) -> None:
        self.closed = True
        await super().aclose()


@pytest.mark.parametrize("args", [(None, None), ("redis://x", _Spy())])
def test_exactly_one_of_url_and_client_is_required(args: tuple[str | None, _Spy | None]) -> None:
    with pytest.raises(ValueError, match="Thing needs exactly one of url and client"):
        RedisConnection(*args, owner="Thing")


async def test_a_given_client_is_used_and_left_open() -> None:
    client = _Spy()
    connection = RedisConnection(None, client, owner="Thing")
    assert connection.client is client
    await connection.close()
    assert not client.closed
    await client.aclose()


async def test_a_client_made_from_the_url_is_closed() -> None:
    connection = RedisConnection("redis://localhost:1/0", None, owner="Thing")
    kwargs = connection.client.connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] == 5.0
    assert kwargs["socket_connect_timeout"] == 5.0
    spy = _Spy()
    connection.client = spy
    await connection.close()
    assert spy.closed


def test_importing_it_without_the_client_names_the_extra() -> None:
    import subprocess
    import sys

    code = "import sys; sys.modules['redis'] = None\nimport asynk._redis\n"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert "pip install 'asynk[redis]'" in out.stderr
