"""Shared skip/fail logic for tests that need a real service."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import redis

URL_ENV = "ASYNK_TEST_REDIS_URL"
REQUIRE_ENV = "ASYNK_TEST_REQUIRE_SERVICES"


@dataclass(frozen=True)
class Decision:
    """What to do with a test that needs a real service."""

    action: Literal["run", "skip", "fail"]
    reason: str


def redis_reachable(url: str) -> bool:
    """Returns whether a Redis server answers PING at `url` within half a second."""
    client = redis.Redis.from_url(url, socket_connect_timeout=0.5, socket_timeout=0.5)
    try:
        return bool(client.ping())
    except redis.RedisError:
        return False
    finally:
        client.close()


def decide_redis(url: str, reachable: Callable[[str], bool], require: bool) -> Decision:
    """Decides whether a test needing Redis runs, skips or fails.

    Args:
        url: The server URL, empty when not configured.
        reachable: Probe returning whether the server at a URL answers.
        require: Whether a missing server is a failure instead of a skip.
    """
    if not url:
        reason = f"{URL_ENV} is not set"
    elif not reachable(url):
        reason = f"no Redis server answers at {url}"
    else:
        return Decision("run", "")
    return Decision("fail" if require else "skip", reason)


def redis_url() -> str:
    """Returns the Redis URL from the environment, empty when unset."""
    return os.environ.get(URL_ENV, "")


def redis_decision() -> Decision:
    """Decides for the current environment."""
    return decide_redis(redis_url(), redis_reachable, require=os.environ.get(REQUIRE_ENV) == "1")
