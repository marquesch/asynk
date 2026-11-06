"""The API reference page covers the whole public surface."""

from pathlib import Path

import asynk

REFERENCE = Path(__file__).parent.parent / "docs" / "reference.md"


def test_reference_page_lists_the_redis_store() -> None:
    lines = {line.rstrip() for line in REFERENCE.read_text().splitlines()}
    assert "::: asynk.store.redis.RedisStore" in lines


def test_reference_page_lists_every_public_name() -> None:
    lines = {line.rstrip() for line in REFERENCE.read_text().splitlines()}
    missing = [
        name for name in asynk.__all__ if name != "__version__" and f"::: asynk.{name}" not in lines
    ]
    assert missing == []
