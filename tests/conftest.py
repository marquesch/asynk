from __future__ import annotations

import pytest
from service_helpers import redis_decision


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skips, or fails when services are required, a `requires_redis` test without a server."""
    if item.get_closest_marker("requires_redis") is None:
        return
    decision = redis_decision()
    if decision.action == "skip":
        pytest.skip(decision.reason)
    if decision.action == "fail":
        pytest.fail(decision.reason, pytrace=False)
