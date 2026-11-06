from __future__ import annotations

import pytest
from service_helpers import REQUIRE_ENV, URL_ENV, Decision, decide_redis, redis_decision


def _up(url: str) -> bool:
    return True


def _down(url: str) -> bool:
    return False


def test_runs_when_url_set_and_reachable() -> None:
    assert decide_redis("redis://h", _up, require=False) == Decision("run", "")


@pytest.mark.parametrize("require, action", [(False, "skip"), (True, "fail")])
def test_unset_url_skips_or_fails(require: bool, action: str) -> None:
    decision = decide_redis("", _up, require=require)
    assert decision.action == action
    assert URL_ENV in decision.reason


@pytest.mark.parametrize("require, action", [(False, "skip"), (True, "fail")])
def test_unreachable_server_skips_or_fails(require: bool, action: str) -> None:
    decision = decide_redis("redis://h", _down, require=require)
    assert decision.action == action
    assert "redis://h" in decision.reason


def test_unset_url_does_not_probe() -> None:
    def boom(url: str) -> bool:
        raise AssertionError("probed")

    assert decide_redis("", boom, require=False).action == "skip"


@pytest.mark.parametrize("value, require", [("1", True), ("", False), ("0", False)])
def test_environment_drives_the_decision(
    monkeypatch: pytest.MonkeyPatch, value: str, require: bool
) -> None:
    monkeypatch.delenv(URL_ENV, raising=False)
    monkeypatch.setenv(REQUIRE_ENV, value)
    assert redis_decision().action == ("fail" if require else "skip")


def test_unreachable_address_is_probed_quickly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(URL_ENV, "redis://127.0.0.1:1/0")
    monkeypatch.delenv(REQUIRE_ENV, raising=False)
    assert redis_decision().action == "skip"
