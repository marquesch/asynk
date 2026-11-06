from __future__ import annotations

import sys
from pathlib import Path

import pytest

from asynk._extras import require_extra


def test_present_package_imports_silently() -> None:
    require_extra("json", "anything")


def test_missing_package_names_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "not_installed_pkg", None)
    with pytest.raises(ImportError) as info:
        require_extra("not_installed_pkg", "fancy")
    assert str(info.value) == (
        "asynk[fancy] needs the not_installed_pkg package: pip install 'asynk[fancy]'"
    )
    assert isinstance(info.value.__cause__, ImportError)


def test_failing_dependency_of_the_package_is_reraised(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "broken_pkg.py").write_text("import not_a_real_dependency\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    with pytest.raises(ImportError) as info:
        require_extra("broken_pkg", "fancy")
    assert info.value.name == "not_a_real_dependency"
