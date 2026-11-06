import tomllib
from importlib.metadata import metadata, version
from importlib.resources import files
from pathlib import Path

import asynk


def test_package_version_matches_pyproject() -> None:
    pyproject = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    assert version("asynk") == pyproject["project"]["version"]


def test_dunder_version_matches_installed_distribution() -> None:
    assert asynk.__version__ == version("asynk")
    assert "__version__" in asynk.__all__


def test_py_typed_marker_exists() -> None:
    assert files("asynk").joinpath("py.typed").is_file()


def test_license_metadata() -> None:
    meta = metadata("asynk")
    assert meta["License-Expression"] == "MIT"
