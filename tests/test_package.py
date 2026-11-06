from importlib.metadata import metadata, version
from importlib.resources import files

import asynk


def test_package_version() -> None:
    assert version("asynk") == "0.1.0"


def test_dunder_version_matches_installed_distribution() -> None:
    assert asynk.__version__ == version("asynk")
    assert "__version__" in asynk.__all__


def test_py_typed_marker_exists() -> None:
    assert files("asynk").joinpath("py.typed").is_file()


def test_license_metadata() -> None:
    meta = metadata("asynk")
    assert meta["License-Expression"] == "MIT"
