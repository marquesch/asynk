from importlib.metadata import version
from importlib.resources import files


def test_package_version() -> None:
    assert version("asynk") == "0.1.0"


def test_py_typed_marker_exists() -> None:
    assert files("asynk").joinpath("py.typed").is_file()
