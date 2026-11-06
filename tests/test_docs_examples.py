"""The getting started examples run, and the page includes each of them."""

import asyncio
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
EXAMPLES = ROOT / "examples" / "getting_started"
NAMES = ["first_app", "defining_tasks", "call_tasks"]


@pytest.mark.parametrize("name", NAMES)
async def test_getting_started_examples_run(name: str) -> None:
    spec = importlib.util.spec_from_file_location(f"_example_{name}", EXAMPLES / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    await asyncio.wait_for(module.main(), 10)


def test_getting_started_page_includes_every_example() -> None:
    page = (ROOT / "docs" / "getting-started.md").read_text()
    missing = [n for n in NAMES if f'--8<-- "examples/getting_started/{n}.py"' not in page]
    assert missing == []
