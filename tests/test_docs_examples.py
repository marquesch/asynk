"""The documentation examples run, and each page includes its examples."""

import asyncio
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent

# Each docs page and the example files it includes.
PAGES = {
    "getting-started.md": [
        "getting_started/first_app.py",
        "getting_started/defining_tasks.py",
        "getting_started/call_tasks.py",
    ],
    "guides/scheduled-tasks.md": ["guides/scheduled_tasks.py"],
    "guides/results.md": ["guides/results_store.py"],
    "guides/metadata.md": ["guides/publish_metadata.py"],
    "advanced/lifecycle-hooks.md": ["advanced/lifecycle_hooks.py"],
    "advanced/custom-transport.md": ["advanced/custom_transport.py"],
    "advanced/custom-store.md": ["advanced/custom_store.py"],
}
EXAMPLES = [example for examples in PAGES.values() for example in examples]


@pytest.mark.parametrize("example", EXAMPLES)
async def test_docs_examples_run(example: str) -> None:
    path = ROOT / "examples" / example
    spec = importlib.util.spec_from_file_location(f"_example_{path.stem}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    await asyncio.wait_for(module.main(), 10)


@pytest.mark.parametrize("page", PAGES)
def test_docs_page_includes_its_examples(page: str) -> None:
    text = (ROOT / "docs" / page).read_text()
    missing = [e for e in PAGES[page] if f'--8<-- "examples/{e}"' not in text]
    assert missing == []
