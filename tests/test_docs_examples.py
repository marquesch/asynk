"""The documentation examples run, and each page includes its examples."""

import asyncio
import importlib.util
import re
from pathlib import Path
from types import ModuleType

import pytest

from asynk.store.redis import RedisStore

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
    "advanced/middleware.md": [
        "advanced/middleware_minimal.py",
        "advanced/middleware_order.py",
        "advanced/middleware_veto.py",
        "advanced/middleware_metrics.py",
    ],
    "advanced/custom-transport.md": ["advanced/custom_transport.py"],
    "advanced/custom-store.md": ["advanced/custom_store.py"],
}
EXAMPLES = [example for examples in PAGES.values() for example in examples]


def _load(example: str) -> ModuleType:
    path = ROOT / "examples" / example
    spec = importlib.util.spec_from_file_location(f"_example_{path.stem}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("example", EXAMPLES)
async def test_docs_examples_run(example: str) -> None:
    await asyncio.wait_for(_load(example).main(), 10)


async def test_middleware_order_example_runs_the_last_registered_layer_outermost() -> None:
    module = _load("advanced/middleware_order.py")
    await asyncio.wait_for(module.main(), 10)
    assert module.order == ["second: in", "first: in", "task", "first: out", "second: out"]


async def test_middleware_metrics_example_counts_and_times_each_task() -> None:
    metrics = await asyncio.wait_for(_load("advanced/middleware_metrics.py").main(), 10)
    assert dict(metrics.counts) == {
        ("add", "success"): 2,
        ("cleanup", "skipped"): 1,
        ("divide", "retries_exceeded"): 1,
    }
    assert set(metrics.seconds) == {"add", "cleanup", "divide"}
    assert all(seconds >= 0 for seconds in metrics.seconds.values())


@pytest.mark.parametrize("page", PAGES)
def test_docs_page_includes_its_examples(page: str) -> None:
    text = (ROOT / "docs" / page).read_text()
    missing = [e for e in PAGES[page] if f'--8<-- "examples/{e}"' not in text]
    assert missing == []


def test_readme_example_is_the_first_app() -> None:
    readme = (ROOT / "README.md").read_text()
    match = re.search(r"```python\n(.*?)```", readme, re.DOTALL)
    assert match is not None
    assert match.group(1) == (ROOT / "examples" / "getting_started" / "first_app.py").read_text()


def test_readme_relative_links_exist() -> None:
    readme = (ROOT / "README.md").read_text()
    targets = re.findall(r"\]\(([^)#]+)(?:#[^)]*)?\)", readme)
    missing = [t for t in targets if "://" not in t and not (ROOT / t).exists()]
    assert missing == []


def test_redis_guide_includes_its_example_and_it_builds_the_app() -> None:
    text = (ROOT / "docs" / "guides" / "redis.md").read_text()
    assert '--8<-- "examples/guides/redis_store.py"' in text
    module = _load("guides/redis_store.py")  # constructing RedisStore does no I/O
    assert isinstance(module.app.store, RedisStore)
