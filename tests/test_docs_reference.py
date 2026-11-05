"""The API reference page covers the whole public surface."""

from pathlib import Path

import asynk

REFERENCE = Path(__file__).parent.parent / "docs" / "reference.md"


def test_reference_page_lists_every_public_name() -> None:
    text = REFERENCE.read_text()
    missing = [
        name for name in asynk.__all__ if name != "__version__" and f"::: asynk.{name}" not in text
    ]
    assert missing == []
