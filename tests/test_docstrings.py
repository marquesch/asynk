from __future__ import annotations

import inspect
from collections.abc import Iterator

import asynk
from asynk import (
    App,
    Codec,
    JsonCodec,
    MemoryStore,
    NullStore,
    Scheduler,
    Store,
    TaskCall,
    TaskHandle,
    Worker,
)
from asynk.envelope import Envelope
from asynk.retry import Exponential, RetryPolicy
from asynk.transport.memory import MemoryConsumer, MemoryDelivery, MemoryProducer

MEMBER_CLASSES = [
    TaskCall,
    Store,
    TaskHandle,
    NullStore,
    MemoryStore,
    Codec,
    JsonCodec,
    App,
    Worker,
    Scheduler,
    Envelope,
    RetryPolicy,
    Exponential,
]

# Transport implementations: the class docstring is checked; their members
# implement the documented Consumer, Delivery and Producer protocols.
CLASS_ONLY = [
    MemoryProducer,
    MemoryConsumer,
    MemoryDelivery,
]


def _documented_objects() -> Iterator[tuple[str, str | None]]:
    for name in asynk.__all__:
        obj = getattr(asynk, name)
        if inspect.isclass(obj) or inspect.isfunction(obj):
            yield name, obj.__doc__
    for cls in CLASS_ONLY:
        yield cls.__name__, cls.__doc__
    for cls in MEMBER_CLASSES:
        for member_name, member in inspect.getmembers(cls):
            if member_name.startswith("_"):
                continue
            qualified = f"{cls.__name__}.{member_name}"
            if isinstance(member, property) or inspect.isroutine(member):
                yield qualified, member.__doc__


def test_public_api_has_docstrings() -> None:
    problems = []
    for name, doc in _documented_objects():
        if not doc or not doc.strip():
            problems.append(f"{name}: missing docstring")
        elif not doc.strip().splitlines()[0].endswith("."):
            problems.append(f"{name}: summary line does not end with a period")
    assert not problems, problems


def test_docstrings_have_no_pitfall_markers() -> None:
    markers = ["NOT ", "Note:", "Known limitation", "forever", "event loop"]
    problems = [
        f"{name}: {marker!r}"
        for name, doc in _documented_objects()
        for marker in markers
        if doc and marker in doc
    ]
    assert not problems, problems
