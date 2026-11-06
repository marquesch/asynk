"""Runs the "core works without the redis client" check once for the whole session."""

from __future__ import annotations

import functools
import json
import subprocess
import sys
from typing import Any

_SCRIPT = """
import json, sys
sys.modules['redis'] = None
import asynk, asynk.cli, asynk.scheduler, asynk.worker
import asynk.store, asynk.store.memory, asynk.transport, asynk.transport.memory

def guard(name):
    try:
        __import__(name)
    except ImportError as exc:
        return [str(exc), type(exc.__cause__).__name__]

print(json.dumps({
    "loaded": sorted(m for m in sys.modules if m.startswith('redis') and sys.modules[m]),
    "asynk.store.redis": guard('asynk.store.redis'),
    "asynk.transport.redis": guard('asynk.transport.redis'),
    "asynk._redis": guard('asynk._redis'),
}))
"""


@functools.cache
def core_without_the_client() -> dict[str, Any]:
    """Imports the core modules in a fresh interpreter where `redis` is not installed.

    The interpreter starts once per session, since starting it dominates the cost.

    Returns:
        `loaded`, the redis client modules the core imported (none expected), and, for
        each redis-backed module, `[message, cause type name]` of the `ImportError` its
        import raised.
    """
    out = subprocess.run(
        [sys.executable, "-c", _SCRIPT], capture_output=True, text=True, check=True
    )
    return json.loads(out.stdout)  # type: ignore[no-any-return]
