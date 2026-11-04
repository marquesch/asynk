"""App shared by the worker and the producer, each running in its own process.

The unix socket transport is for learning and testing only. It keeps messages
in memory, never retries, and loses any message that is unsettled when the
worker stops or crashes. Do not use it in production.
"""

import tempfile
from pathlib import Path

from codec import Base64JsonCodec

from asynk import App
from asynk.transport.unix import unix_transport

SOCKET = Path(tempfile.gettempdir()) / "asynk-example.sock"

# Each process builds both ends but uses only one: the worker's consumer creates
# and listens on the socket file, and the producer connects to it. Both sides
# must be given the same codec.
producer, consumer = unix_transport(SOCKET, Base64JsonCodec())
app = App("unix-example", producer=producer, consumer=consumer)


@app.task()
async def add(a: int, b: int) -> int:
    print(f"{a} + {b} = {a + b}")
    return a + b
