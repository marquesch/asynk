"""Worker, scheduler and producer in separate processes over a unix socket.

Everything lives in this file: a custom codec, the app, and a tiny producer
entry point. Run it from this directory (`cd examples`), each in its own terminal:

1. Worker, which creates and listens on the socket and runs every task:
       python -m asynk.cli worker unix:app
2. Scheduler, which publishes the scheduled tasks (one slot per minute, so wait
   up to a minute for the first one):
       python -m asynk.cli scheduler unix:app
3. Producer, which publishes one-off calls:
       python unix.py

`python -m asynk.cli` is used so the current directory is importable; the
`asynk` command works the same when this directory is on `sys.path`.

The unix socket transport is for learning and testing only. It keeps messages
in memory, never retries, and loses any message that is unsettled when the
worker stops or crashes. Do not use it in production. Publishing fails while no
worker is listening, so always start the worker first.
"""

import asyncio
import base64
import binascii
import tempfile
from pathlib import Path

from asynk import App, DecodeError, Delivery, Envelope, JsonCodec, SkipTask
from asynk.transport.unix import unix_transport

SOCKET = Path(tempfile.gettempdir()) / "asynk-example.sock"


class Base64JsonCodec:
    """JSON wrapped in base64, so messages on the socket are not human-readable.

    A codec only has to turn an Envelope into bytes and back. Producer and
    consumer must use the same one. The unix transport also needs the bytes to
    contain no newline, since a newline ends a message; base64 guarantees that.
    """

    def __init__(self) -> None:
        self._json = JsonCodec()

    def validate(self, envelope: Envelope) -> None:
        self._json.validate(envelope)

    def encode(self, envelope: Envelope) -> bytes:
        return base64.b64encode(self._json.encode(envelope))

    def decode(self, data: bytes) -> Envelope:
        try:
            raw = base64.b64decode(data, validate=True)
        except binascii.Error as exc:
            # Raise DecodeError for bad input; the consumer drops such messages.
            raise DecodeError(f"invalid base64: {exc}") from exc
        return self._json.decode(raw)

    def encode_value(self, value: object) -> bytes:
        return self._json.encode_value(value)

    def decode_value(self, data: bytes) -> object:
        return self._json.decode_value(data)


# Each process builds both ends but uses only the one it needs: the worker's
# consumer creates and listens on the socket file, while the producer and the
# scheduler connect to it. All sides must be given the same codec.
producer, consumer = unix_transport(SOCKET, Base64JsonCodec())
app = App("unix-example", producer=producer, consumer=consumer)


@app.task()
async def add(a: int, b: int) -> int:
    print(f"{a} + {b} = {a + b}")
    return a + b


# A scheduled task is an ordinary task that `asynk scheduler` also publishes on a
# cron schedule, with fixed args and kwargs. This one fires every minute.
@app.scheduled_task(cron="* * * * *", args=[20, 22])
async def heartbeat(a: int, b: int) -> None:
    print(f"heartbeat: {a} + {b} = {a + b}")


# `tz` sets the zone the expression is evaluated in (default: the app's `tz`).
# Plain `def` tasks work too and run in a thread. A non-UTC zone needs a time zone
# database, so install `tzdata` where the system has none.
@app.scheduled_task(cron="30 8 * * mon-fri", tz="America/Sao_Paulo", kwargs={"region": "br"})
def morning_report(*, region: str) -> None:
    print(f"morning report for {region}")


# The scheduler stamps every message with metadata, which a hook can read. Delivery
# is at-least-once, so a hook like this one is where duplicates would be dropped;
# the set only works for one worker, so use a shared store (database, Redis) for more.
seen: set[str] = set()


@app.before_task
def log_and_dedupe(envelope: Envelope, delivery: Delivery) -> None:
    key = envelope.metadata.get("idempotency_key")
    if key is None:
        return  # published by a producer, not by the scheduler
    if key in seen:
        raise SkipTask  # already ran for this slot; the delivery is acked
    seen.add(str(key))
    print(f"scheduled run {key} (slot {envelope.metadata['scheduled_at']})")


async def main() -> None:
    try:
        await add.delay(2, 3)
    except OSError:
        # Nothing is buffered, so publishing fails while no worker is listening.
        raise SystemExit(f"no worker listening on {SOCKET}; start it first") from None
    print("sent")


if __name__ == "__main__":
    asyncio.run(main())
