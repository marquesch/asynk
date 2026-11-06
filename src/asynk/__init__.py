"""asynk: an asyncio task framework."""

from importlib.metadata import PackageNotFoundError, version

from asynk.app import App
from asynk.codec import Codec, DecodeError, EncodeError
from asynk.codec.json import JsonCodec
from asynk.envelope import Envelope, InvalidEnvelopeError
from asynk.errors import NotConfiguredError
from asynk.hooks import Next, TaskCall, TaskFailedError, TaskResult, TaskStatus
from asynk.results import TaskError, TaskHandle, TaskNotFoundError, TaskState
from asynk.runner import Runner
from asynk.scheduled_task import ScheduledTask
from asynk.scheduler import Scheduler
from asynk.store import Store
from asynk.store.memory import MemoryStore
from asynk.store.null import NullStore
from asynk.task import Task
from asynk.transport import Consumer, Delivery, Producer
from asynk.worker import Worker

try:
    __version__ = version("asynk")
except PackageNotFoundError:
    # Imported from a source tree without an installed distribution.
    __version__ = "0+unknown"
"""The installed asynk version."""

__all__ = [
    "__version__",
    "App",
    "Codec",
    "Consumer",
    "DecodeError",
    "Delivery",
    "Envelope",
    "InvalidEnvelopeError",
    "JsonCodec",
    "EncodeError",
    "NotConfiguredError",
    "Next",
    "Producer",
    "Runner",
    "Task",
    "TaskCall",
    "TaskFailedError",
    "TaskResult",
    "TaskStatus",
    "ScheduledTask",
    "Scheduler",
    "Worker",
    "Store",
    "MemoryStore",
    "NullStore",
    "TaskHandle",
    "TaskState",
    "TaskError",
    "TaskNotFoundError",
]
