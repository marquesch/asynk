"""asynk: an asyncio task framework."""

from importlib.metadata import PackageNotFoundError, version

from asynk.app import App
from asynk.codec import Codec
from asynk.codec.json import JsonCodec
from asynk.envelope import Envelope
from asynk.errors import (
    AsynkError,
    CronError,
    DecodeError,
    DeliveryFailedError,
    EncodeError,
    InvalidEnvelopeError,
    NotConfiguredError,
    TaskError,
    TaskNotFoundError,
)
from asynk.hooks import Next, TaskCall, TaskResult, TaskStatus
from asynk.results import TaskHandle, TaskState
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
    "AsynkError",
    "Codec",
    "Consumer",
    "CronError",
    "DecodeError",
    "Delivery",
    "DeliveryFailedError",
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
