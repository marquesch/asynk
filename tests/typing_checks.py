"""Static-only checks, verified by mypy (not collected by pytest)."""

from asynk import Exponential, Next, RetryPolicy, Runner, TaskCall, TaskResult, TaskStatus, Worker
from asynk.app import App
from asynk.results import TaskState
from asynk.transport.memory import memory_transport

producer, consumer = memory_transport()
app = App("typing", producer=producer, consumer=consumer)


@app.task()
async def add(a: int, b: int = 0) -> int:
    return a + b


@app.scheduled_task(cron="* * * * *")
async def scheduled(a: int, b: int = 0) -> int:
    return a + b


@app.task(retry=RetryPolicy(backoff=Exponential(base=1, factor=2, max=60)))
async def with_policy() -> None: ...


@app.task(retry={"max_retries": 3})
async def with_mapping() -> None: ...


@app.task(retry=False)
async def without_retries() -> None: ...


@app.task(max_retries=3)  # type: ignore[call-arg]
async def old_style() -> None: ...


@app.task(retry=True)  # type: ignore[arg-type]
async def retry_true() -> None: ...


async def _checks() -> None:
    await add.delay(1, b=2)
    _task_id: str = (await add.delay(1, b=2)).task_id
    _state: TaskState = await (await add.delay(1, b=2)).state()
    await add.delay("x")  # type: ignore[arg-type]
    await add.delay()  # type: ignore[call-arg]
    await add.delay(1, c=3)  # type: ignore[call-arg]
    await add.with_metadata(a=1).delay(1, b=2)
    await add.with_metadata(a=1).delay("x")  # type: ignore[arg-type]
    await scheduled.delay(1, b=2)
    await scheduled.delay("x")  # type: ignore[arg-type]


@app.middleware
async def ack_exhausted(call: TaskCall, next: Next) -> TaskResult:
    result = await next(call)
    if result.status is TaskStatus.RETRIES_EXCEEDED:
        await call.delivery.ack()
    return result


@app.middleware
async def timed(call: TaskCall, next: Next) -> TaskResult:
    return await next(call)


# Middleware must be async: mypy rejects a sync layer.
@app.middleware  # type: ignore[arg-type]
def sync_layer(call: TaskCall, next: Next) -> TaskResult:
    return call.skip()


@app.before_startup
async def init_telemetry(runner: Runner) -> None:
    if runner.role == "worker":
        return None


@app.after_startup
def warm_up(runner: Runner) -> None:
    del runner


# Lifecycle hooks receive any `Runner`, so a hook typed for `Worker` alone is
# rejected: parameter types are contravariant.
@app.before_shutdown  # type: ignore[arg-type]
async def worker_only(worker: Worker) -> None:
    del worker
