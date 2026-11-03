# asynk Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build asynk v1: app/task registry, typed `delay`, envelope + codec, protocols for producers/consumers, an in-memory transport, a single-process asyncio worker with file-based probes and an after-task hook, and the `asynk` CLI.

**Architecture:** A small stdlib-only package under `src/asynk/`. `Envelope` is the only contract between producers and consumers. `App` holds the registry, producer, consumer and the after-task hook. `Worker` pulls deliveries from `app.consumer`, runs tasks concurrently, and settles each delivery through the hook or the consumer's default `process` context manager.

**Tech Stack:** Python >= 3.14, stdlib only in the core; uv, pytest + pytest-asyncio, ruff, mypy (strict). No other dependency may be added without asking the owner.

**Spec:** `docs/superpowers/specs/2026-11-02-asynk-core-design.md`

## Global Constraints

- Python `>=3.14`; stdlib only in `src/asynk` (no third-party runtime dependencies).
- uv for everything: `uv add --dev`, `uv run pytest`, `uv run ruff check`, `uv run ruff format --check`, `uv run mypy`.
- TDD: each task writes the failing test first and sees it fail before implementing.
- Conventional Commits; **no** `Co-Authored-By` or other attribution trailer; all work on branch `feat/core` (never commit to `main`/`master`); the owner merges via the GitHub UI.
- Task args/kwargs are JSON-friendly only: `None`, `bool`, `int`, finite `float`, `str`, `list`, `dict` with `str` keys.
- `delay` is a coroutine returning whatever `producer.publish` returns; task IDs are UUID4 strings made by asynk; `attempt` starts at 0.
- `@app.task(...)` is used with parentheses only. Retry/DLQ policy is never in the core; there is no max-retries hook (status `RETRIES_EXCEEDED` goes to `after_task`).
- Sync tasks run with `asyncio.to_thread`. Probes are file-based. CLI is `argparse`, console script `asynk`.

## Review Focus

Failure modes the spec implies but no happy-path test covers; each is pinned by a test in the task named in brackets.

1. Shutdown timeout expires with tasks still running: their deliveries must be rejected with `requeue=True`, never lost. [Task 8]
2. `after_task` hook raises: delivery is rejected with `requeue=True` (not left unsettled), worker keeps going. [Task 7]
3. Non-JSON args (tuple, NaN/inf, non-`str` dict keys, nested bad value): `delay` raises before anything is published. [Task 2, Task 5]
4. Malformed bytes in the transport: skipped, worker loop is not killed. [Task 6]
5. `asynk health` with missing probe file or missing directory: exit code 1 with a short message, no traceback. [Task 9]

---

## File Structure

```
src/asynk/__init__.py        public exports
src/asynk/envelope.py        Envelope, JSONValue, ensure_json, NonJsonArgumentError
src/asynk/codec.py           Codec protocol, JsonCodec
src/asynk/transport.py       Producer, Consumer, Delivery protocols
src/asynk/task.py            Task[P, R]
src/asynk/app.py             App, NotConfiguredError
src/asynk/hooks.py           TaskStatus, TaskResult, AfterTaskHook, TaskFailedError
src/asynk/worker.py          Worker
src/asynk/probes.py          ProbeFiles, check_probe
src/asynk/cli.py             main(argv) -> int
src/asynk/transports/__init__.py
src/asynk/transports/memory.py   memory_transport, MemoryProducer, MemoryConsumer
tests/                       one test module per source module, plus tests/typing_checks.py
tests/fixture_app.py         app used by the CLI subprocess test
```

---

### Task 1: Branch and tooling baseline

**Files:**
- Modify: `pyproject.toml`
- Create: `.gitignore`, `tests/test_package.py`
- Modify: `src/asynk/__init__.py` (remove the `hello` placeholder)

**Interfaces:**
- Produces: working `uv run pytest`, `uv run ruff check`, `uv run ruff format --check`, `uv run mypy`; console script entry `asynk = "asynk.cli:main"` declared (module arrives in Task 10).

- [ ] **Step 1:** `git checkout -b feat/core` (from `docs/core-design`, so the spec travels with it).
- [ ] **Step 2:** `uv add --dev ruff mypy` (approved by the owner). In `pyproject.toml` add: `[tool.pytest.ini_options]` with `asyncio_mode = "auto"` and `testpaths = ["tests"]`; `[tool.mypy]` with `strict = true`, `python_version = "3.14"`, `files = ["src", "tests"]`; `[tool.ruff]` with `line-length = 100` and `[tool.ruff.lint] select = ["E", "F", "I", "UP", "B", "ASYNC", "SIM"]`. Do not add `[project.scripts]` yet (the module does not exist).
- [ ] **Step 3:** Create `.gitignore` covering `.venv/`, `__pycache__/`, `.mypy_cache/`, `.ruff_cache/`, `.pytest_cache/`, `dist/`.
- [ ] **Step 4: Write the failing test** `tests/test_package.py::test_package_version`: `from importlib.metadata import version; assert version("asynk") == "0.1.0"`; and `test_py_typed_marker_exists`: `importlib.resources.files("asynk").joinpath("py.typed").is_file()`.
- [ ] **Step 5:** Replace the body of `src/asynk/__init__.py` with an empty module docstring. Run `uv run pytest && uv run ruff check && uv run ruff format --check && uv run mypy`. Expected: all pass.
- [ ] **Step 6: Commit** (include the previously untracked project files, `uv.lock` and `.python-version`): `git add -A && git commit -m "chore: set up tooling baseline"`.

---

### Task 2: Envelope and JSON validation

**Files:**
- Create: `src/asynk/envelope.py`, `tests/test_envelope.py`

**Interfaces:**
- Produces:
  - `type JSONValue = None | bool | int | float | str | list[JSONValue] | dict[str, JSONValue]`
  - `class NonJsonArgumentError(TypeError)`
  - `def ensure_json(value: object, path: str = "value") -> None` — raises `NonJsonArgumentError` whose message includes the offending path, e.g. `kwargs.items[2]`.
  - `@dataclass(frozen=True, slots=True) class Envelope: task: str; args: tuple[JSONValue, ...]; kwargs: dict[str, JSONValue]; id: str; attempt: int = 0; metadata: dict[str, JSONValue] = field(default_factory=dict)`
  - `Envelope.create(task: str, args: Sequence[object], kwargs: Mapping[str, object]) -> Envelope` — validates with `ensure_json`, generates `id=str(uuid.uuid4())`, `attempt=0`.
  - `Envelope.retry(self) -> Envelope` — same id and payload, `attempt + 1`.

- [ ] **Step 1: Write failing tests** in `tests/test_envelope.py`:
  - `test_create_generates_uuid4_and_attempt_zero`: `uuid.UUID(e.id).version == 4`, `e.attempt == 0`.
  - `test_ids_are_unique` for two calls.
  - `test_retry_increments_attempt_and_keeps_id`.
  - `test_envelope_is_frozen`: assigning `e.attempt = 1` raises `dataclasses.FrozenInstanceError`.
  - Parametrized `test_ensure_json_accepts`: `None, True, 1, 1.5, "x", [1, {"a": [None]}]`.
  - Parametrized `test_ensure_json_rejects` (each raises `NonJsonArgumentError`): `(1, 2)` tuple, `float("nan")`, `float("inf")`, `{1: "a"}`, `{"a": {1, 2}}` set, `object()`, `[1, [2, (3,)]]`.
  - `test_error_message_contains_path`: `Envelope.create("t", [], {"items": [1, 2, (3,)]})` raises with `"kwargs.items[2]"` in `str(exc)`.
- [ ] **Step 2:** Run `uv run pytest tests/test_envelope.py -v`; expected FAIL (module missing).
- [ ] **Step 3:** Implement `envelope.py` per the interfaces. `ensure_json` is recursive over `list`/`dict`; `bool` is accepted before `int` is a concern only for ordering; reject `tuple` explicitly (not JSON round-trip safe).
- [ ] **Step 4:** Run tests, `uv run ruff check`, `uv run mypy`; expected PASS.
- [ ] **Step 5: Commit** `feat: add envelope and json validation`.

---

### Task 3: Codec

**Files:**
- Create: `src/asynk/codec.py`, `tests/test_codec.py`

**Interfaces:**
- Consumes: `Envelope`.
- Produces: `class Codec(Protocol): def encode(self, envelope: Envelope) -> bytes; def decode(self, data: bytes) -> Envelope`; `class JsonCodec` implementing it; `class DecodeError(ValueError)`.

- [ ] **Step 1: Write failing tests:**
  - `test_roundtrip`: `JsonCodec().decode(JsonCodec().encode(e)) == e` for an envelope with nested args, kwargs, `attempt=2`, `metadata={"k": "v"}`.
  - `test_encoded_is_utf8_json_object`: `json.loads(data)` is a dict containing keys `id`, `task`, `args`, `kwargs`, `attempt`, `metadata`.
  - `test_decode_rejects_garbage`: `b"not json"`, `b"[]"`, `b'{"task": "t"}'` (missing fields) each raise `DecodeError`.
  - `test_json_codec_satisfies_protocol`: assign `c: Codec = JsonCodec()`.
- [ ] **Step 2:** Run; expected FAIL.
- [ ] **Step 3:** Implement with stdlib `json`; decode converts `args` list back to tuple and validates field types, raising `DecodeError` otherwise.
- [ ] **Step 4:** Run tests, ruff, mypy; expected PASS.
- [ ] **Step 5: Commit** `feat: add codec protocol and json codec`.

---

### Task 4: Transport protocols, Task and App registry

**Files:**
- Create: `src/asynk/transport.py`, `src/asynk/task.py`, `src/asynk/app.py`, `tests/test_app.py`

**Interfaces:**
- Consumes: `Envelope`.
- Produces:
  - `transport.py`: `class Producer(Protocol): async def publish(self, envelope: Envelope) -> Any`. `class Delivery(Protocol)` with read-only properties `envelope: Envelope`, `settled: bool` and methods `async def ack(self) -> None`, `async def reject(self, *, requeue: bool = False) -> None`. `class Consumer(Protocol)` with property `connected: bool`, `async def start(self) -> None`, `async def stop(self) -> None`, `def deliveries(self) -> AsyncIterator[Delivery]`, `def process(self, delivery: Delivery) -> AbstractAsyncContextManager[None]`.
  - `task.py`: `class Task[**P, R]` with attributes `name: str`, `func: Callable[P, R]`, `max_retries: int | None`, `options: dict[str, Any]`, property `is_async: bool` (`inspect.iscoroutinefunction(func)`), `def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R` (runs `func`), constructor `Task(app: App, func, name, max_retries, options)`. `delay` is added in Task 5.
  - `app.py`: `class App` with `__init__(self, name: str, *, producer: Producer | None = None, consumer: Consumer | None = None)`; attributes `name`, `producer`, `consumer`; `def task(self, *, name: str | None = None, max_retries: int | None = None, **options: Any) -> Callable[[Callable[P, R]], Task[P, R]]`; `def get_task(self, name: str) -> Task[..., Any]` raising `KeyError` for unknown names; `tasks` property returning a read-only mapping name → Task.

- [ ] **Step 1: Write failing tests** in `tests/test_app.py`:
  - `test_task_registers_with_default_name`: name equals `f"{fn.__module__}.{fn.__qualname__}"` and `app.get_task(name) is task`.
  - `test_explicit_name_and_options`: `@app.task(name="x", max_retries=3, queue="q")` → `task.name == "x"`, `task.max_retries == 3`, `task.options == {"queue": "q"}`.
  - `test_duplicate_name_raises_value_error`.
  - `test_unknown_task_raises_key_error`.
  - `test_direct_call_runs_function`: sync function returns value; async function returns a coroutine that awaits to the value.
  - `test_is_async_flag`.
  - `test_app_without_producer_or_consumer_is_valid`.
- [ ] **Step 2:** Run; expected FAIL.
- [ ] **Step 3:** Implement the three modules. Use `TYPE_CHECKING` imports to avoid the `Task`↔`App` cycle.
- [ ] **Step 4:** Run tests, ruff, mypy; expected PASS.
- [ ] **Step 5: Commit** `feat: add app, task registry and transport protocols`.

---

### Task 5: Typed `delay`

**Files:**
- Modify: `src/asynk/task.py`, `src/asynk/app.py`
- Create: `tests/test_delay.py`, `tests/typing_checks.py`

**Interfaces:**
- Consumes: `Task`, `App`, `Envelope.create`.
- Produces: `async def Task.delay(self, *args: P.args, **kwargs: P.kwargs) -> Any`; `class NotConfiguredError(RuntimeError)` in `app.py`.

- [ ] **Step 1: Write failing tests** in `tests/test_delay.py` using a `FakeProducer` (records envelopes, returns `"handle-1"`):
  - `test_delay_publishes_envelope_and_returns_producer_result`: envelope has `task == task.name`, `args == (1,)`, `kwargs == {"b": 2}`, `attempt == 0`, a UUID `id`; return value is `"handle-1"`.
  - `test_delay_without_producer_raises_not_configured`.
  - `test_delay_rejects_non_json_args_before_publishing`: calling with a tuple arg raises `NonJsonArgumentError` and `FakeProducer.published == []`.
  - `test_delay_propagates_producer_exception`.
- [ ] **Step 2:** Write `tests/typing_checks.py` (not collected by pytest; checked by mypy). Define `@app.task() async def add(a: int, b: int = 0) -> int`, then inside an `async def _checks()`: `await add.delay(1, b=2)` (valid), `await add.delay("x")  # type: ignore[arg-type]`, `await add.delay()  # type: ignore[call-arg]`, `await add.delay(1, c=3)  # type: ignore[call-arg]`. Under strict mode an unused ignore is an error, so mypy passing proves the checker sees `add`'s own signature.
- [ ] **Step 3:** Run `uv run pytest tests/test_delay.py -v` and `uv run mypy`; expected FAIL.
- [ ] **Step 4:** Implement `delay`: raise `NotConfiguredError` if `app.producer is None`; build `Envelope.create(self.name, args, kwargs)`; `return await app.producer.publish(envelope)`.
- [ ] **Step 5:** Run pytest, ruff, mypy; expected PASS.
- [ ] **Step 6: Commit** `feat: add typed task delay`.

---

### Task 6: In-memory transport

**Files:**
- Create: `src/asynk/transports/__init__.py`, `src/asynk/transports/memory.py`, `tests/test_memory_transport.py`

**Interfaces:**
- Consumes: `Codec`, `JsonCodec`, `Envelope`, `Producer`/`Consumer`/`Delivery` protocols, `DecodeError`.
- Produces: `def memory_transport(codec: Codec | None = None) -> tuple[MemoryProducer, MemoryConsumer]` (both share one `asyncio.Queue[bytes]` and the codec; messages travel as encoded bytes). `MemoryProducer.publish` returns `envelope.id`. `MemoryProducer.publish_raw(data: bytes) -> None` for tests. `MemoryConsumer.connected` is true between `start()` and `stop()`; `deliveries()` yields until `stop()` is called, then ends; `MemoryConsumer.process(delivery)` is an async context manager: on clean exit acks if unsettled; on exception rejects with `requeue=False` if unsettled; never swallows the exception. Delivery `reject(requeue=True)` puts the original bytes back on the queue. Settling a delivery twice raises `RuntimeError`.

- [ ] **Step 1: Write failing tests:**
  - `test_publish_then_consume_roundtrip`: published envelope equals the one yielded.
  - `test_publish_returns_envelope_id`.
  - `test_ack_marks_settled`; `test_double_settle_raises_runtime_error`.
  - `test_reject_requeue_redelivers`; `test_reject_without_requeue_drops`.
  - `test_stop_ends_iteration_when_idle`: a consumer awaiting an empty queue exits `deliveries()` promptly after `stop()` (use `asyncio.wait_for(..., 1)`).
  - `test_connected_flag_follows_start_stop`.
  - `test_process_acks_on_clean_exit`; `test_process_rejects_on_exception_and_propagates`; `test_process_leaves_already_settled_delivery_alone`.
  - `test_malformed_message_is_skipped`: `publish_raw(b"garbage")` then a valid publish; the first yielded delivery is the valid one. [Review Focus 4]
- [ ] **Step 2:** Run; expected FAIL.
- [ ] **Step 3:** Implement. To make `stop()` wake an idle consumer, enqueue a private sentinel object (queue typed accordingly).
- [ ] **Step 4:** Run tests, ruff, mypy; expected PASS.
- [ ] **Step 5: Commit** `feat: add in-memory transport`.

---

### Task 7: Worker processing of one delivery and the after-task hook

**Files:**
- Create: `src/asynk/hooks.py`, `src/asynk/worker.py`, `tests/test_worker_handle.py`
- Modify: `src/asynk/app.py`

**Interfaces:**
- Consumes: `App`, `Task`, `Delivery`, `Consumer.process`.
- Produces:
  - `hooks.py`: `class TaskStatus(enum.Enum)` with members `SUCCESS`, `FAILED`, `RETRIES_EXCEEDED`, `UNKNOWN_TASK`; `@dataclass(frozen=True) class TaskResult: envelope: Envelope; status: TaskStatus; value: Any = None; error: BaseException | None = None`; `type AfterTaskHook = Callable[[TaskResult, Delivery], Awaitable[None]]`; `class TaskFailedError(Exception)` carrying the `TaskResult`.
  - `app.py`: `App.after_task(self, hook: AfterTaskHook) -> AfterTaskHook` (decorator; stores `app.after_task_hook`, attribute defaults to `None`).
  - `worker.py`: `class Worker: __init__(self, app: App, *, concurrency: int = 10, shutdown_timeout: float = 30.0, probe_dir: Path | None = None, heartbeat_interval: float = 5.0)`; `async def handle(self, delivery: Delivery) -> TaskResult` (processes exactly one delivery; never raises for task/hook errors). `Worker.__init__` raises `NotConfiguredError` if `app.consumer is None`.
  - Status rules: unknown task → `UNKNOWN_TASK`; success → `SUCCESS` with `value`; exception (`Exception`) → `FAILED`, or `RETRIES_EXCEEDED` when `task.max_retries is not None and envelope.attempt >= task.max_retries`.
  - Settling: run everything inside `async with consumer.process(delivery)`. After building the result call `app.after_task_hook(result, delivery)` if set. If the delivery is still unsettled and status is not `SUCCESS`, raise `TaskFailedError(result)` inside the context manager so the consumer's default rejects it; `handle` catches `TaskFailedError` and returns the result. If the hook itself raises, log via `logging.getLogger("asynk")`, `reject(requeue=True)` if unsettled, and return the result.

- [ ] **Step 1: Write failing tests** (memory transport, `Worker.handle` driven directly with a delivery taken from `consumer.deliveries()`):
  - `test_success_acks_and_returns_value`.
  - `test_async_and_sync_tasks_both_run`: the sync one asserts it ran in a non-main thread (`threading.current_thread() is not threading.main_thread()`).
  - `test_failure_sets_failed_and_rejects_without_requeue`.
  - `test_retries_exceeded_status`: `max_retries=2`; envelope `attempt=1` → `FAILED`, `attempt=2` → `RETRIES_EXCEEDED`; `max_retries=None` never → `RETRIES_EXCEEDED`.
  - `test_unknown_task_status_and_after_task_called`.
  - `test_after_task_receives_result_and_may_settle`: hook acks a `FAILED` delivery itself; default rejection is not applied (delivery acked, no redelivery).
  - `test_after_task_may_requeue_via_producer`: hook publishes `envelope.retry()` through the app's producer and acks the original; consumer then sees `attempt == 1`.
  - `test_failing_hook_requeues_delivery_and_does_not_raise`. [Review Focus 2]
  - `test_worker_requires_consumer`.
- [ ] **Step 2:** Run; expected FAIL.
- [ ] **Step 3:** Implement `hooks.py`, `App.after_task`, and `Worker.handle`. `asyncio.CancelledError` is not caught by `handle` (it propagates).
- [ ] **Step 4:** Run tests, ruff, mypy; expected PASS.
- [ ] **Step 5: Commit** `feat: add worker delivery handling and after-task hook`.

---

### Task 8: Worker run loop, concurrency and graceful shutdown

**Files:**
- Modify: `src/asynk/worker.py`
- Create: `tests/test_worker_run.py`

**Interfaces:**
- Consumes: `Worker.handle`, `Consumer.start/stop/deliveries`.
- Produces: `async def Worker.run(self) -> None` (starts the consumer, loops until shutdown, drains, stops the consumer); `def Worker.request_shutdown(self) -> None` (idempotent, safe to call from a signal handler on the loop); `async def Worker.run_with_signals(self) -> None` (installs `SIGTERM`/`SIGINT` handlers with `loop.add_signal_handler` that call `request_shutdown`, then `run()`).
- Behavior: acquire the concurrency semaphore **before** fetching the next delivery; the fetch is raced against the shutdown event so an idle worker exits promptly. On shutdown: stop fetching, wait for in-flight handlers up to `shutdown_timeout`; any still running are cancelled and their deliveries rejected with `requeue=True` (if unsettled); finally `consumer.stop()`.

- [ ] **Step 1: Write failing tests** (memory transport, short timeouts):
  - `test_run_processes_published_tasks` (publish 3, let them finish, `request_shutdown`, `run` returns).
  - `test_concurrency_limit_respected`: `concurrency=2`, five tasks that track max simultaneous count with an `asyncio.Event`/counter; max seen == 2.
  - `test_tasks_run_concurrently`: two tasks that each wait for the other (barrier) complete (would deadlock if serial).
  - `test_idle_worker_shuts_down_promptly`: no messages, `request_shutdown()` → `run()` finishes within 1 s.
  - `test_shutdown_waits_for_in_flight_task`: a task still running at shutdown completes and its delivery is acked.
  - `test_shutdown_timeout_requeues_unfinished`: `shutdown_timeout=0.1`, a task sleeping 5 s → run returns quickly; the delivery was redelivered (`requeue=True`) — assert by draining the queue after and seeing the envelope. [Review Focus 1]
  - `test_failed_task_does_not_stop_loop`.
  - `test_sync_blocking_task_does_not_block_loop`: a sync task doing `time.sleep(0.3)` while an async task completes in the meantime.
  - `test_request_shutdown_is_idempotent`.
  - `test_run_with_signals_handles_sigterm`: start `run_with_signals` as a task, `os.kill(os.getpid(), signal.SIGTERM)`, expect it to finish within 1 s.
- [ ] **Step 2:** Run; expected FAIL.
- [ ] **Step 3:** Implement the run loop as described. Track in-flight `asyncio.Task`s in a set so shutdown can wait/cancel them; use `asyncio.wait(..., timeout=...)`.
- [ ] **Step 4:** Run tests, ruff, mypy; expected PASS (run the new test module 5 times to check for flakiness).
- [ ] **Step 5: Commit** `feat: add worker run loop and graceful shutdown`.

---

### Task 9: File-based probes

**Files:**
- Create: `src/asynk/probes.py`, `tests/test_probes.py`
- Modify: `src/asynk/worker.py`, `tests/test_worker_run.py` (probe tests)

**Interfaces:**
- Produces:
  - `class ProbeFiles: __init__(self, directory: Path)`; properties `liveness: Path` (`<dir>/liveness`) and `readiness: Path` (`<dir>/readiness`); `def beat(self, *, ready: bool) -> None` (creates the directory if missing, atomically rewrites `liveness` with `str(time.time())`; if `ready` rewrites `readiness` the same way, else removes it); `def clear(self) -> None` (removes both, tolerating absence).
  - `def check_probe(path: Path, max_age: float, now: float | None = None) -> tuple[bool, str]` — healthy iff the file exists and `now - mtime <= max_age`; the string is a short human message.
  - `def default_probe_dir() -> Path` returning `Path(tempfile.gettempdir()) / "asynk"`.
- Worker integration: when `probe_dir` is not `None`, `run()` starts a heartbeat task calling `beat(ready=...)` every `heartbeat_interval` seconds, where `ready` is `consumer.connected and not shutting_down`; on shutdown start it immediately removes the readiness file; on exit it calls `clear()`.

- [ ] **Step 1: Write failing tests:**
  - `test_beat_creates_dir_and_files`; `test_beat_not_ready_removes_readiness`; `test_clear_is_idempotent`.
  - `test_check_probe_healthy_fresh_file`; `test_check_probe_stale_file` (set mtime with `os.utime`); `test_check_probe_missing_file_and_missing_directory_are_unhealthy_with_message`. [Review Focus 5]
  - In `tests/test_worker_run.py`: `test_worker_writes_probe_files_while_running` (both files appear, liveness mtime advances over two intervals of 0.05 s); `test_readiness_removed_when_shutdown_starts` (a long task keeps the worker draining; readiness file gone while liveness remains); `test_probe_files_cleared_after_exit`; `test_no_files_when_probe_dir_is_none`.
- [ ] **Step 2:** Run; expected FAIL.
- [ ] **Step 3:** Implement; write atomically via a temp file in the same directory plus `os.replace`.
- [ ] **Step 4:** Run tests, ruff, mypy; expected PASS.
- [ ] **Step 5: Commit** `feat: add file-based liveness and readiness probes`.

---

### Task 10: `asynk` CLI

**Files:**
- Create: `src/asynk/cli.py`, `tests/test_cli.py`, `tests/fixture_app.py`
- Modify: `pyproject.toml` (add `[project.scripts] asynk = "asynk.cli:main"`), then `uv sync`.

**Interfaces:**
- Consumes: `Worker`, `ProbeFiles`, `check_probe`, `default_probe_dir`, `App`.
- Produces: `def main(argv: Sequence[str] | None = None) -> int`; `def load_app(path: str) -> App` (`"module:attr"`, imports with `importlib.import_module`, raises `CliError` with a clear message for a missing colon, import failure, missing attribute, or a non-`App` object); `class CliError(Exception)`.
- Commands: `asynk worker MODULE:APP [--concurrency N] [--probe-dir PATH] [--heartbeat-interval S] [--shutdown-timeout S]` (runs `asyncio.run(worker.run_with_signals())`, exit 0 on clean shutdown; probe dir defaults to `default_probe_dir()`); `asynk health {liveness,readiness} --max-age S [--probe-dir PATH]` (prints the message from `check_probe`; exit 0 healthy, 1 unhealthy). `CliError` prints `asynk: <message>` to stderr and returns 2.
- `tests/fixture_app.py` defines `app = App("fixture", producer=..., consumer=...)` from `memory_transport()`, with one trivial task.

- [ ] **Step 1: Write failing tests:**
  - `test_load_app_ok`; `test_load_app_errors` (parametrized: `"nocolon"`, `"no_such_module:app"`, `"tests.fixture_app:missing"`, `"tests.fixture_app:__name__"` → `CliError`).
  - `test_health_exit_codes`: `main(["health","liveness","--max-age","5","--probe-dir",str(tmp)])` returns 1 when empty, 0 after `ProbeFiles(tmp).beat(ready=True)`; readiness returns 1 after `beat(ready=False)`.
  - `test_worker_bad_app_path_returns_2_without_traceback` (capsys shows `asynk:` and no `Traceback`).
  - `test_worker_subprocess_lifecycle`: `subprocess.Popen([sys.executable, "-m", "asynk.cli", ...])` is **not** used; run the installed console script via `uv run asynk worker tests.fixture_app:app --probe-dir <tmp> --heartbeat-interval 0.05` (set `PYTHONPATH` to the repo root), poll up to 5 s for the readiness file, assert `asynk health readiness` returns 0, send `SIGTERM`, assert exit code 0 within 5 s and that both probe files are gone.
- [ ] **Step 2:** Run; expected FAIL.
- [ ] **Step 3:** Implement with `argparse` subparsers; also add `if __name__ == "__main__": raise SystemExit(main())`.
- [ ] **Step 4:** Run tests, ruff, mypy; expected PASS.
- [ ] **Step 5: Commit** `feat: add asynk cli with worker and health commands`.

---

### Task 11: Public API, end-to-end test and README

**Files:**
- Modify: `src/asynk/__init__.py`, `README.md`
- Create: `tests/test_e2e.py`

**Interfaces:**
- Produces: `asynk.__all__` exporting `App`, `Task`, `Envelope`, `Codec`, `JsonCodec`, `Producer`, `Consumer`, `Delivery`, `TaskStatus`, `TaskResult`, `Worker`, `NonJsonArgumentError`, `NotConfiguredError`.

- [ ] **Step 1: Write failing tests** in `tests/test_e2e.py`: `test_import_surface` (every name in the list above importable from `asynk`); `test_delay_to_worker_roundtrip`: app with memory transport and a task that records its arguments; `await task.delay(2, b=3)`; run the worker until the record appears, then shut down; assert the task received `(2, b=3)`; `test_custom_producer_and_consumer_are_decoupled`: a hand-written producer that appends the JSON-encoded envelope to a plain `list[str]` and a hand-written consumer reading from that list (using `JsonCodec`) — no shared code between them besides the codec — runs the task end to end.
- [ ] **Step 2:** Run; expected FAIL.
- [ ] **Step 3:** Add the exports. Rewrite `README.md` with: what asynk is, a short example (app, task, `await task.delay`, `asynk worker`), the producer/consumer protocols, probe usage in a k8s `exec` probe (`asynk health liveness --max-age 15`), and the retry-through-`after_task` pattern. Keep it under ~120 lines.
- [ ] **Step 4:** Run the full suite: `uv run pytest && uv run ruff check && uv run ruff format --check && uv run mypy`; expected all pass.
- [ ] **Step 5: Commit** `feat: expose public api and add readme`.

---

## Self-Review

- **Spec coverage:** App/registry, options (T4); typed `delay`, ID, JSON validation (T2, T5); envelope/codec (T2, T3); protocols and demo transport (T4, T6); worker, sync via thread, shutdown, unknown task (T7, T8); file probes and checks (T9); CLI `worker` and `health` (T10); after-task hook and `RETRIES_EXCEEDED` without a dedicated hook (T7); consumer default `process` (T6, T7); tooling and git rules (T1, Global Constraints). The optional unix-socket pair is deliberately skipped (YAGNI); noted for the owner.
- **Type consistency:** names used across tasks — `Envelope.create/retry`, `Worker.handle/run/request_shutdown/run_with_signals`, `ProbeFiles.beat/clear`, `check_probe`, `memory_transport`, `App.after_task_hook` — match their defining tasks.
- **Plan decisions to flag to the owner:** hooks live on the app (`@app.after_task`); `Task` uses PEP 695 syntax (Python 3.14); `attempt >= max_retries` means `RETRIES_EXCEEDED`; a failing hook requeues the message; probes judged by file mtime.
