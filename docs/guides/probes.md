# Probes

A running worker touches two files every heartbeat, `liveness` and
`readiness`, in its probe directory. Readiness is removed while the consumer is
disconnected (`consumer.connected` is false) or the worker is shutting down. A
[scheduler](scheduled-tasks.md#hooks-and-probes) writes `liveness` only.

The `asynk worker` and `asynk scheduler` commands always set a probe directory,
`--probe-dir`, which defaults to `<tempfile.gettempdir()>/asynk`. A `Worker` or
`Scheduler` built in code writes probe files only when given `probe_dir`.

`asynk health` checks a probe file's age; exit code 0 means healthy. Use it as a
Kubernetes `exec` probe:

```yaml
livenessProbe:
  exec:
    command: ["asynk", "health", "liveness", "--max-age", "15"]
readinessProbe:
  exec:
    command: ["asynk", "health", "readiness", "--max-age", "15"]
```

- `--max-age` should exceed the heartbeat interval (`--heartbeat-interval`,
  default 5 s).
- `asynk health` must use the same `--probe-dir` as the process it checks.
- Two workers in one container need distinct probe directories.
