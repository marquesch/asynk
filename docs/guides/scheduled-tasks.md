# Scheduled tasks

`@app.scheduled_task` registers a task that a scheduler publishes on a cron
schedule. The scheduler only publishes; a worker runs the tasks, as for any
other message.

``` { .python }
--8<-- "examples/guides/scheduled_tasks.py"
```

A scheduled task is an ordinary task with fixed `args` and `kwargs`; it also
accepts `name` and `max_retries`. `tz` defaults to the app's timezone,
`App(name, tz="UTC")`. In production, run the scheduler from the command line:

```bash
asynk scheduler tasks:app --heartbeat-interval 5
```

## Cron syntax

Expressions have five fields (minute, hour, day of month, month, day of week),
with a resolution of one minute. Month and weekday names such as `jan` and
`mon-fri` are accepted. Macros such as `@daily`, `L`, `W`, `#`, `?` and a
seconds field are not.

When both day of month and day of week are restricted, a slot matches if
either does (the classic Vixie cron rule); a field starting with `*` counts as
unrestricted.

## Validation

The cron expression is checked when the decorator is called, and so is the
timezone when `name` is given; otherwise the timezone is checked when the
function is decorated. The fixed arguments are checked with the app's codec
when a `Scheduler` is created.

## Timezones

Only non-UTC timezones need a time zone database. On systems without one
(slim containers, Windows) install the `tzdata` package; `"UTC"` works without
it.

**Daylight saving time.** A local time that does not exist (the spring-forward
gap) is skipped for that day. A local time that occurs twice fires only on its
first occurrence, so an every-minute schedule does not fire during the repeated
hour.

## Startup, missed runs and clock changes

Nothing fires at startup: each scheduled task's first slot is the first one
after the scheduler starts. Shutdown interrupts the wait and does not wait for
slots that are not yet due; a publish in flight is cancelled.

The scheduler keeps no state and never replays the past. A due slot fires if
it is at most `grace` seconds late (default 30 s); an older one, after a stall,
is skipped with a warning and the scheduler resumes at the next slot. Slots
that passed while the scheduler was down are not published. Set grace with
`Scheduler(app, grace=...)`; `asynk scheduler` uses the default.

Fire times are decided with the wall clock, re-read after every wake. If the
clock steps backwards, a scheduler never publishes a slot twice: it waits for
the next slot after the last one it handled.

## Running one scheduler, and dedupe

Each publish carries two metadata keys:

- `idempotency_key`: `"<task name>:<slot as a UTC ISO instant>"`;
- `scheduled_at`: the slot, as a UTC ISO instant.

**With a store**, schedulers claim each slot in the store before publishing it,
so overlapping schedulers publish each slot once. Give them one shared store:
`MemoryStore` covers one process, and several machines need a network store, such as the
[Redis store](redis.md). A
scheduler releases its claim when its publish fails or is cancelled, so another
scheduler can publish that slot. If the store cannot be reached, the scheduler
publishes anyway rather than stop the schedule. A hard crash between the claim
and the publish loses that slot.

**Without a store** there is no locking or coordination: run exactly one
scheduler, since a second one publishes every slot twice. Duplicates are also
possible around restarts and failovers. Deduping on the consuming side with
`metadata["idempotency_key"]` needs a record shared by every worker: a set in
one worker's memory only covers that worker.

There is no guard: every slot is published, and a scheduled task decides for
itself whether there is work to do. A guard inside the scheduler could stall
the other scheduled tasks.

## Hooks and probes

The scheduler publishes with the app's producer and runs the app's
[lifecycle hooks](../advanced/lifecycle-hooks.md), with `runner.role` set to
`"scheduler"`. Keep those hooks quick: the scheduler does not refresh its
liveness probe while one runs, so a hook slower than your probe's staleness
limit gets the process killed, and a slow hook delays shutdown by its full
duration.

The scheduler writes the `liveness` probe file, never `readiness`; use the
liveness probe from [Probes](probes.md) with the same `--probe-dir`. Its main
loop refreshes the probe, so a scheduler stuck in a hung publish (for example on
a half-open connection) stops refreshing it and `asynk health liveness` fails.
That is intended: run the scheduler under a supervisor, such as a Kubernetes
liveness probe, that restarts it.
