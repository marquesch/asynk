# Getting started

## Install

```bash
pip install asynk
```

or, with uv:

```bash
uv add asynk
```

## Your first app

An `App` ties together a transport (where messages travel) and a store (where
results are kept). This one uses the in-memory transport and store, so
everything runs in a single process.

``` { .python }
--8<-- "examples/getting_started/first_app.py"
```

`@app.task()` registers a function as a task. `add.delay(2, b=3)` publishes a
message and returns a handle. A `Worker` consumes the message and runs the task.

## Defining tasks

A task is an async function or a plain `def`. Plain functions run in a thread.
Default argument values work as you would expect, and `max_retries` sets how
often a failing task is retried.

``` { .python }
--8<-- "examples/getting_started/defining_tasks.py"
```

## Calling tasks

`delay` takes the same arguments as the function and is type-checked against
its signature. It returns a handle you can use to wait for the result or read
the task state.

``` { .python }
--8<-- "examples/getting_started/call_tasks.py"
```

## Running a worker

With a transport that connects separate processes, you run workers from the
command line. Point the CLI at the module and the app inside it:

```bash
asynk worker tasks:app --concurrency 10
```

## What next

See the [API reference](reference.md) for every public class and function.
