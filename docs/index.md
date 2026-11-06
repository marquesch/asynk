# asynk

asynk is a lightweight asyncio task framework for Python 3.11+, built on the
standard library only. Define tasks with a decorator, publish them with
`delay`, and run them in workers, behind any broker you plug in: both the
transport and the result store are pluggable.

## Install

```bash
pip install asynk
```

or, with uv:

```bash
uv add asynk
```

## A first look

``` { .python }
--8<-- "examples/getting_started/first_app.py"
```

With a broker transport, producers and workers run in separate processes; start
a worker with `asynk worker tasks:app`.

Continue with [Getting started](getting-started.md) and the
[Concepts](concepts.md), or browse the [API reference](reference.md).
