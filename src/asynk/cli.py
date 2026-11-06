"""The `asynk` command line: `worker`, `scheduler` and `health`."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from asynk.app import App, NotConfiguredError
from asynk.logging import configure_logging
from asynk.probes import check_probe, default_probe_dir
from asynk.scheduler import Scheduler
from asynk.worker import Worker


class CliError(Exception):
    """Raised for user-facing command-line errors."""


def load_app(path: str) -> App:
    """Imports an app from a `module:attr` path.

    Args:
        path: Import path of the form `module:attr`.

    Returns:
        The app.

    Raises:
        CliError: If the path is malformed, cannot be imported, or is not an `App`.
    """
    module_name, sep, attr = path.partition(":")
    if not sep or not module_name or not attr:
        raise CliError(f"invalid app path {path!r}: expected 'module:attr'")
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise CliError(f"cannot import {module_name!r}: {exc}") from exc
    try:
        obj = getattr(module, attr)
    except AttributeError:
        raise CliError(f"module {module_name!r} has no attribute {attr!r}") from None
    if not isinstance(obj, App):
        raise CliError(f"{path!r} is a {type(obj).__name__}, not an asynk App")
    return obj


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asynk")
    commands = parser.add_subparsers(dest="command", required=True)

    worker = commands.add_parser("worker", help="run a worker for an app")
    worker.add_argument("app", metavar="MODULE:APP")
    worker.add_argument("--concurrency", type=int)
    worker.add_argument("--shutdown-timeout", type=float)
    worker.add_argument("--heartbeat-interval", type=float)
    worker.add_argument("--probe-dir", type=Path, default=default_probe_dir())
    worker.add_argument(
        "--log-level",
        default="INFO",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )

    scheduler = commands.add_parser("scheduler", help="publish an app's cron scheduled tasks")
    scheduler.add_argument("app", metavar="MODULE:APP")
    scheduler.add_argument("--heartbeat-interval", type=float)
    scheduler.add_argument("--probe-dir", type=Path, default=default_probe_dir())
    scheduler.add_argument(
        "--log-level",
        default="INFO",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )

    health = commands.add_parser("health", help="check a worker or scheduler probe file")
    health.add_argument("probe", choices=["liveness", "readiness"])
    health.add_argument("--max-age", type=float, required=True)
    health.add_argument("--probe-dir", type=Path, default=default_probe_dir())
    return parser


def _run_worker(args: argparse.Namespace) -> int:
    configure_logging(args.log_level)
    app = load_app(args.app)
    options: dict[str, Any] = {
        key: value
        for key in ("concurrency", "shutdown_timeout", "heartbeat_interval")
        if (value := getattr(args, key)) is not None
    }
    try:
        worker = Worker(app, probe_dir=args.probe_dir, **options)
    except (NotConfiguredError, ValueError) as exc:
        raise CliError(str(exc)) from exc
    logging.getLogger("asynk").info(
        "starting worker for app %r: concurrency=%d shutdown_timeout=%s probe_dir=%s",
        app.name,
        worker.concurrency,
        worker.shutdown_timeout,
        worker.probe_dir,
    )
    asyncio.run(worker.run_with_signals())
    return 0


def _run_scheduler(args: argparse.Namespace) -> int:
    configure_logging(args.log_level)
    app = load_app(args.app)
    options: dict[str, Any] = {}
    if args.heartbeat_interval is not None:
        options["heartbeat_interval"] = args.heartbeat_interval
    try:
        scheduler = Scheduler(app, probe_dir=args.probe_dir, **options)
    except (NotConfiguredError, ValueError) as exc:
        raise CliError(str(exc)) from exc
    logging.getLogger("asynk").info(
        "starting scheduler for app %r: scheduled_tasks=[%s] probe_dir=%s",
        app.name,
        ", ".join(
            f"{t.name} ({t.schedule.expression}, {t.schedule.tz_name})" for t in app.scheduled_tasks
        ),
        scheduler.probe_dir,
    )
    asyncio.run(scheduler.run_with_signals())
    return 0


def _run_health(args: argparse.Namespace) -> int:
    healthy, message = check_probe(args.probe_dir / args.probe, args.max_age)
    print(message)
    return 0 if healthy else 1


def main(argv: Sequence[str] | None = None) -> int:
    """Runs the `asynk` command line.

    Args:
        argv: Arguments to parse; defaults to `sys.argv[1:]`.

    Returns:
        The process exit code.
    """
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "worker":
            return _run_worker(args)
        if args.command == "scheduler":
            return _run_scheduler(args)
        return _run_health(args)
    except CliError as exc:
        print(f"asynk: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
