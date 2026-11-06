"""Errors shared across asynk."""

from __future__ import annotations


class NotConfiguredError(RuntimeError):
    """Raised when an app lacks a component required by the operation."""
