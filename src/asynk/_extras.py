"""Import-time guard shared by the packages that need an optional extra."""

from __future__ import annotations

import importlib


def require_extra(package: str, extra: str) -> None:
    """Imports `package`, or raises an ImportError that names the extra to install.

    Call it at the top of the package that needs the extra.

    Args:
        package: Top-level name of the third-party package the extra installs.
        extra: Name of the extra, as in `pip install 'asynk[<extra>]'`.

    Raises:
        ImportError: If `package` is not installed. An error raised by one of
            its own dependencies is passed on unchanged.
    """
    try:
        importlib.import_module(package)
    except ImportError as exc:
        if exc.name != package:
            raise
        raise ImportError(
            f"asynk[{extra}] needs the {package} package: pip install 'asynk[{extra}]'"
        ) from exc
