from __future__ import annotations

import contextlib
import os
import tempfile
import time
from pathlib import Path


def default_probe_dir() -> Path:
    """Returns the default directory for probe files."""
    return Path(tempfile.gettempdir()) / "asynk"


class ProbeFiles:
    """Liveness and readiness marker files whose mtime is the heartbeat.

    Attributes:
        directory: Directory holding the files.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    @property
    def liveness(self) -> Path:
        """Path of the liveness file."""
        return self.directory / "liveness"

    @property
    def readiness(self) -> Path:
        """Path of the readiness file."""
        return self.directory / "readiness"

    def beat(self, *, ready: bool) -> None:
        """Refreshes the liveness file and sets or clears the readiness file.

        Args:
            ready: Whether to advertise readiness.

        Raises:
            OSError: If a probe file cannot be written.
        """
        self.directory.mkdir(parents=True, exist_ok=True)
        self._write(self.liveness)
        if ready:
            self._write(self.readiness)
        else:
            self.remove_readiness()

    def remove_readiness(self) -> None:
        """Removes the readiness file if present."""
        self.readiness.unlink(missing_ok=True)

    def clear(self) -> None:
        """Removes both files if present."""
        self.readiness.unlink(missing_ok=True)
        self.liveness.unlink(missing_ok=True)

    def _write(self, path: Path) -> None:
        # Same directory as the target so os.replace is an atomic rename.
        fd, tmp = tempfile.mkstemp(dir=self.directory, prefix=f".{path.name}.")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(str(time.time()))
            os.replace(tmp, path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise


def check_probe(path: Path, max_age: float, now: float | None = None) -> tuple[bool, str]:
    """Checks that a probe file exists and was modified recently. Never raises.

    Args:
        path: The probe file.
        max_age: Maximum acceptable age in seconds.
        now: Current time as a Unix timestamp; defaults to the system clock.

    Returns:
        A `(healthy, message)` tuple.
    """
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return False, f"{path.name} probe file missing: {path}"
    except OSError as exc:
        return False, f"{path.name} probe file unreadable: {exc.strerror or exc}"
    age = (time.time() if now is None else now) - mtime
    if age > max_age:
        return False, f"{path.name} probe stale: {age:.1f}s old (max {max_age:g}s)"
    return True, f"{path.name} probe ok: {age:.1f}s old"
