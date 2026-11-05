from __future__ import annotations

import asyncio
import os
import sqlite3
import threading
import time
from collections.abc import Callable


class SqliteStore:
    """Store backed by a SQLite file that several processes can share.

    Expired keys read as missing. `wait` polls the file every `poll_interval`
    seconds.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        ttl: float = 86400.0,
        *,
        poll_interval: float = 0.1,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Initializes the store.

        Args:
            path: Location of the database file.
            ttl: Seconds a key lives after it is written.
            poll_interval: Seconds between reads while in `wait`.
            clock: Wall-clock time source, replaceable for tests.
        """
        self._path = os.fspath(path)
        self._ttl = ttl
        self._poll_interval = poll_interval
        self._clock = clock
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

    async def start(self) -> None:
        """Opens the database and creates the table if needed."""
        await asyncio.to_thread(self._open)

    def _open(self) -> None:
        self._close()
        conn = sqlite3.connect(
            self._path, check_same_thread=False, isolation_level=None, timeout=30.0
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS store "
            "(key TEXT PRIMARY KEY, value BLOB NOT NULL, expires_at REAL NOT NULL)"
        )
        self._conn = conn

    async def stop(self) -> None:
        """Closes the database."""
        await asyncio.to_thread(self._close)

    def _close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SqliteStore is not started; call start() first")
        return self._conn

    async def get(self, key: str) -> bytes | None:
        """Reads a value, or returns None if the key is missing or expired.

        Raises:
            RuntimeError: If the store has not been started.
        """
        return await asyncio.to_thread(self._get, key)

    def _get(self, key: str) -> bytes | None:
        with self._lock:
            conn = self._require_conn()
            now = self._clock()
            row = conn.execute(
                "SELECT value, expires_at FROM store WHERE key = ?", (key,)
            ).fetchone()
            if row is None or row[1] <= now:
                # Read-only on purpose: polling must not contend for the write lock.
                # Expired rows are cleared by the next `set` of the key.
                return None
            return bytes(row[0])

    async def set(self, key: str, value: bytes, *, overwrite: bool = False) -> bool:
        """Writes a value; returns False if the key exists and `overwrite` is false."""
        return await asyncio.to_thread(self._set, key, value, overwrite)

    def _set(self, key: str, value: bytes, overwrite: bool) -> bool:
        with self._lock:
            conn = self._require_conn()
            now = self._clock()
            conflict = (
                "DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at"
                if overwrite
                else "DO NOTHING"
            )
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("DELETE FROM store WHERE key = ? AND expires_at <= ?", (key, now))
                cursor = conn.execute(
                    "INSERT INTO store (key, value, expires_at) VALUES (?, ?, ?) "
                    f"ON CONFLICT(key) {conflict}",
                    (key, value, now + self._ttl),
                )
                written = cursor.rowcount == 1
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return written

    async def wait(self, key: str, timeout: float | None = None) -> bytes | None:
        """Waits until the key has a value; returns None if `timeout` elapses first."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            value = await self.get(key)
            if value is not None:
                return value
            if deadline is None:
                await asyncio.sleep(self._poll_interval)
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(self._poll_interval, remaining))

    async def delete(self, key: str) -> bool:
        """Removes a key; returns False if it is missing or expired."""
        return await asyncio.to_thread(self._delete, key)

    def _delete(self, key: str) -> bool:
        with self._lock:
            conn = self._require_conn()
            now = self._clock()
            cursor = conn.execute("DELETE FROM store WHERE key = ? AND expires_at > ?", (key, now))
            conn.commit()
            return cursor.rowcount == 1
