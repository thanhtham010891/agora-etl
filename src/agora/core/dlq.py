"""agora/core/dlq.py
=====================
Dead-letter queue payloads and sink abstraction.

Phase 1 scope:
- capture record-scoped failures that occur inside the pipeline runner
- preserve the original record, pipeline metadata, and error details
- delegate storage/transport to a normal ``BaseSink``
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

from agora.core.sink import BaseSink
from agora.core.source import BaseSource


@dataclass(frozen=True, slots=True)
class DLQRecord:
    """Serialized dead-letter payload for a failed record.

    ``record`` remains the compatibility/default replay payload. Newer
    runtimes also persist the explicit ``original_record`` and
    ``processed_record`` so replay can choose the right payload depending on
    stage and mode.
    """

    pipeline_id: str
    run_id: str
    stage: str
    error_type: str
    error_message: str
    record: Any
    source: str | None = None
    checkpoint: Any | None = None
    middleware: str | None = None
    sink: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    attempt: int = 0  # retry attempt counter (0 = first attempt)
    max_attempts: int | None = None  # None = unlimited retries
    original_record: Any | None = None
    processed_record: Any | None = None
    _storage_id: int | None = field(default=None, repr=False, compare=False)

    def replay_payload(self, mode: str = "pipeline") -> Any:
        """Return the best payload to replay for the requested mode."""
        if mode == "pipeline":
            if self.original_record is not None:
                return self.original_record
            if self.record is not None:
                return self.record
            return self.processed_record
        if mode == "sink":
            if self.processed_record is not None:
                return self.processed_record
            if self.record is not None:
                return self.record
            return self.original_record
        raise ValueError(f"Unsupported DLQ replay mode: {mode}")


class DLQSink(BaseSink[DLQRecord]):
    """Marker base class for dead-letter sinks."""

    sink_name = "dlq"

    async def replay(self, record: DLQRecord) -> DLQRecord:
        """Default: return record with attempt incremented (no storage write).

        Subclasses may override to write the incremented record back to storage
        before returning it.
        """
        import dataclasses

        return dataclasses.replace(record, attempt=record.attempt + 1)

    async def acknowledge(self, record: DLQRecord) -> None:
        """Mark *record* as successfully replayed.

        Default implementation is a no-op so existing/custom DLQ backends
        remain compatible. Storage-backed sinks should override this to
        delete or mark the record as completed.
        """


class DLQSource(BaseSource[DLQRecord]):
    """Source that replays DLQRecord objects from a DLQ storage backend."""

    source_name = "dlq_source"

    @abstractmethod
    async def _iter_records(self) -> AsyncGenerator[DLQRecord, None]:
        """Yield DLQRecord objects from the underlying storage backend."""
        ...

    async def stream(self) -> AsyncGenerator[DLQRecord, None]:
        """Yield only records that are still eligible for retry."""
        async for record in self._iter_records():
            if record.max_attempts is None or record.attempt < record.max_attempts:
                yield record


# ======================================================================
# SQLiteDLQSink / SQLiteDLQSource — file-backed DLQ for local use
# ======================================================================

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS dlq_records (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id   TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    stage         TEXT NOT NULL,
    error_type    TEXT NOT NULL,
    error_message TEXT NOT NULL,
    record        TEXT,
    original_record TEXT,
    processed_record TEXT,
    source        TEXT,
    checkpoint    TEXT,
    middleware    TEXT,
    sink          TEXT,
    created_at    TEXT NOT NULL,
    attempt       INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER
)
"""


def _record_to_row(r: DLQRecord) -> dict[str, Any]:
    return {
        "pipeline_id": r.pipeline_id,
        "run_id": r.run_id,
        "stage": r.stage,
        "error_type": r.error_type,
        "error_message": r.error_message,
        "record": json.dumps(r.record, default=str),
        "original_record": (
            json.dumps(r.original_record, default=str) if r.original_record is not None else None
        ),
        "processed_record": (
            json.dumps(r.processed_record, default=str) if r.processed_record is not None else None
        ),
        "source": r.source,
        "checkpoint": json.dumps(r.checkpoint, default=str) if r.checkpoint is not None else None,
        "middleware": r.middleware,
        "sink": r.sink,
        "created_at": r.created_at.isoformat(),
        "attempt": r.attempt,
        "max_attempts": r.max_attempts,
    }


def _row_to_record(row: sqlite3.Row) -> DLQRecord:
    def _json_or_raw(column: str) -> Any:
        keys = set(row.keys())
        if column not in keys:
            return None
        raw_value = row[column]
        try:
            return json.loads(raw_value) if raw_value is not None else None
        except (json.JSONDecodeError, TypeError):
            return raw_value

    return DLQRecord(
        pipeline_id=row["pipeline_id"],
        run_id=row["run_id"],
        stage=row["stage"],
        error_type=row["error_type"],
        error_message=row["error_message"],
        record=_json_or_raw("record"),
        source=row["source"],
        checkpoint=_json_or_raw("checkpoint"),
        middleware=row["middleware"],
        sink=row["sink"],
        created_at=datetime.fromisoformat(row["created_at"]),
        attempt=row["attempt"],
        max_attempts=row["max_attempts"],
        original_record=_json_or_raw("original_record"),
        processed_record=_json_or_raw("processed_record"),
        _storage_id=row["id"] if "id" in set(row.keys()) else None,
    )


def _ensure_sqlite_columns(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(dlq_records)").fetchall()}
    if "original_record" not in existing:
        conn.execute("ALTER TABLE dlq_records ADD COLUMN original_record TEXT")
    if "processed_record" not in existing:
        conn.execute("ALTER TABLE dlq_records ADD COLUMN processed_record TEXT")
    conn.commit()


class SQLiteDLQSink(DLQSink):
    """SQLite-backed dead-letter sink for local and single-process use.

    Usage::

        dlq = SQLiteDLQSink(".agora_dlq.db")
        pipeline = Pipeline(src).pipe(mw).build(sink, dlq=dlq)
        await pipeline.run()
    """

    sink_name = "sqlite_dlq"

    def __init__(self, path: str | Path = ".agora_dlq.db") -> None:
        self._path = str(path)
        self._conn: sqlite3.Connection | None = None
        self._conn_lock = threading.RLock()

    async def open(self) -> None:
        self._conn = await asyncio.to_thread(self._connect)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute(_CREATE_TABLE)
        _ensure_sqlite_columns(conn)
        conn.commit()
        return conn

    async def write(self, record: DLQRecord) -> None:
        row = _record_to_row(record)
        await asyncio.to_thread(self._insert, row)

    def _insert(self, row: dict[str, Any]) -> None:
        with self._conn_lock:
            if self._conn is None:
                raise RuntimeError(
                    f"{type(self).__name__} is not open — call open() before writing"
                )
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            self._conn.execute(
                f"INSERT INTO dlq_records ({cols}) VALUES ({placeholders})",
                list(row.values()),
            )
            self._conn.commit()

    async def replay(self, record: DLQRecord) -> DLQRecord:
        updated = await super().replay(record)
        await asyncio.to_thread(self._update_attempt, record, updated.attempt)
        return updated

    async def acknowledge(self, record: DLQRecord) -> None:
        await asyncio.to_thread(self._delete, record)

    def _update_attempt(self, record: DLQRecord, new_attempt: int) -> None:
        with self._conn_lock:
            if self._conn is None:
                raise RuntimeError(
                    f"{type(self).__name__} is not open — call open() before writing"
                )
            if record._storage_id is not None:
                self._conn.execute(
                    "UPDATE dlq_records SET attempt = ? WHERE id = ?",
                    (new_attempt, record._storage_id),
                )
            else:
                self._conn.execute(
                    "UPDATE dlq_records SET attempt = ? "
                    "WHERE id = (SELECT id FROM dlq_records "
                    "WHERE pipeline_id = ? AND run_id = ? AND stage = ? AND created_at = ? "
                    "ORDER BY id LIMIT 1)",
                    (
                        new_attempt,
                        record.pipeline_id,
                        record.run_id,
                        record.stage,
                        record.created_at.isoformat(),
                    ),
                )
            self._conn.commit()

    def _delete(self, record: DLQRecord) -> None:
        with self._conn_lock:
            if self._conn is None:
                raise RuntimeError(
                    f"{type(self).__name__} is not open — call open() before writing"
                )
            if record._storage_id is not None:
                self._conn.execute(
                    "DELETE FROM dlq_records WHERE id = ?",
                    (record._storage_id,),
                )
            else:
                self._conn.execute(
                    "DELETE FROM dlq_records WHERE id = ("
                    "SELECT id FROM dlq_records "
                    "WHERE pipeline_id = ? AND run_id = ? AND stage = ? AND created_at = ? "
                    "ORDER BY id LIMIT 1)",
                    (
                        record.pipeline_id,
                        record.run_id,
                        record.stage,
                        record.created_at.isoformat(),
                    ),
                )
            self._conn.commit()

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        if self._conn is not None:
            await asyncio.to_thread(self._close_conn)

    def _close_conn(self) -> None:
        with self._conn_lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None


class SQLiteDLQSource(DLQSource):
    """Read DLQRecords from a SQLite DLQ database for replay.

    Usage::

        source = SQLiteDLQSource(".agora_dlq.db", pipeline_id="my_pipeline")
        pipeline = Pipeline(source).build(real_sink)
        await pipeline.run()
    """

    source_name = "sqlite_dlq_source"

    def __init__(
        self,
        path: str | Path = ".agora_dlq.db",
        *,
        pipeline_id: str | None = None,
        stage: str | None = None,
        limit: int | None = None,
    ) -> None:
        self._path = str(path)
        self._pipeline_id = pipeline_id
        self._stage = stage
        self._limit = limit
        self._conn: sqlite3.Connection | None = None
        self._conn_lock = threading.RLock()

    async def open(self) -> None:
        self._conn = await asyncio.to_thread(self._connect)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute(_CREATE_TABLE)
        _ensure_sqlite_columns(conn)
        conn.commit()
        return conn

    async def close(self) -> None:
        if self._conn is not None:
            await asyncio.to_thread(self._close_conn)

    def _close_conn(self) -> None:
        with self._conn_lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    async def _iter_records(self) -> AsyncGenerator[DLQRecord, None]:
        rows = await asyncio.to_thread(self._fetch_rows)
        for row in rows:
            yield _row_to_record(row)

    def _fetch_rows(self) -> list[sqlite3.Row]:
        with self._conn_lock:
            if self._conn is None:
                raise RuntimeError(
                    f"{type(self).__name__} is not open — call open() before reading"
                )
            conditions: list[str] = []
            params: list[Any] = []
            if self._pipeline_id is not None:
                conditions.append("pipeline_id = ?")
                params.append(self._pipeline_id)
            if self._stage is not None:
                conditions.append("stage = ?")
                params.append(self._stage)
            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            # Use parameterized LIMIT to avoid SQL injection from non-integer input
            if self._limit is not None:
                limit_clause = "LIMIT ?"
                params.append(self._limit)
            else:
                limit_clause = ""
            sql = f"SELECT * FROM dlq_records {where} ORDER BY id ASC {limit_clause}"
            return self._conn.execute(sql, params).fetchall()


__all__ = [
    "DLQRecord",
    "DLQSink",
    "DLQSource",
    "SQLiteDLQSink",
    "SQLiteDLQSource",
]
