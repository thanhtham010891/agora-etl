"""
tests/integration/test_postgres_integration.py
===============================================
Integration tests for the PostgreSQL plugin (`agora_plugins.postgres` / psycopg).

Requires:
- ``AGORA_RUN_INTEGRATION=1`` environment variable
- Postgres reachable at ``127.0.0.1:55432`` (DSN via ``AGORA_TEST_POSTGRES_DSN``)
- ``psycopg`` installed (`pip install "agora-etl-plugins[postgres]"`)

Run with::

    AGORA_RUN_INTEGRATION=1 pytest tests/integration/test_postgres_integration.py -v

Requirements: 2.18, 2.19
"""

from __future__ import annotations

import pytest

# Skip entire module when psycopg or the PostgreSQL plugin is not installed.
psycopg = pytest.importorskip("psycopg")
agora_postgres = pytest.importorskip("agora_plugins.postgres")

from agora import IterableSource, MapMiddleware, Pipeline  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_N_RECORDS = 6


def _make_records(n: int, suffix: str) -> list[dict]:
    """Return a list of simple dicts suitable for upsert into a test table."""
    return [{"id": i, "name": f"item_{i}_{suffix}", "score": i * 10} for i in range(n)]


async def _create_test_table(conn, table: str) -> None:
    """Create a minimal test table with an integer primary key."""
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id      INTEGER PRIMARY KEY,
            name    TEXT    NOT NULL,
            score   INTEGER NOT NULL DEFAULT 0
        )
        """
    )


async def _drop_test_table(conn, table: str) -> None:
    """Drop the test table (cleanup)."""
    await conn.execute(f"DROP TABLE IF EXISTS {table}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_sink_upsert(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    """Create table, upsert records, assert conflict handling (ON CONFLICT DO UPDATE).

    Requirements: 2.18
    """
    table = f"agora_test_{unique_suffix}"
    records = _make_records(_N_RECORDS, unique_suffix)

    async with await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
        await _create_test_table(conn, table)
        try:
            # --- First upsert: insert all records ---
            sink = agora_postgres.PostgresSink(
                dsn=postgres_dsn,
                table=table,
                row_mapper=lambda record: record,
                conflict_key="id",
            )
            summary = await Pipeline(IterableSource(records)).build(sink).run()
            assert summary.records_written == _N_RECORDS

            # Verify all rows exist
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT COUNT(*) FROM {table}")
                row = await cur.fetchone()
                assert row is not None and row[0] == _N_RECORDS

            # --- Second upsert: update existing records (conflict → update) ---
            updated_records = [
                {"id": r["id"], "name": r["name"], "score": r["score"] + 100} for r in records
            ]
            sink2 = agora_postgres.PostgresSink(
                dsn=postgres_dsn,
                table=table,
                row_mapper=lambda record: record,
                conflict_key="id",
            )
            summary2 = await Pipeline(IterableSource(updated_records)).build(sink2).run()
            assert summary2.records_written == _N_RECORDS

            # Verify row count unchanged (upsert, not insert)
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT COUNT(*) FROM {table}")
                row = await cur.fetchone()
                assert row is not None and row[0] == _N_RECORDS

            # Verify scores were updated
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT id, score FROM {table} ORDER BY id")
                rows = await cur.fetchall()
                for i, (row_id, row_score) in enumerate(rows):
                    assert row_id == i
                    assert row_score == i * 10 + 100

        finally:
            await _drop_test_table(conn, table)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_source_cursor(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    """Insert records, read via PostgresSource, assert all consumed in order.

    Requirements: 2.19
    """
    table = f"agora_cursor_{unique_suffix}"
    records = _make_records(_N_RECORDS, unique_suffix)

    async with await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
        await _create_test_table(conn, table)
        try:
            # --- Seed the table directly ---
            async with conn.cursor() as cur:
                for r in records:
                    await cur.execute(
                        f"INSERT INTO {table} (id, name, score) VALUES (%s, %s, %s)",
                        (r["id"], r["name"], r["score"]),
                    )

            # --- Read via PostgresSource ---
            received: list[dict] = []

            class CollectSink:
                sink_name = "collect"

                async def open(self) -> None:
                    pass

                async def write(self, record) -> None:
                    received.append(record)

                async def flush(self) -> None:
                    pass

                async def close(self) -> None:
                    pass

            pg_source = agora_postgres.PostgresSource(
                dsn=postgres_dsn,
                query=f"SELECT id, name, score FROM {table} ORDER BY id",
                row_mapper=lambda row: row,
            )
            summary = await (
                Pipeline(pg_source)
                .build(CollectSink())  # type: ignore[arg-type]
                .run()
            )

            assert summary.records_consumed == _N_RECORDS
            assert len(received) == _N_RECORDS

            # Verify order and content
            for i, record in enumerate(received):
                assert record["id"] == i
                assert record["name"] == f"item_{i}_{unique_suffix}"
                assert record["score"] == i * 10

        finally:
            await _drop_test_table(conn, table)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_pipeline_roundtrip(
    postgres_dsn: str,
    unique_suffix: str,
) -> None:
    """Full pipeline PostgresSource → transform → PostgresSink.

    Reads from a source table, applies a transform (doubles the score),
    and writes to a destination table. Asserts record count and transformed content.

    Requirements: 2.18, 2.19
    """
    src_table = f"agora_src_{unique_suffix}"
    dst_table = f"agora_dst_{unique_suffix}"
    records = _make_records(_N_RECORDS, unique_suffix)

    async with await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
        await _create_test_table(conn, src_table)
        await _create_test_table(conn, dst_table)
        try:
            # --- Seed source table ---
            async with conn.cursor() as cur:
                for r in records:
                    await cur.execute(
                        f"INSERT INTO {src_table} (id, name, score) VALUES (%s, %s, %s)",
                        (r["id"], r["name"], r["score"]),
                    )

            # --- Run pipeline: PostgresSource → double score → PostgresSink ---
            pg_source = agora_postgres.PostgresSource(
                dsn=postgres_dsn,
                query=f"SELECT id, name, score FROM {src_table} ORDER BY id",
                row_mapper=lambda row: row,
            )
            pg_sink = agora_postgres.PostgresSink(
                dsn=postgres_dsn,
                table=dst_table,
                row_mapper=lambda record: record,
                conflict_key="id",
            )
            summary = await (
                Pipeline(pg_source)
                .pipe(
                    MapMiddleware(
                        lambda r: {**r, "score": r["score"] * 2},
                        name="double_score",
                    )
                )
                .build(pg_sink)
                .run()
            )

            assert summary.records_consumed == _N_RECORDS
            assert summary.records_written == _N_RECORDS

            # --- Verify destination table ---
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT id, score FROM {dst_table} ORDER BY id")
                rows = await cur.fetchall()

            assert len(rows) == _N_RECORDS
            for i, (row_id, row_score) in enumerate(rows):
                assert row_id == i
                assert row_score == i * 10 * 2  # doubled

        finally:
            await _drop_test_table(conn, src_table)
            await _drop_test_table(conn, dst_table)
