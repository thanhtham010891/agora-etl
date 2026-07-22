"""Apply versioned PostgreSQL migrations for the example's durable projections."""

from __future__ import annotations

import asyncio
from os import getenv
from pathlib import Path

from order_command_center.settings import load_settings


def _migration_files() -> list[Path]:
    configured_root = getenv("ORDER_COMMAND_CENTER_SQL_DIR")
    if configured_root:
        root = Path(configured_root)
    else:
        working_directory_root = Path.cwd() / "sql"
        root = (
            working_directory_root
            if working_directory_root.is_dir()
            else Path(__file__).resolve().parents[2] / "sql"
        )
    return sorted(root.glob("[0-9][0-9][0-9]_*.sql"))


async def run() -> list[str]:
    """Apply each migration once; each file and its version marker commit together."""
    import psycopg

    from order_command_center.producer import ensure_topic

    settings = load_settings()
    applied: list[str] = []
    async with await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {settings.tables.schema_migrations} ("
                "version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
            for path in _migration_files():
                await cursor.execute(
                    f"SELECT 1 FROM {settings.tables.schema_migrations} WHERE version = %s",
                    (path.name,),
                )
                if await cursor.fetchone() is not None:
                    continue
                await cursor.execute(path.read_text(encoding="utf-8"), prepare=False)
                await cursor.execute(
                    f"INSERT INTO {settings.tables.schema_migrations} (version) VALUES (%s)",
                    (path.name,),
                )
                applied.append(path.name)
        await connection.commit()
    await ensure_topic(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
        partitions=settings.kafka_topic_partitions,
        replication_factor=settings.kafka_topic_replication_factor,
    )
    return applied


def main() -> None:
    applied = asyncio.run(run())
    print(f"postgres migrations applied={','.join(applied) if applied else 'none'}")


if __name__ == "__main__":
    main()
