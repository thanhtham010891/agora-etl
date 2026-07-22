"""Triage poison records and replay corrected events with immutable audit evidence."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from uuid import uuid4

from agora_plugins.kafka import KafkaSink

from order_command_center.contracts import OrderEvent
from order_command_center.settings import Settings, load_settings


@dataclass(frozen=True, slots=True)
class ReplayPreview:
    """Validated replay intent that has not changed any external state."""

    dlq_record_id: int
    kafka_topic: str
    corrected_event: dict[str, object]
    corrected_payload_sha256: str
    change_ticket: str
    reason: str


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Stable result returned after a replay request has been audited."""

    replay_id: str
    dlq_record_id: int
    kafka_topic: str
    producer_run_id: str
    corrected_event_id: str
    state: str


async def list_records(*, limit: int | None = None) -> list[dict[str, object]]:
    """List DLQ records with their latest replay state, without deleting evidence."""

    settings = load_settings()
    inspection_limit = settings.dlq_inspection_limit if limit is None else limit
    if inspection_limit < 1:
        raise ValueError("limit must be at least 1")
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT dlq.id, dlq.pipeline_id, dlq.stage, dlq.error_type, dlq.error_message, "
            "dlq.created_at, latest.state AS replay_state, latest.replay_id "
            f"FROM {settings.tables.dead_letter_queue} AS dlq "
            "LEFT JOIN LATERAL ("
            f"  SELECT replay_id, state FROM {settings.tables.replay_requests} "
            "  WHERE dlq_record_id = dlq.id ORDER BY requested_at DESC, replay_id DESC LIMIT 1"
            ") AS latest ON true "
            "ORDER BY dlq.created_at DESC, dlq.id DESC LIMIT %s",
            (inspection_limit,),
        )
        rows = await cursor.fetchall()
    return [
        {
            "id": int(row[0]),
            "pipeline_id": str(row[1]),
            "stage": str(row[2]),
            "error_type": str(row[3]),
            "error_message": str(row[4]),
            "created_at": row[5],
            "replay_state": str(row[6]) if row[6] is not None else "untriaged",
            "replay_id": str(row[7]) if row[7] is not None else None,
        }
        for row in rows
    ]


async def show_record(dlq_record_id: int) -> dict[str, object]:
    """Return one poison record, every replay request, and its append-only audit trail."""

    settings = load_settings()
    record = await _load_dlq_record(settings, dlq_record_id)
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT replay_id, kafka_topic, producer_run_id, corrected_event_id, "
            "corrected_payload_sha256, change_ticket, reason, state, requested_at, completed_at, "
            f"failure_detail FROM {settings.tables.replay_requests} "
            "WHERE dlq_record_id = %s ORDER BY requested_at DESC, replay_id DESC",
            (dlq_record_id,),
        )
        replay_rows = await cursor.fetchall()
        await cursor.execute(
            f"SELECT audit.replay_id, audit.event_type, audit.details, audit.recorded_at "
            f"FROM {settings.tables.replay_audit} AS audit "
            f"JOIN {settings.tables.replay_requests} AS request ON request.replay_id = audit.replay_id "
            "WHERE request.dlq_record_id = %s ORDER BY audit.recorded_at ASC, audit.audit_id ASC",
            (dlq_record_id,),
        )
        audit_rows = await cursor.fetchall()
    record["replays"] = [
        {
            "replay_id": str(row[0]),
            "kafka_topic": str(row[1]),
            "producer_run_id": str(row[2]),
            "corrected_event_id": str(row[3]),
            "corrected_payload_sha256": str(row[4]),
            "change_ticket": str(row[5]),
            "reason": str(row[6]),
            "state": str(row[7]),
            "requested_at": row[8],
            "completed_at": row[9],
            "failure_detail": row[10],
        }
        for row in replay_rows
    ]
    record["audit"] = [
        {
            "replay_id": str(row[0]),
            "event_type": str(row[1]),
            "details": _json_value(row[2]),
            "recorded_at": row[3],
        }
        for row in audit_rows
    ]
    return record


async def preview_replay(
    *,
    dlq_record_id: int,
    payload_file: Path,
    change_ticket: str,
    reason: str,
) -> ReplayPreview:
    """Validate a corrected event and required external-approval references."""

    settings = load_settings()
    await _load_dlq_record(settings, dlq_record_id)
    ticket = _required_text(change_ticket, name="change ticket")
    replay_reason = _required_text(reason, name="reason")
    corrected_event = _load_corrected_event(payload_file)
    payload_hash = _payload_sha256(corrected_event)
    return ReplayPreview(
        dlq_record_id=dlq_record_id,
        kafka_topic=settings.kafka_topic,
        corrected_event=corrected_event,
        corrected_payload_sha256=payload_hash,
        change_ticket=ticket,
        reason=replay_reason,
    )


async def execute_replay(preview: ReplayPreview) -> ReplayResult:
    """Publish a validated repair after creating a durable request and audit event.

    PostgreSQL and Kafka are independent systems, so this operation is
    deliberately at-least-once across the two. A request is made durable
    before publishing; an interrupted request remains visibly ``publishing``
    for operator diagnosis rather than being silently treated as complete.
    """

    settings = load_settings()
    replay_id = f"replay_{uuid4().hex}"
    producer_run_id = f"dlq_replay_{replay_id.removeprefix('replay_')}"
    corrected_event = dict(preview.corrected_event)
    corrected_event["producer_run_id"] = producer_run_id
    payload_hash = _payload_sha256(corrected_event)
    await _create_replay_request(
        settings=settings,
        replay_id=replay_id,
        preview=preview,
        producer_run_id=producer_run_id,
        corrected_event=corrected_event,
        payload_hash=payload_hash,
    )
    try:
        await _publish_event(settings=settings, event=corrected_event)
    except Exception as exc:
        await _complete_replay_request(
            settings=settings,
            replay_id=replay_id,
            state="failed",
            failure_detail=f"{type(exc).__name__}: {exc}",
        )
        raise
    await _complete_replay_request(settings=settings, replay_id=replay_id, state="published")
    return ReplayResult(
        replay_id=replay_id,
        dlq_record_id=preview.dlq_record_id,
        kafka_topic=preview.kafka_topic,
        producer_run_id=producer_run_id,
        corrected_event_id=str(corrected_event["event_id"]),
        state="published",
    )


async def reconcile_replay(replay_id: str) -> ReplayResult:
    """Complete an interrupted replay only after PostgreSQL proves Kafka delivery.

    Reconciliation never publishes. It is safe to invoke after a failure
    between Kafka acknowledgement and the replay-request state update because
    it requires exactly one durable ledger delivery for the request's stable
    ``producer_run_id`` and corrected event id.
    """

    request_id = _required_text(replay_id, name="id")
    settings = load_settings()
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT dlq_record_id, kafka_topic, producer_run_id, corrected_event_id, state "
            f"FROM {settings.tables.replay_requests} WHERE replay_id = %s",
            (request_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise ValueError(f"Replay request {request_id!r} was not found.")
        dlq_record_id, kafka_topic, producer_run_id, corrected_event_id, state = row
        if state == "published":
            return ReplayResult(
                replay_id=request_id,
                dlq_record_id=int(dlq_record_id),
                kafka_topic=str(kafka_topic),
                producer_run_id=str(producer_run_id),
                corrected_event_id=str(corrected_event_id),
                state="published",
            )
        if state != "publishing":
            raise RuntimeError(
                f"Replay request {request_id!r} is {state!r}; only publishing requests can reconcile."
            )
        await cursor.execute(
            f"SELECT count(*), count(*) FILTER (WHERE event_id = %s) "
            f"FROM {settings.tables.event_ledger} "
            "WHERE kafka_topic = %s AND producer_run_id = %s",
            (corrected_event_id, kafka_topic, producer_run_id),
        )
        delivery_count, matching_event_count = map(int, await cursor.fetchone())

    if (delivery_count, matching_event_count) != (1, 1):
        raise RuntimeError(
            "Replay cannot reconcile until PostgreSQL proves exactly one corrected Kafka delivery: "
            f"deliveries={delivery_count}, matching_events={matching_event_count}."
        )
    await _complete_replay_request(
        settings=settings,
        replay_id=request_id,
        state="published",
        audit_event_type="reconciled",
        audit_details={
            "verified_ledger_deliveries": delivery_count,
            "verified_matching_events": matching_event_count,
        },
    )
    return ReplayResult(
        replay_id=request_id,
        dlq_record_id=int(dlq_record_id),
        kafka_topic=str(kafka_topic),
        producer_run_id=str(producer_run_id),
        corrected_event_id=str(corrected_event_id),
        state="published",
    )


async def _load_dlq_record(settings: Settings, dlq_record_id: int) -> dict[str, object]:
    if dlq_record_id < 1:
        raise ValueError("DLQ record id must be at least 1")
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT id, pipeline_id, run_id, stage, error_type, error_message, record, "
            f"original_record, processed_record, source, checkpoint, details, middleware, sink, "
            f"created_at, attempt, max_attempts FROM {settings.tables.dead_letter_queue} WHERE id = %s",
            (dlq_record_id,),
        )
        row = await cursor.fetchone()
    if row is None:
        raise ValueError(f"DLQ record {dlq_record_id} was not found.")
    fields = (
        "id",
        "pipeline_id",
        "run_id",
        "stage",
        "error_type",
        "error_message",
        "record",
        "original_record",
        "processed_record",
        "source",
        "checkpoint",
        "details",
        "middleware",
        "sink",
        "created_at",
        "attempt",
        "max_attempts",
    )
    payload = dict(zip(fields, row, strict=True))
    for field in ("record", "original_record", "processed_record", "checkpoint", "details"):
        payload[field] = _json_value(payload[field])
    return payload


async def _create_replay_request(
    *,
    settings: Settings,
    replay_id: str,
    preview: ReplayPreview,
    producer_run_id: str,
    corrected_event: dict[str, object],
    payload_hash: str,
) -> None:
    import psycopg

    audit_details = {
        "dlq_record_id": preview.dlq_record_id,
        "kafka_topic": preview.kafka_topic,
        "producer_run_id": producer_run_id,
        "corrected_event_id": corrected_event["event_id"],
        "corrected_payload_sha256": payload_hash,
        "change_ticket": preview.change_ticket,
        "reason": preview.reason,
    }
    async with await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                f"INSERT INTO {settings.tables.replay_requests} "
                "(replay_id, dlq_record_id, kafka_topic, producer_run_id, corrected_event_id, "
                "corrected_payload, corrected_payload_sha256, change_ticket, reason, state) "
                "VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, 'publishing')",
                (
                    replay_id,
                    preview.dlq_record_id,
                    preview.kafka_topic,
                    producer_run_id,
                    str(corrected_event["event_id"]),
                    json.dumps(corrected_event, sort_keys=True),
                    payload_hash,
                    preview.change_ticket,
                    preview.reason,
                ),
            )
            await cursor.execute(
                f"INSERT INTO {settings.tables.replay_audit} (replay_id, event_type, details) "
                "VALUES (%s, 'requested', %s::jsonb)",
                (replay_id, json.dumps(audit_details, sort_keys=True)),
            )
        await connection.commit()


async def _complete_replay_request(
    *,
    settings: Settings,
    replay_id: str,
    state: str,
    failure_detail: str | None = None,
    audit_event_type: str | None = None,
    audit_details: dict[str, object] | None = None,
) -> None:
    if state not in {"published", "failed"}:
        raise ValueError(f"Unsupported replay completion state: {state!r}")
    event_type = audit_event_type or state
    details = audit_details or {"failure_detail": failure_detail}
    import psycopg

    async with await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                f"UPDATE {settings.tables.replay_requests} "
                "SET state = %s, completed_at = now(), failure_detail = %s "
                "WHERE replay_id = %s AND state = 'publishing'",
                (state, failure_detail, replay_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Replay request {replay_id!r} is not in publishing state.")
            await cursor.execute(
                f"INSERT INTO {settings.tables.replay_audit} (replay_id, event_type, details) "
                "VALUES (%s, %s, %s::jsonb)",
                (
                    replay_id,
                    event_type,
                    json.dumps(details, sort_keys=True),
                ),
            )
        await connection.commit()


async def _publish_event(*, settings: Settings, event: dict[str, object]) -> None:
    sink = KafkaSink(
        topic=settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        serializer=lambda row: json.dumps(row, sort_keys=True).encode(),
        key_fn=lambda row: str(row["order_id"]).encode(),
    )
    await sink.open()
    try:
        await sink.write(event)
        await sink.flush()
    finally:
        await sink.close()


def _load_corrected_event(payload_file: Path) -> dict[str, object]:
    try:
        raw_payload = json.loads(payload_file.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"Cannot read corrected payload {payload_file}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Corrected payload {payload_file} is not valid JSON: {exc}") from exc
    if not isinstance(raw_payload, dict):
        raise TypeError("Corrected payload must be a JSON object.")
    return OrderEvent.model_validate(raw_payload).model_dump(mode="json")


def _payload_sha256(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(canonical).hexdigest()


def _required_text(value: str, *, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"Replay {name} is required.")
    return normalized


def _json_value(value: object) -> object:
    return json.loads(value) if isinstance(value, str) else value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    list_parser = commands.add_parser("list", help="List poison records and latest replay state.")
    list_parser.add_argument("--limit", type=int)
    show_parser = commands.add_parser("show", help="Inspect one poison record and replay audit.")
    show_parser.add_argument("dlq_record_id", type=int)
    replay_parser = commands.add_parser("replay", help="Validate or execute one corrected replay.")
    replay_parser.add_argument("dlq_record_id", type=int)
    replay_parser.add_argument("--payload-file", type=Path, required=True)
    replay_parser.add_argument(
        "--ticket", required=True, help="External approved change or incident ticket."
    )
    replay_parser.add_argument("--reason", required=True)
    replay_parser.add_argument(
        "--execute", action="store_true", help="Publish after validation and audit."
    )
    reconcile_parser = commands.add_parser(
        "reconcile", help="Complete a stranded publishing replay after ledger proof."
    )
    reconcile_parser.add_argument("replay_id")
    arguments = parser.parse_args()

    if arguments.command == "list":
        result: object = asyncio.run(list_records(limit=arguments.limit))
    elif arguments.command == "show":
        result = asyncio.run(show_record(arguments.dlq_record_id))
    elif arguments.command == "reconcile":
        result = asyncio.run(reconcile_replay(arguments.replay_id))
    else:
        preview = asyncio.run(
            preview_replay(
                dlq_record_id=arguments.dlq_record_id,
                payload_file=arguments.payload_file,
                change_ticket=arguments.ticket,
                reason=arguments.reason,
            )
        )
        result = (
            asyncio.run(execute_replay(preview))
            if arguments.execute
            else {"dry_run": asdict(preview)}
        )
    print(
        json.dumps(
            asdict(result) if isinstance(result, ReplayResult) else result, default=str, indent=2
        )
    )


if __name__ == "__main__":
    main()
