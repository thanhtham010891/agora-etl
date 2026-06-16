"""Recovery UX helpers shared by checkpoint/diagnose CLI commands."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from agora.core.failures import FailureClassification

_LARGE_FILE_RESUME_WARNING_THRESHOLD = 100_000
_VERY_LARGE_FILE_RESUME_WARNING_THRESHOLD = 1_000_000


@dataclass(frozen=True, slots=True)
class RecoveryWarning:
    """Structured warning for expensive or surprising recovery behavior."""

    level: str
    code: str
    message: str
    estimated_replay_units: int | None = None
    unit: str | None = None
    threshold: int | None = None


@dataclass(frozen=True, slots=True)
class RecoveryInsight:
    """Human-readable recovery contract for one source family."""

    support: str
    resume_key: str
    granularity: str
    resume_behavior: str
    resume_cost_model: str
    warning: RecoveryWarning | None = None
    runbook_hooks: tuple[str, ...] = ()


def recovery_insight_for_source(
    source_name: str | None,
    *,
    checkpoint_value: Any = None,
) -> RecoveryInsight | None:
    """Return CLI-facing recovery guidance for *source_name* when known."""
    if not source_name:
        return None

    normalized = source_name.lower()
    if normalized in {"csv", "arrow_csv"}:
        return RecoveryInsight(
            support="yes",
            resume_key="row_number",
            granularity="data row number (header excluded)",
            resume_behavior=(
                "re-reads the file from the beginning and skips rows up to the saved "
                "row_number before emitting the next row"
            ),
            resume_cost_model="linear re-read from file start",
            warning=_file_resume_warning(
                normalized,
                checkpoint_value,
                key_name="row_number",
                unit="rows",
            ),
        )
    if normalized in {"jsonl", "arrow_jsonl"}:
        return RecoveryInsight(
            support="yes",
            resume_key="line_number",
            granularity="line number of the last yielded record",
            resume_behavior=(
                "re-reads the file from the beginning and skips lines up to the saved "
                "line_number before emitting the next record"
            ),
            resume_cost_model="linear re-read from file start",
            warning=_file_resume_warning(
                normalized,
                checkpoint_value,
                key_name="line_number",
                unit="lines",
            ),
        )
    if normalized == "parquet":
        return RecoveryInsight(
            support="yes",
            resume_key="row_number",
            granularity="row number across the full file",
            resume_behavior=(
                "re-reads the file from the beginning and skips rows sequentially; "
                "there is no row-group seek at resume time"
            ),
            resume_cost_model="linear re-read from file start",
            warning=_file_resume_warning(
                normalized,
                checkpoint_value,
                key_name="row_number",
                unit="rows",
            ),
        )
    if normalized == "http":
        return RecoveryInsight(
            support="subclass opt-in",
            resume_key="source-defined cursor or watermark",
            granularity="source-defined",
            resume_behavior=(
                "the base HTTP source restarts from the beginning unless the subclass "
                "sets supports_checkpoint=True and implements prepare_resume()"
            ),
            resume_cost_model="source-defined",
        )
    if normalized == "iterable":
        return RecoveryInsight(
            support="no",
            resume_key="not supported",
            granularity="not supported",
            resume_behavior="always restarts from the beginning on the next run",
            resume_cost_model="full restart from source start",
        )
    if normalized == "kafka":
        return RecoveryInsight(
            support="yes",
            resume_key="topic/partition offsets",
            granularity="per Kafka partition offset",
            resume_behavior=(
                "restarts from the saved partition offsets and resumes consumer-group "
                "delivery from those coordinates; rebalance handoff and live-tail "
                "continue from the broker state after restart"
            ),
            resume_cost_model="partition-level offset resume with replay window bounded by the saved offsets",
            runbook_hooks=(
                "Verify the consumer group has stable partition assignment before cutover or replay.",
                "When using manual assign/seek flows, restart with the intended partition-offset set instead of relying on the broker to infer the replay window.",
            ),
        )
    if normalized == "redis_stream":
        return RecoveryInsight(
            support="yes",
            resume_key="message_id",
            granularity="Redis Stream message ID within the configured consumer group",
            resume_behavior=(
                "restarts from the saved message_id and consumer-group state; pending "
                "messages may be reclaimed before new records are read"
            ),
            resume_cost_model="consumer-group resume from the last checkpointed stream ID",
            runbook_hooks=(
                "Check pending entries and reclaim settings before assuming another Redis stream consumer has fully handed off work.",
                "After checkpoint reset, expect the consumer group to resume from stream history according to the configured group position.",
            ),
        )
    if normalized == "postgres":
        if _postgres_checkpoint_cursor_present(checkpoint_value):
            return RecoveryInsight(
                support="yes",
                resume_key="cursor",
                granularity="checkpoint cursor field(s) declared by the Postgres source",
                resume_behavior=(
                    "re-runs the configured query and reapplies the saved cursor before "
                    "yielding new rows; failover recovery is resume-on-rerun, not "
                    "transparent handoff"
                ),
                resume_cost_model="checkpoint rerun from the configured query start",
                runbook_hooks=(
                    "After primary promotion or route recovery, restart the pipeline so the Postgres source can reapply its checkpoint cursor.",
                    "Treat checkpoint reset as intentional replay: the next run will re-read from the query start until it reaches new rows.",
                ),
            )
        return RecoveryInsight(
            support="config-dependent",
            resume_key="cursor (when checkpoint field(s) are configured)",
            granularity="checkpoint cursor field(s) declared by the Postgres source",
            resume_behavior=(
                "with checkpoint field(s), restart re-runs the query and reapplies the "
                "saved cursor; without them, the source restarts from the configured "
                "query start"
            ),
            resume_cost_model="checkpoint rerun when cursor support is configured, otherwise full rerun",
            runbook_hooks=(
                "Verify checkpoint field(s) are configured before relying on failover resume semantics for PostgresSource.",
                "After Postgres topology changes, rerun the pipeline instead of expecting transparent failover.",
            ),
        )
    return RecoveryInsight(
        support="custom/plugin source",
        resume_key="source-defined",
        granularity="source-defined",
        resume_behavior=(
            "inspect the source implementation or plugin docs for exact resume semantics"
        ),
        resume_cost_model="source-defined",
    )


def failure_runbook_hooks(
    details: Any,
    *,
    record: Any = None,
    source_name: str | None = None,
) -> tuple[str, ...]:
    """Return operator-facing remediation hooks derived from DLQ details."""
    normalized_source = source_name.lower() if isinstance(source_name, str) else None
    kafka_hooks = _kafka_failure_runbook_hooks(
        details, record=record, source_name=normalized_source
    )
    if kafka_hooks:
        return kafka_hooks

    if not isinstance(details, dict):
        return ()
    postgres = details.get("postgres")
    if not isinstance(postgres, dict):
        return ()

    classification = str(postgres.get("classification") or "").strip().lower()
    reason = str(postgres.get("reason") or "").strip().lower()
    metadata = postgres.get("details")
    detail_map = metadata if isinstance(metadata, dict) else {}

    if classification == FailureClassification.SCHEMA_DRIFT.value:
        hooks = [
            "Compare incoming payload columns against the target Postgres table DDL before replay.",
        ]
        missing = detail_map.get("missing_required_columns")
        if isinstance(missing, list) and missing:
            hooks.append(
                "Backfill or derive the missing required columns before replay: "
                + ", ".join(str(item) for item in missing)
            )
        elif reason in {"undefined_table", "undefined_column"}:
            hooks.append(
                "Repair the missing Postgres table/column or align the write mapping, then replay the affected DLQ records."
            )
        return tuple(hooks)

    if classification == FailureClassification.CONSTRAINT_VIOLATION.value:
        hooks = [
            "Inspect the violated Postgres constraint and fix the offending data before replay.",
        ]
        if reason == "foreign_key_violation":
            hooks.append(
                "Restore parent rows or replay records in dependency order before replaying the blocked child rows."
            )
        elif reason == "unique_violation":
            hooks.append(
                "Decide whether the duplicate should be deduplicated upstream or handled with a different upsert/key policy."
            )
        return tuple(hooks)

    if classification == FailureClassification.TYPE_MISMATCH.value:
        return (
            "Fix row mapping or casting so values match the target Postgres column types before replay.",
            "Check source serialization and transform defaults for fields mentioned in the failure details.",
        )

    if classification == FailureClassification.UNKNOWN.value:
        return (
            "Inspect the raw Postgres DLQ details and database logs before replaying the record.",
        )

    return ()


def _file_resume_warning(
    source_name: str,
    checkpoint_value: Any,
    *,
    key_name: str,
    unit: str,
) -> RecoveryWarning | None:
    offset = _checkpoint_offset(checkpoint_value, key_name=key_name)
    if offset is None or offset < _LARGE_FILE_RESUME_WARNING_THRESHOLD:
        return None

    level = "warning"
    if offset >= _VERY_LARGE_FILE_RESUME_WARNING_THRESHOLD:
        level = "critical"

    if source_name == "parquet":
        return RecoveryWarning(
            level=level,
            code="high_resume_offset",
            message=(
                f"High resume offset detected ({key_name}={offset}). Built-in Parquet "
                "resume re-reads from the beginning and skips rows sequentially, so "
                "restart cost is proportional to that offset. Expect a full-file scan "
                "up to roughly the saved row position before new output appears."
            ),
            estimated_replay_units=offset,
            unit=unit,
            threshold=_LARGE_FILE_RESUME_WARNING_THRESHOLD,
        )
    return RecoveryWarning(
        level=level,
        code="high_resume_offset",
        message=(
            f"High resume offset detected ({key_name}={offset}). Built-in file-source "
            f"resume re-reads from the beginning and skips {unit} up to that offset, so "
            "restart cost grows with file size and checkpoint position. Expect the next "
            "run to scan from file start before reaching new data."
        ),
        estimated_replay_units=offset,
        unit=unit,
        threshold=_LARGE_FILE_RESUME_WARNING_THRESHOLD,
    )


def _checkpoint_offset(checkpoint_value: Any, *, key_name: str) -> int | None:
    if isinstance(checkpoint_value, dict) and key_name in checkpoint_value:
        value = checkpoint_value.get(key_name)
    else:
        value = None

    if value is None:
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return None


def _postgres_checkpoint_cursor_present(checkpoint_value: Any) -> bool:
    return isinstance(checkpoint_value, dict) and "cursor" in checkpoint_value


def _kafka_failure_runbook_hooks(
    details: Any,
    *,
    record: Any,
    source_name: str | None,
) -> tuple[str, ...]:
    if source_name != "kafka":
        return ()

    poison_payload = record if isinstance(record, dict) else {}
    if isinstance(details, dict) and "poison" in details and not poison_payload:
        poison_payload = details
    poison = poison_payload.get("poison") if isinstance(poison_payload, dict) else None
    if not isinstance(poison, dict):
        return ()

    classification = str(poison.get("classification") or "").strip().lower()
    policy = str(poison.get("policy") or "").strip().lower()
    topic = poison_payload.get("topic")
    partition = poison_payload.get("partition")
    offset = poison_payload.get("offset")
    location = (
        f"topic={topic}, partition={partition}, offset={offset}"
        if topic is not None and partition is not None and offset is not None
        else None
    )

    hooks: list[str] = []
    if classification == FailureClassification.DESERIALIZATION.value:
        hooks.append(
            "Inspect the producer payload encoding, compression, and serializer contract before replaying the Kafka record."
        )
    elif classification == FailureClassification.SCHEMA_EVOLUTION.value:
        hooks.append(
            "Compare producer and consumer schemas, then re-check compatibility mode before replaying the Kafka record."
        )
    elif classification == FailureClassification.SCHEMA_VALIDATION.value:
        hooks.append(
            "Fix the record shape or required fields so it satisfies the registered schema before replay."
        )
    elif classification == FailureClassification.SCHEMA_REGISTRY_BINDING_MISMATCH.value:
        hooks.append(
            "Verify schema registry subject, schema ID, and Protobuf message-index binding instead of trusting only local message_type configuration."
        )
    elif classification == FailureClassification.UNKNOWN.value:
        hooks.append(
            "Inspect the raw Kafka poison payload and broker-side serialization path before replay."
        )

    if location is not None:
        hooks.append(f"Reproduce and validate the failing Kafka payload at {location}.")
    if policy in {"dlq_and_continue", "dlq_and_fail_closed"}:
        hooks.append(
            "After the fix is confirmed, replay the Kafka DLQ record or seek back to the saved partition offsets to reopen the replay window."
        )
    elif policy == "fail_closed":
        hooks.append(
            "Unblock the source-side failure first, then restart the consumer so the saved offsets can be retried safely."
        )
    return tuple(hooks)


def recovery_insight_to_dict(insight: RecoveryInsight | None) -> dict[str, Any] | None:
    """Convert *insight* into a JSON-serializable dict for CLI output."""
    if insight is None:
        return None
    return asdict(insight)


__all__ = [
    "RecoveryInsight",
    "RecoveryWarning",
    "failure_runbook_hooks",
    "recovery_insight_for_source",
    "recovery_insight_to_dict",
]
