"""Recovery UX helpers shared by checkpoint/diagnose CLI commands."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

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
    return RecoveryInsight(
        support="custom/plugin source",
        resume_key="source-defined",
        granularity="source-defined",
        resume_behavior=(
            "inspect the source implementation or plugin docs for exact resume semantics"
        ),
        resume_cost_model="source-defined",
    )


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


def recovery_insight_to_dict(insight: RecoveryInsight | None) -> dict[str, Any] | None:
    """Convert *insight* into a JSON-serializable dict for CLI output."""
    if insight is None:
        return None
    return asdict(insight)


__all__ = [
    "RecoveryInsight",
    "RecoveryWarning",
    "recovery_insight_for_source",
    "recovery_insight_to_dict",
]
