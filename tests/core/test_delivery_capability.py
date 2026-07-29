"""Tests for machine-readable pipeline delivery reports."""

from __future__ import annotations

from typing import Any

import pytest

from agora import DeliveryConfig, IterableSource, Pipeline
from agora.core.checkpoint import InMemoryCheckpointStore
from agora.core.delivery import (
    DeliveryPolicy,
    DeliveryPolicyMismatchError,
    IdempotencyMode,
    SinkDeliveryCapability,
)
from agora.sources.file import CsvSource


class _Sink:
    sink_name = "test_sink"

    def __init__(self) -> None:
        self.opened = False

    async def open(self) -> None:
        self.opened = True

    async def write(self, record: Any) -> None:
        del record

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _ReplaySafeSink(_Sink):
    sink_name = "replay_safe_sink"

    def delivery_capability(self) -> SinkDeliveryCapability:
        return SinkDeliveryCapability(
            sink_name=self.sink_name,
            idempotency=IdempotencyMode.SINK_NATIVE,
            replay_safe=True,
            notes=("deduplicates by record key",),
        )


def test_explain_reports_conservative_delivery_defaults() -> None:
    explain = Pipeline(IterableSource([{"id": 1}])).build(_Sink()).explain()

    assert explain.delivery.guarantee.value == "at_least_once"
    assert explain.delivery.source_checkpointing_enabled is False
    assert explain.delivery.source_identity_supported is False
    assert explain.delivery.replay_safe is False
    assert explain.delivery.sinks[0].idempotency == IdempotencyMode.UNKNOWN
    assert "checkpoint_not_enabled" in explain.delivery.risk_flags
    assert "sink:test_sink:idempotency_unknown" in explain.delivery.risk_flags

    payload = explain.to_dict()["delivery"]
    assert payload["duplicate_delivery_possible"] is True
    assert payload["transactional_checkpoint_coupling"] is False
    assert payload["sinks"][0]["idempotency"] == "unknown"


def test_explain_reports_identity_and_replay_safe_sink(tmp_path) -> None:
    path = tmp_path / "records.csv"
    path.write_text("id\n1\n", encoding="utf-8")
    explain = (
        Pipeline(CsvSource(path=path, row_mapper=lambda row: row))
        .build(_ReplaySafeSink(), config=DeliveryConfig(checkpoint=InMemoryCheckpointStore()))
        .explain()
    )

    assert explain.delivery.source_checkpointing_enabled is True
    assert explain.delivery.source_identity_supported is True
    assert explain.delivery.replay_safe is True
    assert explain.delivery.sinks == (
        SinkDeliveryCapability(
            sink_name="replay_safe_sink",
            idempotency=IdempotencyMode.SINK_NATIVE,
            replay_safe=True,
            notes=("deduplicates by record key",),
        ),
    )


def test_explain_reports_delivery_policy_mismatches() -> None:
    explain = (
        Pipeline(IterableSource([{"id": 1}]))
        .build(
            _Sink(),
            config=DeliveryConfig(
                delivery_policy=DeliveryPolicy(
                    require_replay_safe=True,
                    require_idempotent_sinks=True,
                )
            ),
        )
        .explain()
    )

    assert [mismatch.code for mismatch in explain.delivery.policy_mismatches] == [
        "checkpoint_not_enabled",
        "source_identity_not_advertised",
        "sink_not_replay_safe",
        "sink_idempotency_unknown",
    ]
    payload = explain.to_dict()["delivery"]
    assert payload["policy"] == {
        "require_replay_safe": True,
        "require_idempotent_sinks": True,
    }
    assert payload["policy_mismatches"][2]["sink_name"] == "test_sink"


async def test_delivery_policy_blocks_before_pipeline_execution() -> None:
    sink = _Sink()
    pipeline = Pipeline(IterableSource([{"id": 1}]), id="policy-test").build(
        sink,
        config=DeliveryConfig(delivery_policy=DeliveryPolicy(require_replay_safe=True)),
    )

    with pytest.raises(DeliveryPolicyMismatchError) as exc_info:
        await pipeline.run()

    assert exc_info.value.pipeline_id == "policy-test"
    assert exc_info.value.stage == "delivery_policy"
    assert "checkpoint_not_enabled" in str(exc_info.value)
    assert sink.opened is False


def test_delivery_policy_accepts_identity_checked_replay_safe_pipeline(tmp_path) -> None:
    path = tmp_path / "records.csv"
    path.write_text("id\n1\n", encoding="utf-8")
    explain = (
        Pipeline(CsvSource(path=path, row_mapper=lambda row: row))
        .build(
            _ReplaySafeSink(),
            config=DeliveryConfig(
                checkpoint=InMemoryCheckpointStore(),
                delivery_policy=DeliveryPolicy(
                    require_replay_safe=True,
                    require_idempotent_sinks=True,
                ),
            ),
        )
        .explain()
    )

    assert explain.delivery.replay_safe is True
    assert explain.delivery.policy_mismatches == ()
