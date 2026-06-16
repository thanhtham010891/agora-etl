from __future__ import annotations

from agora.core.recovery import SourceRecoveryContractSnapshot, SourceRecoveryMode


def test_source_recovery_contract_snapshot_to_dict_uses_common_shape() -> None:
    snapshot = SourceRecoveryContractSnapshot(
        mode=SourceRecoveryMode.CHECKPOINT_RERUN,
        supports_checkpoint=True,
        checkpoint_fields=("created_at", "id"),
        checkpoint_params={"created_at": "last_created_at", "id": "last_id"},
        on_record_error="dlq_and_continue",
    )

    assert snapshot.to_dict() == {
        "mode": "checkpoint_rerun",
        "supports_checkpoint": True,
        "requires_pipeline_rerun": True,
        "transparent_failover": False,
        "checkpoint_fields": ["created_at", "id"],
        "checkpoint_params": {"created_at": "last_created_at", "id": "last_id"},
        "on_record_error": "dlq_and_continue",
    }


def test_source_recovery_contract_defaults_to_full_rerun_shape() -> None:
    snapshot = SourceRecoveryContractSnapshot(
        mode=SourceRecoveryMode.FULL_RERUN,
        supports_checkpoint=False,
    )

    assert snapshot.to_dict() == {
        "mode": "full_rerun",
        "supports_checkpoint": False,
        "requires_pipeline_rerun": True,
        "transparent_failover": False,
        "checkpoint_fields": [],
        "checkpoint_params": {},
        "on_record_error": "fail_closed",
    }
