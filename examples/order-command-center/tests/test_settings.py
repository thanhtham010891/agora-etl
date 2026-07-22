import pytest
from order_command_center.pipelines import postgres, redis
from order_command_center.settings import load_settings


def test_projection_modules_take_identity_and_storage_names_from_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_PIPELINE_ID", "ledger-projection")
    monkeypatch.setenv("POSTGRES_PROCESS_NAME", "ledger-worker")
    monkeypatch.setenv("POSTGRES_GROUP", "ledger-v2")
    monkeypatch.setenv("EVENT_LEDGER_TABLE", "commerce_ledger")
    monkeypatch.setenv("DLQ_TABLE", "commerce_dlq")
    monkeypatch.setenv("REPLAY_AUDIT_TABLE", "commerce_replay_audit")

    settings = load_settings()

    assert settings.postgres_projection.pipeline_id == "ledger-projection"
    assert settings.postgres_projection.process_name == "ledger-worker"
    assert settings.postgres_projection.consumer_group == "ledger-v2"
    assert settings.tables.event_ledger == "commerce_ledger"
    assert settings.tables.dead_letter_queue == "commerce_dlq"
    assert settings.tables.replay_audit == "commerce_replay_audit"
    assert callable(postgres.run)
    assert callable(redis.run)


def test_database_table_settings_reject_unsafe_sql_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EVENT_LEDGER_TABLE", "ledger; DROP TABLE orders")

    with pytest.raises(ValueError, match="EVENT_LEDGER_TABLE"):
        load_settings()
