from __future__ import annotations

from agora.core import DLQPayloadPolicy


class _ReverseCipher:
    def encrypt(self, value: bytes) -> bytes:
        return value[::-1]

    def decrypt(self, value: bytes) -> bytes:
        return value[::-1]


def test_dlq_payload_policy_redacts_shared_sensitive_fields() -> None:
    payload = {
        "token": "secret-token",
        "nested": {"api_key": "abc123"},
        "headers": [
            {"key": "authorization", "value": {"encoding": "utf-8", "data": "Bearer token"}},
            {"key": "content-type", "value": {"encoding": "utf-8", "data": "application/json"}},
        ],
    }

    policy = DLQPayloadPolicy.redacted()
    redacted = policy.apply(payload)

    assert redacted["token"] == "[REDACTED]"
    assert redacted["nested"]["api_key"] == "[REDACTED]"
    assert redacted["headers"][0]["value"] == {"encoding": "redacted", "data": "[REDACTED]"}
    assert redacted["headers"][1]["value"]["data"] == "application/json"


def test_dlq_payload_policy_encrypts_and_decrypts_payload() -> None:
    payload = {"pipeline_id": "orders", "secret": "value"}
    policy = DLQPayloadPolicy.encrypted(
        encryptor=_ReverseCipher(),
        encryption_algorithm="reverse",
        encryption_key_id="test-key",
    )

    envelope = policy.encrypt_payload(payload)

    assert envelope["payload_encoding"] == "encrypted"
    assert envelope["payload_algorithm"] == "reverse"
    assert envelope["payload_key_id"] == "test-key"
    assert policy.decrypt_payload(envelope) == payload
