from __future__ import annotations

from typing import TYPE_CHECKING

from agora_plugins.redis import RedisBackend, RedisDLQSink

from agora.core.checkpoint import BackendCheckpointStore

if TYPE_CHECKING:
    from settings import Settings


def build_checkpoint_store(settings: Settings) -> BackendCheckpointStore:
    return BackendCheckpointStore(
        backend=RedisBackend(
            url=settings.redis_url,
            prefix=settings.redis_checkpoint_prefix,
        ),
        namespace="checkpoint",
    )


def build_dlq_sink(settings: Settings) -> RedisDLQSink:
    return RedisDLQSink(
        url=settings.redis_url,
        key_prefix=settings.redis_dlq_prefix,
    )
