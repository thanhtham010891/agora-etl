"""
agora/config/base.py
====================
``AgoraSettings`` — root Pydantic settings for any agora project.

User projects extend this::

    # src/settings.py
    from agora.config import AgoraSettings
    from agora_plugins.kafka import KafkaConfig
    from agora_plugins.postgres import PostgresConfig

    class Settings(AgoraSettings):
        kafka: KafkaConfig = KafkaConfig()
        postgres: PostgresConfig = PostgresConfig()

    def get_settings() -> Settings:
        return Settings()
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AgoraSettings(BaseSettings):
    """Root settings — extend this in your project's settings.py.

    Reads from:
    1. Environment variables
    2. ``agora.env`` file (if present)
    3. Python defaults

    Core fields
    -----------
    ``LOG_LEVEL``       — logging verbosity (DEBUG, INFO, WARNING, ERROR)
    ``AGORA_ENV``       — environment tag (dev, staging, prod)
    """

    model_config = SettingsConfigDict(
        env_file="agora.env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    env: str = Field(default="dev", alias="AGORA_ENV")
