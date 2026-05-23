"""
Project settings — extend AgoraSettings with your own config.
"""

from __future__ import annotations

from functools import lru_cache

from agora.config import AgoraSettings


class Settings(AgoraSettings):
    pass


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
