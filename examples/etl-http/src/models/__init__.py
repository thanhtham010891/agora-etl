from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Post:
    id: int
    user_id: int
    title: str
    body: str
