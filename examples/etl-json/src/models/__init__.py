from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Event:
    id: str
    type: str
    user_id: str
    timestamp: str
    payload: dict
