from __future__ import annotations

from models import Post


def json_to_post(row: dict) -> Post | None:
    try:
        return Post(
            id=int(row["id"]),
            user_id=int(row["userId"]),
            title=str(row["title"]),
            body=str(row["body"]),
        )
    except (KeyError, ValueError, TypeError):
        return None
