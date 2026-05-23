from __future__ import annotations

from typing import TYPE_CHECKING

from models import Post
from normalizers import json_to_post

from agora.core.pipeline import BoundPipeline, Pipeline
from agora.sinks.file.jsonlines import JsonLinesSink
from agora.sources.http.http import HTTPSource, StopFetching

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

OUTPUT_PATH = "output/posts.jsonl"
BASE_URL = "https://jsonplaceholder.typicode.com"


class PostsSource(HTTPSource[Post]):
    """Fetch posts from JSONPlaceholder API (paginated)."""

    source_name = "jsonplaceholder_posts"

    def __init__(self) -> None:
        super().__init__(
            base_url=BASE_URL,
            requests_per_second=2.0,
        )
        self._page = 1
        self._per_page = 10

    async def fetch_batch(self) -> AsyncIterator[Post]:
        resp = await self.get(
            "/posts",
            params={"_page": self._page, "_limit": self._per_page},
        )
        items = resp.json()
        if not items:
            raise StopFetching
        for item in items:
            post = json_to_post(item)
            if post is not None:
                yield post
        self._page += 1


async def build_pipeline() -> BoundPipeline:
    source = PostsSource()
    sink = JsonLinesSink(path=OUTPUT_PATH)
    return (
        Pipeline(source, id="etl-http")
        .filter(lambda p: len(p.title) > 10, name="long_title")
        .build(sink, batch_size=20)
    )
