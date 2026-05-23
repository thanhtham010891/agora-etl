from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.mark.asyncio
async def test_http_pipeline_filters_short_titles(tmp_path: Path) -> None:
    from pipelines.example import PostsSource

    from agora.core.pipeline import Pipeline
    from agora.sinks.file.jsonlines import JsonLinesSink

    posts_data = [
        {"id": 1, "userId": 1, "title": "Hi", "body": "short title — should be dropped"},
        {"id": 2, "userId": 1, "title": "A longer post title here", "body": "body text"},
        {"id": 3, "userId": 2, "title": "Another good title", "body": "more body"},
        {"id": 4, "userId": 2, "title": "Ok", "body": "also short — dropped"},
        {"id": 5, "userId": 3, "title": "Yet another valid title", "body": "body"},
    ]

    mock_response = MagicMock()
    mock_response.json = MagicMock(side_effect=[posts_data, []])

    source = PostsSource()
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=mock_response)
    source._client = mock_client

    output = tmp_path / "posts.jsonl"
    sink = JsonLinesSink(path=output)
    pipeline = (
        Pipeline(source, id="test-http")
        .filter(lambda p: len(p.title) > 10, name="long_title")
        .build(sink)
    )
    summary = await pipeline.run()

    assert summary.records_consumed == 5
    assert summary.records_written == 3  # titles > 10 chars
    assert summary.records_dropped == 2  # "Hi" and "Ok"
