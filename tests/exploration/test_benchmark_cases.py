from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from agora.core.data_plane import DataPlane
from agora.core.source import source_data_plane_spec
from benchmarks._cases import _build_case, _run
from benchmarks._generate import generate

if TYPE_CHECKING:
    from pathlib import Path

_ARROW_CASES = (
    ("csv", "arrow"),
    ("csv", "arrow_map"),
    ("csv", "arrow_filter"),
    ("csv", "arrow_to_csv"),
    ("jsonl", "arrow"),
    ("jsonl", "arrow_map"),
    ("jsonl", "arrow_filter"),
    ("jsonl", "arrow_to_jsonl"),
    ("parquet", "arrow"),
    ("parquet", "arrow_map"),
    ("parquet", "arrow_filter"),
)


@pytest.fixture(scope="module")
def benchmark_data_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    pytest.importorskip("pyarrow")
    data_dir = tmp_path_factory.mktemp("benchmark-cases")
    generate(data_dir, rows=32)
    return data_dir


@pytest.mark.parametrize(("lane", "case"), _ARROW_CASES)
def test_arrow_benchmark_cases_use_arrow_emitting_sources(
    benchmark_data_dir: Path,
    lane: str,
    case: str,
) -> None:
    pipeline, _ = _build_case(benchmark_data_dir, lane, case)

    spec = source_data_plane_spec(pipeline._source)

    assert spec.emitted_plane is DataPlane.ARROW_BATCHES
    assert spec.supports_batch_emit is True
    assert spec.emits_arrow_batches is True


@pytest.mark.parametrize(
    ("lane", "case"),
    (
        ("csv", "arrow_map"),
        ("csv", "arrow_filter"),
        ("jsonl", "arrow_map"),
        ("jsonl", "arrow_filter"),
        ("parquet", "arrow_map"),
        ("parquet", "arrow_filter"),
        ("csv", "arrow_to_csv"),
        ("jsonl", "arrow_to_jsonl"),
    ),
)
def test_arrow_benchmark_cases_run_successfully(
    benchmark_data_dir: Path,
    lane: str,
    case: str,
) -> None:
    result = asyncio.run(_run(benchmark_data_dir, lane, case))

    assert result["lane"] == lane
    assert result["case"] == case
    assert result["rows"] > 0
    assert result["throughput_rps"] > 0
