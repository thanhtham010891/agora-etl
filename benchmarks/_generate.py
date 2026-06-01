"""Data generation for benchmarks — writes CSV, JSONL, and Parquet fixtures."""

from __future__ import annotations

import csv
import json
import random
import string
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

_WORDS = [
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
    "india",
    "juliet",
    "kilo",
    "lima",
    "mike",
    "november",
    "oscar",
    "papa",
]


def _rand_str(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def _make_row(i: int) -> dict:
    return {
        "id": i,
        "name": random.choice(_WORDS) + "_" + _rand_str(4),
        "value": round(random.uniform(0.0, 1000.0), 4),
        "score": random.randint(0, 100),
        "active": random.choice([True, False]),
        "category": random.choice(["A", "B", "C", "D"]),
        "tag": _rand_str(6),
    }


def generate(data_dir: Path, rows: int) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)

    rows_data = [_make_row(i) for i in range(rows)]

    # CSV
    csv_path = data_dir / "input.csv"
    fieldnames = list(rows_data[0].keys())
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_data)
    print(f"  wrote {csv_path} ({rows} rows)")

    # JSONL
    jsonl_path = data_dir / "input.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for row in rows_data:
            f.write(json.dumps(row) + "\n")
    print(f"  wrote {jsonl_path} ({rows} rows)")

    # Parquet
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table(
            {
                "id": pa.array([r["id"] for r in rows_data], type=pa.int64()),
                "name": pa.array([r["name"] for r in rows_data], type=pa.string()),
                "value": pa.array([r["value"] for r in rows_data], type=pa.float64()),
                "score": pa.array([r["score"] for r in rows_data], type=pa.int32()),
                "active": pa.array([r["active"] for r in rows_data], type=pa.bool_()),
                "category": pa.array([r["category"] for r in rows_data], type=pa.string()),
                "tag": pa.array([r["tag"] for r in rows_data], type=pa.string()),
            }
        )
        parquet_path = data_dir / "input.parquet"
        pq.write_table(table, parquet_path, compression="snappy")
        print(f"  wrote {parquet_path} ({rows} rows)")
    except ImportError:
        print("  skipped parquet (pyarrow not installed)")
