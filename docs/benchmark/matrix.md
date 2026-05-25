# Agora ETL — Benchmark Matrix

## Environment

| | |
| --- | --- |
| **Date** | 2026-05-25 |
| **OS** | Darwin 24.6.0 |
| **CPU** | x86_64 |
| **RAM** | 32 GB |
| **Python** | 3.11.9 |
| **Repeat** | median of 3 isolated runs per scenario |

## Source Summary

This section isolates source read cost using `Direct + Null`.

| Source | Median Time | Median Rows/s | Median MB/s | Median Peak Py Heap |
| --- | --- | --- | --- | --- |
| CSV | 0.65s | 15,466.6 r/s | 1.5 MB/s | 7.1 MB |
| JSONL | 0.59s | 17,090.4 r/s | 3.7 MB/s | 7.4 MB |
| Parquet | 0.87s | 11,493.5 r/s | 0.6 MB/s | 6.8 MB |

## Sink Summary

This section isolates sink cost using `Direct` scenarios. `Median vs Null` shows how much throughput each sink retains compared with the same-source `Null` baseline.

| Sink | Median Direct Rows/s | Median Direct MB/s | Median vs Null | Median Peak Py Heap |
| --- | --- | --- | --- | --- |
| Null | 15,466.6 r/s | 1.5 MB/s | 100.0% | 7.1 MB |
| JSONL | 13,482.2 r/s | 1.3 MB/s | 90.3% | 13.4 MB |
| CSV | 13,963.3 r/s | 1.4 MB/s | 90.0% | 7.2 MB |
| Parquet | 11,981.8 r/s | 1.2 MB/s | 77.5% | 10.4 MB |
| Stdout | 14,377.7 r/s | 1.4 MB/s | 93.0% | 7.3 MB |

## Buffered Overhead

This section isolates buffered runtime overhead using the `Null` sink.

| Source | Direct Null Rows/s | Buffered Null Rows/s | Buffered Retention | Buffered In-Flight |
| --- | --- | --- | --- | --- |
| CSV | 15,466.6 r/s | 5,776.3 r/s | 37.3% | 4/4 |
| JSONL | 17,090.4 r/s | 6,439.0 r/s | 37.7% | 4/4 |
| Parquet | 11,493.5 r/s | 5,467.4 r/s | 47.6% | 4/4 |

## Full Matrix

Rows per scenario: `10,000`

| Source | Middleware | Sink | Repeat | Median Time | Median Rows/s | Median MB/s | Median Peak Py Heap | Buffered |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| CSV | Direct | Null | 3 | 0.65s | 15,467 r/s | 1.5 MB/s | 7.1 MB | — |
| CSV | Direct | JSONL | 3 | 0.74s | 13,482 r/s | 1.3 MB/s | 13.4 MB | — |
| CSV | Direct | CSV | 3 | 0.72s | 13,963 r/s | 1.4 MB/s | 7.2 MB | — |
| CSV | Direct | Parquet | 3 | 0.83s | 11,982 r/s | 1.2 MB/s | 10.4 MB | — |
| CSV | Direct | Stdout | 3 | 0.70s | 14,378 r/s | 1.4 MB/s | 7.3 MB | — |
| CSV | Map | Null | 3 | 0.94s | 10,609 r/s | 1.0 MB/s | 7.1 MB | — |
| CSV | Map | JSONL | 3 | 0.94s | 10,605 r/s | 1.0 MB/s | 13.4 MB | — |
| CSV | Map | CSV | 3 | 1.08s | 9,265 r/s | 0.9 MB/s | 7.2 MB | — |
| CSV | Map | Parquet | 3 | 1.15s | 8,692 r/s | 0.9 MB/s | 10.4 MB | — |
| CSV | Map | Stdout | 3 | 0.99s | 10,140 r/s | 1.0 MB/s | 7.3 MB | — |
| CSV | Buffered | Null | 3 | 1.73s | 5,776 r/s | 0.6 MB/s | 7.0 MB | 4/4 |
| CSV | Buffered | JSONL | 3 | 1.78s | 5,634 r/s | 0.6 MB/s | 13.4 MB | 4/4 |
| CSV | Buffered | CSV | 3 | 1.69s | 5,929 r/s | 0.6 MB/s | 7.2 MB | 4/4 |
| CSV | Buffered | Parquet | 3 | 1.88s | 5,329 r/s | 0.5 MB/s | 10.4 MB | 4/4 |
| CSV | Buffered | Stdout | 3 | 1.78s | 5,627 r/s | 0.6 MB/s | 7.2 MB | 4/4 |
| JSONL | Direct | Null | 3 | 0.59s | 17,090 r/s | 3.7 MB/s | 7.4 MB | — |
| JSONL | Direct | JSONL | 3 | 0.62s | 16,128 r/s | 3.5 MB/s | 13.4 MB | — |
| JSONL | Direct | CSV | 3 | 0.67s | 14,937 r/s | 3.2 MB/s | 7.3 MB | — |
| JSONL | Direct | Parquet | 3 | 0.81s | 12,399 r/s | 2.7 MB/s | 10.4 MB | — |
| JSONL | Direct | Stdout | 3 | 0.67s | 14,875 r/s | 3.2 MB/s | 7.6 MB | — |
| JSONL | Map | Null | 3 | 0.86s | 11,602 r/s | 2.5 MB/s | 7.4 MB | — |
| JSONL | Map | JSONL | 3 | 0.88s | 11,308 r/s | 2.4 MB/s | 13.4 MB | — |
| JSONL | Map | CSV | 3 | 0.95s | 10,525 r/s | 2.3 MB/s | 7.3 MB | — |
| JSONL | Map | Parquet | 3 | 1.01s | 9,860 r/s | 2.1 MB/s | 10.4 MB | — |
| JSONL | Map | Stdout | 3 | 0.81s | 12,404 r/s | 2.7 MB/s | 7.6 MB | — |
| JSONL | Buffered | Null | 3 | 1.55s | 6,439 r/s | 1.4 MB/s | 7.1 MB | 4/4 |
| JSONL | Buffered | JSONL | 3 | 1.65s | 6,072 r/s | 1.3 MB/s | 13.4 MB | 4/4 |
| JSONL | Buffered | CSV | 3 | 1.70s | 5,889 r/s | 1.3 MB/s | 7.2 MB | 4/4 |
| JSONL | Buffered | Parquet | 3 | 1.85s | 5,413 r/s | 1.2 MB/s | 10.5 MB | 4/4 |
| JSONL | Buffered | Stdout | 3 | 1.65s | 6,077 r/s | 1.3 MB/s | 7.2 MB | 4/4 |
| Parquet | Direct | Null | 3 | 0.87s | 11,493 r/s | 0.6 MB/s | 6.8 MB | — |
| Parquet | Direct | JSONL | 3 | 0.96s | 10,381 r/s | 0.5 MB/s | 13.0 MB | — |
| Parquet | Direct | CSV | 3 | 0.97s | 10,345 r/s | 0.5 MB/s | 6.9 MB | — |
| Parquet | Direct | Parquet | 3 | 0.90s | 11,122 r/s | 0.5 MB/s | 7.2 MB | — |
| Parquet | Direct | Stdout | 3 | 0.87s | 11,432 r/s | 0.6 MB/s | 6.9 MB | — |
| Parquet | Map | Null | 3 | 1.16s | 8,651 r/s | 0.4 MB/s | 6.8 MB | — |
| Parquet | Map | JSONL | 3 | 1.21s | 8,262 r/s | 0.4 MB/s | 13.0 MB | — |
| Parquet | Map | CSV | 3 | 1.29s | 7,768 r/s | 0.4 MB/s | 6.9 MB | — |
| Parquet | Map | Parquet | 3 | 1.21s | 8,233 r/s | 0.4 MB/s | 7.2 MB | — |
| Parquet | Map | Stdout | 3 | 1.18s | 8,453 r/s | 0.4 MB/s | 6.9 MB | — |
| Parquet | Buffered | Null | 3 | 1.83s | 5,467 r/s | 0.3 MB/s | 6.8 MB | 4/4 |
| Parquet | Buffered | JSONL | 3 | 2.01s | 4,979 r/s | 0.2 MB/s | 13.0 MB | 4/4 |
| Parquet | Buffered | CSV | 3 | 2.00s | 4,988 r/s | 0.2 MB/s | 6.9 MB | 4/4 |
| Parquet | Buffered | Parquet | 3 | 1.87s | 5,348 r/s | 0.3 MB/s | 7.2 MB | 4/4 |
| Parquet | Buffered | Stdout | 3 | 1.86s | 5,383 r/s | 0.3 MB/s | 6.9 MB | 4/4 |

## Notes

- Each scenario reports the median of 3 isolated subprocess runs.
- `MB/s` uses the generated input file size for each source, scaled by consumed rows.
- Source Summary uses `Direct + Null` to isolate source read cost.
- Sink Summary uses `Direct` scenarios and compares each sink to the same-source `Null` baseline.
- Buffered Overhead uses the `Null` sink to isolate runtime coordination cost.
- Peak Py Heap uses tracemalloc (Python heap only — excludes native memory from pyarrow, uvloop).
- Use `--generate` to regenerate benchmark input data.
