# Agora ETL — Benchmark Matrix

[Kafka Benchmark](kafka.md) | [Redis Benchmark](redis.md)

## Environment

| | |
| --- | --- |
| **Date** | 2026-05-27 |
| **OS** | Darwin 24.6.0 |
| **CPU** | Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz (x86_64) |
| **RAM** | 32 GB |
| **Python** | 3.11.9 |
| **Repeat** | median of 3 isolated runs per scenario |

## Source Summary

This section isolates source read cost using `Direct + Null`.

| Source | Median Time | Median Rows/s | Median MB/s | Median Peak Py Heap |
| --- | --- | --- | --- | --- |
| CSV | 5.24s | 19,092.7 r/s | 1.9 MB/s | 6.6 MB |
| JSONL | 4.50s | 22,213.9 r/s | 4.8 MB/s | 6.9 MB |
| Parquet | 6.80s | 14,713.1 r/s | 0.7 MB/s | 6.3 MB |

## Sink Summary

This section isolates sink cost using `Direct` scenarios. `Median vs Null` shows how much throughput each sink retains compared with the same-source `Null` baseline.

| Sink | Median Direct Rows/s | Median Direct MB/s | Median vs Null | Median Peak Py Heap |
| --- | --- | --- | --- | --- |
| Null | 19,092.7 r/s | 1.9 MB/s | 100.0% | 6.6 MB |
| JSONL | 16,919.0 r/s | 1.7 MB/s | 88.6% | 13.1 MB |
| CSV | 16,961.9 r/s | 1.7 MB/s | 88.8% | 6.8 MB |
| Parquet | 17,493.6 r/s | 1.8 MB/s | 91.6% | 9.1 MB |
| Stdout | 16,369.3 r/s | 1.6 MB/s | 85.7% | 7.4 MB |

## Buffered Overhead

This section isolates buffered runtime overhead using the `Null` sink.

| Source | Direct Null Rows/s | Buffered Null Rows/s | Buffered Retention | Buffered In-Flight |
| --- | --- | --- | --- | --- |
| CSV | 19,092.7 r/s | 6,954.6 r/s | 36.4% | 4/4 |
| JSONL | 22,213.9 r/s | 7,326.4 r/s | 33.0% | 4/4 |
| Parquet | 14,713.1 r/s | 6,528.6 r/s | 44.4% | 4/4 |

## Full Matrix

Rows per scenario: `100,000`

| Source | Middleware | Sink | Repeat | Median Time | Median Rows/s | Median MB/s | Median Peak Py Heap | Buffered |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| CSV | Direct | Null | 3 | 5.24s | 19,093 r/s | 1.9 MB/s | 6.6 MB | — |
| CSV | Direct | JSONL | 3 | 5.91s | 16,919 r/s | 1.7 MB/s | 13.1 MB | — |
| CSV | Direct | CSV | 3 | 5.90s | 16,962 r/s | 1.7 MB/s | 6.8 MB | — |
| CSV | Direct | Parquet | 3 | 5.72s | 17,494 r/s | 1.8 MB/s | 9.1 MB | — |
| CSV | Direct | Stdout | 3 | 6.11s | 16,369 r/s | 1.6 MB/s | 7.4 MB | — |
| CSV | Map | Null | 3 | 7.22s | 13,859 r/s | 1.4 MB/s | 6.6 MB | — |
| CSV | Map | JSONL | 3 | 7.80s | 12,816 r/s | 1.3 MB/s | 13.1 MB | — |
| CSV | Map | CSV | 3 | 7.91s | 12,642 r/s | 1.3 MB/s | 6.8 MB | — |
| CSV | Map | Parquet | 3 | 8.44s | 11,853 r/s | 1.2 MB/s | 9.1 MB | — |
| CSV | Map | Stdout | 3 | 8.20s | 12,202 r/s | 1.2 MB/s | 7.4 MB | — |
| CSV | Buffered | Null | 3 | 14.38s | 6,955 r/s | 0.7 MB/s | 6.6 MB | 4/4 |
| CSV | Buffered | JSONL | 3 | 15.03s | 6,652 r/s | 0.7 MB/s | 13.1 MB | 4/4 |
| CSV | Buffered | CSV | 3 | 14.44s | 6,926 r/s | 0.7 MB/s | 6.8 MB | 4/4 |
| CSV | Buffered | Parquet | 3 | 14.82s | 6,749 r/s | 0.7 MB/s | 9.1 MB | 4/4 |
| CSV | Buffered | Stdout | 3 | 15.06s | 6,640 r/s | 0.7 MB/s | 7.4 MB | 4/4 |
| JSONL | Direct | Null | 3 | 4.50s | 22,214 r/s | 4.8 MB/s | 6.9 MB | — |
| JSONL | Direct | JSONL | 3 | 5.15s | 19,415 r/s | 4.2 MB/s | 13.1 MB | — |
| JSONL | Direct | CSV | 3 | 5.06s | 19,774 r/s | 4.3 MB/s | 7.1 MB | — |
| JSONL | Direct | Parquet | 3 | 4.47s | 22,355 r/s | 4.9 MB/s | 9.5 MB | — |
| JSONL | Direct | Stdout | 3 | 5.26s | 19,005 r/s | 4.1 MB/s | 7.7 MB | — |
| JSONL | Map | Null | 3 | 6.42s | 15,579 r/s | 3.4 MB/s | 6.9 MB | — |
| JSONL | Map | JSONL | 3 | 7.38s | 13,546 r/s | 3.0 MB/s | 13.1 MB | — |
| JSONL | Map | CSV | 3 | 8.24s | 12,133 r/s | 2.6 MB/s | 7.1 MB | — |
| JSONL | Map | Parquet | 3 | 7.34s | 13,620 r/s | 3.0 MB/s | 9.5 MB | — |
| JSONL | Map | Stdout | 3 | 7.73s | 12,939 r/s | 2.8 MB/s | 7.7 MB | — |
| JSONL | Buffered | Null | 3 | 13.65s | 7,326 r/s | 1.6 MB/s | 6.8 MB | 4/4 |
| JSONL | Buffered | JSONL | 3 | 14.05s | 7,117 r/s | 1.6 MB/s | 13.1 MB | 4/4 |
| JSONL | Buffered | CSV | 3 | 14.37s | 6,957 r/s | 1.5 MB/s | 6.9 MB | 4/4 |
| JSONL | Buffered | Parquet | 3 | 13.50s | 7,409 r/s | 1.6 MB/s | 9.2 MB | 4/4 |
| JSONL | Buffered | Stdout | 3 | 14.02s | 7,132 r/s | 1.6 MB/s | 7.4 MB | 4/4 |
| Parquet | Direct | Null | 3 | 6.80s | 14,713 r/s | 0.7 MB/s | 6.3 MB | — |
| Parquet | Direct | JSONL | 3 | 7.58s | 13,199 r/s | 0.6 MB/s | 12.7 MB | — |
| Parquet | Direct | CSV | 3 | 8.52s | 11,740 r/s | 0.6 MB/s | 6.5 MB | — |
| Parquet | Direct | Parquet | 3 | 7.55s | 13,253 r/s | 0.6 MB/s | 6.9 MB | — |
| Parquet | Direct | Stdout | 3 | 7.23s | 13,828 r/s | 0.7 MB/s | 7.0 MB | — |
| Parquet | Map | Null | 3 | 8.46s | 11,820 r/s | 0.6 MB/s | 6.3 MB | — |
| Parquet | Map | JSONL | 3 | 9.52s | 10,499 r/s | 0.5 MB/s | 12.7 MB | — |
| Parquet | Map | CSV | 3 | 10.18s | 9,822 r/s | 0.5 MB/s | 6.5 MB | — |
| Parquet | Map | Parquet | 3 | 9.26s | 10,801 r/s | 0.5 MB/s | 6.9 MB | — |
| Parquet | Map | Stdout | 3 | 10.03s | 9,973 r/s | 0.5 MB/s | 7.0 MB | — |
| Parquet | Buffered | Null | 3 | 15.32s | 6,529 r/s | 0.3 MB/s | 6.2 MB | 4/4 |
| Parquet | Buffered | JSONL | 3 | 16.28s | 6,141 r/s | 0.3 MB/s | 12.7 MB | 4/4 |
| Parquet | Buffered | CSV | 3 | 16.95s | 5,900 r/s | 0.3 MB/s | 6.4 MB | 4/4 |
| Parquet | Buffered | Parquet | 3 | 16.02s | 6,241 r/s | 0.3 MB/s | 6.9 MB | 4/4 |
| Parquet | Buffered | Stdout | 3 | 15.45s | 6,471 r/s | 0.3 MB/s | 7.0 MB | 4/4 |

`Peak Py Heap` reflects Python heap only. It does not include native memory from components such as `pyarrow` or `uvloop`.
