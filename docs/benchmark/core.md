# Agora ETL — Benchmark Matrix

[Kafka Benchmark](kafka.md) | [Redis Benchmark](redis.md)

## Environment

| | |
| --- | --- |
| **Date** | 2026-05-27 |
| **OS** | Linux 6.17.0-1012-aws |
| **CPU** | Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz (x86_64) |
| **RAM** | 31 GB |
| **Python** | 3.12.3 |
| **Repeat** | median of 3 isolated runs per scenario |

## Source Summary

This section isolates source read cost using `Direct + Null`.

| Source | Median Time | Median Rows/s | Median MB/s |
| --- | --- | --- | --- |
| CSV | 1.11s | 89,795.2 r/s | 9.0 MB/s |
| JSONL | 1.03s | 97,328.2 r/s | 21.2 MB/s |
| Parquet | 1.67s | 59,839.6 r/s | 2.9 MB/s |

## Sink Summary

This section isolates sink cost using `Direct` scenarios. `Median vs Null` shows how much throughput each sink retains compared with the same-source `Null` baseline.

| Sink | Median Direct Rows/s | Median Direct MB/s | Median vs Null |
| --- | --- | --- | --- |
| Null | 89,795.2 r/s | 9.0 MB/s | 100.0% |
| JSONL | 73,876.7 r/s | 7.4 MB/s | 83.2% |
| CSV | 68,614.8 r/s | 6.9 MB/s | 76.4% |
| Parquet | 68,352.3 r/s | 6.9 MB/s | 77.7% |
| Stdout | 77,701.1 r/s | 7.8 MB/s | 88.5% |

## Buffered Overhead

This section isolates buffered runtime overhead using the `Null` sink.

| Source | Direct Null Rows/s | Buffered Null Rows/s | Buffered Retention | Buffered In-Flight |
| --- | --- | --- | --- | --- |
| CSV | 89,795.2 r/s | 39,249.6 r/s | 43.7% | 4/4 |
| JSONL | 97,328.2 r/s | 39,219.9 r/s | 40.3% | 4/4 |
| Parquet | 59,839.6 r/s | 31,184.2 r/s | 52.1% | 4/4 |

## Full Matrix

Rows per scenario: `100,000`

| Source | Middleware | Sink | Repeat | Median Time | Median Rows/s | Median MB/s | Buffered |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| CSV | Direct | Null | 3 | 1.11s | 89,795 r/s | 9.0 MB/s | — |
| CSV | Direct | JSONL | 3 | 1.35s | 73,877 r/s | 7.4 MB/s | — |
| CSV | Direct | CSV | 3 | 1.46s | 68,615 r/s | 6.9 MB/s | — |
| CSV | Direct | Parquet | 3 | 1.46s | 68,352 r/s | 6.9 MB/s | — |
| CSV | Direct | Stdout | 3 | 1.29s | 77,701 r/s | 7.8 MB/s | — |
| CSV | Map | Null | 3 | 1.40s | 71,347 r/s | 7.2 MB/s | — |
| CSV | Map | JSONL | 3 | 1.70s | 58,928 r/s | 5.9 MB/s | — |
| CSV | Map | CSV | 3 | 1.78s | 56,212 r/s | 5.7 MB/s | — |
| CSV | Map | Parquet | 3 | 1.83s | 54,764 r/s | 5.5 MB/s | — |
| CSV | Map | Stdout | 3 | 1.60s | 62,630 r/s | 6.3 MB/s | — |
| CSV | Buffered | Null | 3 | 2.55s | 39,250 r/s | 4.0 MB/s | 4/4 |
| CSV | Buffered | JSONL | 3 | 2.81s | 35,584 r/s | 3.6 MB/s | 4/4 |
| CSV | Buffered | CSV | 3 | 2.87s | 34,837 r/s | 3.5 MB/s | 4/4 |
| CSV | Buffered | Parquet | 3 | 2.90s | 34,472 r/s | 3.5 MB/s | 4/4 |
| CSV | Buffered | Stdout | 3 | 2.74s | 36,502 r/s | 3.7 MB/s | 4/4 |
| JSONL | Direct | Null | 3 | 1.03s | 97,328 r/s | 21.2 MB/s | — |
| JSONL | Direct | JSONL | 3 | 1.23s | 80,983 r/s | 17.7 MB/s | — |
| JSONL | Direct | CSV | 3 | 1.42s | 70,214 r/s | 15.3 MB/s | — |
| JSONL | Direct | Parquet | 3 | 1.32s | 75,600 r/s | 16.5 MB/s | — |
| JSONL | Direct | Stdout | 3 | 1.16s | 86,143 r/s | 18.8 MB/s | — |
| JSONL | Map | Null | 3 | 1.28s | 78,407 r/s | 17.1 MB/s | — |
| JSONL | Map | JSONL | 3 | 1.58s | 63,395 r/s | 13.8 MB/s | — |
| JSONL | Map | CSV | 3 | 1.82s | 55,032 r/s | 12.0 MB/s | — |
| JSONL | Map | Parquet | 3 | 1.65s | 60,468 r/s | 13.2 MB/s | — |
| JSONL | Map | Stdout | 3 | 1.49s | 67,036 r/s | 14.6 MB/s | — |
| JSONL | Buffered | Null | 3 | 2.55s | 39,220 r/s | 8.6 MB/s | 4/4 |
| JSONL | Buffered | JSONL | 3 | 2.64s | 37,807 r/s | 8.2 MB/s | 4/4 |
| JSONL | Buffered | CSV | 3 | 2.84s | 35,271 r/s | 7.7 MB/s | 4/4 |
| JSONL | Buffered | Parquet | 3 | 2.69s | 37,151 r/s | 8.1 MB/s | 4/4 |
| JSONL | Buffered | Stdout | 3 | 2.68s | 37,353 r/s | 8.1 MB/s | 4/4 |
| Parquet | Direct | Null | 3 | 1.67s | 59,840 r/s | 2.9 MB/s | — |
| Parquet | Direct | JSONL | 3 | 1.82s | 54,966 r/s | 2.6 MB/s | — |
| Parquet | Direct | CSV | 3 | 2.06s | 48,578 r/s | 2.3 MB/s | — |
| Parquet | Direct | Parquet | 3 | 1.83s | 54,567 r/s | 2.6 MB/s | — |
| Parquet | Direct | Stdout | 3 | 1.78s | 56,056 r/s | 2.7 MB/s | — |
| Parquet | Map | Null | 3 | 2.07s | 48,219 r/s | 2.3 MB/s | — |
| Parquet | Map | JSONL | 3 | 2.16s | 46,310 r/s | 2.2 MB/s | — |
| Parquet | Map | CSV | 3 | 2.37s | 42,237 r/s | 2.0 MB/s | — |
| Parquet | Map | Parquet | 3 | 2.20s | 45,523 r/s | 2.2 MB/s | — |
| Parquet | Map | Stdout | 3 | 2.10s | 47,657 r/s | 2.3 MB/s | — |
| Parquet | Buffered | Null | 3 | 3.21s | 31,184 r/s | 1.5 MB/s | 4/4 |
| Parquet | Buffered | JSONL | 3 | 3.41s | 29,293 r/s | 1.4 MB/s | 4/4 |
| Parquet | Buffered | CSV | 3 | 3.62s | 27,645 r/s | 1.3 MB/s | 4/4 |
| Parquet | Buffered | Parquet | 3 | 3.42s | 29,264 r/s | 1.4 MB/s | 4/4 |
| Parquet | Buffered | Stdout | 3 | 3.29s | 30,359 r/s | 1.5 MB/s | 4/4 |

Core throughput is measured without `tracemalloc` so the reported rows/s and MB/s are not distorted by heap sampling overhead.