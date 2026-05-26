# Agora ETL — Redis Benchmark

[Core Benchmark](core.md) | [Kafka Benchmark](kafka.md)

This page records the current redis benchmark snapshot for Agora ETL.

## Environment

| | |
| --- | --- |
| **Date** | 2026-05-27 |
| **OS** | Darwin 24.6.0 |
| **CPU** | Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz (x86_64) |
| **RAM** | 32 GB |
| **Python** | 3.11.9 |
| **Redis** | redis://127.0.0.1:16379/0 |
| **Repeat** | median of 3 isolated runs per scenario |
| **Pipeline batch size** | 500 |
| **Redis sink batch size** | 5000 |
| **Redis stream batch size** | 2000 |
| **Redis stream ack batch size** | 2000 |
| **Redis stream block ms** | 10 |

## Scenarios

| Scenario | Purpose |
| --- | --- |
| `Redis / SET` | Measures key-value writes through `SET`/`MSET`. |
| `Redis / XADD` | Measures stream write throughput only. |
| `Redis / XREADGROUP` | Measures consumer-group read throughput on a pre-seeded stream. |
| `Redis / Stream RT` | Measures `XADD` and `XREADGROUP` together in one end-to-end pass. |

## Results

Rows per scenario: `100,000`

| Scenario | Repeat | Median Time | Median Rows/s | Median MB/s | Median Peak Py Heap |
| --- | ---: | ---: | ---: | ---: | ---: |
| Redis / SET | 3 | 3.99s | 25,040 r/s | 3.0 MB/s | 7.2 MB |
| Redis / XADD | 3 | 11.62s | 8,607 r/s | 1.0 MB/s | 7.8 MB |
| Redis / XREADGROUP | 3 | 17.63s | 5,672 r/s | 0.7 MB/s | 3.0 MB |
| Redis / Stream RT | 3 | 29.45s | 3,395 r/s | 0.4 MB/s | 7.8 MB |

## Reading the results

- `SET` and `XADD` isolate write-side cost.
- `XREADGROUP` isolates consumer-group read cost.
- `Stream RT` captures the combined cost of stream writes and consumer-group reads.

`Peak Py Heap` reflects Python heap only. It does not include broker/server memory or native allocations.
