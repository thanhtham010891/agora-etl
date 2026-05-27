# Agora ETL — Redis Benchmark

[Core Benchmark](core.md) | [Kafka Benchmark](kafka.md)

This page records the current redis benchmark snapshot for Agora ETL.

## Environment

| | |
| --- | --- |
| **Date** | 2026-05-27 |
| **OS** | Linux 6.17.0-1012-aws |
| **CPU** | Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz (x86_64) |
| **RAM** | 31 GB |
| **Python** | 3.12.3 |
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

| Scenario | Repeat | Median Time | Median Rows/s | Median MB/s |
| --- | ---: | ---: | ---: | ---: |
| Redis / SET | 3 | 0.74s | 135,603 r/s | 16.1 MB/s |
| Redis / XADD | 3 | 1.87s | 53,545 r/s | 6.4 MB/s |
| Redis / XREADGROUP | 3 | 3.19s | 31,321 r/s | 3.8 MB/s |
| Redis / Stream RT | 3 | 5.06s | 19,766 r/s | 2.5 MB/s |

## Reading the results

- `SET` and `XADD` isolate write-side cost.
- `XREADGROUP` isolates consumer-group read cost.
- `Stream RT` captures the combined cost of stream writes and consumer-group reads.

Plugin throughput is measured without `tracemalloc` so the reported rows/s and MB/s are not distorted by heap sampling overhead.