# Agora ETL — Kafka Benchmark

[Core Benchmark](core.md) | [Redis Benchmark](redis.md)

This page records the current kafka benchmark snapshot for Agora ETL.

## Environment

| | |
| --- | --- |
| **Date** | 2026-05-28 |
| **OS** | Linux 6.17.0-1012-aws |
| **CPU** | Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz (x86_64) |
| **RAM** | 31 GB |
| **Python** | 3.12.3 |
| **Kafka** | 127.0.0.1:19092 |
| **Repeat** | median of 3 isolated runs per scenario |
| **Pipeline batch size** | 500 |
| **Kafka max pending acks** | 500 |
| **Kafka commit every** | 500 |

## Scenarios

| Scenario | Purpose |
| --- | --- |
| `Kafka / Produce` | Measures producer throughput only. |
| `Kafka / Roundtrip` | Measures produce and consume together on one topic. |

## Results

Rows per scenario: `1,000,000`

| Scenario | Repeat | Median Time | Median Rows/s | Median MB/s |
| --- | ---: | ---: | ---: | ---: |
| Kafka / Produce | 3 | 15.40s | 64,927 r/s | 8.4 MB/s |
| Kafka / Roundtrip | 3 | 40.10s | 24,939 r/s | 3.0 MB/s |

## Reading the results

- `Produce` isolates write-side throughput.
- `Roundtrip` includes producer, consumer, commit, and broker coordination cost.

Plugin throughput is measured without `tracemalloc` so the reported rows/s and MB/s are not distorted by heap sampling overhead.
