# Agora ETL — Kafka Benchmark

[Core Benchmark](core.md) | [Redis Benchmark](redis.md)

This page records the current kafka benchmark snapshot for Agora ETL.

## Environment

| | |
| --- | --- |
| **Date** | 2026-05-27 |
| **OS** | Darwin 24.6.0 |
| **CPU** | Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz (x86_64) |
| **RAM** | 32 GB |
| **Python** | 3.11.9 |
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

Rows per scenario: `100,000`

| Scenario | Repeat | Median Time | Median Rows/s | Median MB/s | Median Peak Py Heap |
| --- | ---: | ---: | ---: | ---: | ---: |
| Kafka / Produce | 3 | 5.44s | 18,398 r/s | 2.3 MB/s | 1.1 MB |
| Kafka / Roundtrip | 3 | 16.02s | 6,242 r/s | 0.7 MB/s | 7.1 MB |

## Reading the results

- `Produce` isolates write-side throughput.
- `Roundtrip` includes producer, consumer, commit, and broker coordination cost.

`Peak Py Heap` reflects Python heap only. It does not include broker/server memory or native allocations.
