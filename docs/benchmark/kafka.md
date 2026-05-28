# Agora ETL — Kafka Benchmark

[Core Benchmark](core.md) | [Redis Benchmark](redis.md)

This page records the current kafka benchmark snapshot for Agora ETL.

## Environment

## Scenarios

## Results

## Reading the results

- `Produce` isolates write-side throughput.
- `Roundtrip` includes producer, consumer, commit, and broker coordination cost.
- Kafka scenarios use a throughput-biased producer profile (`acks=1`, idempotence disabled, `linger_ms=0`), so treat them as transport benchmarks rather than durability-maximized production settings.

Plugin throughput is measured without `tracemalloc` so the reported rows/s and MB/s are not distorted by heap sampling overhead.
