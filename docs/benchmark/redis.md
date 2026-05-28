# Agora ETL — Redis Benchmark

[Core Benchmark](core.md) | [Kafka Benchmark](kafka.md)

This page records the current redis benchmark snapshot for Agora ETL.

## Environment

## Scenarios

## Results

## Reading the results

- `SET` and `XADD` isolate write-side cost.
- `XREADGROUP` isolates consumer-group read cost.
- `Stream RT` captures the combined cost of stream writes and consumer-group reads.

Plugin throughput is measured without `tracemalloc` so the reported rows/s and MB/s are not distorted by heap sampling overhead.