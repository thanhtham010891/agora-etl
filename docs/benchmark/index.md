# Benchmark

This section collects the current benchmark snapshots for Agora ETL.

Available reports:

- [Core Benchmark](core.md)
- [Kafka Benchmark](kafka.md)
- [Redis Benchmark](redis.md)

## Reading the benchmark pages

Start with the page that matches the subsystem you want to inspect.

### Core Benchmark

The [Core Benchmark](core.md) focuses on built-in file sources, sinks, and
runtime overhead. Read the summaries first, then use the full matrix when you
need detail.

### Kafka and Redis Benchmarks

The [Kafka Benchmark](kafka.md) and [Redis Benchmark](redis.md) focus on the
first-party plugin lanes. They break results down by scenario so producer,
consumer, and end-to-end costs can be read separately.

## What the numbers mean

- `Rows/s` is the primary throughput metric.
- `MB/s` helps when payload size differs across scenarios.
- `Peak Py Heap` reflects Python heap only, not total process memory.
- `Repeat` is the number of isolated runs used to produce the reported median.
