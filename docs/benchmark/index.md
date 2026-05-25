# Benchmark

This section exists to publish a readable performance snapshot for Agora ETL.

The main page to read is the benchmark matrix:

- [Benchmark Matrix](matrix.md)

## How to read the matrix

Start with the summaries, not the full table.

### Source Summary

This shows source read cost with the lightest path:

- `Direct`
- `Null`

Use it to compare how expensive each source is before sink or buffered-runtime
cost is added.

### Sink Summary

This shows sink cost on `Direct` scenarios.

The important column here is retention versus `Null`. It answers the simple
question:

How much throughput survives once the sink is real?

### Buffered Overhead

This isolates the runtime cost of buffered execution with the `Null` sink.

Use it when you want to understand coordination overhead separately from actual
I/O.

### Full Matrix

The full table is there for detail, but it should be read after the summaries.

It is most useful for:

- spotting regressions
- comparing one source or sink family across middlewares
- checking whether a slowdown belongs to reading, writing, or buffering

## What the numbers mean

- `Rows/s` is the easiest top-line throughput number
- `MB/s` is better when comparing formats with very different payload sizes
- `Peak Py Heap` is Python heap only, not total process memory
- `Repeat` is the number of isolated runs used to produce the median
