# Benchmark

Agora ships a standalone benchmark matrix CLI under `benchmarks/run.py` for
comparing source, middleware, and sink combinations with one consistent output
format.

The goal is not to replace a full perf lab. The goal is to make repeatable
core-runtime checks easy to run, easy to extend, and easy to publish into docs.

## Install benchmark dependencies

Benchmark runs require the `benchmark` extra:

```bash
pip install 'agora-etl[benchmark]'
```

If you are working from this repository directly:

```bash
cd packages/agora
./.venv/bin/pip install -e '.[benchmark]'
```

The script validates these runtime dependencies before it starts:

- `agora-etl[file]`
- `uvloop>=0.21,<1`
- `pyinstrument>=5.0,<6`

## Run the matrix

The CLI always runs the benchmark matrix. Use `--generate` when you want to
refresh input data first:

```bash
cd packages/agora
./.venv/bin/python benchmarks/run.py --generate --rows 1000000
```

Run without regenerating input data:

```bash
cd packages/agora
./.venv/bin/python benchmarks/run.py
```

By default, each scenario runs `3` times and the report shows the median. For a
quick smoke check, use:

```bash
./.venv/bin/python benchmarks/run.py --repeat 1
```

The default matrix runs all registered:

- sources
- middlewares
- sinks

These benchmark profiles are defined directly in `benchmarks/run.py`. If you
want to add a plugin-backed source, sink, or middleware later, extend the code
registry there rather than passing scenario selection through the CLI.

The built-in sink matrix is explicit:

- `Null`
- `JSONL`
- `CSV`
- `Parquet`
- `Stdout`

and prints a Rich table with:

- median elapsed time
- median rows/sec
- median MB/sec
- median peak Python heap
- buffered in-flight summary

## Export for docs

Write the benchmark report table into `docs/benchmark/matrix.md`:

```bash
./.venv/bin/python benchmarks/run.py \
  --rows 100000 \
  --generate \
  --markdown
```

That file is meant to be published directly by the docs site as a readable
benchmark matrix page. The generated page includes:

- environment snapshot
- source summary
- sink summary
- buffered overhead summary
- full scenario matrix

## Extend the script

The benchmark matrix is intentionally kept as one standalone CLI script.

To add a new scenario, update the profile sections inside `benchmarks/run.py`:

- source profiles
- sink profiles
- middleware profiles

That keeps benchmark experiments out of Agora's built-in runtime surface and
avoids coupling benchmark helpers to the public package API.

## Notes

- `Peak Py Heap` comes from `tracemalloc`, so it reflects Python heap only.
- `MB/s` is derived from generated input file size, scaled by consumed rows.
- CPU and RAM labels are collected best-effort from the local host. In
  restricted environments, the report may fall back to architecture-level CPU
  labels instead of a full brand string.
- Native memory from `pyarrow`, `uvloop`, or other C extensions is not included.
- The matrix is meant for comparable local baselines, not absolute cross-machine claims.
