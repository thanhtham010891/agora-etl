# Performance

Agora 0.4.0 treats acceleration as an explicit runtime choice and makes
`agora-etl-rs` the optional acceleration substrate for core hot paths.

Install the optional Rust layer with:

```bash
pip install "agora-etl[rs]"
```

The Python runtime still owns pipeline semantics. `agora-etl-rs` provides
hot-path primitives for buffering, metrics accumulation, linear batching,
prefetch, direct-flush support, and checkpoint bookkeeping.

## Acceleration Modes

Declarative configs can set acceleration per document or per pipeline:

```toml
[performance]
acceleration = "auto"
profile = "balanced"
```

Modes:

| Mode | Behavior |
|---|---|
| `auto` | Use compatible Rust paths when `agora-etl-rs` is installed. |
| `off` | Force pure Python paths for debugging and benchmark comparison. |
| `required` | Fail before source/sink open if acceleration is unavailable. |

Profiles:

| Profile | Intent |
|---|---|
| `balanced` | Default behavior with no hidden semantic changes. |
| `throughput` | Larger default writer batches, flush cadence, buffer limits, adaptive backpressure, and opt-in Rust prefetch outside buffered lanes. |
| `low_latency` | Smaller buffer limits and faster flush cadence for tighter feedback loops. |

Profiles resolve to explicit settings surfaced in explain output and run
summaries. Manual code-level settings such as `DeliveryConfig(batch_size=...)`,
prefetch limits, and backpressure settings remain explicit and win over profile
defaults.

## Visible Fast Paths

Check selection before a run:

```python
explain = pipeline.explain()
print(explain)
print(explain.to_dict()["acceleration"])
```

Check the final run:

```python
summary = await pipeline.run()
print(summary.runtime.to_dict())
```

Relevant fields include:

- `acceleration_mode`
- `acceleration_profile_settings`
- `acceleration_available`
- `acceleration_package_version`
- `acceleration_compatible`
- `rust_checkpoint_state_active`
- `rust_metrics_accumulator_active`
- `rust_linear_batch_buffer_active`
- `rust_record_buffer_active`
- `direct_flush_active`
- `direct_flush_inactive_reason`
- `rust_prefetch_inactive_reason`

`agora doctor --config agora.toml` also reports the configured mode, profile,
installed `agora-etl-rs` version, compatibility, capabilities, and
required-mode failures. Use `agora doctor --json --config agora.toml` for a
machine-readable report.

## Repeatable core benchmark

Use the checked-in harness to compare core runtime profiles or Python and Rust
paths on the same host:

```bash
cd packages/agora
PYTHONPATH=src .venv/bin/python scripts/benchmark_agora.py \
  --records 20000 --payload-bytes 128 --batch-size 500 \
  --acceleration-mode auto --performance-profile throughput --pretty
```

It emits stable JSON with workload configuration, p50/p95 latency, throughput,
logical payload rate, peak traced memory, and a per-run checksum. Repeat a
comparison with `--acceleration-mode off`; preserve the JSON artifacts with the
machine and Python/Rust versions used for the comparison.

This is deliberately a synthetic Python-row dispatch workload with a
batch-capable in-memory sink. It does not benchmark a connector, claim a
delivery guarantee, or replace crash-window and backend integration gates.

## Fast-Path Checklist

For high throughput runs:

- install `agora-etl-rs`
- set `performance.acceleration = "auto"` or `"required"`
- use `writer_batch_size` / `DeliveryConfig(batch_size=...)` greater than `1`
- prefer sinks with native `write_batch(...)` or Arrow batch paths
- use file sources that expose synchronous batch reads when measuring prefetch
- inspect `Pipeline.explain()` for direct flush and row materialization reasons
