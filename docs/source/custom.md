# Custom Source

_Use this when: no built-in or plugin source matches the upstream system._

Custom sources subclass `BaseSource[T]` and implement `stream()`.

## Minimum shape

```python
from collections.abc import AsyncGenerator

from agora import BaseSource, SourceRecordError


class MySource(BaseSource[dict]):
    source_name = "my_source"

    async def open(self) -> None:
        self._client = await create_client()

    async def close(self) -> None:
        await self._client.close()

    async def stream(self) -> AsyncGenerator[dict, None]:
        async for raw in self._client.fetch():
            try:
                yield transform(raw)
            except Exception as exc:
                raise SourceRecordError(exc, record=raw) from exc
```

## Add checkpointing

If the source must resume after restart:

```python
class MyCheckpointableSource(BaseSource[dict]):
    source_name = "my_source"
    supports_checkpoint = True

    def current_checkpoint(self) -> dict | None:
        return {"cursor": self._cursor}

    async def prepare_resume(self, checkpoint) -> None:
        if checkpoint:
            self._cursor = checkpoint.value["cursor"]
```

## Source limits and bounded runs

`pipeline.run(max_records=N)` is now implemented by wrapping the source in
`source.limit(N)` before execution starts.

That means custom sources automatically participate in bounded runs through the
base wrapper:

- `stream()` sources stop after `N` emitted records
- `stream_batches()` sources have their final emitted batch trimmed at the
  source boundary
- the same behavior applies under `.build()`, `.fan_out()`, `.route()`,
  `.run()`, and `.run_sync()`

Most custom sources do not need to override anything here. The default
`BaseSource.limit()` implementation returns a `LimitedSource` wrapper that
delegates lifecycle, checkpoint, and runtime metrics back to the original
source.

If a custom source can enforce a limit more efficiently at the upstream system
boundary, it may override `limit()` and return a source-specific wrapper.

## Add batch or Arrow emission

If the upstream system is batch-native, the source can advertise batch
capabilities explicitly by overriding `data_plane_spec()`:

```python
from agora import BaseSource, DataPlane, SourceDataPlaneSpec


class MyBatchSource(BaseSource[dict]):
    source_name = "my_batch_source"

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.PYTHON_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=False,
        )

    async def stream_batches(self):
        ...
```

For Arrow-native sources:

- return `DataPlane.ARROW_BATCHES` from `data_plane_spec()`
- yield `pyarrow.RecordBatch` from `stream_batches()`

Use the runtime-facing helper in tests when the exact same validation path as
the planner matters:

```python
from agora import source_data_plane_spec

spec = source_data_plane_spec(MyBatchSource())
assert spec.emitted_plane is DataPlane.PYTHON_BATCHES
```

You can also introspect the advertised contract directly with:

- `source.data_plane_spec().emitted_plane`
- `source.emitted_data_plane`

The public data-plane vocabulary is importable directly from `agora`:

```python
from agora import DataPlane, SourceDataPlaneSpec, source_data_plane_spec
```

Legacy note:

- `supports_batch_emit`
- `emits_arrow_batches`

still work in `0.3.x`, but they now emit `DeprecationWarning` when Agora uses
them to infer the source plane. Prefer `data_plane_spec()` for new code, and
prefer `source_data_plane_spec(source)` in tests when runtime-equivalent
validation matters. Legacy bool flags are planned for removal in `0.4.0`.

Once a source emits Arrow batches, the middleware chain must stay internally
consistent:

- all Arrow middleware is valid
- all Python-row middleware is valid, with one materialization step before the chain
- mixing Arrow middleware with Python-row/list-batch middleware in one chain is invalid

## Practical advice

- Raise `SourceRecordError` for per-record parse failures.
- Let true infrastructure failures propagate normally.
- Keep checkpoint values small and serializable.
- If the upstream system is batch-native, consider designing the source to emit
  batches instead of only individual records.
