# Custom Sink

_Use this when: no built-in or plugin sink matches the destination system._

Custom sinks subclass `BaseSink[T]`.

## Minimum shape

```python
from agora import BaseSink


class MySink(BaseSink[dict]):
    sink_name = "my_sink"

    async def open(self) -> None:
        self._client = await connect()

    async def write(self, record: dict) -> None:
        await self._client.send(record)

    async def close(self) -> None:
        await self._client.close()
```

Every sink always accepts Python rows. Additional native planes are optional and
advertised through the sink's public data-plane contract:

```python
from agora import DataPlane, sink_data_plane_spec

spec = sink_data_plane_spec(MySink())
assert spec.accepted_planes == (DataPlane.PYTHON_ROWS,)
assert spec.native_planes == (DataPlane.PYTHON_ROWS,)
```

## Native batch support

If the destination can write many records at once, implement `write_batch()`
and advertise batch-native planes explicitly:

```python
from agora import BaseSink, DataPlane


class MyBatchSink(BaseSink[dict]):
    sink_name = "my_batch_sink"
    accepted_data_planes = (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
    )
    native_data_planes = accepted_data_planes

    async def write_batch(self, records: list[dict]) -> None:
        await self._client.send_many(records)
```

That sink will now advertise:

- `accepted_planes = (python_rows, python_batches)`
- `native_planes = (python_rows, python_batches)`

## Arrow boundary support

If the sink can accept Arrow batches directly at the writer/sink boundary,
implement `write_arrow_batch()` and advertise Arrow in `native_data_planes`:

```python
import pyarrow as pa

from agora import BaseSink, DataPlane


class MyArrowSink(BaseSink[dict]):
    sink_name = "my_arrow_sink"
    accepted_data_planes = (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
        DataPlane.ARROW_BATCHES,
    )
    native_data_planes = accepted_data_planes

    async def write(self, record: dict) -> None:
        await self._client.send_one(record)

    async def write_batch(self, records: list[dict]) -> None:
        await self._client.send_many(records)

    async def write_arrow_batch(self, batch: pa.RecordBatch) -> None:
        await self._client.send_arrow(batch)
```

When the upstream pipeline is on the Arrow chain:

- sinks with Arrow support receive `RecordBatch`
- sinks without Arrow support are downgraded at the sink boundary

For custom sinks, prefer checking `sink_data_plane_spec(sink)` in tests instead
of asserting only on internal flags. That helper uses the same normalization
path the runtime planner/writer uses.

Legacy note:

- `batch_writable_native`
- `arrow_passthrough_native`

still work in `0.3.x`, but Agora now emits `DeprecationWarning` when it has to
infer sink planes from those booleans. Prefer explicit data planes for new
code, and prefer `sink_data_plane_spec(sink)` in tests. Legacy bool flags are
planned for removal in `0.4.0`.

## Practical advice

- make writes idempotent if retries are possible
- use native batch writes when the destination supports them
- test `sink_data_plane_spec()` for every non-row sink
- keep shutdown behavior explicit by flushing or closing resources in `close()`
