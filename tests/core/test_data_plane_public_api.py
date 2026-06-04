from __future__ import annotations

from agora import DataPlane, SinkDataPlaneSpec, SourceDataPlaneSpec
from agora.core import (
    DataPlane as CoreDataPlane,
)
from agora.core import (
    SinkDataPlaneSpec as CoreSinkDataPlaneSpec,
)
from agora.core import (
    SourceDataPlaneSpec as CoreSourceDataPlaneSpec,
)
from agora.core.sink import BaseSink
from agora.core.source import BaseSource


class _ArrowSource(BaseSource[object]):
    source_name = "arrow_source"

    async def stream(self):
        if False:
            yield None

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.ARROW_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=True,
        )


class _ArrowSink(BaseSink[dict[str, object]]):
    sink_name = "arrow_sink"
    accepted_data_planes = (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
        DataPlane.ARROW_BATCHES,
    )
    native_data_planes = accepted_data_planes

    async def write(self, record: dict[str, object]) -> None:
        del record

    async def write_batch(self, records: list[dict[str, object]]) -> None:
        del records

    async def write_arrow_batch(self, batch: object) -> None:
        del batch


def test_data_plane_types_are_exported_from_public_namespaces() -> None:
    assert DataPlane is CoreDataPlane
    assert SourceDataPlaneSpec is CoreSourceDataPlaneSpec
    assert SinkDataPlaneSpec is CoreSinkDataPlaneSpec


def test_source_data_plane_contract_uses_public_spec_type() -> None:
    spec = _ArrowSource().data_plane_spec()

    assert isinstance(spec, SourceDataPlaneSpec)
    assert spec.emitted_plane is DataPlane.ARROW_BATCHES
    assert spec.supports_batch_emit is True
    assert spec.emits_arrow_batches is True


def test_sink_data_plane_contract_uses_public_spec_type() -> None:
    spec = _ArrowSink().data_plane_spec()

    assert isinstance(spec, SinkDataPlaneSpec)
    assert spec.accepted_planes == (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
        DataPlane.ARROW_BATCHES,
    )
    assert spec.native_planes == (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
        DataPlane.ARROW_BATCHES,
    )
