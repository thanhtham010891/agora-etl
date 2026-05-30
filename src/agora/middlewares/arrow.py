"""agora/middlewares/arrow.py — Arrow-native batch middleware helpers.

These middlewares operate on ``pa.RecordBatch`` objects directly, keeping data
columnar throughout the pipeline. Use them with an Arrow-native source
(``emits_arrow_batches=True``) and an Arrow-native sink to avoid any per-row
Python object allocation.

**Only for vectorisable transforms.** Arithmetic, comparison, cast, string ops,
fill-null, and similar ``pyarrow.compute`` operations belong here. Arbitrary
per-row Python logic (branching on record state, calling external APIs, etc.)
belongs on a regular ``MapMiddleware`` on the per-record lane — not here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.batch import ArrowBatchMiddleware

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.context import PipelineContext


class ArrowMapMiddleware(ArrowBatchMiddleware):
    """Apply a vectorised transform to a ``pa.RecordBatch``.

    *fn* receives the full ``pa.RecordBatch`` and must return a
    ``pa.RecordBatch`` (same or different schema). Use ``pyarrow.compute``
    kernels inside *fn* to stay columnar::

        import pyarrow.compute as pc

        def scale_price(batch):
            idx = batch.schema.get_field_index("price")
            scaled = pc.multiply(pc.cast(batch.column(idx), pa.float64()), 100.0)
            return batch.set_column(idx, "price", scaled)

        pipeline.pipe(ArrowMapMiddleware(scale_price))
    """

    name = "arrow_map"

    def __init__(
        self,
        fn: Callable[[Any], Any],
        name: str = "arrow_map",
    ) -> None:
        self.name = name
        self._fn = fn

    async def process_arrow_batch(self, batch: Any, ctx: PipelineContext) -> Any:
        try:
            import pyarrow  # noqa: F401 — pre-import to surface missing dep early
        except ImportError as exc:
            raise ImportError(
                "ArrowMapMiddleware requires pyarrow. Install via: pip install 'agora-etl[file]'"
            ) from exc
        del ctx
        return self._fn(batch)


class ArrowFilterMiddleware(ArrowBatchMiddleware):
    """Filter rows in a ``pa.RecordBatch`` using a vectorised predicate.

    *predicate* receives the ``pa.RecordBatch`` and must return a
    ``pa.BooleanArray`` mask. Rows where the mask is ``False`` are dropped.
    The returned batch may have fewer rows; a zero-row result drops the whole
    batch::

        import pyarrow.compute as pc

        pipeline.pipe(ArrowFilterMiddleware(lambda b: pc.greater(b.column("price"), 0.0)))
    """

    name = "arrow_filter"

    def __init__(
        self,
        predicate: Callable[[Any], Any],
        name: str = "arrow_filter",
    ) -> None:
        self.name = name
        self._predicate = predicate

    async def process_arrow_batch(self, batch: Any, ctx: PipelineContext) -> Any:
        try:
            import pyarrow  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "ArrowFilterMiddleware requires pyarrow. Install via: pip install 'agora-etl[file]'"
            ) from exc
        del ctx
        mask = self._predicate(batch)
        return batch.filter(mask)
