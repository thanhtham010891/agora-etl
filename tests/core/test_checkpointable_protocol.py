"""
Test CheckpointableSource Protocol compliance.

Verifies that the CheckpointableSource Protocol works correctly with
isinstance() checks and that sources properly implement the protocol.
"""

import pytest

from agora.core.checkpoint import CheckpointableSource, CheckpointValue, is_checkpoint_capable
from agora.core.source import BaseSource


class _MockCheckpointableSource(BaseSource[dict]):
    """Mock source that implements CheckpointableSource protocol."""

    source_name = "mock_checkpointable"
    supports_checkpoint = True

    def __init__(self):
        self._offset = 0

    def current_checkpoint(self) -> CheckpointValue:
        return {"offset": self._offset}

    async def prepare_resume(self, checkpoint):
        if checkpoint is not None:
            self._offset = checkpoint.value["offset"]

    async def stream(self):
        for i in range(self._offset, self._offset + 3):
            self._offset = i + 1
            yield {"id": i}


class _MockNonCheckpointableSource(BaseSource[dict]):
    """Mock source that does NOT implement CheckpointableSource protocol."""

    source_name = "mock_non_checkpointable"
    supports_checkpoint = False

    async def stream(self):
        for i in range(3):
            yield {"id": i}


def test_checkpointable_source_protocol_isinstance():
    """CheckpointableSource Protocol works with isinstance()."""
    checkpointable = _MockCheckpointableSource()
    non_checkpointable = _MockNonCheckpointableSource()

    # Protocol check works (structural typing)
    assert isinstance(checkpointable, CheckpointableSource)
    # Both match Protocol structurally, but supports_checkpoint differs
    assert isinstance(non_checkpointable, CheckpointableSource)

    # Runtime detection goes through the helper
    assert is_checkpoint_capable(checkpointable) is True
    assert is_checkpoint_capable(non_checkpointable) is False


def test_checkpointable_source_has_required_attributes():
    """CheckpointableSource has required attributes."""
    source = _MockCheckpointableSource()

    assert hasattr(source, "source_name")
    assert hasattr(source, "supports_checkpoint")
    assert hasattr(source, "current_checkpoint")
    assert hasattr(source, "prepare_resume")
    assert hasattr(source, "stream")


def test_checkpointable_source_current_checkpoint_returns_value():
    """current_checkpoint() returns CheckpointValue."""
    source = _MockCheckpointableSource()
    checkpoint = source.current_checkpoint()

    assert checkpoint is not None
    assert isinstance(checkpoint, dict)
    assert "offset" in checkpoint


@pytest.mark.asyncio
async def test_checkpointable_source_prepare_resume():
    """prepare_resume() restores state from checkpoint."""
    from agora.core.checkpoint import Checkpoint

    source = _MockCheckpointableSource()

    # Create checkpoint at offset 5
    checkpoint = Checkpoint(
        pipeline_id="test",
        run_id="run1",
        source="mock_checkpointable",
        value={"offset": 5},
    )

    # Resume from checkpoint
    await source.prepare_resume(checkpoint)

    # Verify offset was restored
    assert source._offset == 5
    assert source.current_checkpoint() == {"offset": 5}


@pytest.mark.asyncio
async def test_checkpointable_source_stream_updates_checkpoint():
    """Streaming updates checkpoint value."""
    source = _MockCheckpointableSource()

    # Initial checkpoint
    assert source.current_checkpoint() == {"offset": 0}

    # Stream records
    records = []
    async for record in source.stream():
        records.append(record)

    # Checkpoint should be updated
    assert len(records) == 3
    assert source.current_checkpoint() == {"offset": 3}


def test_non_checkpointable_source_not_protocol_compliant():
    """Non-checkpointable source has supports_checkpoint=False."""
    source = _MockNonCheckpointableSource()

    # Structurally matches Protocol (has all methods)
    assert isinstance(source, CheckpointableSource)

    # But supports_checkpoint=False indicates no checkpoint support
    assert source.supports_checkpoint is False
    assert is_checkpoint_capable(source) is False
