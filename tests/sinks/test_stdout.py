"""
tests/sinks/test_stdout.py
============================
Tests for StdoutSink — no I/O mocking needed beyond capsys.
"""

from __future__ import annotations

import json

import pytest

from agora.sinks.io.stdout import StdoutSink


class TestStdoutSink:
    async def test_write_dict_outputs_json(self, capsys) -> None:
        sink = StdoutSink(prefix="")
        await sink.write({"key": "value"})
        out = capsys.readouterr().out
        parsed = json.loads(out.strip())
        assert parsed == {"key": "value"}

    async def test_custom_prefix(self, capsys) -> None:
        sink = StdoutSink(prefix=">>> ")
        await sink.write({"id": 1})
        out = capsys.readouterr().out
        assert out.startswith(">>> ")

    async def test_custom_formatter(self, capsys) -> None:
        sink = StdoutSink(formatter=lambda r: f"id={r['id']}", prefix="")
        await sink.write({"id": 42})
        assert capsys.readouterr().out.strip() == "id=42"

    async def test_write_batch_writes_all(self, capsys) -> None:
        sink = StdoutSink(prefix="")
        await sink.write_batch([{"n": 1}, {"n": 2}, {"n": 3}])
        lines = [line for line in capsys.readouterr().out.strip().splitlines() if line]
        assert len(lines) == 3

    async def test_flush_is_noop(self) -> None:
        sink = StdoutSink()
        await sink.flush()  # should not raise

    async def test_close_is_noop(self) -> None:
        sink = StdoutSink()
        await sink.close()  # should not raise

    async def test_open_is_noop(self) -> None:
        sink = StdoutSink()
        await sink.open()  # should not raise

    async def test_pydantic_model_formatted(self, capsys) -> None:
        """Pydantic models should be JSON-serialized via model_dump()."""
        try:
            from pydantic import BaseModel
        except ImportError:
            pytest.skip("pydantic not installed")

        class MyModel(BaseModel):
            x: int = 1

        sink = StdoutSink(prefix="")
        await sink.write(MyModel(x=99))
        out = capsys.readouterr().out
        parsed = json.loads(out.strip())
        assert parsed["x"] == 99

    async def test_unserializable_falls_back_to_repr(self, capsys) -> None:
        """Objects that can't be JSON-serialized AND have no __dict__ fall back to repr().
        StdoutSink checks __dict__ before repr — so we need a built-in type
        that has no __dict__ and is not JSON-serializable directly.
        """

        # A set is not JSON-serializable and has no __dict__, so it reaches repr path
        class NoDict:
            """Slots-based class has no __dict__."""

            __slots__ = ()

            def __repr__(self) -> str:
                return "<NoDict>"

        sink = StdoutSink(prefix="")
        await sink.write(NoDict())
        assert "<NoDict>" in capsys.readouterr().out
