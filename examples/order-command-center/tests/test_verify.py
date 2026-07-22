import sys

import pytest
from order_command_center import verify


def test_verify_reports_an_actionable_precondition_when_no_batch_exists(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    async def no_published_run(*, wait_seconds: float = 0.0) -> None:
        del wait_seconds
        raise verify.NoPublishedProducerRunError(
            "No completed producer batch is registered for topic 'orders'. "
            "Run `make producer` to publish a batch, then run `make verify` again."
        )

    monkeypatch.setattr(verify, "run", no_published_run)
    monkeypatch.setattr(sys, "argv", ["order-demo-verify"])

    with pytest.raises(SystemExit) as exc_info:
        verify.main()

    assert exc_info.value.code == 2
    assert "verification precondition failed" in capsys.readouterr().err


def test_verify_reports_pending_projection_without_a_traceback(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    async def projection_is_catching_up(*, wait_seconds: float = 0.0) -> None:
        del wait_seconds
        raise verify.ProjectionNotConvergedError("PostgreSQL ledger is catching up: 0/3.")

    monkeypatch.setattr(verify, "run", projection_is_catching_up)
    monkeypatch.setattr(sys, "argv", ["order-demo-verify"])

    with pytest.raises(SystemExit) as exc_info:
        verify.main()

    assert exc_info.value.code == 1
    assert "verification pending" in capsys.readouterr().err
