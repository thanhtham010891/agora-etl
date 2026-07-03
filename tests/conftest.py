from __future__ import annotations

import pytest

from tests.utils.process_pool import process_pool_unavailable_reason


@pytest.fixture(autouse=True)
def _require_process_pool_for_marked_tests(request: pytest.FixtureRequest) -> None:
    if request.node.get_closest_marker("requires_process_pool") is None:
        return

    reason = process_pool_unavailable_reason()
    if reason is not None:
        pytest.skip(f"Process pool unavailable in this environment: {reason}")
