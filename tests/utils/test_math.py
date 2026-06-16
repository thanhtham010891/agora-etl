from __future__ import annotations

import pytest

from agora.utils.math import cosine_similarity


def test_cosine_similarity_rejects_mismatched_dimensions() -> None:
    with pytest.raises(ValueError, match="zip"):
        cosine_similarity([1.0, 2.0], [1.0])
