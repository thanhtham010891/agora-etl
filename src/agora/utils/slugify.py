"""
agora/utils/slugify.py
========================
Vietnamese/Unicode text → URL-safe ASCII slug.

Moved from ``collector/utils/slugify.py`` into agora so every ETL project
can share the same normalisation logic without duplicating it.

Algorithm
---------
  1. Lowercase + replace đ/Đ (not NFD-decomposable)
  2. NFD decompose — splits composed chars into base + combining marks
  3. Strip combining marks (U+0300-U+036F) — drops all tone/accent marks
  4. Keep only ASCII word chars, spaces, hyphens
  5. Collapse whitespace/underscores → single hyphen
  6. Strip leading/trailing hyphens; truncate to ``max_length``

Examples::

    >>> from agora.utils.slugify import to_slug
    >>> to_slug("Bún Bò Huế Mụ Reng")
    'bun-bo-hue-mu-reng'
    >>> to_slug("Đà Nẵng")
    'da-nang'
    >>> to_slug("Nhà Hàng Mì Quảng")
    'nha-hang-mi-quang'
"""

from __future__ import annotations

import re
import unicodedata
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ["safe_path", "sanitize_path", "to_slug"]

_MAX_SLUG_LEN = 80
_COMBINING = re.compile(r"[\u0300-\u036f]")


def to_slug(text: str, max_length: int = _MAX_SLUG_LEN) -> str:
    """Convert a Unicode/Vietnamese string to a URL-safe ASCII slug.

    Parameters
    ----------
    text:
        Input string (any language).
    max_length:
        Maximum length of the returned slug (default: 80).

    Returns
    -------
    str
        Lowercase ASCII slug, e.g. ``'da-nang'``.
        Returns ``""`` for empty/whitespace input.
    """
    if not text or not text.strip():
        return ""

    # 1. Lowercase; replace đ/Đ (cannot be NFD-decomposed).
    s = text.lower().replace("đ", "d")

    # 2. NFD decompose → base letters + combining marks.
    s = unicodedata.normalize("NFD", s)

    # 3. Strip combining marks (tones, accents).
    s = _COMBINING.sub("", s)

    # 4. Keep only ASCII word chars, spaces, hyphens.
    s = re.sub(r"[^\w\s-]", "", s, flags=re.ASCII)

    # 5. Collapse whitespace / underscores → single hyphen.
    s = re.sub(r"[\s_]+", "-", s)

    # 6. Collapse consecutive hyphens; strip edges.
    s = re.sub(r"-+", "-", s).strip("-")

    return s[:max_length]


def sanitize_path(component: str) -> str:
    """Sanitize a string for safe use as a filesystem path component.

    Prevents directory traversal attacks:
      - Removes ``/``, ``\\``, null bytes
      - Removes ``..`` sequences
      - Applies ``to_slug()`` for consistent ASCII output

    Parameters
    ----------
    component:
        Raw string from external source (city name, slug, etc.)

    Returns
    -------
    str
        Safe ASCII path component.

    Raises
    ------
    ValueError
        If the result is empty or contains only punctuation.

    Examples
    --------
    >>> sanitize_path("ha-noi")
    'ha-noi'
    >>> sanitize_path("../../../etc/passwd")
    Traceback (most recent call last):
        ...
    ValueError: Invalid path component: ''
    """
    if not component or not component.strip():
        raise ValueError("Path component cannot be empty")

    text = component.replace("\x00", "")
    text = text.replace("/", "-").replace("\\", "-")
    text = text.replace("..", "")
    text = to_slug(text)

    if not text or set(text) <= {"-", "_", "."}:
        raise ValueError(f"Invalid path component: {component!r}")

    return text


def safe_path(root: Path, *components: str) -> Path:
    """Build a resolved path inside ``root``, preventing traversal attacks.

    Parameters
    ----------
    root:
        Absolute base directory.
    *components:
        Path components to join (each is sanitized via ``sanitize_path``).

    Returns
    -------
    Path
        Resolved absolute path guaranteed to be inside ``root``.

    Raises
    ------
    ValueError
        If any component is invalid or the result escapes ``root``.

    Examples
    --------
    >>> safe_path(Path("/data"), "01-POI", "ha-noi", "pho.txt")
    PosixPath('/data/01-POI/ha-noi/pho.txt')
    """
    if not root.is_absolute():
        root = root.resolve()

    safe_components = [sanitize_path(c) for c in components]
    result = root.joinpath(*safe_components).resolve()

    try:
        result.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"Path escapes root: {result} not under {root}") from exc

    return result
