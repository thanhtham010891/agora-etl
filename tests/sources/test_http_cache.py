from __future__ import annotations

import time

from agora.sources._internal.cache import HttpCache, _make_key


def test_make_key_varies_by_source():
    a = _make_key("https://api.example.com/places", None, None, "google")
    b = _make_key("https://api.example.com/places", None, None, "overture")
    assert a != b


def test_make_key_varies_by_headers():
    a = _make_key(
        "https://api.example.com/places",
        None,
        {"Authorization": "Bearer a", "Accept-Language": "vi"},
        "source",
    )
    b = _make_key(
        "https://api.example.com/places",
        None,
        {"Authorization": "Bearer b", "Accept-Language": "vi"},
        "source",
    )
    assert a != b


def test_http_cache_keeps_distinct_entries_for_header_variants(tmp_path):
    cache = HttpCache(db_path=tmp_path / "http_cache.db", ttl=3600)

    cache.set(
        "https://api.example.com/places?q=hanoi",
        "body-a",
        headers={"Authorization": "Bearer a"},
        source="source",
    )
    cache.set(
        "https://api.example.com/places?q=hanoi",
        "body-b",
        headers={"Authorization": "Bearer b"},
        source="source",
    )

    assert (
        cache.get(
            "https://api.example.com/places?q=hanoi",
            headers={"Authorization": "Bearer a"},
            source="source",
        )
        == "body-a"
    )
    assert (
        cache.get(
            "https://api.example.com/places?q=hanoi",
            headers={"Authorization": "Bearer b"},
            source="source",
        )
        == "body-b"
    )


def test_http_cache_kv_store_uses_ttl_key_value_store(tmp_path):
    cache = HttpCache(db_path=tmp_path / "http_cache.db", ttl=3600)

    cache.kv_set("cursor", {"page": 2})

    assert cache.kv_get("cursor") == {"page": 2}
    assert cache.stats()["kv_store"] == 1


def test_http_cache_kv_store_expires_values(tmp_path):
    cache = HttpCache(db_path=tmp_path / "http_cache.db", ttl=3600)

    cache.kv_set("short", "value", ttl=1)
    time.sleep(1.1)

    assert cache.kv_get("short") is None


def test_http_cache_stats_ignore_expired_entries(tmp_path):
    cache = HttpCache(db_path=tmp_path / "http_cache.db", ttl=1)

    cache.set("https://api.example.com/places", "body", source="source")
    cache.kv_set("cursor", {"page": 1}, ttl=1)
    time.sleep(1.1)

    assert cache.stats() == {"http_cache": 0, "kv_store": 0}


def test_http_cache_reset_clears_http_and_kv_entries(tmp_path):
    cache = HttpCache(db_path=tmp_path / "http_cache.db", ttl=3600)
    cache.set("https://api.example.com/places", "body", source="source")
    cache.kv_set("cursor", {"page": 1})

    before = cache.stats()
    cache.reset()
    after = cache.stats()

    assert before["http_cache"] == 1
    assert before["kv_store"] == 1
    assert after == {"http_cache": 0, "kv_store": 0}
