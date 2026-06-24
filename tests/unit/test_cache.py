from tacit.cache import TTLCache


def test_cache_stats_distinguish_hits_and_misses():
    cache = TTLCache()

    assert cache.get("missing") is None
    cache.set("present", 1)
    assert cache.get("present") == 1
    assert cache.stats == {"hits": 1, "misses": 1, "size": 1}

    cache.reset_stats()
    assert cache.stats == {"hits": 0, "misses": 0, "size": 1}
