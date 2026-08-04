from tacit.cache import TTLCache


def test_cache_stats_distinguish_hits_and_misses():
    cache = TTLCache()

    assert cache.get("missing") is None
    cache.set("present", 1)
    assert cache.get("present") == 1
    assert cache.stats == {"hits": 1, "misses": 1, "size": 1}

    cache.reset_stats()
    assert cache.stats == {"hits": 0, "misses": 0, "size": 1}


def test_cache_prunes_expired_entries_during_unrelated_writes():
    cache = TTLCache(max_entries=2)
    cache.set("expired", 1, ttl=-1)

    cache.set("current", 2)

    assert cache.size == 1
    assert cache.get("current") == 2


def test_cache_evicts_the_least_recently_used_entry_at_capacity():
    cache = TTLCache(max_entries=2)
    cache.set("first", 1)
    cache.set("second", 2)
    assert cache.get("first") == 1

    cache.set("third", 3)

    assert cache.get("second") is None
    assert cache.get("first") == 1
    assert cache.get("third") == 3


def test_cache_evicts_least_recently_used_values_to_meet_weight_budget():
    cache = TTLCache(max_entries=10, max_total_weight=3)
    cache.set("first", [1, 2])
    cache.set("second", [3, 4])

    assert cache.get("first") is None
    assert cache.get("second") == [3, 4]
    assert cache.weight == 2


def test_cache_does_not_retain_one_oversized_value():
    cache = TTLCache(max_entries=10, max_total_weight=10, max_value_weight=3)
    cache.set("small", [1, 2])
    cache.set("small", [1, 2, 3, 4])

    assert cache.get("small") is None
    assert cache.weight == 0
