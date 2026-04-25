from agnaradie_pricing import orchestrator


def test_cache_max_age_is_30_days() -> None:
    assert orchestrator.CACHE_MAX_AGE_HOURS == 24 * 30
