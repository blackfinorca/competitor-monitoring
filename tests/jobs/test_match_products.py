from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def _load_match_products_module():
    module_path = Path(__file__).resolve().parents[2] / "jobs" / "match_products.py"
    spec = spec_from_file_location("match_products", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Factory:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def __call__(self):
        return self


def test_llm_match_uses_vector_candidates_when_token_prefilter_would_fail(monkeypatch) -> None:
    match_products = _load_match_products_module()
    toolzone = [
        {"id": 10, "brand": "Knipex", "mpn": "87-01-250", "ean": "4003773012345", "title": "Knipex Cobra 250 mm"},
    ]
    unmatched = [
        {
            "id": 20,
            "competitor_id": "example_sk",
            "brand": "Other",
            "mpn": "",
            "ean": "",
            "title": "Water pump pliers 10 inch",
        }
    ]

    seen_candidate_counts: list[int] = []

    class FakeVectorIndex:
        def __init__(self, products):
            self.products = products

        def search(self, listing, *, limit):
            del listing
            assert limit == 20
            return self.products[:limit]

        def search_many(self, listings, *, limit, batch_size=256):
            del batch_size
            for listing in listings:
                yield self.search(listing, limit=limit)

    def fake_find_best_llm_match(listing, candidates, *, llm_client):
        del listing, llm_client
        seen_candidate_counts.append(len(candidates))
        return candidates[0], ("llm_fuzzy", 0.80)

    monkeypatch.setattr(match_products, "TitleVectorIndex", FakeVectorIndex)
    monkeypatch.setattr(match_products, "find_best_llm_match", fake_find_best_llm_match)
    monkeypatch.setattr(match_products, "_save_matches", lambda session, records: len(records))

    saved = match_products._llm_match(
        toolzone,
        unmatched,
        llm_client=object(),
        factory=_Factory(),
        already_matched=set(),
    )

    assert saved == 1
    assert seen_candidate_counts == [1]
