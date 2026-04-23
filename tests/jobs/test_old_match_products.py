from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def _load_old_match_products_module():
    module_path = Path(__file__).resolve().parents[2] / "jobs" / "old_match_products.py"
    spec = spec_from_file_location("old_match_products", module_path)
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


def test_legacy_llm_match_uses_token_prefilter(monkeypatch) -> None:
    old_match_products = _load_old_match_products_module()
    toolzone = [
        {"id": 10, "brand": "Knipex", "mpn": "87-01-250", "ean": "4003773012345", "title": "Knipex Cobra 250 mm"},
    ]
    unmatched = [
        {
            "id": 20,
            "competitor_id": "example_sk",
            "brand": "Knipex",
            "mpn": "",
            "ean": "",
            "title": "Knipex Cobra 250",
        }
    ]
    prefilter_calls: list[int] = []

    def fake_pre_filter_candidates(listing, products):
        del listing
        prefilter_calls.append(len(products))
        return products

    def fake_find_best_llm_match(listing, candidates, *, llm_client):
        del listing, llm_client
        return candidates[0], ("llm_fuzzy", 0.80)

    monkeypatch.setattr(old_match_products, "pre_filter_candidates", fake_pre_filter_candidates)
    monkeypatch.setattr(old_match_products, "find_best_llm_match", fake_find_best_llm_match)
    monkeypatch.setattr(old_match_products, "_save_matches", lambda session, records: len(records))

    saved = old_match_products._llm_match(
        toolzone,
        unmatched,
        llm_client=object(),
        factory=_Factory(),
        already_matched=set(),
    )

    assert saved == 1
    assert prefilter_calls == [1]
