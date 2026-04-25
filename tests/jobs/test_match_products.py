from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import threading

import pytest


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
        backend_description = "hashing-fallback(dimensions=512)"

        def __init__(self, products):
            self.products = products

        def search(self, listing, *, limit):
            del listing
            assert limit == 200
            return self.products[:limit]

        def search_many_with_scores(self, listings, *, limit, batch_size=256):
            del batch_size
            for listing in listings:
                yield [(candidate, 1.0) for candidate in self.search(listing, limit=limit)]

    def fake_find_best_llm_match(listing, candidates, *, llm_client):
        del listing, llm_client
        seen_candidate_counts.append(len(candidates))
        return candidates[0], ("llm_fuzzy", 0.86)

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


def test_llm_match_debug_failures_prints_query_candidates_and_expected_presence(monkeypatch, capsys) -> None:
    match_products = _load_match_products_module()
    toolzone = [
        {
            "id": 10,
            "brand": "Knipex",
            "mpn": "",
            "ean": "",
            "title": "Knipex Cobra 250 mm",
        },
        {
            "id": 11,
            "brand": "Knipex",
            "mpn": "",
            "ean": "",
            "title": "Knipex Cobra 300 mm",
        },
        {
            "id": 12,
            "brand": "Wiha",
            "mpn": "",
            "ean": "",
            "title": "Wiha specialne klieste 180 mm",
        },
    ]
    unmatched = [
        {
            "id": 20,
            "competitor_id": "example_sk",
            "brand": "Knípex",
            "mpn": "",
            "ean": "",
            "title": "Knipex Cobra 250 mm",
        }
    ]

    class FakeVectorIndex:
        backend_description = "hashing-fallback(dimensions=64)"

        def __init__(self, products):
            self.products = products

        def search_many_with_scores(self, listings, *, limit, batch_size=256):
            del batch_size
            assert limit == 200
            for _listing in listings:
                yield [
                    (self.products[1], 0.91),
                    (self.products[2], 0.77),
                ]

    monkeypatch.setattr(match_products, "TitleVectorIndex", FakeVectorIndex)
    monkeypatch.setattr(match_products, "find_best_llm_match", lambda listing, candidates, *, llm_client: None)

    saved = match_products._llm_match(
        toolzone,
        unmatched,
        llm_client=object(),
        factory=_Factory(),
        already_matched=set(),
        debug_failures=True,
    )

    assert saved == 0
    output = capsys.readouterr().out
    assert "Vector retrieval backend: hashing-fallback(dimensions=64)" in output
    assert "normalized query:" in output
    assert "anchors: category=cobra brand=KNIPEX model_tokens=250" in output
    assert "brand=KNIPEX" in output
    assert "title=knipex cobra 250 mm" in output
    assert "[1] score=0.9100 id=11" in output
    assert "expected shortlist presence: no (exact_title)" in output


@pytest.mark.parametrize(
    ("listing", "candidates", "expected_ids"),
    [
        (
            {"title": "sekera gr fiber tvarovana rukovat", "brand": "", "mpn": "", "ean": ""},
            [
                ({"id": 1, "title": "Stetec plochy Baupro", "brand": "Baupro", "mpn": "", "ean": ""}, 0.92),
                ({"id": 2, "title": "Sekera Fiber rukovat 600 g", "brand": "", "mpn": "", "ean": ""}, 0.81),
            ],
            [2],
        ),
        (
            {"title": "stetec akryl 1", "brand": "", "mpn": "", "ean": ""},
            [
                ({"id": 3, "title": "Sekera s fiberglass rukovatou", "brand": "", "mpn": "", "ean": ""}, 0.95),
                ({"id": 4, "title": "Stetec plochy 1\" akryl", "brand": "", "mpn": "", "ean": ""}, 0.72),
            ],
            [4],
        ),
        (
            {"title": "zavitnik obojstranny g1/4 volkel", "brand": "", "mpn": "", "ean": ""},
            [
                ({"id": 5, "title": "Zavitnik obojstranny G 1/4", "brand": "Volkel", "mpn": "", "ean": ""}, 0.84),
                ({"id": 6, "title": "Zavitnik obojstranny G 3/8", "brand": "Volkel", "mpn": "", "ean": ""}, 0.87),
                ({"id": 7, "title": "Sada vrtakov HSS", "brand": "Volkel", "mpn": "", "ean": ""}, 0.90),
            ],
            [5],
        ),
        (
            {"title": "zmetak a lopatka B20 Baupro", "brand": "", "mpn": "", "ean": ""},
            [
                ({"id": 8, "title": "Zmetak a lopatka B20", "brand": "Baupro", "mpn": "", "ean": ""}, 0.76),
                ({"id": 9, "title": "Zmetak a lopatka B38", "brand": "Baupro", "mpn": "", "ean": ""}, 0.89),
                ({"id": 10, "title": "Lopatka plastova", "brand": "Baupro", "mpn": "", "ean": ""}, 0.91),
            ],
            [8],
        ),
        (
            {"title": "bruska stolova tgd150 dvojkotucova 250w baupro", "brand": "", "mpn": "", "ean": ""},
            [
                ({"id": 11, "title": "Bruska stolova TGD150 dvojkotucova 250W", "brand": "Baupro", "mpn": "", "ean": ""}, 0.79),
                ({"id": 12, "title": "Bruska uhlova 1200W", "brand": "Baupro", "mpn": "", "ean": ""}, 0.88),
                ({"id": 13, "title": "Stolik dielensky", "brand": "Baupro", "mpn": "", "ean": ""}, 0.92),
            ],
            [11],
        ),
        (
            {"title": "fasadny tanierik 60mm baupro", "brand": "", "mpn": "", "ean": ""},
            [
                ({"id": 14, "title": "Fasadny tanierik 60 mm", "brand": "Baupro", "mpn": "", "ean": ""}, 0.75),
                ({"id": 15, "title": "Fasadny tanierik 90 mm", "brand": "Baupro", "mpn": "", "ean": ""}, 0.83),
                ({"id": 16, "title": "Brusny papier Baupro", "brand": "Baupro", "mpn": "", "ean": ""}, 0.90),
            ],
            [14],
        ),
    ],
)
def test_lexical_shortlist_prefers_anchor_tokens(listing, candidates, expected_ids) -> None:
    match_products = _load_match_products_module()

    reranked = match_products._lexical_shortlist(
        listing,
        candidates,
        brand_token_map={"baupro": "BAUPRO", "volkel": "VOLKEL"},
        limit=40,
        min_limit=1,
    )

    assert [candidate["id"] for candidate, _score in reranked[: len(expected_ids)]] == expected_ids


def test_lexical_shortlist_backfills_to_minimum_and_caps_at_maximum() -> None:
    match_products = _load_match_products_module()

    listing = {"title": "stetec akryl 1", "brand": "", "mpn": "", "ean": ""}
    candidates = [
        ({"id": idx, "title": f"Produkt {idx}", "brand": "", "mpn": "", "ean": ""}, 1.0 - (idx * 0.01))
        for idx in range(1, 40)
    ]
    candidates[6] = (
        {"id": 99, "title": "Stetec plochy 1 akryl", "brand": "", "mpn": "", "ean": ""},
        0.62,
    )

    reranked = match_products._lexical_shortlist(
        listing,
        candidates,
        brand_token_map={},
        limit=30,
        min_limit=5,
    )

    assert reranked[0][0]["id"] == 99
    assert len(reranked) == 5

    capped = match_products._lexical_shortlist(
        {"title": "produkt", "brand": "", "mpn": "", "ean": ""},
        candidates,
        brand_token_map={},
        limit=30,
        min_limit=5,
    )

    assert len(capped) == 30


def test_llm_match_parallelizes_only_for_openai_clients(monkeypatch) -> None:
    match_products = _load_match_products_module()
    toolzone = [
        {"id": 10, "brand": "Knipex", "mpn": "", "ean": "", "title": "Knipex Cobra 250 mm"},
    ]
    unmatched = [
        {"id": 20 + idx, "competitor_id": "example_sk", "brand": "Knipex", "mpn": "", "ean": "", "title": f"Knipex Cobra {idx}"}
        for idx in range(3)
    ]

    class FakeVectorIndex:
        backend_description = "hashing-fallback(dimensions=512)"

        def __init__(self, products):
            self.products = products

        def search_many_with_scores(self, listings, *, limit, batch_size=256):
            del limit, batch_size
            for _listing in listings:
                yield [(self.products[0], 1.0)]

    state = {"active": 0, "max_active": 0}
    lock = threading.Lock()
    ready = threading.Event()

    def fake_llm_match_one(job, *, llm_client):
        del llm_client
        with lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            if state["active"] >= 2:
                ready.set()
        ready.wait(timeout=0.2)
        with lock:
            state["active"] -= 1
        return (job["candidates"][0], ("llm_fuzzy", 0.86))

    monkeypatch.setattr(match_products, "TitleVectorIndex", FakeVectorIndex)
    monkeypatch.setattr(match_products, "_llm_match_one", fake_llm_match_one)
    monkeypatch.setattr(match_products, "_save_matches", lambda session, records: len(records))

    client = match_products.OpenAIClient(api_key="test")
    saved = match_products._llm_match(
        toolzone,
        unmatched,
        llm_client=client,
        factory=_Factory(),
        already_matched=set(),
        openai_workers=3,
    )

    assert saved == 3
    assert state["max_active"] >= 2


def test_llm_match_keeps_non_openai_clients_sequential(monkeypatch) -> None:
    match_products = _load_match_products_module()
    toolzone = [
        {"id": 10, "brand": "Knipex", "mpn": "", "ean": "", "title": "Knipex Cobra 250 mm"},
    ]
    unmatched = [
        {"id": 20, "competitor_id": "example_sk", "brand": "Knipex", "mpn": "", "ean": "", "title": "Knipex Cobra 250"}
    ]

    class FakeVectorIndex:
        backend_description = "hashing-fallback(dimensions=512)"

        def __init__(self, products):
            self.products = products

        def search_many_with_scores(self, listings, *, limit, batch_size=256):
            del limit, batch_size
            for _listing in listings:
                yield [(self.products[0], 1.0)]

    def fail_if_executor_used(*args, **kwargs):
        raise AssertionError("ThreadPoolExecutor should not be used for non-OpenAI clients")

    monkeypatch.setattr(match_products, "TitleVectorIndex", FakeVectorIndex)
    monkeypatch.setattr(match_products, "ThreadPoolExecutor", fail_if_executor_used)
    monkeypatch.setattr(
        match_products,
        "_llm_match_one",
        lambda job, *, llm_client: (job["candidates"][0], ("llm_fuzzy", 0.86)),
    )
    monkeypatch.setattr(match_products, "_save_matches", lambda session, records: len(records))

    saved = match_products._llm_match(
        toolzone,
        unmatched,
        llm_client=object(),
        factory=_Factory(),
        already_matched=set(),
        openai_workers=4,
    )

    assert saved == 1


def test_llm_match_rejects_below_threshold_and_accepts_at_threshold(monkeypatch) -> None:
    match_products = _load_match_products_module()
    toolzone = [
        {"id": 10, "brand": "Knipex", "mpn": "", "ean": "", "title": "Knipex Cobra 250 mm"},
    ]
    unmatched = [
        {"id": 20, "competitor_id": "example_sk", "brand": "Knipex", "mpn": "", "ean": "", "title": "Knipex Cobra 250"},
        {"id": 21, "competitor_id": "example_sk", "brand": "Knipex", "mpn": "", "ean": "", "title": "Knipex Cobra 251"},
    ]

    class FakeVectorIndex:
        backend_description = "hashing-fallback(dimensions=512)"

        def __init__(self, products):
            self.products = products

        def search_many_with_scores(self, listings, *, limit, batch_size=256):
            del limit, batch_size
            for _listing in listings:
                yield [(self.products[0], 1.0)]

    hits = iter([
        (toolzone[0], ("llm_fuzzy", 0.80)),
        (toolzone[0], ("llm_fuzzy", 0.81)),
    ])

    monkeypatch.setattr(match_products, "TitleVectorIndex", FakeVectorIndex)
    monkeypatch.setattr(match_products, "_llm_match_one", lambda job, *, llm_client: next(hits))
    monkeypatch.setattr(match_products, "_save_matches", lambda session, records: len(records))

    saved = match_products._llm_match(
        toolzone,
        unmatched,
        llm_client=object(),
        factory=_Factory(),
        already_matched=set(),
        openai_workers=1,
    )

    assert saved == 1
