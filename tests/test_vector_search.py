from agnaradie_pricing.matching.vector_search import (
    HashingTextEmbedder,
    TitleVectorIndex,
    make_default_embedder,
)


def test_title_vector_index_returns_similar_products_first() -> None:
    products = [
        {"id": 1, "brand": "Knipex", "mpn": "87-01-250", "title": "Knipex Cobra 250 mm kleste"},
        {"id": 2, "brand": "Bosch", "mpn": "GBH 18V-21", "title": "Bosch aku vrtacie kladivo"},
        {"id": 3, "brand": "Makita", "mpn": "DGA511Z", "title": "Makita uhlova bruska aku 125 mm"},
    ]
    index = TitleVectorIndex(products, embedder=HashingTextEmbedder(dimensions=128))

    matches = index.search(
        {"brand": "Knipex", "mpn": "", "title": "KNIPEX kliestie Cobra 250"},
        limit=2,
    )

    assert matches[0]["id"] == 1
    assert len(matches) == 2


def test_title_vector_index_honors_limit() -> None:
    products = [
        {"id": idx, "brand": "Brand", "mpn": f"MPN{idx}", "title": f"Product {idx}"}
        for idx in range(10)
    ]
    index = TitleVectorIndex(products, embedder=HashingTextEmbedder(dimensions=64))

    assert len(index.search({"title": "Product", "brand": "Brand", "mpn": ""}, limit=4)) == 4


def test_title_vector_index_search_many_batches_queries() -> None:
    products = [
        {"id": 1, "brand": "Knipex", "mpn": "87-01-250", "title": "Knipex Cobra 250 mm"},
        {"id": 2, "brand": "Makita", "mpn": "DGA511Z", "title": "Makita uhlova bruska"},
    ]
    listings = [
        {"brand": "Knipex", "title": "Knipex Cobra 250"},
        {"brand": "Makita", "title": "Makita bruska aku"},
    ]
    index = TitleVectorIndex(products, embedder=HashingTextEmbedder(dimensions=128))

    results = list(index.search_many(listings, limit=1, batch_size=1))

    assert [[candidate["id"] for candidate in candidates] for candidates in results] == [[1], [2]]


def test_title_vector_index_search_many_with_scores_batches_queries() -> None:
    products = [
        {"id": 1, "brand": "Knipex", "mpn": "87-01-250", "title": "Knipex Cobra 250 mm"},
        {"id": 2, "brand": "Makita", "mpn": "DGA511Z", "title": "Makita uhlova bruska"},
    ]
    listings = [
        {"brand": "Knipex", "title": "Knipex Cobra 250"},
        {"brand": "Makita", "title": "Makita bruska aku"},
    ]
    index = TitleVectorIndex(products, embedder=HashingTextEmbedder(dimensions=128))

    results = list(index.search_many_with_scores(listings, limit=1, batch_size=1))

    assert [[candidate["id"] for candidate, _score in candidates] for candidates in results] == [[1], [2]]
    assert all(len(candidates) == 1 for candidates in results)
    assert all(isinstance(candidates[0][1], float) for candidates in results)


def test_title_vector_index_exposes_backend_description() -> None:
    index = TitleVectorIndex([], embedder=HashingTextEmbedder(dimensions=64))

    assert index.backend_description == "hashing-fallback(dimensions=64)"


def test_default_embedder_uses_hashing_without_opt_in(monkeypatch) -> None:
    monkeypatch.delenv("MATCHING_EMBEDDING_BACKEND", raising=False)

    embedder = make_default_embedder()

    assert isinstance(embedder, HashingTextEmbedder)


def test_default_embedder_allows_sentence_transformers_opt_in(monkeypatch) -> None:
    created = []

    class _FakeSentenceEmbedder:
        model_name = "fake-model"

        def __init__(self) -> None:
            created.append(True)

        def encode(self, texts):
            return [[1.0] for _text in texts]

    monkeypatch.setenv("MATCHING_EMBEDDING_BACKEND", "sentence-transformers")
    monkeypatch.setattr(
        "agnaradie_pricing.matching.vector_search.SentenceTransformerEmbedder",
        _FakeSentenceEmbedder,
    )

    embedder = make_default_embedder()

    assert isinstance(embedder, _FakeSentenceEmbedder)
    assert created == [True]
