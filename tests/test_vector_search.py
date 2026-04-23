from agnaradie_pricing.matching.vector_search import HashingTextEmbedder, TitleVectorIndex


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
