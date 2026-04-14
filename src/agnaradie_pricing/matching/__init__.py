"""Product matching pipeline.

Layers (tried in order, first hit wins):
  1. exact_ean          — EAN barcode exact match              confidence 1.00
  2. exact_mpn          — brand + MPN exact match              confidence 1.00
  3. regex_ean_title    — EAN-13 extracted from title          confidence 0.93
  4. regex_mpn_title    — MPN from title + brand match         confidence 0.90
  5. regex_mpn_no_brand — MPN from title, brand absent         confidence 0.72–0.78
  6. llm_fuzzy          — Claude title/spec similarity         confidence 0.75–0.84
                          (only when llm_client is supplied)

Single usage
------------
    from agnaradie_pricing.matching import match_product

    result = match_product(product, listing)                          # layers 1–5
    result = match_product(product, listing, llm_client=Anthropic())  # + layer 6

Bulk LLM usage (daily_match.py)
--------------------------------
    from agnaradie_pricing.matching import match_product_bulk

    matches = match_product_bulk(unmatched, all_products, llm_client=client)
    # returns {listing_id: (matched_product, (match_type, confidence))}
"""

from __future__ import annotations

from typing import Any

from agnaradie_pricing.matching.deterministic import match_deterministic
from agnaradie_pricing.matching.regex_matcher import match_regex
from agnaradie_pricing.matching.llm_matcher import find_best_llm_match, pre_filter_candidates

MatchResult = tuple[str, float]


def match_product(
    product: dict[str, Any],
    listing: dict[str, Any],
    *,
    llm_client=None,
) -> MatchResult | None:
    """Run matching layers 1–5 (and optionally 6) for a single product/listing pair.

    Parameters
    ----------
    product     Catalogue product: id, brand, mpn, ean, title.
    listing     Competitor listing: brand, mpn, ean, title.
    llm_client  anthropic.Anthropic() instance; enables layer 6 when set.
    """
    result = match_deterministic(product, listing) or match_regex(product, listing)
    if result:
        return result

    if llm_client is not None:
        candidates = pre_filter_candidates(listing, [product])
        if candidates:
            hit = find_best_llm_match(listing, candidates, llm_client=llm_client)
            if hit:
                _product, match_result = hit
                return match_result

    return None


def match_product_bulk(
    unmatched_listings: list[dict[str, Any]],
    products: list[dict[str, Any]],
    *,
    llm_client,
    on_match=None,
) -> dict[int, tuple[dict, MatchResult]]:
    """LLM bulk matcher: one API call per listing (not per product pair).

    Only runs the LLM layer.  Run layers 1–5 first with match_product() and
    pass only the listings that still have no match here.

    Parameters
    ----------
    unmatched_listings  Listing dicts with keys: id, brand, mpn, ean, title.
    products            Full catalogue product list.
    llm_client          Initialised anthropic.Anthropic() instance.
    on_match            Optional callback(listing_id, product, result) per hit.

    Returns
    -------
    {listing_id: (matched_product_dict, (match_type, confidence))}
    """
    results: dict[int, tuple[dict, MatchResult]] = {}

    for listing in unmatched_listings:
        candidates = pre_filter_candidates(listing, products)
        if not candidates:
            continue

        hit = find_best_llm_match(listing, candidates, llm_client=llm_client)
        if hit is None:
            continue

        matched_product, match_result = hit
        listing_id = listing["id"]
        results[listing_id] = (matched_product, match_result)

        if on_match:
            on_match(listing_id, matched_product, match_result)

    return results
