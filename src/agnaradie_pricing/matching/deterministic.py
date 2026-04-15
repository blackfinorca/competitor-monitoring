"""Deterministic product matching."""

from collections.abc import Mapping
from typing import Any

from agnaradie_pricing.catalogue.normalise import normalise_brand, normalise_mpn

MatchResult = tuple[str, float]


def match_deterministic(
    product: Mapping[str, Any], listing: Mapping[str, Any]
) -> MatchResult | None:
    product_ean = _clean_identifier(product.get("ean"))
    listing_ean = _clean_identifier(listing.get("ean"))
    if product_ean and listing_ean and product_ean == listing_ean:
        return ("exact_ean", 1.0)

    product_brand = normalise_brand(product.get("brand"))
    listing_brand = normalise_brand(listing.get("brand"))
    product_mpn = normalise_mpn(product.get("mpn"))
    listing_mpn = normalise_mpn(listing.get("mpn"))
    if (
        product_brand
        and listing_brand
        and product_mpn
        and listing_mpn
        and product_brand == listing_brand
        and product_mpn == listing_mpn
    ):
        return ("exact_mpn", 1.0)

    # Listing has a structured MPN but no brand field — high confidence match
    # (covers JS-rendered competitors like boukal.cz that don't surface brand)
    if product_mpn and listing_mpn and product_mpn == listing_mpn and not listing_brand:
        return ("mpn_no_brand", 0.90)

    return None


def _clean_identifier(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None

