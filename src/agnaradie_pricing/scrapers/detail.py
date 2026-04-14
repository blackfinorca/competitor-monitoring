"""Detail-page enrichment for competitor scrapers.

After a search-by-MPN returns a listing, this module fetches the product's
own detail page and fills in any missing EAN / MPN / brand from its JSON-LD.

JSON-LD Product schema fields we extract (in priority order):
  EAN  : gtin13  →  gtin8  →  gtin  →  sku (if 8–14 digits, all numeric)
  MPN  : mpn  →  sku (if it looks like a manufacturer code, not all-numeric)
  Brand: brand.name  →  brand (string)

Usage
-----
    from agnaradie_pricing.scrapers.detail import enrich_from_detail_page

    listing = enrich_from_detail_page(listing, client, min_rps=1.0)
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from html.parser import HTMLParser

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing
from agnaradie_pricing.scrapers.http import polite_get

# EAN: 8, 12, or 13 digit all-numeric string
_EAN_RE = re.compile(r"^\d{8}(?:\d{4,5})?$")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def enrich_from_detail_page(
    listing: CompetitorListing,
    client: httpx.Client,
    *,
    min_rps: float = 1.0,
    referer: str | None = None,
) -> CompetitorListing:
    """Fetch listing.url and backfill missing EAN / MPN / brand from JSON-LD.

    Returns the original listing unchanged if:
    - All three fields already present.
    - No URL available.
    - Network/parse error (enrichment is best-effort).
    """
    if listing.ean and listing.mpn and listing.brand:
        return listing
    if not listing.url:
        return listing

    try:
        resp = polite_get(
            client,
            listing.url,
            min_rps=min_rps,
            referer=referer,
        )
        enriched = _extract_identifiers(resp.text)
    except Exception:
        return listing

    updates: dict = {}
    if not listing.ean and enriched.get("ean"):
        updates["ean"] = enriched["ean"]
        if not listing.competitor_sku:
            updates["competitor_sku"] = enriched["ean"]
    if not listing.mpn and enriched.get("mpn"):
        updates["mpn"] = enriched["mpn"]
        if not listing.competitor_sku and not updates.get("competitor_sku"):
            updates["competitor_sku"] = enriched["mpn"]
    if not listing.brand and enriched.get("brand"):
        updates["brand"] = enriched["brand"]

    if updates:
        return listing.__class__(**{**asdict(listing), **updates})
    return listing


# ---------------------------------------------------------------------------
# JSON-LD parsing
# ---------------------------------------------------------------------------

def _extract_identifiers(html: str) -> dict:
    """Return dict with keys ean, mpn, brand (all optional) from page JSON-LD."""
    parser = _JsonLdParser()
    parser.feed(html)

    for payload in parser.payloads:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        result = _identifiers_from_jsonld(data)
        if result:
            return result

    return {}


def _identifiers_from_jsonld(data) -> dict | None:
    """Recursively walk JSON-LD looking for the first Product node."""
    if isinstance(data, list):
        for item in data:
            r = _identifiers_from_jsonld(item)
            if r:
                return r
        return None

    if not isinstance(data, dict):
        return None

    if data.get("@type") == "Product":
        return _product_identifiers(data)

    # ItemList wrapping
    if data.get("@type") == "ItemList":
        for el in data.get("itemListElement", []):
            if isinstance(el, dict):
                r = _identifiers_from_jsonld(el.get("item"))
                if r:
                    return r

    # BreadcrumbList or other types — recurse into nested objects
    for v in data.values():
        if isinstance(v, (dict, list)):
            r = _identifiers_from_jsonld(v)
            if r:
                return r

    return None


def _product_identifiers(data: dict) -> dict:
    result: dict = {}

    # --- EAN ---
    ean = (
        _as_str(data.get("gtin13"))
        or _as_str(data.get("gtin8"))
        or _ean_from_gtin(data.get("gtin"))
        or _ean_from_sku(data.get("sku"))
    )
    if ean:
        result["ean"] = ean

    # --- MPN ---
    mpn = _as_str(data.get("mpn"))
    if not mpn:
        # Use sku as MPN only if it looks like a manufacturer code (contains letters
        # or dashes), not a plain numeric EAN
        sku = _as_str(data.get("sku"))
        if sku and not _EAN_RE.match(sku):
            mpn = sku
    if mpn:
        result["mpn"] = mpn

    # --- Brand ---
    brand_raw = data.get("brand")
    if isinstance(brand_raw, dict):
        brand = _as_str(brand_raw.get("name"))
    else:
        brand = _as_str(brand_raw)
    if brand:
        result["brand"] = brand

    return result


def _as_str(value) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _ean_from_gtin(value) -> str | None:
    """Accept a raw gtin field value if it looks like an EAN (8/12/13 digits)."""
    s = _as_str(value)
    if s and _EAN_RE.match(s):
        return s
    return None


def _ean_from_sku(value) -> str | None:
    """Accept sku as EAN only if it is all-numeric with 8–14 digits."""
    s = _as_str(value)
    if s and _EAN_RE.match(s):
        return s
    return None


# ---------------------------------------------------------------------------
# Minimal JSON-LD script tag extractor
# ---------------------------------------------------------------------------

class _JsonLdParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.payloads: list[str] = []
        self._active = False
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag == "script" and dict(attrs).get("type") == "application/ld+json":
            self._active = True
            self._chunks = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self._active:
            self.payloads.append("".join(self._chunks).strip())
            self._active = False
            self._chunks = []

    def handle_data(self, data: str) -> None:
        if self._active:
            self._chunks.append(data)
