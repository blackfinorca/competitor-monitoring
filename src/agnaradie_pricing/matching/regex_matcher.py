"""Regex-based product matching — layer 2 after exact EAN/MPN.

Handles the common case where a competitor listing has no structured MPN/EAN
field but the product code is embedded in the title string, e.g.:

    "Knipex 87-03-250 Cobra 250mm"   → MPN candidate: 8703250
    "KNIPEX 8703250 kliešte Cobra"   → MPN candidate: 8703250
    "Kliešte Knipex 87 03 250"       → MPN candidate: 8703250

Also handles EAN-13 barcodes that occasionally appear in title/description.

Confidence tiers
----------------
  regex_ean_title     0.93   EAN-13 found in title — highly specific
  regex_mpn_title     0.90   MPN found in title AND brand matches
  regex_mpn_no_brand  0.78   MPN found in title, brand absent/mismatch
"""

from __future__ import annotations

import re
from typing import Any

from agnaradie_pricing.catalogue.normalise import normalise_brand, normalise_mpn

MatchResult = tuple[str, float]

# ---------------------------------------------------------------------------
# Patterns for extracting codes from free-form text
# ---------------------------------------------------------------------------

# EAN-13: 13 consecutive digits NOT preceded/followed by another digit
_EAN13_RE = re.compile(r"(?<!\d)(\d{13})(?!\d)")

# Tool-style MPN patterns (cover most hand-tool manufacturers):
#  • 7–8 digit codes:               8703250, 97221240
#  • dashed 2-2-3 (Knipex-style):   87-03-250
#  • spaced 2-2-3:                  87 03 250
#  • 3+ alphanumeric with dash/dot: 840/1-Z, 900H-1, E7451656010
_MPN_PATTERNS: list[re.Pattern] = [
    # dashed / spaced 2-2-3 (Knipex, Klein, …)
    re.compile(r"\b(\d{2})[-\s](\d{2})[-\s](\d{3})\b"),
    # plain 7–8 digit numeric code
    re.compile(r"(?<!\d)(\d{7,8})(?!\d)"),
    # 6-digit numeric (some compact codes)
    re.compile(r"(?<!\d)(\d{6})(?!\d)"),
    # alphanumeric with separator (Wera 840/1 Z, Bahco 2444, …)
    re.compile(r"\b([A-Z0-9]{2,}[-/][A-Z0-9]{1,}(?:[-/][A-Z0-9]+)*)\b"),
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def match_regex(
    product: dict[str, Any],
    listing: dict[str, Any],
) -> MatchResult | None:
    """Try to match product ↔ listing using codes extracted from title fields.

    Only runs when structured fields (mpn/ean) are absent on the listing side,
    or when the product MPN is set and we can search for it in the listing title.
    Returns a (match_type, confidence) tuple or None.
    """
    # --- EAN from listing title ---
    product_ean = _clean(product.get("ean"))
    listing_ean = _clean(listing.get("ean"))
    if not listing_ean:
        for candidate in _extract_eans(listing.get("title") or ""):
            if product_ean and candidate == product_ean:
                return ("regex_ean_title", 0.93)

    # --- MPN from listing title (when listing.mpn is absent) ---
    product_mpn_norm = normalise_mpn(product.get("mpn"))
    listing_mpn_norm = normalise_mpn(listing.get("mpn"))

    if product_mpn_norm and not listing_mpn_norm:
        candidates = _extract_mpn_candidates(listing.get("title") or "")
        for candidate_norm in candidates:
            if candidate_norm == product_mpn_norm:
                # Check brand agreement
                pb = normalise_brand(product.get("brand"))
                lb = normalise_brand(listing.get("brand"))
                if pb and lb and pb == lb:
                    return ("regex_mpn_title", 0.90)
                # Brand absent on listing side is common — still useful match
                if not lb or not pb:
                    return ("regex_mpn_no_brand", 0.78)
                # Brand present but different → weaker signal
                return ("regex_mpn_no_brand", 0.72)

    # --- MPN from product title (when product.mpn is absent) ---
    listing_mpn_norm = normalise_mpn(listing.get("mpn"))
    if listing_mpn_norm and not normalise_mpn(product.get("mpn")):
        candidates = _extract_mpn_candidates(product.get("title") or "")
        for candidate_norm in candidates:
            if candidate_norm == listing_mpn_norm:
                pb = normalise_brand(product.get("brand"))
                lb = normalise_brand(listing.get("brand"))
                if pb and lb and pb == lb:
                    return ("regex_mpn_title", 0.90)
                if not lb or not pb:
                    return ("regex_mpn_no_brand", 0.78)
                return ("regex_mpn_no_brand", 0.72)

    return None


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _extract_eans(text: str) -> list[str]:
    return _EAN13_RE.findall(text)


def _extract_mpn_candidates(text: str) -> list[str]:
    """Return normalised MPN candidates extracted from a free-form string."""
    upper = text.upper()
    candidates: list[str] = []
    seen: set[str] = set()

    for pattern in _MPN_PATTERNS:
        for match in pattern.finditer(upper):
            # Join all capturing groups (e.g. the three groups of 2-2-3 pattern)
            raw = "".join(g for g in match.groups() if g is not None)
            if not raw:
                raw = match.group(0)
            normed = normalise_mpn(raw)
            if normed and normed not in seen:
                seen.add(normed)
                candidates.append(normed)

    return candidates


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None
