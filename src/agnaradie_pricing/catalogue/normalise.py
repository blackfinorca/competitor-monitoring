"""Product identifier normalisation."""

from __future__ import annotations

import re
import unicodedata
from typing import Any


def fold_diacritics(text: str) -> str:
    """Strip combining diacritical marks via NFD decomposition."""
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


BRAND_ALIASES = {
    "KNIPEX GMBH": "KNIPEX",
    "KNIPEX GMBH & CO KG": "KNIPEX",
    "KNIPEX GMBH & CO. KG": "KNIPEX",
    "FEIN AG": "FEIN",
    "FEIN GMBH": "FEIN",
    "C. & E. FEIN GMBH": "FEIN",
    "WERA TOOLS": "WERA",
    "WERA GMBH": "WERA",
    "WIHA GMBH": "WIHA",
    "WIHA WERKZEUGE GMBH": "WIHA",
    "STANLEY TOOLS": "STANLEY",
    "STANLEY BLACK & DECKER": "STANLEY",
    "STANLEY BLACK&DECKER": "STANLEY",
    "IRWIN TOOLS": "IRWIN",
    "IRWIN INDUSTRIAL TOOLS": "IRWIN",
    "C.K TOOLS": "CK",
    "CK TOOLS": "CK",
    "C.K": "CK",
    "BAHCO AB": "BAHCO",
    "GEDORE TOOLS": "GEDORE",
    "BETA UTENSILI": "BETA",
    "STAHLWILLE GMBH": "STAHLWILLE",
}


def normalise_brand(value: str | None) -> str | None:
    if value is None:
        return None
    normalised = fold_diacritics(value.strip().upper())
    if not normalised:
        return None
    return BRAND_ALIASES.get(normalised, normalised)


def normalise_mpn(value: str | None) -> str | None:
    if value is None:
        return None
    normalised = re.sub(r"[\s.\-]+", "", fold_diacritics(value.strip().upper()))
    return normalised or None


def normalise_ean(value: Any) -> str | None:
    """Return a canonical EAN string, or None if value is not a valid EAN.

    Handles float artifacts (e.g. "4003773025559.0" → "4003773025559") and
    rejects non-digit strings and lengths outside the 8–14 digit EAN range.
    """
    if value is None:
        return None
    s = str(value).strip()
    if "." in s:
        integer_part, frac = s.split(".", 1)
        if frac.lstrip("0") == "":
            s = integer_part
    if not s.isdigit() or not (8 <= len(s) <= 14):
        return None
    return s

