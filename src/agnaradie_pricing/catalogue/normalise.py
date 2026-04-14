"""Product identifier normalisation."""

import re


BRAND_ALIASES = {
    "KNIPEX GMBH": "KNIPEX",
}


def normalise_brand(value: str | None) -> str | None:
    if value is None:
        return None
    normalised = value.strip().upper()
    if not normalised:
        return None
    return BRAND_ALIASES.get(normalised, normalised)


def normalise_mpn(value: str | None) -> str | None:
    if value is None:
        return None
    normalised = re.sub(r"[\s.\-]+", "", value.strip().upper())
    return normalised or None

