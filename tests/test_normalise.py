from agnaradie_pricing.catalogue.normalise import normalise_brand, normalise_mpn


def test_normalise_mpn_strips_common_separators_and_uppercases() -> None:
    assert normalise_mpn("  gbh-18.v 21 ") == "GBH18V21"


def test_normalise_brand_applies_known_aliases() -> None:
    assert normalise_brand(" Knipex GmbH ") == "KNIPEX"


def test_normalise_brand_handles_empty_values() -> None:
    assert normalise_brand(None) is None

