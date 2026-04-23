from agnaradie_pricing.matching.deterministic import match_deterministic
from agnaradie_pricing.matching.llm_matcher import OpenAIClient


def test_match_deterministic_prefers_ean_match() -> None:
    product = {"ean": "4003773075316", "brand": "Knipex", "mpn": "86-03-180"}
    listing = {"ean": "4003773075316", "brand": "Other", "mpn": "WRONG"}

    match = match_deterministic(product, listing)

    assert match == ("exact_ean", 1.0)


def test_match_deterministic_matches_normalised_brand_and_mpn() -> None:
    product = {"ean": None, "brand": "Knipex GmbH", "mpn": "86-03-180"}
    listing = {"ean": None, "brand": "knipex", "mpn": "8603180"}

    match = match_deterministic(product, listing)

    assert match == ("exact_mpn", 1.0)


def test_match_deterministic_returns_none_when_identifiers_differ() -> None:
    product = {"ean": "111", "brand": "Bosch", "mpn": "GBH 18V-21"}
    listing = {"ean": "222", "brand": "Bosch", "mpn": "GBH 18V-22"}

    assert match_deterministic(product, listing) is None


def test_openai_client_defaults_to_gpt_5_nano() -> None:
    assert OpenAIClient(api_key="test").model == "gpt-5-nano"
