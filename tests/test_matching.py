from agnaradie_pricing.catalogue.normalise import normalise_brand, normalise_mpn
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


def test_normalise_brand_folds_diacritics() -> None:
    assert normalise_brand("Wíha GmbH") == normalise_brand("Wiha GmbH")


def test_normalise_mpn_folds_diacritics() -> None:
    # Simulate an encoding artefact introducing a diacritic into a product code
    assert normalise_mpn("86-03-180") == normalise_mpn("86-03-180")
    assert normalise_mpn("261šo") == normalise_mpn("261so")


def test_match_deterministic_matches_despite_diacritics() -> None:
    product = {"ean": None, "brand": "Wiha GmbH", "mpn": "26150"}
    listing = {"ean": None, "brand": "Wíha GmbH", "mpn": "26150"}
    assert match_deterministic(product, listing) == ("exact_mpn", 1.0)


def test_openai_client_defaults_to_gpt_5_nano() -> None:
    assert OpenAIClient(api_key="test").model == "gpt-5-nano"


def test_openai_client_uses_gpt_5_chat_completion_payload() -> None:
    captured: dict = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"choices": [{"message": {"content": "{}"}}]}

    class FakeHttp:
        def post(self, url, *, headers, json):
            captured["url"] = url
            captured["headers"] = headers
            captured["json"] = json
            return FakeResponse()

    client = OpenAIClient(api_key="test", model="gpt-5-nano")
    client._http = FakeHttp()  # type: ignore[assignment]

    assert client.complete("match this") == "{}"
    payload = captured["json"]
    assert payload["model"] == "gpt-5-nano"
    assert payload["max_completion_tokens"] == 256
    assert payload["reasoning_effort"] == "low"
    assert "max_tokens" not in payload
    assert "temperature" not in payload
