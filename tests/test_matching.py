from agnaradie_pricing.catalogue.normalise import normalise_brand, normalise_mpn
from agnaradie_pricing.matching.deterministic import match_deterministic
from agnaradie_pricing.matching.llm_matcher import OpenAIClient, find_best_llm_match


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
    assert payload["max_completion_tokens"] == 512
    assert payload["reasoning_effort"] == "minimal"
    assert "max_tokens" not in payload
    assert "temperature" not in payload


def test_openai_client_retries_empty_reasoning_response_with_larger_budget() -> None:
    payloads: list[dict] = []
    responses = iter(
        [
            {
                "choices": [{"message": {"content": ""}, "finish_reason": "length"}],
                "usage": {"completion_tokens_details": {"reasoning_tokens": 512}},
            },
            {
                "choices": [{"message": {"content": "{}"}, "finish_reason": "stop"}],
                "usage": {"completion_tokens_details": {"reasoning_tokens": 32}},
            },
        ]
    )

    class FakeResponse:
        def __init__(self, body: dict) -> None:
            self._body = body

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._body

    class FakeHttp:
        def post(self, url, *, headers, json):
            del url, headers
            payloads.append(json)
            return FakeResponse(next(responses))

    client = OpenAIClient(api_key="test", model="gpt-5-nano")
    client._http = FakeHttp()  # type: ignore[assignment]

    assert client.complete("match this") == "{}"
    assert len(payloads) == 2
    assert payloads[0]["max_completion_tokens"] == 512
    assert payloads[1]["max_completion_tokens"] == 1024
    assert payloads[0]["reasoning_effort"] == "minimal"
    assert payloads[1]["reasoning_effort"] == "minimal"


def test_find_best_llm_match_normalises_names_in_prompt() -> None:
    captured: dict[str, str] = {}

    class FakeLLM:
        def complete(self, prompt: str) -> str:
            captured["prompt"] = prompt
            return '{"match_index": 1, "confidence": 0.91}'

    hit = find_best_llm_match(
        {
            "brand": "Wíha GmbH",
            "mpn": "261šo",
            "ean": None,
            "title": "Špeciálne kliešte 180 mm",
        },
        [
            {
                "id": 1,
                "brand": "Wiha GmbH",
                "mpn": "261so",
                "ean": None,
                "title": "Špeciálne kliešte 180 mm",
            }
        ],
        llm_client=FakeLLM(),
    )

    assert hit is not None
    prompt = captured["prompt"]
    assert "Brand: WIHA" in prompt
    assert "Title: specialne klieste 180 mm" in prompt
    assert "Špeciálne kliešte 180 mm" not in prompt


def test_find_best_llm_match_rejects_confidence_below_0_81() -> None:
    class FakeLLM:
        def complete(self, prompt: str) -> str:
            del prompt
            return '{"match_index": 1, "confidence": 0.80}'

    hit = find_best_llm_match(
        {"brand": "Wiha", "title": "specialne klieste 180 mm"},
        [{"id": 1, "brand": "Wiha", "title": "specialne klieste 180 mm"}],
        llm_client=FakeLLM(),
    )

    assert hit is None


def test_find_best_llm_match_keeps_confidence_above_threshold() -> None:
    class FakeLLM:
        def complete(self, prompt: str) -> str:
            del prompt
            return '{"match_index": 1, "confidence": 0.81}'

    hit = find_best_llm_match(
        {"brand": "Wiha", "title": "specialne klieste 180 mm"},
        [{"id": 1, "brand": "Wiha", "title": "specialne klieste 180 mm"}],
        llm_client=FakeLLM(),
    )

    assert hit is not None
    assert hit[1][1] == 0.81
