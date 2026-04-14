"""LLM-based fuzzy product matching — layer 3 after exact and regex.

Strategy
--------
Only runs when layers 1 & 2 (exact EAN/MPN and regex title extraction) both
fail.  Before calling the API we do a cheap pre-filter to build a short
candidate list:

  1. Brand agreement  — both brands present and equal (normalised),
                        OR at least one side has no brand (unknown).
  2. Title token overlap ≥ 2 — at least 2 meaningful words in common after
                        stop-word removal.  Eliminates clearly unrelated products.

We then call the LLM once per unmatched listing, passing the listing + up to
MAX_CANDIDATES candidates, and ask it to return the best match as JSON.

Default model: Qwen (Alibaba DashScope OpenAI-compatible endpoint).
The client is model-agnostic — anything that implements LLMClient works.

Confidence tiers
----------------
  llm_fuzzy  0.75–0.84   LLM matched on title similarity / specs

Threshold: results below MIN_CONFIDENCE are silently discarded.

Usage
-----
    from agnaradie_pricing.matching.llm_matcher import QwenClient
    from agnaradie_pricing.matching import match_product

    client = QwenClient(api_key="sk-...", model="qwen-plus")
    result = match_product(product, listing, llm_client=client)
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Protocol, runtime_checkable

import httpx

from agnaradie_pricing.catalogue.normalise import normalise_brand

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_MAX_CANDIDATES = 5          # products sent to LLM per listing
_MIN_CONFIDENCE = 0.75       # discard weaker LLM hits
_MIN_TOKEN_OVERLAP = 2       # pre-filter: minimum shared meaningful words
_STOP_WORDS = frozenset(
    "na pre s z a so pri od do mm cm kg set kus ks sada the and for with"
    " von fur mit fur zu und die der das".split()
)

MatchResult = tuple[str, float]


# ---------------------------------------------------------------------------
# LLM client protocol — any backend that can complete a prompt
# ---------------------------------------------------------------------------

@runtime_checkable
class LLMClient(Protocol):
    def complete(self, prompt: str) -> str:
        """Send prompt, return the model's text response."""
        ...


# ---------------------------------------------------------------------------
# Qwen (Alibaba DashScope) client
# ---------------------------------------------------------------------------

_QWEN_BASE_URL = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"


class QwenClient:
    """Thin httpx wrapper around the Qwen OpenAI-compatible chat endpoint."""

    def __init__(
        self,
        api_key: str,
        model: str = "qwen-plus",
        timeout: float = 30.0,
        max_tokens: int = 256,
    ) -> None:
        self._api_key = api_key
        self.model = model
        self._timeout = timeout
        self._max_tokens = max_tokens
        self._http = httpx.Client(timeout=timeout)

    def complete(self, prompt: str) -> str:
        payload: dict = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": self._max_tokens,
            "temperature": 0.0,      # deterministic — consistent matches
            # Disable chain-of-thought thinking for Qwen3/3.5 models.
            # Without this the model emits <think>…</think> tokens before the
            # answer, which wastes tokens and slows down the response.
            "enable_thinking": False,
        }
        response = self._http.post(
            f"{_QWEN_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]

    def __repr__(self) -> str:
        return f"QwenClient(model={self.model!r})"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_best_llm_match(
    listing: dict[str, Any],
    candidates: list[dict[str, Any]],
    *,
    llm_client: LLMClient,
) -> tuple[dict[str, Any], MatchResult] | None:
    """Send listing + candidates to the LLM; return (matched_product, result) or None.

    Parameters
    ----------
    listing     Competitor listing dict: brand, mpn, ean, title.
    candidates  Pre-filtered catalogue products: id, brand, mpn, ean, title.
    llm_client  Any object with a .complete(prompt: str) -> str method.

    Returns
    -------
    (product_dict, (match_type, confidence))  or  None if no confident match.
    """
    if not candidates:
        return None

    prompt = _build_prompt(listing, candidates)
    try:
        raw = llm_client.complete(prompt)
    except Exception as exc:
        logger.warning("LLM call failed (%s): %s", type(llm_client).__name__, exc)
        return None

    return _parse_response(raw, candidates)


def pre_filter_candidates(
    listing: dict[str, Any],
    products: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return products that pass brand + token-overlap pre-filters (≤ MAX_CANDIDATES)."""
    listing_brand = normalise_brand(listing.get("brand"))
    listing_tokens = _title_tokens(listing.get("title") or "")

    scored: list[tuple[int, dict]] = []
    for product in products:
        product_brand = normalise_brand(product.get("brand"))
        if listing_brand and product_brand and listing_brand != product_brand:
            continue

        overlap = len(listing_tokens & _title_tokens(product.get("title") or ""))
        if overlap < _MIN_TOKEN_OVERLAP:
            continue

        scored.append((overlap, product))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored[:_MAX_CANDIDATES]]


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------

def _build_prompt(listing: dict, candidates: list[dict]) -> str:
    def _fmt(d: dict) -> str:
        parts = []
        if d.get("brand"):
            parts.append(f"Brand: {d['brand']}")
        if d.get("mpn"):
            parts.append(f"MPN: {d['mpn']}")
        if d.get("ean"):
            parts.append(f"EAN: {d['ean']}")
        parts.append(f"Title: {d.get('title', '')}")
        return "\n  ".join(parts)

    cand_block = "\n\n".join(
        f"Candidate {i + 1} (id={c.get('id')}):\n  {_fmt(c)}"
        for i, c in enumerate(candidates)
    )

    return f"""You are a product-matching assistant for a Slovak hardware store pricing system.
Products are hand tools sold in Slovakia and Central Europe — titles may be in Slovak, Czech, German, or English.

Competitor listing to match:
  {_fmt(listing)}

Catalogue candidates:
{cand_block}

Task: decide which candidate (if any) is the SAME physical product as the listing.
Same product = identical model, same size/specification variant.
Different size or variant of the same model line is NOT a match.

Return JSON only:
{{
  "match_index": <1-based index of best match, or null if no confident match>,
  "confidence": <float 0.0-1.0>,
  "reasoning": "<one sentence>"
}}"""


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def _parse_response(
    raw: str, candidates: list[dict]
) -> tuple[dict, MatchResult] | None:
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.DOTALL)
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if not m:
            logger.warning("LLM returned non-JSON: %s", raw[:200])
            return None
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError:
            logger.warning("LLM JSON parse failed: %s", raw[:200])
            return None

    idx = data.get("match_index")
    confidence = data.get("confidence")
    reasoning = data.get("reasoning", "")

    if idx is None or confidence is None:
        return None

    try:
        idx = int(idx)
        confidence = float(confidence)
    except (TypeError, ValueError):
        return None

    if confidence < _MIN_CONFIDENCE:
        logger.debug("LLM match below threshold (%.2f): %s", confidence, reasoning)
        return None

    if idx < 1 or idx > len(candidates):
        logger.warning("LLM returned out-of-range index %d for %d candidates", idx, len(candidates))
        return None

    matched_product = candidates[idx - 1]
    result: MatchResult = ("llm_fuzzy", round(min(confidence, 0.84), 2))
    logger.debug(
        "LLM matched → product_id=%s  conf=%.2f  reason: %s",
        matched_product.get("id"), result[1], reasoning,
    )
    return (matched_product, result)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _title_tokens(text: str) -> frozenset[str]:
    """Lower-case words ≥3 chars, excluding stop words and pure digit strings."""
    words = re.findall(r"[a-záäčďéíľĺňóôŕšťúýž0-9]{3,}", text.lower())
    return frozenset(w for w in words if w not in _STOP_WORDS and not w.isdigit())
