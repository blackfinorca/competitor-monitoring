"""LLM-based fuzzy product matching.

Strategy
--------
Only runs when exact EAN matching fails.  The production matcher first narrows
ToolZone products with local vector search, then passes a short candidate list
to this verifier.

The older token pre-filter remains available for smaller in-process matching
calls; it requires brand compatibility and at least two shared title tokens.
Both paths call the LLM once per unmatched listing, passing the listing + up to
MAX_CANDIDATES candidates, and ask it to return the best match as JSON.

Default backend: OpenAI (gpt-5-nano or any compatible model).
The client is model-agnostic — anything that implements LLMClient works.

Confidence tiers
----------------
  llm_fuzzy  0.75–0.84   LLM matched on title similarity / specs

Threshold: results below MIN_CONFIDENCE are silently discarded.

Usage
-----
    from agnaradie_pricing.matching.llm_matcher import OpenAIClient
    from agnaradie_pricing.matching import match_product

    client = OpenAIClient(api_key="sk-...", model="gpt-5-nano")
    result = match_product(product, listing, llm_client=client)
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections import deque
from threading import Lock
from typing import Any, Protocol, runtime_checkable

import httpx

from agnaradie_pricing.catalogue.normalise import normalise_brand

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_MAX_CANDIDATES = 20         # products sent to LLM per listing
_MIN_CONFIDENCE = 0.75       # discard weaker LLM hits
_MIN_TOKEN_OVERLAP = 2       # pre-filter: minimum shared meaningful words
_STOP_WORDS = frozenset(
    "na pre s z a so pri od do mm cm kg set kus ks sada the and for with"
    " von fur mit fur zu und die der das".split()
)

# ---------------------------------------------------------------------------
# Rate limits (TPM, RPM) per model — 90 % safety margin applied on use
# Source: OpenAI platform limits (Tier 1)
# ---------------------------------------------------------------------------

_MODEL_LIMITS: dict[str, tuple[int, int]] = {
    "gpt-5.1":                 (500_000, 500),
    "gpt-5-mini":              (500_000, 500),
    "gpt-5-nano":              (200_000, 500),
    "gpt-4.1":                 (30_000,  500),
    "gpt-4.1-mini":            (200_000, 500),
    "gpt-4.1-nano":            (200_000, 500),
    "o3":                      (30_000,  500),
    "o4-mini":                 (200_000, 500),
    "gpt-4o":                  (30_000,  500),
    "gpt-4o-realtime-preview": (40_000,  200),
    # Legacy
    "gpt-4o-mini":             (200_000, 500),
}

_SAFETY_FACTOR = 0.90   # stay at 90 % of the published limit


# ---------------------------------------------------------------------------
# Sliding-window rate limiter (thread-safe)
# ---------------------------------------------------------------------------

class _RateLimiter:
    """Token-bucket limiter over a 60-second sliding window.

    Tracks (timestamp, token_count) entries.  Before each request it:
      1. Prunes entries older than 60 s.
      2. Checks whether adding this request would exceed RPM or TPM.
      3. If yes, sleeps until the oldest blocking entry falls out of the window.
    """

    def __init__(self, tpm: int, rpm: int) -> None:
        self._tpm = int(tpm * _SAFETY_FACTOR)
        self._rpm = int(rpm * _SAFETY_FACTOR)
        self._window: deque[tuple[float, int]] = deque()  # (ts, tokens)
        self._lock = Lock()

    def acquire(self, estimated_tokens: int) -> None:
        """Block until capacity is available, then record the request."""
        while True:
            with self._lock:
                now = time.monotonic()
                cutoff = now - 60.0

                # Prune old entries
                while self._window and self._window[0][0] < cutoff:
                    self._window.popleft()

                current_rpm = len(self._window)
                current_tpm = sum(t for _, t in self._window)

                rpm_ok = current_rpm + 1 <= self._rpm
                tpm_ok = current_tpm + estimated_tokens <= self._tpm

                if rpm_ok and tpm_ok:
                    self._window.append((now, estimated_tokens))
                    return

                # Calculate how long to wait
                if not rpm_ok and self._window:
                    # Wait until the oldest entry leaves the 60 s window
                    sleep_for = (self._window[0][0] + 60.0) - now + 0.05
                elif not tpm_ok and self._window:
                    # Wait until enough tokens clear
                    tokens_to_free = (current_tpm + estimated_tokens) - self._tpm
                    cleared = 0
                    sleep_for = 0.0
                    for ts, tok in self._window:
                        cleared += tok
                        if cleared >= tokens_to_free:
                            sleep_for = (ts + 60.0) - now + 0.05
                            break
                    if sleep_for <= 0:
                        sleep_for = 1.0
                else:
                    sleep_for = 1.0

            logger.debug("Rate limit: sleeping %.1f s", sleep_for)
            time.sleep(max(sleep_for, 0.05))

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
# OpenAI client
# ---------------------------------------------------------------------------

_OPENAI_BASE_URL = "https://api.openai.com/v1"

_DEFAULT_MODEL = "gpt-5-nano"


class OpenAIClient:
    """Thin httpx wrapper around the OpenAI chat completions endpoint.

    Automatically enforces the per-model TPM and RPM limits from
    _MODEL_LIMITS using a sliding 60-second window rate limiter.
    Unknown models fall back to the most conservative published limit
    (30,000 TPM / 500 RPM).
    """

    def __init__(
        self,
        api_key: str,
        model: str = _DEFAULT_MODEL,
        timeout: float = 30.0,
        max_tokens: int = 256,
    ) -> None:
        self._api_key = api_key
        self.model = model
        self._timeout = timeout
        self._max_tokens = max_tokens
        self._http = httpx.Client(timeout=timeout)

        tpm, rpm = _MODEL_LIMITS.get(model, (30_000, 500))
        self._rate_limiter = _RateLimiter(tpm=tpm, rpm=rpm)
        logger.debug(
            "OpenAIClient: model=%s  limits=TPM %d / RPM %d  (90%% safety)",
            model, int(tpm * _SAFETY_FACTOR), int(rpm * _SAFETY_FACTOR),
        )

    def complete(self, prompt: str) -> str:
        # Estimate tokens: ~1 token per 4 chars for the prompt + max response
        estimated_tokens = len(prompt) // 4 + self._max_tokens
        self._rate_limiter.acquire(estimated_tokens)

        # Reasoning-capable models use max_completion_tokens,
        # do not accept temperature, and should use reasoning_effort="low"
        # since product name matching needs no chain-of-thought.
        is_reasoning = self.model.startswith(("o1", "o3", "o4", "gpt-5"))
        payload: dict = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
        }
        if is_reasoning:
            payload["max_completion_tokens"] = self._max_tokens
            payload["reasoning_effort"] = "low"
        else:
            payload["max_tokens"] = self._max_tokens
            payload["temperature"] = 0.0

        response = self._http.post(
            f"{_OPENAI_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]

    def __repr__(self) -> str:
        return f"OpenAIClient(model={self.model!r})"


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
  "confidence": <float 0.0-1.0>
}}"""


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def _parse_response(
    raw: str, candidates: list[dict]
) -> tuple[dict, MatchResult] | None:
    if not raw or not raw.strip():
        # Happens when max_completion_tokens was exhausted by reasoning before output
        logger.warning("LLM returned empty response (token budget exhausted by reasoning)")
        return None
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.DOTALL)
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if not m:
            logger.warning("LLM returned non-JSON (%d chars): %s", len(raw), raw[:200])
            return None
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError:
            logger.warning("LLM JSON parse failed (%d chars): %s", len(raw), raw[:200])
            return None

    idx = data.get("match_index")
    confidence = data.get("confidence")

    if idx is None or confidence is None:
        return None

    try:
        idx = int(idx)
        confidence = float(confidence)
    except (TypeError, ValueError):
        return None

    if confidence < _MIN_CONFIDENCE:
        logger.debug("LLM match below threshold (%.2f)", confidence)
        return None

    if idx < 1 or idx > len(candidates):
        logger.warning("LLM returned out-of-range index %d for %d candidates", idx, len(candidates))
        return None

    matched_product = candidates[idx - 1]
    result: MatchResult = ("llm_fuzzy", round(min(confidence, 0.84), 2))
    logger.debug("LLM matched → product_id=%s  conf=%.2f", matched_product.get("id"), result[1])
    return (matched_product, result)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _title_tokens(text: str) -> frozenset[str]:
    """Lower-case words ≥3 chars, excluding stop words and pure digit strings."""
    words = re.findall(r"[a-záäčďéíľĺňóôŕšťúýž0-9]{3,}", text.lower())
    return frozenset(w for w in words if w not in _STOP_WORDS and not w.isdigit())
