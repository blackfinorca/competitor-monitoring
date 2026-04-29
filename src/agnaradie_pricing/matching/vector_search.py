"""Vector retrieval for narrowing ToolZone candidates before LLM verification."""

from __future__ import annotations

import hashlib
import logging
import math
import os
import re
from typing import Any, Protocol

logger = logging.getLogger(__name__)

_DEFAULT_EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
_TOKEN_RE = re.compile(r"[a-zA-Z0-9áäčďéíľĺňóôŕšťúýžÁÄČĎÉÍĽĹŇÓÔŔŠŤÚÝŽ]{2,}")


class TextEmbedder(Protocol):
    def encode(self, texts: list[str]) -> list[list[float]]:
        ...


class SentenceTransformerEmbedder:
    """Local sentence-transformers embedder.

    Uses a multilingual MiniLM model by default because product titles are
    commonly Slovak, Czech, German, and English.
    """

    def __init__(self, model_name: str | None = None) -> None:
        from sentence_transformers import SentenceTransformer

        self.model_name = model_name or os.getenv("MATCHING_EMBEDDING_MODEL", _DEFAULT_EMBEDDING_MODEL)
        self._model = SentenceTransformer(self.model_name)

    def encode(self, texts: list[str]) -> list[list[float]]:
        vectors = self._model.encode(
            texts,
            batch_size=128,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        return vectors.tolist()


class HashingTextEmbedder:
    """Small deterministic fallback embedder used when local ML models are unavailable."""

    def __init__(self, dimensions: int = 512) -> None:
        self.dimensions = dimensions

    def encode(self, texts: list[str]) -> list[list[float]]:
        return [_normalise(_hash_tokens(text, self.dimensions)) for text in texts]


def make_default_embedder() -> TextEmbedder:
    backend = os.getenv("MATCHING_EMBEDDING_BACKEND", "hashing").strip().lower()
    if backend in {"hashing", "hash", "fallback"}:
        return HashingTextEmbedder()

    if backend not in {"sentence-transformers", "sentence_transformers", "st", "auto"}:
        logger.warning(
            "Unknown MATCHING_EMBEDDING_BACKEND=%r; using hashing vector search",
            backend,
        )
        return HashingTextEmbedder()

    try:
        return SentenceTransformerEmbedder()
    except Exception as exc:
        logger.warning("Falling back to hashing vector search; sentence-transformers unavailable: %s", exc)
        return HashingTextEmbedder()


def describe_embedder(embedder: TextEmbedder) -> str:
    if isinstance(embedder, SentenceTransformerEmbedder):
        return f"sentence-transformers(model={embedder.model_name})"
    if isinstance(embedder, HashingTextEmbedder):
        return f"hashing-fallback(dimensions={embedder.dimensions})"
    return type(embedder).__name__


class TitleVectorIndex:
    def __init__(
        self,
        products: list[dict[str, Any]],
        *,
        embedder: TextEmbedder | None = None,
    ) -> None:
        self.products = products
        self._embedder = embedder or make_default_embedder()
        self.backend_description = describe_embedder(self._embedder)
        self._vectors = self._embedder.encode([_record_text(p) for p in products]) if products else []

    def search(self, listing: dict[str, Any], *, limit: int = 20) -> list[dict[str, Any]]:
        if not self.products or limit <= 0:
            return []
        query_text = _record_text(listing)
        if not query_text:
            return []
        query_vector = self._embedder.encode([query_text])[0]
        return self._search_vector(query_vector, limit=limit)

    def search_with_scores(
        self,
        listing: dict[str, Any],
        *,
        limit: int = 20,
    ) -> list[tuple[dict[str, Any], float]]:
        if not self.products or limit <= 0:
            return []
        query_text = _record_text(listing)
        if not query_text:
            return []
        query_vector = self._embedder.encode([query_text])[0]
        return self._search_vector_with_scores(query_vector, limit=limit)

    def search_many(
        self,
        listings: list[dict[str, Any]],
        *,
        limit: int = 20,
        batch_size: int = 256,
    ):
        if not self.products or limit <= 0:
            for _listing in listings:
                yield []
            return
        for start in range(0, len(listings), max(batch_size, 1)):
            batch = listings[start : start + max(batch_size, 1)]
            vectors = self._embedder.encode([_record_text(listing) for listing in batch])
            for vector in vectors:
                yield self._search_vector(vector, limit=limit)

    def search_many_with_scores(
        self,
        listings: list[dict[str, Any]],
        *,
        limit: int = 20,
        batch_size: int = 256,
    ):
        if not self.products or limit <= 0:
            for _listing in listings:
                yield []
            return
        for start in range(0, len(listings), max(batch_size, 1)):
            batch = listings[start : start + max(batch_size, 1)]
            vectors = self._embedder.encode([_record_text(listing) for listing in batch])
            for vector in vectors:
                yield self._search_vector_with_scores(vector, limit=limit)

    def _search_vector(self, query_vector: list[float], *, limit: int) -> list[dict[str, Any]]:
        return [
            product
            for product, _score in self._search_vector_with_scores(query_vector, limit=limit)
        ]

    def _search_vector_with_scores(
        self,
        query_vector: list[float],
        *,
        limit: int,
    ) -> list[tuple[dict[str, Any], float]]:
        scored = [
            (_dot(query_vector, product_vector), index)
            for index, product_vector in enumerate(self._vectors)
        ]
        scored.sort(key=lambda item: item[0], reverse=True)
        return [(self.products[index], score) for score, index in scored[:limit]]


def _record_text(record: dict[str, Any]) -> str:
    parts = [
        str(record.get("brand") or ""),
        str(record.get("mpn") or ""),
        str(record.get("ean") or ""),
        str(record.get("title") or ""),
    ]
    return " ".join(part for part in parts if part).strip()


def _hash_tokens(text: str, dimensions: int) -> list[float]:
    from agnaradie_pricing.catalogue.normalise import fold_diacritics
    vector = [0.0] * dimensions
    for token in _TOKEN_RE.findall(fold_diacritics(text.lower())):
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        bucket = int.from_bytes(digest[:4], "big") % dimensions
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        vector[bucket] += sign
    return vector


def _normalise(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector
    return [value / norm for value in vector]


def _dot(left: list[float], right: list[float]) -> float:
    return sum(a * b for a, b in zip(left, right))
