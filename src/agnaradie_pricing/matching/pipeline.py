"""Unified matching pipeline.

Matches every competitor listing to a Product using a layered strategy:

  1. exact_ean          EAN identical on both sides          confidence 1.0  → approved
  2. exact_mpn          brand + MPN identical                confidence 1.0  → approved
  3. mpn_no_brand       MPN identical, listing has no brand  confidence 0.9  → approved
  4. regex_ean_title    EAN-13 found in listing title        confidence 1.0  → approved
  5. regex_mpn_title    MPN found in title + brand agrees    confidence 0.9  → approved
  6. regex_mpn_no_brand MPN found in title, brand absent     confidence 0.9  → approved
  7. vector_llm         cosine ≥ 0.9, LLM confirms          similarity ≥ 0.96 → approved
                                                             similarity < 0.96 → pending
  8. derived_ean        listing has EAN, no product exists   confidence 1.0  → approved
                        → creates a derived Product row automatically

Results are upserted into product_matches (one row per listing_id).
Run with force=True to re-match already-matched listings.
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import numpy as np
from sqlalchemy import select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from agnaradie_pricing.catalogue.normalise import normalise_brand, normalise_ean, normalise_mpn
from agnaradie_pricing.db.models import Product, ProductMatch
from agnaradie_pricing.matching.deterministic import match_deterministic
from agnaradie_pricing.matching.regex_matcher import match_regex
from agnaradie_pricing.matching.llm_matcher import find_best_llm_match
from agnaradie_pricing.matching.vector_search import TitleVectorIndex

logger = logging.getLogger(__name__)

_VECTOR_SIM_THRESHOLD = 0.90          # minimum cosine to send to LLM
_SIMILARITY_AUTO_APPROVE = Decimal("0.96")  # auto-approve threshold
_TOP_K = 5
_PROGRESS_EVERY = 50


def _say(msg: str) -> None:
    print(msg, flush=True)
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Product index helpers
# ---------------------------------------------------------------------------

def _load_products(session) -> list[dict]:
    rows = session.execute(
        text("SELECT id, sku, brand, mpn, ean, title FROM products ORDER BY id")
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def _build_ean_index(products: list[dict]) -> dict[str, int]:
    idx: dict[str, int] = {}
    for p in products:
        ean = normalise_ean(p.get("ean"))
        if ean:
            idx[ean] = p["id"]
    return idx


def _build_mpn_index(products: list[dict]) -> dict[tuple[str, str], int]:
    idx: dict[tuple[str, str], int] = {}
    for p in products:
        brand = normalise_brand(p.get("brand"))
        mpn = normalise_mpn(p.get("mpn"))
        if brand and mpn:
            idx[(brand, mpn)] = p["id"]
    return idx


def _build_mpn_nobrand_index(products: list[dict]) -> dict[str, int]:
    idx: dict[str, int] = {}
    for p in products:
        mpn = normalise_mpn(p.get("mpn"))
        if mpn:
            idx[mpn] = p["id"]
    return idx


# ---------------------------------------------------------------------------
# Derived product creation
# ---------------------------------------------------------------------------

def _get_or_create_product(session, *, ean: str | None, brand: str | None,
                            mpn: str | None, title: str) -> int:
    """Return product_id for (ean or brand+mpn), creating a derived row if needed."""
    if ean:
        existing = session.execute(
            select(Product.id).where(Product.ean == ean)
        ).scalar_one_or_none()
        if existing:
            return existing

    if brand and mpn:
        existing = session.execute(
            select(Product.id).where(
                Product.brand == brand, Product.mpn == mpn
            )
        ).scalar_one_or_none()
        if existing:
            return existing

    product = Product(
        sku=None,
        brand=brand,
        mpn=mpn,
        ean=ean,
        title=title,
        source="derived",
    )
    session.add(product)
    session.flush()
    return product.id


# ---------------------------------------------------------------------------
# Upsert helper
# ---------------------------------------------------------------------------

def _upsert_match(session, *, listing_id: int, product_id: int, match_type: str,
                  confidence: Decimal, similarity: Decimal | None = None,
                  llm_confidence: Decimal | None = None, status: str = "approved") -> None:
    """Insert or update a product_matches row keyed on listing_id."""
    values = dict(
        listing_id=listing_id,
        product_id=product_id,
        match_type=match_type,
        confidence=confidence,
        similarity=similarity,
        llm_confidence=llm_confidence,
        status=status,
        created_at=datetime.now(UTC),
    )
    try:
        stmt = (
            pg_insert(ProductMatch)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["listing_id"],
                set_={k: v for k, v in values.items() if k != "listing_id"},
            )
        )
    except Exception:
        stmt = (
            sqlite_insert(ProductMatch)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["listing_id"],
                set_={k: v for k, v in values.items() if k != "listing_id"},
            )
        )
    session.execute(stmt)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_matching(session, *, llm_client=None, force: bool = False) -> dict[str, int]:
    """Match all unmatched competitor listings to products.

    Parameters
    ----------
    session     Active SQLAlchemy session.
    llm_client  Optional OpenAIClient; enables vector+LLM phase.
    force       Re-match listings that already have a product_matches row.

    Returns
    -------
    Counts dict: exact, regex, vector_llm, derived, pending, skipped.
    """
    t0 = time.monotonic()

    # Load all listings
    listing_rows = session.execute(
        text(
            """
            SELECT id, competitor_id, brand, mpn, ean, title
            FROM   competitor_listings
            ORDER  BY id
            """
        )
    ).fetchall()
    all_listings = [dict(r._mapping) for r in listing_rows]

    # Which listings already have a match?
    already_matched: set[int] = set()
    if not force:
        matched_rows = session.execute(
            text("SELECT listing_id FROM product_matches")
        ).fetchall()
        already_matched = {r.listing_id for r in matched_rows}

    listings = [li for li in all_listings if li["id"] not in already_matched]

    products = _load_products(session)
    ean_idx = _build_ean_index(products)
    mpn_idx = _build_mpn_index(products)
    mpn_nb_idx = _build_mpn_nobrand_index(products)
    products_by_id = {p["id"]: p for p in products}

    counts: dict[str, int] = dict(
        exact=0, regex=0, vector_llm=0, derived=0, pending=0, skipped=0
    )

    _say(
        f"[pipeline] listings total={len(all_listings)}"
        f"  to_match={len(listings)}"
        f"  already_matched={len(already_matched)}"
        f"  products={len(products)}"
        f"  llm={'yes' if llm_client else 'no'}"
    )

    unmatched: list[dict] = []

    # ------------------------------------------------------------------
    # Phase 1 — deterministic + regex (fast, no ML)
    # ------------------------------------------------------------------
    for li in listings:
        listing_ean = normalise_ean(li.get("ean"))
        listing_brand = normalise_brand(li.get("brand"))
        listing_mpn = normalise_mpn(li.get("mpn"))

        product_id: int | None = None
        match_type: str = ""
        confidence: Decimal = Decimal("0")

        # 1. exact_ean
        if listing_ean and listing_ean in ean_idx:
            product_id = ean_idx[listing_ean]
            match_type, confidence = "exact_ean", Decimal("1.0")

        # 2. exact_mpn
        elif listing_brand and listing_mpn and (listing_brand, listing_mpn) in mpn_idx:
            product_id = mpn_idx[(listing_brand, listing_mpn)]
            match_type, confidence = "exact_mpn", Decimal("1.0")

        # 3. mpn_no_brand
        elif listing_mpn and not listing_brand and listing_mpn in mpn_nb_idx:
            product_id = mpn_nb_idx[listing_mpn]
            match_type, confidence = "mpn_no_brand", Decimal("0.9")

        else:
            # Try regex against all products (use existing regex_matcher)
            listing_dict = {k: li.get(k) for k in ("brand", "mpn", "ean", "title")}
            best_result: tuple[str, float] | None = None
            best_pid: int | None = None
            for p in products:
                result = match_regex(p, listing_dict)
                if result:
                    mt, conf = result
                    if best_result is None or conf > best_result[1]:
                        best_result = (mt, conf)
                        best_pid = p["id"]
            if best_result and best_pid is not None:
                match_type, conf_f = best_result
                product_id = best_pid
                confidence = Decimal(str(round(conf_f, 2)))

        if product_id is not None:
            _upsert_match(session, listing_id=li["id"], product_id=product_id,
                          match_type=match_type, confidence=confidence)
            counts["exact" if "exact" in match_type or "ean" in match_type else "regex"] += 1
        else:
            unmatched.append(li)

    session.commit()
    _say(
        f"[pipeline] phase-1 done  exact/regex={counts['exact']+counts['regex']}"
        f"  unmatched={len(unmatched)}"
        f"  elapsed={time.monotonic()-t0:.1f}s"
    )

    if not unmatched:
        return counts

    # ------------------------------------------------------------------
    # Phase 2 — vector + LLM (brand-bucketed)
    # ------------------------------------------------------------------
    if llm_client is None:
        # No LLM: derived-product fallback for EAN-bearing orphans
        for li in unmatched:
            ean = normalise_ean(li.get("ean"))
            if ean:
                pid = _get_or_create_product(
                    session, ean=ean,
                    brand=normalise_brand(li.get("brand")),
                    mpn=normalise_mpn(li.get("mpn")),
                    title=li.get("title") or "",
                )
                _upsert_match(session, listing_id=li["id"], product_id=pid,
                              match_type="derived_ean", confidence=Decimal("1.0"))
                counts["derived"] += 1
            else:
                counts["skipped"] += 1
        session.commit()
        return counts

    # Brand-bucket orphans
    brand_pools: dict[str, list[dict]] = {}
    for li in unmatched:
        b = normalise_brand(li.get("brand")) or "__no_brand__"
        brand_pools.setdefault(b, []).append(li)

    # Extend brand pools with all products in that brand for vector comparison
    product_brand_pools: dict[str, list[dict]] = {}
    for p in products:
        b = normalise_brand(p.get("brand")) or "__no_brand__"
        product_brand_pools.setdefault(b, []).append(p)

    total_orphans = len(unmatched)
    done = 0

    for brand, orphans in brand_pools.items():
        pool = product_brand_pools.get(brand, []) + orphans
        if len(pool) < 2:
            # Nothing to compare against — fall through to derived
            for li in orphans:
                ean = normalise_ean(li.get("ean"))
                if ean:
                    pid = _get_or_create_product(
                        session, ean=ean,
                        brand=normalise_brand(li.get("brand")),
                        mpn=normalise_mpn(li.get("mpn")),
                        title=li.get("title") or "",
                    )
                    _upsert_match(session, listing_id=li["id"], product_id=pid,
                                  match_type="derived_ean", confidence=Decimal("1.0"))
                    counts["derived"] += 1
                else:
                    counts["skipped"] += 1
            continue

        index = TitleVectorIndex(pool)
        if not index._vectors:
            counts["skipped"] += len(orphans)
            continue

        matrix = np.asarray(index._vectors, dtype=np.float32)
        pool_ids = [item["id"] for item in pool]
        id_to_row = {item["id"]: idx for idx, item in enumerate(pool)}

        for li in orphans:
            if li["id"] not in id_to_row:
                counts["skipped"] += 1
                continue

            row_idx = id_to_row[li["id"]]
            scores = matrix @ matrix[row_idx]
            scores[row_idx] = -1.0

            k = min(_TOP_K, len(scores) - 1)
            if k <= 0:
                counts["skipped"] += 1
                continue

            top_idx = np.argpartition(-scores, k - 1)[:k]
            top_idx = top_idx[np.argsort(-scores[top_idx])]
            candidates = [
                (pool[j], float(scores[j]))
                for j in top_idx
                if scores[j] >= _VECTOR_SIM_THRESHOLD
            ]
            if not candidates:
                ean = normalise_ean(li.get("ean"))
                if ean:
                    pid = _get_or_create_product(
                        session, ean=ean,
                        brand=normalise_brand(li.get("brand")),
                        mpn=normalise_mpn(li.get("mpn")),
                        title=li.get("title") or "",
                    )
                    _upsert_match(session, listing_id=li["id"], product_id=pid,
                                  match_type="derived_ean", confidence=Decimal("1.0"))
                    counts["derived"] += 1
                else:
                    counts["skipped"] += 1
                continue

            candidate_items = [c for c, _ in candidates]
            sim_lookup = {c["id"]: s for c, s in candidates}

            hit = find_best_llm_match(li, candidate_items, llm_client=llm_client)
            if hit is None:
                counts["skipped"] += 1
                continue

            matched_item, (_mt, llm_conf) = hit
            similarity = Decimal(str(round(sim_lookup.get(matched_item["id"], 0.0), 3)))
            llm_conf_d = Decimal(str(round(llm_conf, 2)))
            status = "approved" if similarity >= _SIMILARITY_AUTO_APPROVE else "pending"

            # Find or create the product for the matched item
            if matched_item.get("id") in products_by_id:
                # Matched to an existing product
                product_id = matched_item["id"]
            else:
                # Matched to another orphan listing — create shared derived product
                product_id = _get_or_create_product(
                    session,
                    ean=normalise_ean(matched_item.get("ean")),
                    brand=normalise_brand(matched_item.get("brand")),
                    mpn=normalise_mpn(matched_item.get("mpn")),
                    title=matched_item.get("title") or "",
                )
                # Also match the candidate to the new product if not yet matched
                existing = session.execute(
                    select(ProductMatch.id).where(
                        ProductMatch.listing_id == matched_item["id"]
                    )
                ).scalar_one_or_none()
                if existing is None:
                    _upsert_match(
                        session, listing_id=matched_item["id"],
                        product_id=product_id,
                        match_type="vector_llm",
                        confidence=llm_conf_d,
                        similarity=similarity,
                        llm_confidence=llm_conf_d,
                        status=status,
                    )

            _upsert_match(
                session,
                listing_id=li["id"],
                product_id=product_id,
                match_type="vector_llm",
                confidence=llm_conf_d,
                similarity=similarity,
                llm_confidence=llm_conf_d,
                status=status,
            )

            if status == "approved":
                counts["vector_llm"] += 1
            else:
                counts["pending"] += 1

            done += 1
            if done % _PROGRESS_EVERY == 0:
                _say(
                    f"[pipeline] phase-2  {done}/{total_orphans}"
                    f"  approved={counts['vector_llm']}"
                    f"  pending={counts['pending']}"
                    f"  elapsed={time.monotonic()-t0:.1f}s"
                )

        session.commit()

    # ------------------------------------------------------------------
    # Phase 3 — derived products for remaining EAN-bearing orphans
    # ------------------------------------------------------------------
    still_unmatched_ids: set[int] = {li["id"] for li in unmatched}
    matched_now = session.execute(
        text("SELECT listing_id FROM product_matches WHERE listing_id IN :ids"),
        {"ids": tuple(still_unmatched_ids) or (0,)},
    ).fetchall()
    matched_now_ids = {r.listing_id for r in matched_now}

    for li in unmatched:
        if li["id"] in matched_now_ids:
            continue
        ean = normalise_ean(li.get("ean"))
        if ean:
            pid = _get_or_create_product(
                session, ean=ean,
                brand=normalise_brand(li.get("brand")),
                mpn=normalise_mpn(li.get("mpn")),
                title=li.get("title") or "",
            )
            _upsert_match(session, listing_id=li["id"], product_id=pid,
                          match_type="derived_ean", confidence=Decimal("1.0"))
            counts["derived"] += 1
        else:
            counts["skipped"] += 1

    session.commit()

    elapsed = time.monotonic() - t0
    _say(
        f"[pipeline] done  exact={counts['exact']}  regex={counts['regex']}"
        f"  vector_llm={counts['vector_llm']}  pending={counts['pending']}"
        f"  derived={counts['derived']}  skipped={counts['skipped']}"
        f"  elapsed={elapsed:.1f}s"
    )
    return counts
