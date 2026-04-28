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
from sqlalchemy import bindparam, select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from agnaradie_pricing.catalogue.normalise import normalise_brand, normalise_ean, normalise_mpn
from agnaradie_pricing.db.models import Product, ProductMatch
from agnaradie_pricing.matching.deterministic import match_deterministic
from agnaradie_pricing.matching.regex_matcher import match_regex
from agnaradie_pricing.matching.llm_matcher import find_best_llm_match
from agnaradie_pricing.matching.vector_search import TitleVectorIndex, make_default_embedder

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

    sku = (
        f"derived-ean-{ean}" if ean
        else f"derived-{brand}-{mpn}".replace(" ", "-")[:80] if brand and mpn
        else None  # NULL — SQLite UNIQUE allows multiple NULLs
    )

    product = Product(
        sku=sku,
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

def run_matching(
    session,
    *,
    llm_client=None,
    force: bool = False,
    llm_only: bool = False,
) -> dict[str, int]:
    """Match all unmatched competitor listings to products.

    Parameters
    ----------
    session     Active SQLAlchemy session.
    llm_client  Optional OpenAIClient; enables vector+LLM phase.
    force       Re-match listings that already have a product_matches row.
    llm_only    Skip exact, regex, and derived fallback paths; run vector+LLM only.

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
        f"  llm_only={'yes' if llm_only else 'no'}"
    )

    # ------------------------------------------------------------------
    # Phase 0 — EAN clustering: every unique EAN becomes a product
    # ------------------------------------------------------------------
    # Group all unmatched listings by their normalised EAN. For each EAN
    # not yet in the products table, create a derived Product and match all
    # listings with that EAN to it immediately (exact_ean, approved).
    # This eliminates ~95% of orphans before the expensive LLM phase.
    # ------------------------------------------------------------------
    t_phase0 = time.monotonic()

    ean_to_listings: dict[str, list[dict]] = {}
    no_ean_listings: list[dict] = []
    for li in listings:
        if llm_only:
            no_ean_listings.append(li)
            continue
        ean = normalise_ean(li.get("ean"))
        if ean:
            ean_to_listings.setdefault(ean, []).append(li)
        else:
            no_ean_listings.append(li)

    new_eans = [ean for ean in ean_to_listings if ean not in ean_idx]
    _say(
        f"[phase-0] EAN clustering  unique_eans={len(ean_to_listings)}"
        f"  known_eans={len(ean_to_listings) - len(new_eans)}"
        f"  new_eans={len(new_eans)}"
        f"  no_ean={len(no_ean_listings)}"
    )

    report_every_0 = max(500, len(ean_to_listings) // 20)
    processed = 0
    for ean, ean_listings in ean_to_listings.items():
        if ean in ean_idx:
            product_id = ean_idx[ean]
        else:
            # Pick the most informative listing as representative
            rep = max(ean_listings, key=lambda li: bool(li.get("brand")) + bool(li.get("mpn")))
            product_id = _get_or_create_product(
                session,
                ean=ean,
                brand=normalise_brand(rep.get("brand")),
                mpn=normalise_mpn(rep.get("mpn")),
                title=rep.get("title") or "",
            )
            ean_idx[ean] = product_id  # keep index up to date

        for li in ean_listings:
            _upsert_match(session, listing_id=li["id"], product_id=product_id,
                          match_type="exact_ean", confidence=Decimal("1.0"))
            counts["exact"] += 1

        processed += 1
        if processed % report_every_0 == 0 or processed == len(ean_to_listings):
            elapsed = time.monotonic() - t_phase0
            rate = processed / elapsed if elapsed > 0 else 0
            _say(
                f"[phase-0] {processed:>6}/{len(ean_to_listings)}"
                f"  {processed/len(ean_to_listings)*100:5.1f}%"
                f"  matched={counts['exact']}"
                f"  {rate:.0f} eans/s"
            )
            session.commit()

    session.commit()
    _say(
        f"[phase-0] done  ean_matched={counts['exact']}"
        f"  remaining_no_ean={len(no_ean_listings)}"
        f"  elapsed={time.monotonic()-t_phase0:.1f}s"
    )

    # Remaining unmatched are listings with no EAN — feed into phase 1+
    listings = no_ean_listings

    # Rebuild product index — derived products were just added
    products = _load_products(session)
    ean_idx = _build_ean_index(products)
    mpn_idx = _build_mpn_index(products)
    mpn_nb_idx = _build_mpn_nobrand_index(products)
    products_by_id = {p["id"]: p for p in products}

    unmatched: list[dict] = []

    # Build brand-bucketed product index for regex (avoids O(listings × all_products))
    products_by_brand: dict[str, list[dict]] = {}
    for p in products:
        b = normalise_brand(p.get("brand")) or "__no_brand__"
        products_by_brand.setdefault(b, []).append(p)

    # ------------------------------------------------------------------
    # Phase 1 — deterministic + regex (fast, no ML)
    # ------------------------------------------------------------------
    phase1_listings = [] if llm_only else listings
    n = len(phase1_listings)
    report_every = max(1000, n // 20)   # print ~20 updates across the whole run
    t_phase1 = time.monotonic()

    for i, li in enumerate(phase1_listings, start=1):
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
            # Regex: only scan products in the same brand bucket
            listing_dict = {k: li.get(k) for k in ("brand", "mpn", "ean", "title")}
            bucket = products_by_brand.get(listing_brand or "__no_brand__", [])
            if listing_brand and not bucket:
                bucket = products_by_brand.get("__no_brand__", [])
            best_result: tuple[str, float] | None = None
            best_pid: int | None = None
            for p in bucket:
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

        if i % report_every == 0 or i == n:
            elapsed = time.monotonic() - t_phase1
            rate = i / elapsed if elapsed > 0 else 0
            eta = (n - i) / rate if rate > 0 else 0
            pct = i / n * 100
            _say(
                f"[phase-1] {i:>7}/{n}  {pct:5.1f}%"
                f"  exact={counts['exact']}  regex={counts['regex']}"
                f"  unmatched={len(unmatched)}"
                f"  {rate:.0f}/s  ETA {eta:.0f}s"
            )
            session.commit()

    if llm_only:
        unmatched = list(listings)
        _say(
            f"[phase-1] skipped deterministic/regex in llm-only mode"
            f"  unmatched={len(unmatched)}"
        )

    session.commit()
    _say(
        f"[phase-1] done  exact={counts['exact']}  regex={counts['regex']}"
        f"  unmatched={len(unmatched)}"
        f"  elapsed={time.monotonic()-t_phase1:.1f}s"
    )

    if not unmatched:
        return counts

    # ------------------------------------------------------------------
    # Phase 2 — vector + LLM (brand-bucketed)
    # ------------------------------------------------------------------
    if llm_client is None:
        if llm_only:
            counts["skipped"] += len(unmatched)
            session.commit()
            return counts
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
    t_phase2 = time.monotonic()
    n_brands = len(brand_pools)
    phase2_embedder = None
    _say(f"[phase-2] starting  orphans={total_orphans}  brands={n_brands}")

    for brand_idx, (brand, orphans) in enumerate(brand_pools.items(), start=1):
        if brand == "__no_brand__":
            for li in orphans:
                ean = normalise_ean(li.get("ean"))
                if ean and not llm_only:
                    pid = _get_or_create_product(
                        session,
                        ean=ean,
                        brand=normalise_brand(li.get("brand")),
                        mpn=normalise_mpn(li.get("mpn")),
                        title=li.get("title") or "",
                    )
                    _upsert_match(
                        session,
                        listing_id=li["id"],
                        product_id=pid,
                        match_type="derived_ean",
                        confidence=Decimal("1.0"),
                    )
                    counts["derived"] += 1
                else:
                    counts["skipped"] += 1
            done += len(orphans)
            session.commit()
            _say(
                f"[phase-2] skipped no-brand bucket  "
                f"orphans={len(orphans)}  done={done}/{total_orphans}"
            )
            continue

        pool = product_brand_pools.get(brand, []) + orphans
        if len(pool) < 2:
            # Nothing to compare against — fall through to derived
            for li in orphans:
                ean = normalise_ean(li.get("ean"))
                if ean and not llm_only:
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
            done += len(orphans)
            continue

        _say(
            f"[phase-2] brand {brand_idx}/{n_brands}  '{brand}'  "
            f"orphans={len(orphans)}  pool={len(pool)}"
            f"  done={done}/{total_orphans}"
            f"  elapsed={time.monotonic()-t_phase2:.0f}s"
        )

        t_index = time.monotonic()
        _say(f"[phase-2] indexing '{brand}'  records={len(pool)}")
        if phase2_embedder is None:
            _say("[phase-2] initializing vector embedder")
            phase2_embedder = make_default_embedder()
        index = TitleVectorIndex(pool, embedder=phase2_embedder)
        _say(
            f"[phase-2] indexed '{brand}'  records={len(pool)}"
            f"  elapsed={time.monotonic()-t_index:.1f}s"
            f"  backend={getattr(index, 'backend_description', 'unknown')}"
        )
        if not index._vectors:
            counts["skipped"] += len(orphans)
            done += len(orphans)
            continue

        matrix = np.asarray(index._vectors, dtype=np.float32)
        id_to_row = {item["id"]: idx for idx, item in enumerate(pool)}

        brand_done = 0

        def report_phase2_progress(*, force: bool = False) -> None:
            if not force and brand_done % _PROGRESS_EVERY != 0:
                return
            elapsed2 = time.monotonic() - t_phase2
            rate2 = done / elapsed2 if elapsed2 > 0 else 0
            eta2 = (total_orphans - done) / rate2 if rate2 > 0 else 0
            _say(
                f"[phase-2] progress '{brand}'"
                f"  brand={brand_done}/{len(orphans)}"
                f"  done={done}/{total_orphans}"
                f"  skipped={counts['skipped']}"
                f"  llm_ok={counts['vector_llm']}"
                f"  pending={counts['pending']}"
                f"  {rate2:.1f}/s  ETA {eta2:.0f}s"
            )

        for li in orphans:
            brand_done += 1
            done += 1

            if li["id"] not in id_to_row:
                counts["skipped"] += 1
                report_phase2_progress()
                continue

            row_idx = id_to_row[li["id"]]
            scores = matrix @ matrix[row_idx]
            scores[row_idx] = -1.0

            k = min(_TOP_K, len(scores) - 1)
            if k <= 0:
                counts["skipped"] += 1
                report_phase2_progress()
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
                if ean and not llm_only:
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
                report_phase2_progress()
                continue

            candidate_items = [c for c, _ in candidates]
            sim_lookup = {c["id"]: s for c, s in candidates}

            hit = find_best_llm_match(li, candidate_items, llm_client=llm_client)
            if hit is None:
                counts["skipped"] += 1
                report_phase2_progress()
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

            report_phase2_progress()

        elapsed2 = time.monotonic() - t_phase2
        rate2 = done / elapsed2 if elapsed2 > 0 else 0
        eta2 = (total_orphans - done) / rate2 if rate2 > 0 else 0
        session.commit()
        _say(
            f"[phase-2] brand done  '{brand}'  "
            f"done={done}/{total_orphans}  {done/total_orphans*100:.1f}%"
            f"  llm_ok={counts['vector_llm']}  pending={counts['pending']}"
            f"  {rate2:.1f}/s  ETA {eta2:.0f}s"
        )

    if llm_only:
        session.commit()
        elapsed = time.monotonic() - t0
        _say(
            f"[pipeline] done  exact={counts['exact']}  regex={counts['regex']}"
            f"  vector_llm={counts['vector_llm']}  pending={counts['pending']}"
            f"  derived={counts['derived']}  skipped={counts['skipped']}"
            f"  elapsed={elapsed:.1f}s"
        )
        return counts

    # ------------------------------------------------------------------
    # Phase 3 — derived products for remaining EAN-bearing orphans
    # ------------------------------------------------------------------
    still_unmatched_ids: set[int] = {li["id"] for li in unmatched}
    matched_now = session.execute(
        text("SELECT listing_id FROM product_matches WHERE listing_id IN :ids")
        .bindparams(bindparam("ids", expanding=True)),
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

    session.commit()

    elapsed = time.monotonic() - t0
    _say(
        f"[pipeline] done  exact={counts['exact']}  regex={counts['regex']}"
        f"  vector_llm={counts['vector_llm']}  pending={counts['pending']}"
        f"  derived={counts['derived']}  skipped={counts['skipped']}"
        f"  elapsed={elapsed:.1f}s"
    )
    return counts
