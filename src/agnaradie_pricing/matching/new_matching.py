"""EAN-led cross-store product clustering.

Two phases:
  1. EAN clustering — every competitor_listings row sharing a normalised EAN
     joins one ProductCluster (status='approved', method='ean').
  2. Vector + LLM fallback — listings without an EAN cluster get a light
     embedding (sentence-transformers if available, hashing otherwise);
     for each orphan we find the most similar candidate (cosine ≥ 0.85),
     ask gpt-5-nano to confirm same-product, and on hit either join the
     candidate's cluster or create a new fuzzy cluster.

Auto-approve threshold: vector similarity ≥ 0.96 → approved, else pending
(visible in the Matching review tab). LLM confidence is retained as evidence,
but it does not auto-approve below-threshold fuzzy matches.
"""

from __future__ import annotations

import logging
import sys
import time
from decimal import Decimal
from typing import Any

import numpy as np
from sqlalchemy import select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from agnaradie_pricing.catalogue.normalise import normalise_brand, normalise_ean
from agnaradie_pricing.db.models import ClusterMember, ProductCluster
from agnaradie_pricing.matching.llm_matcher import (
    OpenAIClient,
    find_best_llm_match,
)
from agnaradie_pricing.matching.vector_search import TitleVectorIndex

logger = logging.getLogger(__name__)

_VECTOR_SIM_THRESHOLD = 0.85
_SIMILARITY_AUTO_APPROVE = Decimal("0.96")
_TOP_K_CANDIDATES = 5
_TOOLZONE_ID = "toolzone_sk"
_PROGRESS_EVERY = 25  # print orphan-progress every N items within a brand


def _say(msg: str) -> None:
    """Print to stdout with immediate flush, so progress shows up live."""
    print(msg, flush=True)
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_all_listings(session) -> list[dict]:
    rows = session.execute(
        text(
            """
            SELECT id, competitor_id, brand, mpn, ean, title
            FROM competitor_listings
            ORDER BY id
            """
        )
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def _truncate_clusters(session) -> None:
    session.execute(text("DELETE FROM cluster_members"))
    session.execute(text("DELETE FROM product_clusters"))
    session.commit()


def reset_all_matches(session) -> None:
    """Clear generated match state without deleting listings or products."""
    session.execute(text("DELETE FROM cluster_members"))
    session.execute(text("DELETE FROM product_clusters"))
    session.execute(text("DELETE FROM listing_matches"))
    session.execute(text("DELETE FROM product_matches"))
    session.commit()


# ---------------------------------------------------------------------------
# Phase 1 — EAN clustering
# ---------------------------------------------------------------------------

def _phase_ean(session, listings: list[dict], existing_member_ids: set[int]) -> dict:
    """Group listings by normalised EAN, create/update clusters."""
    by_ean: dict[str, list[dict]] = {}
    for li in listings:
        if li["id"] in existing_member_ids:
            continue
        ean = normalise_ean(li.get("ean"))
        if not ean:
            continue
        by_ean.setdefault(ean, []).append(li)

    clusters_made = 0
    members_made = 0

    for ean, members in by_ean.items():
        # Get-or-create cluster on this EAN
        cluster = session.scalar(
            select(ProductCluster).where(ProductCluster.ean == ean)
        )
        if cluster is None:
            rep = _pick_representative(members)
            cluster = ProductCluster(
                ean=ean,
                cluster_method="ean",
                representative_brand=normalise_brand(rep.get("brand")),
                representative_title=rep.get("title"),
            )
            session.add(cluster)
            session.flush()
            clusters_made += 1

        rows = [
            {
                "cluster_id": cluster.id,
                "listing_id": li["id"],
                "match_method": "ean",
                "similarity": None,
                "llm_confidence": None,
                "status": "approved",
            }
            for li in members
        ]
        if rows:
            stmt = sqlite_insert(ClusterMember).values(rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=["listing_id"])
            result = session.execute(stmt)
            members_made += result.rowcount or 0

    session.commit()
    return {"ean_clusters_created": clusters_made, "ean_members_added": members_made}


def _pick_representative(members: list[dict]) -> dict:
    for m in members:
        if m.get("competitor_id") == _TOOLZONE_ID:
            return m
    return members[0]


# ---------------------------------------------------------------------------
# Phase 2 — Vector + LLM fuzzy matching
# ---------------------------------------------------------------------------

def _phase_fuzzy(
    session,
    listings: list[dict],
    llm_client: OpenAIClient,
) -> dict:
    """Brand-bucketed vector + LLM matching with live progress.

    Listings are grouped by `normalise_brand(brand)`. Brands are processed
    smallest-pool-first, so encoding + matching for the first brand starts
    almost immediately (a 5-listing pool encodes in milliseconds). Within
    each brand we print one line per encode, one per match start, and a
    rolling progress line every `_PROGRESS_EVERY` orphans.

    Cross-brand matches are intentionally impossible. Listings with no brand
    are skipped (no pool to match against).
    """
    member_to_cluster = _load_membership_map(session)

    orphans = [li for li in listings if li["id"] not in member_to_cluster]
    if not orphans:
        return _zero_fuzzy_counters()

    # Bucket every listing by brand.
    brand_pools: dict[str, list[dict]] = {}
    for li in listings:
        brand = normalise_brand(li.get("brand"))
        if not brand:
            continue
        brand_pools.setdefault(brand, []).append(li)

    no_brand_orphans = sum(1 for li in orphans if not normalise_brand(li.get("brand")))
    if no_brand_orphans:
        _say(f"  ⚠ Skipping {no_brand_orphans} orphans without brand info")

    # Only brands that have at least one orphan AND ≥2 listings to match against.
    actionable_brands = [
        brand
        for brand, pool in brand_pools.items()
        if len(pool) >= 2 and any(li["id"] not in member_to_cluster for li in pool)
    ]
    # Process smallest pools first → first visible result appears in seconds.
    actionable_brands.sort(key=lambda b: len(brand_pools[b]))

    total_orphans = sum(
        sum(1 for li in brand_pools[b] if li["id"] not in member_to_cluster)
        for b in actionable_brands
    )
    _say(
        f"\n=== Fuzzy phase ===\n"
        f"  brand pools     : {len(actionable_brands)}\n"
        f"  total orphans   : {total_orphans}\n"
        f"  vector threshold: {_VECTOR_SIM_THRESHOLD}\n"
        f"  auto-approve at : similarity >= {_SIMILARITY_AUTO_APPROVE}\n"
    )

    attempted = approved = pending = 0
    orphans_done_global = 0
    t0 = time.monotonic()

    for brand_idx, brand in enumerate(actionable_brands, start=1):
        pool = brand_pools[brand]
        bucket_orphans = [li for li in pool if li["id"] not in member_to_cluster]
        if not bucket_orphans:
            continue

        # ---- Encode this brand's pool ----
        t_enc = time.monotonic()
        _say(
            f"[{brand_idx}/{len(actionable_brands)}] {brand:<25} "
            f"encoding {len(pool)} listings…"
        )
        index = TitleVectorIndex(pool)
        if not index._vectors:
            _say(f"    ↳ empty embeddings, skipping")
            continue
        matrix = np.asarray(index._vectors, dtype=np.float32)
        id_to_row = {li["id"]: idx for idx, li in enumerate(pool)}
        enc_secs = time.monotonic() - t_enc
        _say(
            f"    ↳ encoded in {enc_secs:.1f}s · matching {len(bucket_orphans)} orphans "
            f"(backend: {index.backend_description})"
        )

        # ---- Match each orphan ----
        bucket_attempted = bucket_approved = bucket_pending = 0
        t_brand = time.monotonic()

        for i, orphan in enumerate(bucket_orphans, start=1):
            if orphan["id"] in member_to_cluster:
                continue

            row_idx = id_to_row[orphan["id"]]
            scores = matrix @ matrix[row_idx]
            scores[row_idx] = -1.0

            k = min(_TOP_K_CANDIDATES, len(scores) - 1)
            if k <= 0:
                continue
            top_idx = np.argpartition(-scores, k - 1)[:k]
            top_idx = top_idx[np.argsort(-scores[top_idx])]
            candidate_pairs = [
                (pool[j], float(scores[j]))
                for j in top_idx
                if scores[j] >= _VECTOR_SIM_THRESHOLD
            ]
            if not candidate_pairs:
                if i % _PROGRESS_EVERY == 0:
                    _say(
                        f"    [{i}/{len(bucket_orphans)}] {brand} · attempted={bucket_attempted}"
                        f" approved={bucket_approved} pending={bucket_pending}"
                    )
                continue

            bucket_attempted += 1
            attempted += 1
            candidate_listings = [c for c, _ in candidate_pairs]
            sim_lookup = {c["id"]: s for c, s in candidate_pairs}

            hit = find_best_llm_match(orphan, candidate_listings, llm_client=llm_client)
            if hit is not None:
                matched_listing, (_method, conf) = hit
                confidence = Decimal(str(round(conf, 2)))
                similarity = Decimal(str(round(sim_lookup.get(matched_listing["id"], 0.0), 3)))
                status = "approved" if similarity >= _SIMILARITY_AUTO_APPROVE else "pending"

                cluster_id = member_to_cluster.get(matched_listing["id"])
                if cluster_id is None:
                    rep = _pick_representative([orphan, matched_listing])
                    cluster = ProductCluster(
                        ean=None,
                        cluster_method="fuzzy",
                        representative_brand=brand,
                        representative_title=rep.get("title"),
                    )
                    session.add(cluster)
                    session.flush()
                    cluster_id = cluster.id
                    session.add(
                        ClusterMember(
                            cluster_id=cluster_id,
                            listing_id=matched_listing["id"],
                            match_method="vector_llm",
                            similarity=similarity,
                            llm_confidence=confidence,
                            status=status,
                        )
                    )
                    member_to_cluster[matched_listing["id"]] = cluster_id

                session.add(
                    ClusterMember(
                        cluster_id=cluster_id,
                        listing_id=orphan["id"],
                        match_method="vector_llm",
                        similarity=similarity,
                        llm_confidence=confidence,
                        status=status,
                    )
                )
                member_to_cluster[orphan["id"]] = cluster_id

                if status == "approved":
                    approved += 1
                    bucket_approved += 1
                else:
                    pending += 1
                    bucket_pending += 1

            if i % _PROGRESS_EVERY == 0 or i == len(bucket_orphans):
                _say(
                    f"    [{i}/{len(bucket_orphans)}] {brand} · attempted={bucket_attempted}"
                    f" approved={bucket_approved} pending={bucket_pending}"
                )

        session.commit()  # commit once per brand, not per match
        orphans_done_global += len(bucket_orphans)
        brand_secs = time.monotonic() - t_brand
        elapsed = time.monotonic() - t0
        rate = orphans_done_global / elapsed if elapsed else 0.0
        _say(
            f"    ✓ {brand} done in {brand_secs:.1f}s "
            f"(global: {orphans_done_global}/{total_orphans} orphans, "
            f"{rate:.1f}/s, approved={approved} pending={pending})\n"
        )

    return {
        "fuzzy_pairs_attempted": attempted,
        "fuzzy_approved": approved,
        "fuzzy_pending": pending,
    }


def _zero_fuzzy_counters() -> dict:
    return {"fuzzy_pairs_attempted": 0, "fuzzy_approved": 0, "fuzzy_pending": 0}


def _load_membership_map(session) -> dict[int, int]:
    rows = session.execute(
        text("SELECT listing_id, cluster_id FROM cluster_members")
    ).fetchall()
    return {r.listing_id: r.cluster_id for r in rows}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_new_matching(
    session_factory,
    settings,
    *,
    force: bool = False,
    use_llm: bool = True,
    reset_all: bool = False,
) -> dict:
    """Run EAN clustering + optional fuzzy LLM matching end to end.

    Returns a dict of counters: ean_clusters_created, ean_members_added,
    fuzzy_pairs_attempted, fuzzy_approved, fuzzy_pending.
    """
    counters: dict[str, int] = {}

    with session_factory() as session:
        if reset_all:
            _say("--reset-all-matches: clearing clusters plus legacy match tables")
            reset_all_matches(session)
        elif force:
            _say("--force: truncating cluster_members and product_clusters")
            _truncate_clusters(session)

        _say("Loading listings from DB…")
        t0 = time.monotonic()
        listings = _load_all_listings(session)
        existing = _load_membership_map(session)
        _say(
            f"  loaded {len(listings)} listings ({len(existing)} already clustered)"
            f" in {time.monotonic() - t0:.1f}s"
        )

        _say("\n=== EAN phase ===")
        t0 = time.monotonic()
        ean_stats = _phase_ean(session, listings, set(existing.keys()))
        counters.update(ean_stats)
        _say(
            f"  ean clusters created : {ean_stats['ean_clusters_created']}\n"
            f"  ean members added    : {ean_stats['ean_members_added']}\n"
            f"  done in {time.monotonic() - t0:.1f}s"
        )

    if use_llm:
        api_key = getattr(settings, "openai_api_key", None)
        if not api_key:
            import os

            api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.warning("OPENAI_API_KEY not set; skipping fuzzy LLM phase")
            counters.update(_zero_fuzzy_counters())
        else:
            model = getattr(settings, "openai_model", "gpt-5-nano") or "gpt-5-nano"
            llm_client = OpenAIClient(api_key=api_key, model=model)
            with session_factory() as session:
                listings = _load_all_listings(session)
                fuzzy_stats = _phase_fuzzy(session, listings, llm_client)
            counters.update(fuzzy_stats)
            logger.info("Fuzzy phase: %s", fuzzy_stats)
    else:
        counters.update(_zero_fuzzy_counters())

    return counters
