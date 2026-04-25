"""Standalone product-matching job.

Matches ToolZone reference listings against all other competitor listings and
saves results to the `listing_matches` table.  Optionally runs the full scrape
pipeline first so the whole flow is a single command.

Matching layers (in order, first hit wins):
  1. exact_ean   — EAN identical on both sides                  confidence 1.00
  2. exact_mpn   — brand + MPN identical                        confidence 1.00
  3. mpn_no_brand— MPN identical, listing has no brand          confidence 0.90
  4. regex_ean   — EAN extracted from listing title text        confidence 0.95
  5. llm_fuzzy   — vector shortlist + OpenAI verification (opt-in) confidence ≥ 0.81

Examples
--------
    # Scrape + EAN match + LLM + report (full pipeline)
    python jobs/match_products.py --manufacturer knipex --scrape --llm

    # EAN match only (no scrape, no LLM)
    python jobs/match_products.py --manufacturer knipex

    # EAN + LLM fallback (already scraped)
    python jobs/match_products.py --manufacturer knipex --llm

    # Restrict to specific competitors
    python jobs/match_products.py --manufacturer knipex --only boukal_cz bo_import_cz --llm

    # Re-match from scratch
    python jobs/match_products.py --manufacturer knipex --force --llm

    # All manufacturers in DB
    python jobs/match_products.py --llm
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from decimal import Decimal
from sqlalchemy import text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from agnaradie_pricing.catalogue.normalise import fold_diacritics, normalise_brand, normalise_ean, normalise_mpn
from agnaradie_pricing.db.models import ListingMatch
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.matching.llm_matcher import OpenAIClient, find_best_llm_match
from agnaradie_pricing.matching.vector_search import TitleVectorIndex
from agnaradie_pricing.settings import Settings

logger = logging.getLogger(__name__)

_MIN_LLM_CONFIDENCE = 0.81
_LLM_MIN_CANDIDATE_LIMIT = 5
_LLM_CANDIDATE_LIMIT = 30
_RAW_VECTOR_CANDIDATE_LIMIT = 200
_DEBUG_TOP_CANDIDATES = 10
_EAN_IN_TEXT_RE = re.compile(r"(?<!\d)(\d{8,14})(?!\d)")
_MODEL_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9./-]*\d[a-z0-9./-]*", re.IGNORECASE)
_CATEGORY_SKIP_TOKENS = frozenset(
    {
        "a",
        "aj",
        "akryl",
        "aku",
        "al",
        "do",
        "fiber",
        "fiberglass",
        "fasadna",
        "fasadne",
        "fasadny",
        "gr",
        "na",
        "nasadou",
        "obojstranna",
        "obojstranne",
        "obojstranny",
        "pojazdny",
        "premium",
        "pre",
        "profilova",
        "profilove",
        "profilovy",
        "rukovat",
        "rukovatou",
        "rukovatou",
        "s",
        "set",
        "so",
        "specialna",
        "specialne",
        "specialny",
        "stolova",
        "stolove",
        "stolovy",
        "tvarovana",
        "tvarovane",
        "tvarovany",
        "v",
        "z",
    }
)

# Competitors that exclusively carry a single brand — used to infer missing brand fields.
_COMPETITOR_SINGLE_BRAND: dict[str, str] = {
    "bo_import_cz": "KNIPEX",
    "agi_sk":       "KNIPEX",
}

# Competitor display names for the report
_DISPLAY_NAMES = {
    "bo_import_cz":        "BO-Import",
    "boukal_cz":           "Boukal",
    "madmat_sk":           "MadMat",
    "centrumnaradia_sk":   "CentrumNáradia",
    "doktorkladivo_sk":    "DoktorKladivo",
    "ahprofi_sk":          "AhProfi",
    "naradieshop_sk":      "NaradieShop",
    "rebiop_sk":           "Rebiop",
    "agi_sk":              "AGI",
    "toolzone_sk":         "ToolZone",
}


def _disp(cid: str) -> str:
    return _DISPLAY_NAMES.get(cid, cid)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _load_toolzone_listings(session, manufacturer: str | None) -> list[dict]:
    if manufacturer:
        rows = session.execute(
            text("""
                SELECT id, ean, brand, mpn, title, url, price_eur, in_stock
                FROM competitor_listings
                WHERE competitor_id = 'toolzone_sk'
                  AND LOWER(brand) LIKE :brand
                ORDER BY title
            """),
            {"brand": f"%{manufacturer.lower()}%"},
        ).fetchall()
    else:
        rows = session.execute(
            text("""
                SELECT id, ean, brand, mpn, title, url, price_eur, in_stock
                FROM competitor_listings
                WHERE competitor_id = 'toolzone_sk'
                ORDER BY brand, title
            """),
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def _load_competitor_listings(session, manufacturer: str | None, only: list[str] | None) -> list[dict]:
    conditions = ["competitor_id != 'toolzone_sk'"]
    params: dict = {}
    if manufacturer:
        conditions.append("LOWER(brand) LIKE :brand")
        params["brand"] = f"%{manufacturer.lower()}%"
    if only:
        placeholders = ", ".join(f":cid_{i}" for i in range(len(only)))
        conditions.append(f"competitor_id IN ({placeholders})")
        for i, cid in enumerate(only):
            params[f"cid_{i}"] = cid

    where = " AND ".join(conditions)
    rows = session.execute(
        text(f"""
            SELECT id, competitor_id, ean, brand, mpn, title, url, price_eur, in_stock
            FROM competitor_listings
            WHERE {where}
        """),
        params,
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def _already_matched_ids(session) -> set[int]:
    rows = session.execute(
        text("SELECT competitor_listing_id FROM listing_matches")
    ).fetchall()
    return {r[0] for r in rows}


def _save_matches(session, matches: list[dict]) -> int:
    if not matches:
        return 0
    stmt = sqlite_insert(ListingMatch).values(matches)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["toolzone_listing_id", "competitor_listing_id"]
    )
    result = session.execute(stmt)
    session.commit()
    return result.rowcount or 0


# ---------------------------------------------------------------------------
# Matching logic
# ---------------------------------------------------------------------------

def _apply_brand_inference(listings: list[dict]) -> list[dict]:
    """Fill in missing brand fields for single-brand competitors (shallow copy)."""
    result = []
    for cl in listings:
        inferred = _COMPETITOR_SINGLE_BRAND.get(cl.get("competitor_id", ""))
        if inferred and not cl.get("brand"):
            cl = {**cl, "brand": inferred}
        result.append(cl)
    return result


def _ean_match(
    toolzone_listings: list[dict],
    competitor_listings: list[dict],
    already_matched: set[int],
) -> tuple[list[dict], list[dict]]:
    ean_index: dict[str, dict] = {}
    for tz in toolzone_listings:
        ean = normalise_ean(tz.get("ean"))
        if ean:
            ean_index[ean] = tz

    match_records: list[dict] = []
    unmatched: list[dict] = []

    for cl in competitor_listings:
        if cl["id"] in already_matched:
            continue
        ean = normalise_ean(cl.get("ean"))
        if ean and ean in ean_index:
            tz = ean_index[ean]
            match_records.append({
                "toolzone_listing_id": tz["id"],
                "competitor_listing_id": cl["id"],
                "match_type": "exact_ean",
                "confidence": Decimal("1.00"),
            })
        else:
            unmatched.append(cl)

    return match_records, unmatched


def _deterministic_mpn_match(
    toolzone_listings: list[dict],
    competitor_listings: list[dict],
    already_matched: set[int],
) -> tuple[list[dict], list[dict]]:
    """Layers 2–4: brand+MPN, MPN-no-brand, and regex EAN from title text."""
    import re as _re

    # Build brand+MPN index
    brand_mpn_idx: dict[tuple[str, str], dict] = {}
    for tz in toolzone_listings:
        brand = normalise_brand(tz.get("brand"))
        mpn = normalise_mpn(tz.get("mpn"))
        if brand and mpn:
            brand_mpn_idx[(brand, mpn)] = tz

    # Build unambiguous MPN index (only MPNs unique across the TZ catalog)
    mpn_counts: dict[str, int] = {}
    for tz in toolzone_listings:
        mpn = normalise_mpn(tz.get("mpn"))
        if mpn:
            mpn_counts[mpn] = mpn_counts.get(mpn, 0) + 1
    mpn_uniq_idx: dict[str, dict] = {}
    for tz in toolzone_listings:
        mpn = normalise_mpn(tz.get("mpn"))
        if mpn and mpn_counts[mpn] == 1:
            mpn_uniq_idx[mpn] = tz

    # Build normalised EAN index for regex title scan
    ean_norm_idx: dict[str, dict] = {}
    for tz in toolzone_listings:
        ean = normalise_ean(tz.get("ean"))
        if ean:
            ean_norm_idx[ean] = tz

    _ean_in_text = _re.compile(r"(?<!\d)(\d{8,14})(?!\d)")

    match_records: list[dict] = []
    still_unmatched: list[dict] = []

    for cl in competitor_listings:
        if cl["id"] in already_matched:
            continue

        cl_brand = normalise_brand(cl.get("brand"))
        cl_mpn = normalise_mpn(cl.get("mpn"))

        matched_tz: dict | None = None
        match_type: str = ""
        confidence: float = 0.0

        if cl_brand and cl_mpn:
            tz = brand_mpn_idx.get((cl_brand, cl_mpn))
            if tz:
                matched_tz, match_type, confidence = tz, "exact_mpn", 1.00

        if not matched_tz and cl_mpn and not cl_brand:
            tz = mpn_uniq_idx.get(cl_mpn)
            if tz:
                matched_tz, match_type, confidence = tz, "mpn_no_brand", 0.90

        if not matched_tz:
            title = cl.get("title") or ""
            for m in _ean_in_text.finditer(title):
                candidate = normalise_ean(m.group(1))
                if candidate and candidate in ean_norm_idx:
                    matched_tz = ean_norm_idx[candidate]
                    match_type, confidence = "regex_ean", 0.95
                    break

        if matched_tz is not None:
            match_records.append({
                "toolzone_listing_id": matched_tz["id"],
                "competitor_listing_id": cl["id"],
                "match_type": match_type,
                "confidence": Decimal(str(round(confidence, 2))),
            })
        else:
            still_unmatched.append(cl)

    return match_records, still_unmatched


def _rerank_by_brand(candidates: list[dict], listing: dict) -> list[dict]:
    """Move brand-matching candidates to the front of the list."""
    listing_brand = normalise_brand(listing.get("brand"))
    if not listing_brand:
        return candidates
    match = [c for c in candidates if normalise_brand(c.get("brand")) == listing_brand]
    other = [c for c in candidates if normalise_brand(c.get("brand")) != listing_brand]
    return match + other


def _normalise_debug_text(value: str) -> str:
    folded = fold_diacritics((value or "").lower())
    cleaned = re.sub(r"[^a-z0-9]+", " ", folded)
    return " ".join(cleaned.split())


def _dense_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", fold_diacritics((value or "").lower()))


def _extract_model_tokens(value: str) -> set[str]:
    folded = fold_diacritics((value or "").lower())
    return {_dense_token(token) for token in _MODEL_TOKEN_RE.findall(folded) if _dense_token(token)}


def _build_brand_token_map(toolzone_listings: list[dict]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for listing in toolzone_listings:
        brand = normalise_brand(listing.get("brand"))
        if not brand:
            continue
        for token in _normalise_debug_text(brand).split():
            if len(token) < 3:
                continue
            mapping.setdefault(token, brand)
    return mapping


def _detect_listing_brand(listing: dict, brand_token_map: dict[str, str]) -> str | None:
    brand = normalise_brand(listing.get("brand"))
    if brand:
        return brand
    matched = {
        canonical
        for token, canonical in brand_token_map.items()
        if token in set(_normalise_debug_text(listing.get("title") or "").split())
    }
    if len(matched) == 1:
        return next(iter(matched))
    return None


def _extract_category_anchor(listing: dict, brand_token_map: dict[str, str]) -> str | None:
    title_tokens = _normalise_debug_text(listing.get("title") or "").split()
    brand_tokens = {token for token in title_tokens if token in brand_token_map}
    for token in title_tokens:
        if len(token) < 3 or token.isdigit() or any(ch.isdigit() for ch in token):
            continue
        if token in brand_tokens or token in _CATEGORY_SKIP_TOKENS:
            continue
        return token
    return None


def _candidate_token_set(candidate: dict) -> set[str]:
    fields = [
        candidate.get("title") or "",
        candidate.get("brand") or "",
        candidate.get("mpn") or "",
    ]
    return set(_normalise_debug_text(" ".join(fields)).split())


def _candidate_dense_text(candidate: dict) -> str:
    return _dense_token(
        f"{candidate.get('title') or ''} {candidate.get('brand') or ''} {candidate.get('mpn') or ''}"
    )


def _lexical_shortlist(
    listing: dict,
    scored_candidates: list[tuple[dict, float]],
    *,
    brand_token_map: dict[str, str],
    limit: int,
    min_limit: int,
) -> list[tuple[dict, float]]:
    if not scored_candidates or limit <= 0:
        return []

    detected_brand = _detect_listing_brand(listing, brand_token_map)
    category_anchor = _extract_category_anchor(listing, brand_token_map)
    model_tokens = _extract_model_tokens(listing.get("title") or "")
    listing_tokens = set(_normalise_debug_text(listing.get("title") or "").split())

    ranked: list[tuple[int, int, int, int, float, dict]] = []
    for candidate, score in scored_candidates:
        candidate_tokens = _candidate_token_set(candidate)
        candidate_dense = _candidate_dense_text(candidate)
        brand_match = int(bool(detected_brand and normalise_brand(candidate.get("brand")) == detected_brand))
        category_match = int(bool(category_anchor and category_anchor in candidate_tokens))
        model_match_count = sum(1 for token in model_tokens if token in candidate_dense)
        overlap = len(listing_tokens & candidate_tokens)
        ranked.append((category_match, brand_match, model_match_count, overlap, score, candidate))

    filtered = ranked
    if category_anchor and any(item[0] for item in filtered):
        filtered = [item for item in filtered if item[0]]
    if detected_brand and any(item[1] for item in filtered):
        filtered = [item for item in filtered if item[1]]
    if model_tokens:
        best_model_match_count = max((item[2] for item in filtered), default=0)
        if best_model_match_count > 0:
            filtered = [item for item in filtered if item[2] == best_model_match_count]

    filtered.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]), reverse=True)
    shortlisted = [(candidate, score) for _cat, _brand, _model, _overlap, score, candidate in filtered[:limit]]
    if len(shortlisted) >= min_limit:
        return shortlisted

    seen_ids = {candidate.get("id") for candidate, _score in shortlisted}
    fallback = [
        (candidate, score)
        for candidate, score in scored_candidates
        if candidate.get("id") not in seen_ids
    ]
    return (shortlisted + fallback)[: min(limit, max(min_limit, len(shortlisted)))]


def _normalised_query_string(listing: dict) -> str:
    parts: list[str] = []
    brand = normalise_brand(listing.get("brand"))
    mpn = normalise_mpn(listing.get("mpn"))
    ean = normalise_ean(listing.get("ean"))
    title = _normalise_debug_text(listing.get("title") or "")
    if brand:
        parts.append(f"brand={brand}")
    if mpn:
        parts.append(f"mpn={mpn}")
    if ean:
        parts.append(f"ean={ean}")
    parts.append(f"title={title}")
    return " ".join(parts)


def _build_expected_candidate_lookup(toolzone_listings: list[dict]) -> dict[str, dict]:
    ean_index: dict[str, dict] = {}
    brand_mpn_index: dict[tuple[str, str], dict] = {}
    mpn_counts: dict[str, int] = {}
    title_index: dict[str, list[dict]] = {}

    for tz in toolzone_listings:
        ean = normalise_ean(tz.get("ean"))
        if ean:
            ean_index[ean] = tz

        brand = normalise_brand(tz.get("brand"))
        mpn = normalise_mpn(tz.get("mpn"))
        if brand and mpn:
            brand_mpn_index[(brand, mpn)] = tz
        if mpn:
            mpn_counts[mpn] = mpn_counts.get(mpn, 0) + 1

        title = _normalise_debug_text(tz.get("title") or "")
        if title:
            title_index.setdefault(title, []).append(tz)

    unique_mpn_index: dict[str, dict] = {}
    for tz in toolzone_listings:
        mpn = normalise_mpn(tz.get("mpn"))
        if mpn and mpn_counts.get(mpn) == 1:
            unique_mpn_index[mpn] = tz

    return {
        "ean_index": ean_index,
        "brand_mpn_index": brand_mpn_index,
        "unique_mpn_index": unique_mpn_index,
        "title_index": title_index,
    }


def _infer_expected_candidate(lookup: dict[str, dict], listing: dict) -> tuple[dict, str] | None:
    ean = normalise_ean(listing.get("ean"))
    if ean and ean in lookup["ean_index"]:
        return lookup["ean_index"][ean], "listing_ean"

    brand = normalise_brand(listing.get("brand"))
    mpn = normalise_mpn(listing.get("mpn"))
    if brand and mpn:
        candidate = lookup["brand_mpn_index"].get((brand, mpn))
        if candidate:
            return candidate, "exact_mpn"
    if not brand and mpn:
        candidate = lookup["unique_mpn_index"].get(mpn)
        if candidate:
            return candidate, "mpn_no_brand"

    title = listing.get("title") or ""
    for match in _EAN_IN_TEXT_RE.finditer(title):
        candidate = lookup["ean_index"].get(normalise_ean(match.group(1)))
        if candidate:
            return candidate, "regex_ean"

    normalised_title = _normalise_debug_text(title)
    title_matches = lookup["title_index"].get(normalised_title, [])
    if len(title_matches) == 1:
        return title_matches[0], "exact_title"

    return None


def _print_failure_debug(
    listing: dict,
    scored_candidates: list[tuple[dict, float]],
    *,
    expected_candidate: tuple[dict, str] | None,
    brand_token_map: dict[str, str] | None = None,
) -> None:
    print(f"           normalized query: {_normalised_query_string(listing)}")
    if brand_token_map is not None:
        detected_brand = _detect_listing_brand(listing, brand_token_map)
        category_anchor = _extract_category_anchor(listing, brand_token_map)
        model_tokens = sorted(_extract_model_tokens(listing.get("title") or ""))
        print(
            "           anchors:"
            f" category={category_anchor or '—'}"
            f" brand={detected_brand or '—'}"
            f" model_tokens={','.join(model_tokens) if model_tokens else '—'}"
        )
    print("           top retrieved candidates:")
    for rank, (candidate, score) in enumerate(scored_candidates[:_DEBUG_TOP_CANDIDATES], start=1):
        candidate_brand = normalise_brand(candidate.get("brand"))
        candidate_mpn = normalise_mpn(candidate.get("mpn"))
        candidate_ean = normalise_ean(candidate.get("ean"))
        candidate_title = _normalise_debug_text(candidate.get("title") or "")
        details = [f"id={candidate.get('id')}", f"title={candidate_title}"]
        if candidate_brand:
            details.insert(1, f"brand={candidate_brand}")
        if candidate_mpn:
            details.insert(2, f"mpn={candidate_mpn}")
        if candidate_ean:
            details.insert(3, f"ean={candidate_ean}")
        print(f"             [{rank}] score={score:.4f} " + " ".join(details))

    if not scored_candidates:
        print("             (no retrieved candidates)")

    if expected_candidate is None:
        print("           expected shortlist presence: unknown")
        return

    expected_product, reason = expected_candidate
    candidate_ids = {candidate.get("id") for candidate, _score in scored_candidates}
    status = "yes" if expected_product.get("id") in candidate_ids else "no"
    print(f"           expected shortlist presence: {status} ({reason})")


def _llm_match_one(job: dict, *, llm_client) -> tuple[dict, tuple[str, float]] | None:
    return find_best_llm_match(job["listing"], job["candidates"], llm_client=llm_client)


def _handle_llm_match_result(
    job: dict,
    hit,
    *,
    factory,
    debug_failures: bool,
    expected_lookup: dict[str, dict] | None,
    brand_token_map: dict[str, str],
) -> int:
    ordinal = job["ordinal"]
    total = job["total"]
    cl = job["listing"]
    scored_candidates = job["scored_candidates"]

    if hit is None or hit[1][1] < _MIN_LLM_CONFIDENCE:
        print(f"  [{ordinal}/{total}] {cl['competitor_id']}  no match for: {cl['title'][:60]}")
        if debug_failures:
            _print_failure_debug(
                cl,
                scored_candidates,
                expected_candidate=_infer_expected_candidate(expected_lookup, cl) if expected_lookup else None,
                brand_token_map=brand_token_map,
            )
        return 0

    matched_product, (match_type, confidence) = hit
    record = {
        "toolzone_listing_id": matched_product["id"],
        "competitor_listing_id": cl["id"],
        "match_type": match_type,
        "confidence": Decimal(str(round(confidence, 2))),
    }
    with factory() as session:
        n = _save_matches(session, [record])

    status = "✓ saved" if n else "· duplicate"
    print(
        f"  [{ordinal}/{total}] {status}  [{_disp(cl['competitor_id'])}] {cl['title'][:50]}"
        f"\n           → [TZ] {matched_product['title'][:50]}  conf={confidence:.2f}"
    )
    return n


def _llm_match(
    toolzone_listings: list[dict],
    unmatched: list[dict],
    llm_client: OpenAIClient,
    factory,
    *,
    already_matched: set[int],
    debug_failures: bool = False,
    openai_workers: int = 4,
) -> int:
    """Run LLM matching, print each hit to stdout, and save immediately. Returns total saved."""
    saved = 0
    pending = [cl for cl in unmatched if cl["id"] not in already_matched]
    total = len(pending)
    candidate_index = TitleVectorIndex(toolzone_listings)
    brand_token_map = _build_brand_token_map(toolzone_listings)
    expected_lookup = _build_expected_candidate_lookup(toolzone_listings) if debug_failures else None

    print(f"Vector retrieval backend: {candidate_index.backend_description}")

    jobs: list[dict] = []
    for done, (cl, raw_scored_candidates) in enumerate(
        zip(pending, candidate_index.search_many_with_scores(pending, limit=_RAW_VECTOR_CANDIDATE_LIMIT)),
        start=1,
    ):
        scored_candidates = _lexical_shortlist(
            cl,
            raw_scored_candidates,
            brand_token_map=brand_token_map,
            limit=_LLM_CANDIDATE_LIMIT,
            min_limit=_LLM_MIN_CANDIDATE_LIMIT,
        )
        candidates = [candidate for candidate, _score in scored_candidates]
        if not candidates:
            print(f"  [{done}/{total}] {cl['competitor_id']}  no candidates — skip")
            if debug_failures:
                _print_failure_debug(
                    cl,
                    scored_candidates,
                    expected_candidate=_infer_expected_candidate(expected_lookup, cl) if expected_lookup else None,
                    brand_token_map=brand_token_map,
                )
            continue

        jobs.append(
            {
                "ordinal": done,
                "total": total,
                "listing": cl,
                "scored_candidates": scored_candidates,
                "candidates": _rerank_by_brand(candidates, cl),
            }
        )

    worker_count = max(1, min(openai_workers, 4))
    should_parallelize = isinstance(llm_client, OpenAIClient) and worker_count > 1 and len(jobs) > 1
    if should_parallelize:
        print(f"OpenAI concurrency: {worker_count} workers")
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_job = {
                executor.submit(_llm_match_one, job, llm_client=llm_client): job
                for job in jobs
            }
            for future in as_completed(future_to_job):
                job = future_to_job[future]
                hit = future.result()
                saved += _handle_llm_match_result(
                    job,
                    hit,
                    factory=factory,
                    debug_failures=debug_failures,
                    expected_lookup=expected_lookup,
                    brand_token_map=brand_token_map,
                )
        return saved

    for job in jobs:
        hit = _llm_match_one(job, llm_client=llm_client)
        saved += _handle_llm_match_result(
            job,
            hit,
            factory=factory,
            debug_failures=debug_failures,
            expected_lookup=expected_lookup,
            brand_token_map=brand_token_map,
        )

    return saved


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def _print_report(factory, manufacturer: str | None) -> None:
    """Print a price-comparison table for all ToolZone products that have at
    least one competitor match, grouped by product."""
    with factory() as session:
        rows = session.execute(
            text("""
                SELECT
                    tz.id            AS tz_id,
                    tz.brand         AS brand,
                    tz.title         AS title,
                    tz.ean           AS ean,
                    tz.price_eur     AS tz_price,
                    tz.in_stock      AS tz_stock,
                    cl.competitor_id AS competitor_id,
                    cl.price_eur     AS comp_price,
                    cl.in_stock      AS comp_stock,
                    lm.match_type    AS match_type,
                    cl.url           AS comp_url
                FROM listing_matches lm
                JOIN competitor_listings tz ON tz.id = lm.toolzone_listing_id
                JOIN competitor_listings cl ON cl.id = lm.competitor_listing_id
                WHERE tz.competitor_id = 'toolzone_sk'
                  AND (:brand IS NULL OR LOWER(tz.brand) LIKE :brand)
                ORDER BY tz.brand, tz.title, cl.competitor_id
            """),
            {"brand": f"%{manufacturer.lower()}%" if manufacturer else None},
        ).fetchall()

    if not rows:
        print("No matches found.")
        return

    # Group by ToolZone product
    from collections import defaultdict
    products: dict[int, dict] = {}
    comp_prices: dict[int, list[dict]] = defaultdict(list)

    for r in rows:
        if r.tz_id not in products:
            products[r.tz_id] = {
                "brand": r.brand,
                "title": r.title,
                "ean": r.ean,
                "tz_price": float(r.tz_price) if r.tz_price else None,
                "tz_stock": r.tz_stock,
            }
        comp_prices[r.tz_id].append({
            "competitor_id": r.competitor_id,
            "price": float(r.comp_price) if r.comp_price else None,
            "stock": r.comp_stock,
            "match_type": r.match_type,
        })

    # Collect all competitor IDs that appear
    all_competitors = sorted({
        c["competitor_id"]
        for comps in comp_prices.values()
        for c in comps
    })

    # Header
    col_w = 14
    title_w = 52
    header = f"{'Product':<{title_w}}  {'EAN':<14}  {'TZ €':>7}  " + \
             "  ".join(f"{_disp(c):>{col_w}}" for c in all_competitors)
    sep = "-" * len(header)
    print()
    print(sep)
    if manufacturer:
        print(f"  Price comparison — {manufacturer.upper()}")
    print(sep)
    print(header)
    print(sep)

    total = len(products)
    cheaper_count = 0
    dearer_count = 0

    for tz_id, prod in products.items():
        tz_price = prod["tz_price"]
        title = prod["title"] or ""
        if len(title) > title_w:
            title = title[:title_w - 1] + "…"

        ean_str = (prod["ean"] or "")[:14]
        tz_str = f"{tz_price:.2f}" if tz_price else "—"

        # Build per-competitor cells
        comp_map: dict[str, dict] = {c["competitor_id"]: c for c in comp_prices[tz_id]}
        cells = []
        for cid in all_competitors:
            c = comp_map.get(cid)
            if not c or c["price"] is None:
                cells.append(f"{'—':>{col_w}}")
            else:
                p = c["price"]
                if tz_price:
                    diff = (p - tz_price) / tz_price * 100
                    if diff > 0.5:
                        tag = f"+{diff:.0f}%"
                        cheaper_count += 1
                    elif diff < -0.5:
                        tag = f"{diff:.0f}%"
                        dearer_count += 1
                    else:
                        tag = "≈"
                    cell = f"{p:.2f}({tag})"
                else:
                    cell = f"{p:.2f}"
                cells.append(f"{cell:>{col_w}}")

        row = f"{title:<{title_w}}  {ean_str:<14}  {tz_str:>7}  " + "  ".join(cells)
        print(row)

    print(sep)
    print(f"  {total} products  |  {cheaper_count} competitor prices above TZ  |  {dearer_count} below TZ")
    print(sep)
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(
    manufacturer: str | None = None,
    use_llm: bool = False,
    only: list[str] | None = None,
    force: bool = False,
    scrape: bool = False,
    report: bool = True,
    debug_failures: bool = False,
    openai_workers: int = 4,
) -> dict:
    """Run optional scrape → match → report pipeline.

    Args:
        manufacturer: Brand name filter (e.g. 'knipex'). None = all.
        use_llm:      Enable LLM fallback for EAN-unmatched listings.
        only:         Restrict to these competitor IDs.
        force:        Re-match listings that already have a match record.
        scrape:       Run manufacturer scraping step before matching.
        report:       Print price-comparison table after matching.
    """
    settings = Settings()
    factory = make_session_factory(settings)

    # Ensure listing_matches table exists
    from agnaradie_pricing.db.models import Base
    from agnaradie_pricing.db.session import make_engine
    Base.metadata.create_all(make_engine(settings))

    # --- Step 0: optional scrape ---
    if scrape:
        if not manufacturer:
            logger.error("--scrape requires --manufacturer (cannot scrape all brands at once)")
            sys.exit(1)
        logger.info("=== Step 0: Scraping %s across all competitors ===", manufacturer)
        import manufacturer_scrape
        scrape_counts = manufacturer_scrape.main(
            manufacturer_slug=manufacturer,
            brand_name=manufacturer.replace("-", " ").title(),
            only=only,
        )
        logger.info("Scrape complete: %s", scrape_counts)

    # --- Step 1: load data ---
    with factory() as session:
        logger.info("Loading ToolZone reference listings (manufacturer=%s) …", manufacturer or "all")
        toolzone = _load_toolzone_listings(session, manufacturer)
        logger.info("  %d ToolZone listings loaded", len(toolzone))

        logger.info("Loading competitor listings …")
        competitors = _load_competitor_listings(session, manufacturer, only)
        logger.info("  %d competitor listings loaded", len(competitors))

        already_matched: set[int] = set() if force else _already_matched_ids(session)
        logger.info("  %d listings already matched (skipping)", len(already_matched))

    if not toolzone:
        logger.warning("No ToolZone listings found — nothing to match against.")
        return {"ean_matches": 0, "llm_matches": 0, "total_saved": 0}

    # Infer missing brand fields for single-brand competitors
    competitors = _apply_brand_inference(competitors)

    # --- Step 2: EAN matching ---
    logger.info("=== Step 1: EAN matching ===")
    ean_records, unmatched = _ean_match(toolzone, competitors, already_matched)
    logger.info("  EAN matches: %d  |  unmatched: %d", len(ean_records), len(unmatched))

    with factory() as session:
        ean_saved = _save_matches(session, ean_records)
    logger.info("  Saved %d EAN match records", ean_saved)

    # --- Step 3: deterministic MPN / regex matching ---
    det_saved = 0
    if unmatched:
        logger.info("=== Step 2: Deterministic MPN + regex matching ===")
        det_records, unmatched = _deterministic_mpn_match(toolzone, unmatched, already_matched)
        logger.info("  MPN/regex matches: %d  |  still unmatched: %d", len(det_records), len(unmatched))
        with factory() as session:
            det_saved = _save_matches(session, det_records)
        logger.info("  Saved %d MPN/regex match records", det_saved)

    llm_saved = 0

    # --- Step 4: LLM matching (optional) ---
    if use_llm and unmatched:
        api_key = settings.openai_api_key
        if not api_key:
            import os
            api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.error("OPENAI_API_KEY not set — skipping LLM matching.")
        else:
            model = settings.openai_model
            logger.info("=== Step 3: LLM matching (%s) for %d listings ===", model, len(unmatched))
            llm_client = OpenAIClient(api_key=api_key, model=model)
            with factory() as session:
                already_matched_updated = _already_matched_ids(session)
            llm_saved = _llm_match(
                toolzone, unmatched, llm_client, factory,
                already_matched=already_matched_updated,
                debug_failures=debug_failures,
                openai_workers=openai_workers,
            )
            logger.info("  LLM total saved: %d", llm_saved)

    total_saved = ean_saved + det_saved + llm_saved

    # --- Step 4: match summary ---
    with factory() as session:
        summary_rows = session.execute(
            text("""
                SELECT cl.competitor_id, lm.match_type, count(*) c
                FROM listing_matches lm
                JOIN competitor_listings cl ON cl.id = lm.competitor_listing_id
                WHERE (:brand IS NULL OR LOWER(cl.brand) LIKE :brand)
                GROUP BY cl.competitor_id, lm.match_type
                ORDER BY cl.competitor_id, lm.match_type
            """),
            {"brand": f"%{manufacturer.lower()}%" if manufacturer else None},
        ).fetchall()

    logger.info("\n=== Match summary%s ===", f" — {manufacturer}" if manufacturer else "")
    by_competitor: dict[str, dict] = {}
    for row in summary_rows:
        by_competitor.setdefault(row.competitor_id, {})[row.match_type] = row.c
    for cid, types in sorted(by_competitor.items()):
        parts = ", ".join(f"{t}: {n}" for t, n in sorted(types.items()))
        total_c = sum(types.values())
        logger.info("  %-28s  total: %4d  (%s)", _disp(cid), total_c, parts)

    # --- Step 5: price-comparison report ---
    if report:
        _print_report(factory, manufacturer)

    return {
        "ean_matches": ean_saved,
        "det_matches": det_saved,
        "llm_matches": llm_saved,
        "total_saved": total_saved,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape → match → report pipeline for a manufacturer.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python jobs/match_products.py --manufacturer knipex --scrape --llm
  python jobs/match_products.py --manufacturer knipex --llm
  python jobs/match_products.py --manufacturer knipex --only boukal_cz bo_import_cz
  python jobs/match_products.py --manufacturer knipex --force
        """,
    )
    parser.add_argument(
        "--manufacturer",
        metavar="BRAND",
        default=None,
        help="Brand to scrape/match (e.g. knipex). Omit for all brands.",
    )
    parser.add_argument(
        "--scrape",
        action="store_true",
        help="Run manufacturer_scrape.py before matching (requires --manufacturer).",
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        help="Enable LLM fallback for listings without an EAN match.",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        metavar="COMPETITOR_ID",
        help="Restrict scraping/matching to these competitor IDs.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-match listings that already have a match record.",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="Skip the price-comparison table at the end.",
    )
    parser.add_argument(
        "--debug-failures",
        action="store_true",
        help="For each LLM miss, print the normalized query, top 10 retrieved candidates, scores, and shortlist presence diagnostics.",
    )
    parser.add_argument(
        "--openai-workers",
        type=int,
        default=4,
        help="Parallel OpenAI verification workers for match_products.py (1-4, OpenAI only).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = main(
        manufacturer=args.manufacturer,
        use_llm=args.llm,
        only=args.only,
        force=args.force,
        scrape=args.scrape,
        report=not args.no_report,
        debug_failures=args.debug_failures,
        openai_workers=args.openai_workers,
    )
    print(
        f"\nNew matches saved: {result['ean_matches']} EAN"
        f" + {result['det_matches']} MPN/regex"
        f" + {result['llm_matches']} LLM"
        f" = {result['total_saved']} total"
    )
