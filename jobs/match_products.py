"""Standalone product-matching job.

Matches ToolZone reference listings against all other competitor listings and
saves results to the `listing_matches` table.  Optionally runs the full scrape
pipeline first so the whole flow is a single command.

Matching layers (in order, first hit wins):
  1. exact_ean   — EAN identical on both sides                  confidence 1.00
  2. exact_mpn   — brand + MPN identical                        confidence 1.00
  3. mpn_no_brand— MPN identical, listing has no brand          confidence 0.90
  4. regex_ean   — EAN extracted from listing title text        confidence 0.95
  5. llm_fuzzy   — vector top-20 + OpenAI verification (opt-in) confidence 0.75–0.84

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

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from decimal import Decimal
from sqlalchemy import text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from agnaradie_pricing.catalogue.normalise import normalise_brand, normalise_ean, normalise_mpn
from agnaradie_pricing.db.models import ListingMatch
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.matching.llm_matcher import OpenAIClient, find_best_llm_match
from agnaradie_pricing.matching.vector_search import TitleVectorIndex
from agnaradie_pricing.settings import Settings

logger = logging.getLogger(__name__)

_MIN_LLM_CONFIDENCE = 0.75
_LLM_CANDIDATE_LIMIT = 20

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


def _llm_match(
    toolzone_listings: list[dict],
    unmatched: list[dict],
    llm_client: OpenAIClient,
    factory,
    *,
    already_matched: set[int],
) -> int:
    """Run LLM matching, print each hit to stdout, and save immediately. Returns total saved."""
    saved = 0
    pending = [cl for cl in unmatched if cl["id"] not in already_matched]
    total = len(pending)
    candidate_index = TitleVectorIndex(toolzone_listings)

    for done, (cl, candidates) in enumerate(
        zip(pending, candidate_index.search_many(pending, limit=_LLM_CANDIDATE_LIMIT)),
        start=1,
    ):
        if not candidates:
            print(f"  [{done}/{total}] {cl['competitor_id']}  no candidates — skip")
            continue

        candidates = _rerank_by_brand(candidates, cl)
        hit = find_best_llm_match(cl, candidates, llm_client=llm_client)
        if hit is None or hit[1][1] < _MIN_LLM_CONFIDENCE:
            print(f"  [{done}/{total}] {cl['competitor_id']}  no match for: {cl['title'][:60]}")
            continue

        matched_product, (match_type, confidence) = hit
        record = {
            "toolzone_listing_id": matched_product["id"],
            "competitor_listing_id": cl["id"],
            "match_type": match_type,
            "confidence": Decimal(str(round(confidence, 2))),
        }
        with factory() as session:
            n = _save_matches(session, [record])
        saved += n

        status = "✓ saved" if n else "· duplicate"
        print(
            f"  [{done}/{total}] {status}  [{_disp(cl['competitor_id'])}] {cl['title'][:50]}"
            f"\n           → [TZ] {matched_product['title'][:50]}  conf={confidence:.2f}"
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
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = main(
        manufacturer=args.manufacturer,
        use_llm=args.llm,
        only=args.only,
        force=args.force,
        scrape=args.scrape,
        report=not args.no_report,
    )
    print(
        f"\nNew matches saved: {result['ean_matches']} EAN"
        f" + {result['det_matches']} MPN/regex"
        f" + {result['llm_matches']} LLM"
        f" = {result['total_saved']} total"
    )
