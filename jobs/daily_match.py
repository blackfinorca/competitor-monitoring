"""Daily matching entrypoint.

Runs the full matching pipeline against all competitor_listings that don't
yet have a product_match row:

  Layer 1–5 (always): exact EAN, exact MPN, regex MPN/EAN from titles.
  Layer 6 (opt-in):   LLM fuzzy matching via --llm flag.

Usage
-----
    python -m jobs.daily_match              # layers 1–5 only
    python -m jobs.daily_match --llm        # + LLM fuzzy layer
    python -m jobs.daily_match --llm --min-confidence 0.80
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import argparse
import logging
from decimal import Decimal

from sqlalchemy import select

from agnaradie_pricing.db.models import CompetitorListing, Product, ProductMatch
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.matching import match_product, match_product_bulk
from agnaradie_pricing.settings import Settings

logger = logging.getLogger(__name__)


def main(argv=None) -> dict[str, int]:
    parser = argparse.ArgumentParser(description="Run product matching pipeline.")
    parser.add_argument(
        "--llm",
        action="store_true",
        help="Enable LLM fuzzy layer (layer 6) for remaining unmatched listings.",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.75,
        metavar="FLOAT",
        help="Minimum confidence to accept an LLM match (default: 0.75).",
    )
    args = parser.parse_args(argv)

    settings = Settings()
    factory = make_session_factory(settings)
    counts = {"matched": 0, "llm_matched": 0, "skipped": 0, "already_matched": 0}

    with factory() as session:
        products = session.scalars(select(Product)).all()
        product_list = [
            {"id": p.id, "brand": p.brand, "mpn": p.mpn, "ean": p.ean, "title": p.title}
            for p in products
        ]

        already_matched_skus: set[tuple] = set(
            session.execute(
                select(ProductMatch.competitor_id, ProductMatch.competitor_sku)
            ).all()
        )

        listings = session.scalars(select(CompetitorListing)).all()

        unmatched_for_llm: list[dict] = []

        # -----------------------------------------------------------------
        # Layers 1–5: deterministic + regex
        # -----------------------------------------------------------------
        for listing in listings:
            key = (listing.competitor_id, listing.competitor_sku)
            if key in already_matched_skus:
                counts["already_matched"] += 1
                continue

            listing_dict = {
                "brand": listing.brand,
                "mpn": listing.mpn,
                "ean": listing.ean,
                "title": listing.title,
            }

            best_match = None
            best_result = None
            for product in product_list:
                result = match_product(product, listing_dict)
                if result is not None:
                    if best_result is None or result[1] > best_result[1]:
                        best_match = product
                        best_result = result
                    if result[1] == 1.0:
                        break

            if best_match is None:
                if args.llm:
                    unmatched_for_llm.append({
                        "id": listing.id,
                        "brand": listing.brand,
                        "mpn": listing.mpn,
                        "ean": listing.ean,
                        "title": listing.title,
                        "competitor_id": listing.competitor_id,
                        "competitor_sku": listing.competitor_sku,
                    })
                counts["skipped"] += 1
                continue

            _save_match(session, best_match, listing, best_result)
            counts["matched"] += 1
            logger.debug(
                "Matched %s (%s) → product_id=%d via %s",
                listing.title[:50], listing.competitor_id,
                best_match["id"], best_result[0],
            )

        session.commit()

        # -----------------------------------------------------------------
        # Layer 6: LLM fuzzy (opt-in)
        # -----------------------------------------------------------------
        if args.llm and unmatched_for_llm:
            if not settings.openai_api_key:
                logger.error(
                    "OPENAI_API_KEY not set — cannot run LLM layer. "
                    "Add it to your .env file."
                )
            else:
                from agnaradie_pricing.matching.llm_matcher import OpenAIClient
                client = OpenAIClient(
                    api_key=settings.openai_api_key,
                    model=settings.openai_model,
                )

                logger.info(
                    "LLM layer: processing %d unmatched listings …",
                    len(unmatched_for_llm),
                )

                def _on_match(listing_id, product, result):
                    logger.debug(
                        "LLM matched listing_id=%d → product_id=%d  conf=%.2f",
                        listing_id, product["id"], result[1],
                    )

                llm_results = match_product_bulk(
                    unmatched_for_llm,
                    product_list,
                    llm_client=client,
                    on_match=_on_match,
                )

                # Apply min-confidence filter and save
                listing_by_id = {l["id"]: l for l in unmatched_for_llm}
                for listing_id, (matched_product, match_result) in llm_results.items():
                    if match_result[1] < args.min_confidence:
                        continue
                    raw_listing = listing_by_id[listing_id]
                    _save_match_raw(session, matched_product, raw_listing, match_result)
                    counts["llm_matched"] += 1

                session.commit()

    logger.info(
        "Matching complete: %d deterministic, %d LLM, %d skipped, %d already matched",
        counts["matched"], counts["llm_matched"],
        counts["skipped"], counts["already_matched"],
    )
    return counts


def _save_match(session, product: dict, listing, result: tuple) -> None:
    match_type, confidence = result
    existing = session.scalars(
        select(ProductMatch).where(
            ProductMatch.ag_product_id == product["id"],
            ProductMatch.competitor_id == listing.competitor_id,
            ProductMatch.competitor_sku == listing.competitor_sku,
        )
    ).first()
    if existing is None:
        session.add(ProductMatch(
            ag_product_id=product["id"],
            competitor_id=listing.competitor_id,
            competitor_sku=listing.competitor_sku,
            match_type=match_type,
            confidence=Decimal(str(confidence)),
        ))


def _save_match_raw(session, product: dict, listing_dict: dict, result: tuple) -> None:
    match_type, confidence = result
    existing = session.scalars(
        select(ProductMatch).where(
            ProductMatch.ag_product_id == product["id"],
            ProductMatch.competitor_id == listing_dict["competitor_id"],
            ProductMatch.competitor_sku == listing_dict["competitor_sku"],
        )
    ).first()
    if existing is None:
        session.add(ProductMatch(
            ag_product_id=product["id"],
            competitor_id=listing_dict["competitor_id"],
            competitor_sku=listing_dict["competitor_sku"],
            match_type=match_type,
            confidence=Decimal(str(confidence)),
        ))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = main()
    print(f"  Deterministic:   {result['matched']}")
    print(f"  LLM fuzzy:       {result['llm_matched']}")
    print(f"  Skipped:         {result['skipped']}")
    print(f"  Already matched: {result['already_matched']}")
