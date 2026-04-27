"""Search orchestrator — cache-first, live-fetch fallback.

Flow
----
1. Classify query (EAN / MPN / text).
2. Cache lookup in ``products`` table (EAN → MPN+brand → title LIKE).
   If the product AND its competitor listings are fresh (< CACHE_MAX_AGE_HOURS)
   the result is returned immediately without any HTTP requests.
3. Live fetch from ToolZone.sk using ``toolzone_scraper.search_by_query()``.
   The result is upserted into ``products`` and saved as a ``toolzone_sk``
   competitor listing.
4. For each competitor scraper: try EAN search first, then brand+MPN, then
   full title fragment.  Each hit is saved to ``competitor_listings``.
5. Run the full matching pipeline (deterministic → regex → LLM) for every
   new listing against the reference product.
6. Return a ``SearchResult`` dataclass ready for the dashboard to render.

LLM usage
---------
  * Layer 3 matching (fuzzy) — runs automatically when ``llm_client`` is set.
  * If a scraper returns no result even after all three search strategies the
    orchestrator records an error in ``SearchResult.errors`` but does NOT call
    the LLM to generate web searches (out of scope for now).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Callable
from urllib.parse import urlparse

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from agnaradie_pricing.catalogue.normalise import fold_diacritics, normalise_ean
from agnaradie_pricing.db.models import (
    ClusterMember,
    CompetitorListing as DBListing,
    Product,
    ProductCluster,
    ProductMatch,
)
from agnaradie_pricing.matching import match_product
from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.persistence import save_competitor_listings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CACHE_MAX_AGE_HOURS = 24 * 30  # 30-day freshness threshold for both products and listings
_MPN_RE = re.compile(r"^\d{2}[-\s]\d{2}[-\s]\d{3}$|^[A-Z0-9]{2,}(?:[-/][A-Z0-9]+)+$", re.I)
_TEXT_TOKEN_RE = re.compile(r"[a-z0-9]+")


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class SearchResult:
    """Structured result returned to the dashboard."""

    product: Product | None = None
    """The ToolZone reference product (from cache or live fetch)."""

    tz_listing: DBListing | None = None
    """Latest ToolZone competitor_listing row (for price / freshness)."""

    competitor_hits: list[DBListing] = field(default_factory=list)
    """All fresh competitor listings found in this search (non-ToolZone)."""

    matches: list[ProductMatch] = field(default_factory=list)
    """ProductMatch records written/updated during this search."""

    match_info: dict[tuple[str, str | None], tuple[str, float]] = field(default_factory=dict)
    """Display match metadata keyed by (competitor_id, competitor_sku)."""

    from_cache: bool = False
    """True when the result was served entirely from the DB without HTTP calls."""

    errors: dict[str, str] = field(default_factory=dict)
    """competitor_id → human-readable error string for anything that failed."""

    query: str = ""
    """Original search query for display purposes."""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def search_product(
    query: str,
    session: Session,
    *,
    competitor_scrapers: dict[str, CompetitorScraper],
    toolzone_scraper,
    llm_client=None,
    force_refresh: bool = False,
    on_progress: Callable[[str], None] | None = None,
) -> SearchResult:
    """Search for a product, using the DB as a cache and live scrapers as source.

    Parameters
    ----------
    query               EAN, MPN, or free-text product name.
    session             Open SQLAlchemy session.
    competitor_scrapers {competitor_id: scraper_instance} for every live competitor.
    toolzone_scraper    ToolZoneScraper instance for the reference store.
    llm_client          Optional QwenClient; enables LLM fuzzy matching (layer 3).
    force_refresh       Bypass cache and re-fetch everything from the web.
    on_progress         Optional callback(message) for UI progress updates.
    """
    query = _normalise_search_query(query)
    result = SearchResult(query=query)

    def _progress(msg: str) -> None:
        logger.info(msg)
        if on_progress:
            on_progress(msg)

    # ------------------------------------------------------------------
    # Step 1 — cache check
    # ------------------------------------------------------------------
    _progress("Checking local database cache…")
    product = _find_product(query, session)

    if product and not force_refresh:
        age = _product_age_hours(product)
        if age is not None and age < CACHE_MAX_AGE_HOURS:
            # Check whether competitor data is also fresh
            comp_listings = _latest_competitor_listings(product.id, session)
            if comp_listings:
                result.product = product
                result.tz_listing = _latest_tz_listing(product.id, session)
                result.competitor_hits = comp_listings
                result.matches = _product_matches(product.id, session)
                result.match_info = _product_match_info(product.id, session)
                result.from_cache = True
                _progress(f"Cache hit — {len(comp_listings)} competitor listings (< {CACHE_MAX_AGE_HOURS}h old)")
                return result

    # ------------------------------------------------------------------
    # Step 2 — live fetch from ToolZone
    # ------------------------------------------------------------------
    _progress("Searching ToolZone.sk…")
    tz_listing: CompetitorListing | None = None
    try:
        tz_listing = toolzone_scraper.search_by_query(query)
    except Exception as exc:
        logger.warning("ToolZone search failed: %s", exc)
        result.errors["toolzone_sk"] = str(exc)

    # ToolZone's search results are JS-rendered (AJAX), so search_by_query often
    # returns None even for products that exist. If we already know the product
    # from a previous scrape, fetch its stored product-page URL directly — detail
    # pages serve full JSON-LD and are not AJAX-dependent.
    if tz_listing is None and product is not None:
        stored_tz = _latest_tz_listing(product.id, session)
        if stored_tz and stored_tz.url:
            try:
                tz_listing = toolzone_scraper._scrape_product_page(stored_tz.url)
                if tz_listing:
                    _progress(f"Refreshed ToolZone price from stored URL")
            except Exception as exc:
                logger.debug("Direct ToolZone URL fetch failed: %s", exc)

    if tz_listing is None and product is None:
        # Nothing on ToolZone and nothing in cache — dead end
        _progress("No results found on ToolZone.sk and nothing in cache.")
        return result

    if tz_listing is not None:
        _progress(f"Found on ToolZone: {tz_listing.title[:60]}")
        product = _upsert_product(tz_listing, session)
        session.flush()
        _save_toolzone_listing(tz_listing, session)
        session.flush()

    result.product = product

    # ------------------------------------------------------------------
    # Step 3 — search each competitor
    # ------------------------------------------------------------------
    for cid, scraper in competitor_scrapers.items():
        _progress(f"Searching {cid}…")
        listing = _search_competitor(scraper, product, result.errors)
        if listing is None:
            continue
        try:
            rows = save_competitor_listings(session, [listing])
            session.flush()
            # Run matching immediately for this listing
            _match_and_save(product, listing, rows[0], session, llm_client=llm_client)
            session.flush()
        except Exception as exc:
            logger.warning("Failed to save listing from %s: %s", cid, exc)
            result.errors[cid] = str(exc)

    session.commit()

    # ------------------------------------------------------------------
    # Step 4 — build result from DB
    # ------------------------------------------------------------------
    result.tz_listing = _latest_tz_listing(product.id, session)
    result.competitor_hits = _latest_competitor_listings(product.id, session)
    result.matches = _product_matches(product.id, session)
    result.match_info = _product_match_info(product.id, session)
    result.from_cache = False

    _progress(f"Done — {len(result.competitor_hits)} competitor listings found.")
    return result


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _classify_query(query: str) -> str:
    """Return 'ean', 'mpn', or 'text'."""
    q = query.strip()
    if normalise_ean(q):
        return "ean"
    if _MPN_RE.match(q):
        return "mpn"
    return "text"


def _normalise_search_query(query: str) -> str:
    q = query.strip()
    compact_identifier = re.sub(r"[\s-]+", "", q)
    ean = normalise_ean(q) or normalise_ean(compact_identifier)
    if ean:
        return ean
    return re.sub(r"\s+", " ", q)


def _find_product(query: str, session: Session) -> Product | None:
    q = _normalise_search_query(query)
    kind = _classify_query(q)

    if kind == "ean":
        return _preferred_product_by_ean(q, session)

    if kind == "mpn":
        product = session.execute(
            select(Product).where(func.lower(Product.mpn) == q.lower()).limit(1)
        ).scalar_one_or_none()
        if product is None:
            # Query looks like a SKU (e.g. "TZ-7102200") — the "TZ-" prefix is a
            # store prefix, not part of the MPN, so also try an exact SKU lookup.
            product = session.execute(
                select(Product).where(func.lower(Product.sku) == q.lower()).limit(1)
            ).scalar_one_or_none()
        if product is not None and product.ean:
            return _preferred_product_by_ean(product.ean, session) or product
        return product

    return _find_product_by_text(q, session)


def _preferred_product_by_ean(ean: str, session: Session) -> Product | None:
    products = list(session.execute(
        select(Product)
        .where(Product.ean == ean)
        .order_by(Product.updated_at.desc(), Product.id.desc())
    ).scalars().all())
    if not products:
        return None

    product_by_id = {product.id: product for product in products}
    toolzone_product_id = session.execute(
        select(ProductMatch.ag_product_id)
        .where(
            ProductMatch.ag_product_id.in_(product_by_id),
            ProductMatch.competitor_id == "toolzone_sk",
        )
        .order_by(ProductMatch.created_at.desc(), ProductMatch.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if toolzone_product_id in product_by_id:
        return product_by_id[toolzone_product_id]

    return products[0]


def _find_product_by_text(query: str, session: Session) -> Product | None:
    tokens = _text_tokens(query)
    if not tokens:
        return None
    folded_query = fold_diacritics(query.lower())

    token_filters = []
    for token in tokens:
        pattern = f"%{token}%"
        token_filters.append(
            or_(
                func.lower(Product.title).like(pattern),
                func.lower(Product.brand).like(pattern),
                func.lower(Product.mpn).like(pattern),
                func.lower(Product.sku).like(pattern),
            )
        )

    candidates = list(session.execute(
        select(Product)
        .where(or_(*token_filters))
        .order_by(Product.updated_at.desc(), Product.id.desc())
        .limit(200)
    ).scalars().all())

    best_product, best_score = _best_text_product(candidates, tokens, folded_query)

    if best_score is None or best_score[0] == 0 or (len(tokens) > 1 and best_score[2] == 0):
        all_products = list(session.execute(
            select(Product).order_by(Product.updated_at.desc(), Product.id.desc())
        ).scalars().all())
        best_product, best_score = _best_text_product(all_products, tokens, folded_query)

    if best_product is not None and best_product.ean:
        return _preferred_product_by_ean(best_product.ean, session) or best_product
    return best_product


def _best_text_product(
    products: list[Product],
    tokens: list[str],
    folded_query: str,
) -> tuple[Product | None, tuple[int, int, int, int] | None]:
    best_product: Product | None = None
    best_score: tuple[int, int, int, int] | None = None
    for product in products:
        haystack = _product_text_haystack(product)
        haystack_tokens = set(_TEXT_TOKEN_RE.findall(haystack))
        matched = sum(1 for token in tokens if token in haystack_tokens)
        if matched == 0:
            continue
        all_tokens_matched = int(matched == len(tokens))
        phrase_matched = int(folded_query in haystack)
        adjacent_matched = sum(
            1
            for left, right in zip(tokens, tokens[1:])
            if f"{left} {right}" in haystack
        )
        score = (all_tokens_matched, phrase_matched, adjacent_matched, matched)
        if best_score is None or score > best_score:
            best_product = product
            best_score = score
    return best_product, best_score


def _text_tokens(query: str) -> list[str]:
    folded = fold_diacritics(query.lower())
    return _TEXT_TOKEN_RE.findall(folded)


def _product_text_haystack(product: Product) -> str:
    return fold_diacritics(
        " ".join(
            str(value or "").lower()
            for value in (product.title, product.brand, product.mpn, product.sku, product.ean)
        )
    )


def _product_age_hours(product: Product) -> float | None:
    if product.updated_at is None:
        return None
    now = datetime.now(UTC)
    updated = product.updated_at
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=UTC)
    return (now - updated).total_seconds() / 3600


def _latest_competitor_listings(product_id: int, session: Session) -> list[DBListing]:
    """Return fresh non-ToolZone listings for the product, newest first."""
    cutoff = datetime.now(UTC) - timedelta(hours=CACHE_MAX_AGE_HOURS)
    cluster_rows = _latest_cluster_listings(
        product_id,
        session,
        include_toolzone=False,
        cutoff=cutoff,
    )
    if cluster_rows:
        return cluster_rows

    rows = session.execute(
        select(DBListing)
        .join(
            ProductMatch,
            (ProductMatch.competitor_id == DBListing.competitor_id)
            & (
                (ProductMatch.competitor_sku == DBListing.competitor_sku)
                | (ProductMatch.competitor_sku.is_(None) & DBListing.competitor_sku.is_(None))
            ),
        )
        .where(
            ProductMatch.ag_product_id == product_id,
            DBListing.competitor_id != "toolzone_sk",
            DBListing.scraped_at >= cutoff,
        )
        .order_by(DBListing.scraped_at.desc())
    ).scalars().all()

    # Deduplicate: one row per competitor_id
    seen: set[str] = set()
    result: list[DBListing] = []
    for row in rows:
        if row.competitor_id not in seen:
            seen.add(row.competitor_id)
            result.append(row)
    return result


def _latest_tz_listing(product_id: int, session: Session) -> DBListing | None:
    cluster_rows = _latest_cluster_listings(
        product_id,
        session,
        include_toolzone=True,
        only_toolzone=True,
    )
    if cluster_rows:
        return cluster_rows[0]

    return session.execute(
        select(DBListing)
        .join(
            ProductMatch,
            (ProductMatch.competitor_id == DBListing.competitor_id)
            & (
                (ProductMatch.competitor_sku == DBListing.competitor_sku)
                | (ProductMatch.competitor_sku.is_(None) & DBListing.competitor_sku.is_(None))
            ),
        )
        .where(
            ProductMatch.ag_product_id == product_id,
            DBListing.competitor_id == "toolzone_sk",
        )
        .order_by(DBListing.scraped_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def _latest_cluster_listings(
    product_id: int,
    session: Session,
    *,
    include_toolzone: bool,
    only_toolzone: bool = False,
    cutoff: datetime | None = None,
) -> list[DBListing]:
    product = session.get(Product, product_id)
    if product is None or not product.ean:
        return []

    filters = [
        ProductCluster.ean == product.ean,
        ClusterMember.status == "approved",
    ]
    if only_toolzone:
        filters.append(DBListing.competitor_id == "toolzone_sk")
    elif not include_toolzone:
        filters.append(DBListing.competitor_id != "toolzone_sk")
    if cutoff is not None:
        filters.append(DBListing.scraped_at >= cutoff)

    return list(session.execute(
        select(DBListing)
        .join(ClusterMember, ClusterMember.listing_id == DBListing.id)
        .join(ProductCluster, ProductCluster.id == ClusterMember.cluster_id)
        .where(*filters)
        .order_by(DBListing.scraped_at.desc(), DBListing.id.desc())
    ).scalars().all())


def _product_matches(product_id: int, session: Session) -> list[ProductMatch]:
    return list(session.execute(
        select(ProductMatch).where(ProductMatch.ag_product_id == product_id)
    ).scalars().all())


def _product_match_info(product_id: int, session: Session) -> dict[tuple[str, str | None], tuple[str, float]]:
    info: dict[tuple[str, str | None], tuple[str, float]] = {
        (pm.competitor_id, pm.competitor_sku): (pm.match_type, float(pm.confidence))
        for pm in _product_matches(product_id, session)
    }

    product = session.get(Product, product_id)
    if product is None or not product.ean:
        return info

    rows = session.execute(
        select(DBListing.competitor_id, DBListing.competitor_sku, ClusterMember.match_method,
               ClusterMember.similarity, ClusterMember.llm_confidence)
        .join(ClusterMember, ClusterMember.listing_id == DBListing.id)
        .join(ProductCluster, ProductCluster.id == ClusterMember.cluster_id)
        .where(
            ProductCluster.ean == product.ean,
            ClusterMember.status == "approved",
        )
    ).fetchall()

    method_map = {
        "ean": "exact_ean",
        "vector_llm": "llm_fuzzy",
        "manual": "manual",
    }
    for row in rows:
        confidence = row.llm_confidence or row.similarity
        if row.match_method == "ean" and confidence is None:
            confidence = Decimal("1.00")
        info[(row.competitor_id, row.competitor_sku)] = (
            method_map.get(row.match_method, row.match_method),
            float(confidence) if confidence is not None else 0.0,
        )
    return info


# ---------------------------------------------------------------------------
# ToolZone product upsert
# ---------------------------------------------------------------------------

def _upsert_product(listing: CompetitorListing, session: Session) -> Product:
    """Find or create a Product row from a ToolZone listing."""
    existing: Product | None = None

    # Prefer EAN match (most reliable)
    if listing.ean:
        existing = session.execute(
            select(Product).where(Product.ean == listing.ean).limit(1)
        ).scalar_one_or_none()

    # Fall back to brand + MPN
    if existing is None and listing.brand and listing.mpn:
        existing = session.execute(
            select(Product).where(
                func.lower(Product.brand) == listing.brand.lower(),
                func.lower(Product.mpn) == listing.mpn.lower(),
            ).limit(1)
        ).scalar_one_or_none()

    # Fall back to title match (limit 1 — titles are not guaranteed unique)
    if existing is None and listing.title:
        existing = session.execute(
            select(Product)
            .where(func.lower(Product.title) == listing.title.lower())
            .limit(1)
        ).scalar_one_or_none()

    if existing:
        # Refresh identifiers and price
        if listing.ean and not existing.ean:
            existing.ean = listing.ean
        if listing.mpn and not existing.mpn:
            existing.mpn = listing.mpn
        if listing.brand and not existing.brand:
            existing.brand = listing.brand
        if listing.price_eur:
            existing.price_eur = Decimal(str(listing.price_eur))
        existing.updated_at = datetime.now(UTC)
        return existing

    # Insert new product
    sku = _sku_from_listing(listing)
    product = Product(
        sku=sku,
        brand=listing.brand,
        mpn=listing.mpn,
        ean=listing.ean,
        title=listing.title,
        category="Ruční nářadí",
        price_eur=Decimal(str(listing.price_eur)) if listing.price_eur else None,
        updated_at=datetime.now(UTC),
    )
    session.add(product)
    return product


def _sku_from_listing(listing: CompetitorListing) -> str:
    """Derive a stable SKU string from a ToolZone listing."""
    if listing.url:
        path = urlparse(listing.url).path
        slug = path.strip("/").split("/")[-1]
        for ext in (".htm", ".html", "/"):
            slug = slug.replace(ext, "")
        if slug:
            return f"TZ-{slug[:80]}"
    if listing.ean:
        return f"TZ-EAN-{listing.ean}"
    if listing.mpn:
        return f"TZ-MPN-{listing.mpn}"
    return f"TZ-{abs(hash(listing.title)) % 999999}"


def _save_toolzone_listing(listing: CompetitorListing, session: Session) -> None:
    """Save ToolZone listing to competitor_listings and create a self-match."""
    from agnaradie_pricing.scrapers.persistence import save_competitor_listings

    save_competitor_listings(session, [listing])
    session.flush()

    # Find the product we just upserted
    product = _find_product_for_tz_listing(listing, session)
    if product is None:
        return

    # Upsert the self-match so the dashboard can find the ToolZone card
    existing_match = session.execute(
        select(ProductMatch).where(
            ProductMatch.ag_product_id == product.id,
            ProductMatch.competitor_id == "toolzone_sk",
            ProductMatch.competitor_sku == listing.competitor_sku,
        )
    ).scalar_one_or_none()

    if existing_match is None:
        session.add(ProductMatch(
            ag_product_id=product.id,
            competitor_id="toolzone_sk",
            competitor_sku=listing.competitor_sku,
            match_type="exact_ean" if listing.ean else "exact_mpn",
            confidence=Decimal("1.00"),
        ))


def _find_product_for_tz_listing(listing: CompetitorListing, session: Session) -> Product | None:
    if listing.ean:
        return session.execute(
            select(Product).where(Product.ean == listing.ean).limit(1)
        ).scalar_one_or_none()
    if listing.brand and listing.mpn:
        return session.execute(
            select(Product).where(
                func.lower(Product.brand) == listing.brand.lower(),
                func.lower(Product.mpn) == listing.mpn.lower(),
            ).limit(1)
        ).scalar_one_or_none()
    return session.execute(
        select(Product).where(func.lower(Product.title) == listing.title.lower()).limit(1)
    ).scalar_one_or_none()


# ---------------------------------------------------------------------------
# Competitor search strategy
# ---------------------------------------------------------------------------

def _search_competitor(
    scraper: CompetitorScraper,
    product: Product,
    errors: dict[str, str],
) -> CompetitorListing | None:
    """Try three strategies in order: EAN → brand+MPN → title fragment."""
    strategies: list[tuple[str, Any]] = []

    if product.ean:
        strategies.append(("EAN", lambda: scraper.search_by_query(product.ean)))
    if product.brand and product.mpn:
        strategies.append(("MPN", lambda: scraper.search_by_mpn(product.brand, product.mpn)))
    if product.title:
        fragment = f"{product.brand or ''} {product.title[:50]}".strip()
        strategies.append(("title", lambda f=fragment: scraper.search_by_query(f)))

    for strategy_name, fn in strategies:
        try:
            listing = fn()
            if listing:
                logger.debug(
                    "%s: found via %s — %s", scraper.competitor_id, strategy_name, listing.title[:50]
                )
                return listing
        except Exception as exc:
            logger.warning("%s search (%s) failed: %s", scraper.competitor_id, strategy_name, exc)
            errors[scraper.competitor_id] = f"{strategy_name}: {exc}"

    return None


# ---------------------------------------------------------------------------
# Matching helper
# ---------------------------------------------------------------------------

def _match_and_save(
    product: Product,
    listing: CompetitorListing,
    listing_row: DBListing,
    session: Session,
    *,
    llm_client=None,
) -> ProductMatch | None:
    """Run match_product and upsert result into product_matches."""
    product_dict: dict[str, Any] = {
        "id": product.id,
        "brand": product.brand,
        "mpn": product.mpn,
        "ean": product.ean,
        "title": product.title,
    }
    listing_dict: dict[str, Any] = {
        "brand": listing.brand,
        "mpn": listing.mpn,
        "ean": listing.ean,
        "title": listing.title,
    }

    match_result = match_product(product_dict, listing_dict, llm_client=llm_client)
    if match_result is None:
        return None

    match_type, confidence = match_result

    # Upsert
    existing = session.execute(
        select(ProductMatch).where(
            ProductMatch.ag_product_id == product.id,
            ProductMatch.competitor_id == listing.competitor_id,
            ProductMatch.competitor_sku == listing.competitor_sku,
        )
    ).scalar_one_or_none()

    if existing:
        existing.match_type = match_type
        existing.confidence = Decimal(str(confidence))
        return existing

    pm = ProductMatch(
        ag_product_id=product.id,
        competitor_id=listing.competitor_id,
        competitor_sku=listing.competitor_sku,
        match_type=match_type,
        confidence=Decimal(str(confidence)),
    )
    session.add(pm)
    return pm
