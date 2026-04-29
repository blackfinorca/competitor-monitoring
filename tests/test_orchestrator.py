from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from agnaradie_pricing import orchestrator
from agnaradie_pricing.db.models import (
    Base,
    CompetitorListing,
    Product,
    ProductMatch,
)


def test_cache_max_age_is_30_days() -> None:
    assert orchestrator.CACHE_MAX_AGE_HOURS == 24 * 30


def _session():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def test_find_product_normalises_pasted_ean_float_artifact() -> None:
    session = _session()
    product = Product(
        sku="TZ-EAN-4003773022022",
        brand="Knipex",
        mpn="87-01-250",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    session.add(product)
    session.commit()

    found = orchestrator._find_product("4003773022022.0", session)

    assert found is not None
    assert found.id == product.id


def test_find_product_matches_non_contiguous_name_tokens() -> None:
    session = _session()
    product = Product(
        sku="TZ-Cobra-250",
        brand="Knipex",
        mpn="87-01-250",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm s plastovou rukoväťou KNIPEX",
    )
    session.add(product)
    session.commit()

    found = orchestrator._find_product("KNIPEX Cobra 250", session)

    assert found is not None
    assert found.id == product.id


def test_find_product_name_search_prefers_adjacent_model_terms() -> None:
    session = _session()
    basic_cobra = Product(
        sku="TZ-Cobra-250",
        brand="Knipex",
        mpn="87-01-250",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm s plastovou rukoväťou KNIPEX",
    )
    quickset_cobra = Product(
        sku="TZ-QuickSet-250",
        brand="Knipex",
        mpn="87-22-250",
        ean="4003773077794",
        title="Kliešte SIKO Cobra QuickSet 8722250 s 2-zložkovými rukoväťami 250 mm KNIPEX",
    )
    session.add_all([basic_cobra, quickset_cobra])
    session.commit()

    found = orchestrator._find_product("KNIPEX Cobra 250", session)

    assert found is not None
    assert found.id == basic_cobra.id


def test_find_product_name_search_folds_diacritics_beyond_first_candidates() -> None:
    session = _session()
    for idx in range(250):
        session.add(
            Product(
                sku=f"TZ-FILLER-{idx}",
                brand="AMF",
                mpn=f"M16-{idx}",
                ean=f"100000000{idx:04d}",
                title=f"Nastavovacia matica M16 AMF filler {idx}",
            )
        )
    target = Product(
        sku="TZ-E7433840080",
        brand="AMF",
        mpn="E7433840080",
        ean="4020772082413",
        title="Šesťhranná matica DIN6330B M16 AMF",
    )
    session.add(target)
    session.commit()

    found = orchestrator._find_product("Sesthranna matica DIN6330B M16 AMF", session)

    assert found is not None
    assert found.id == target.id


def test_find_product_prefers_toolzone_reference_for_duplicate_ean() -> None:
    session = _session()
    ag_product = Product(
        sku="AG-KNIPEX-8701250",
        brand="KNIPEX",
        mpn="87-01-250",
        ean="4003773022022",
        title="KNIPEX Klieste instalaterske Cobra 8701250",
    )
    tz_product = Product(
        sku="TZ-E7455930250",
        brand="Knipex",
        mpn="E7455930250",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    session.add_all([ag_product, tz_product])
    session.flush()
    tz_listing = CompetitorListing(
        competitor_id="toolzone_sk",
        brand="Knipex",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
        price_eur=Decimal("22.05"),
        currency="EUR",
        url="https://toolzone.test/cobra",
        scraped_at=datetime.now(UTC),
    )
    session.add(tz_listing)
    session.flush()
    session.add(
        ProductMatch(
            listing_id=tz_listing.id,
            product_id=tz_product.id,
            match_type="exact_ean",
            confidence=Decimal("1.00"),
            status="approved",
        )
    )
    session.commit()

    found = orchestrator._find_product("87-01-250", session)

    assert found is not None
    assert found.id == tz_product.id


def test_latest_competitor_listings_filters_stale_legacy_matches() -> None:
    session = _session()
    product = Product(
        sku="TZ-Cobra-250",
        brand="Knipex",
        mpn="87-01-250",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    session.add(product)
    session.flush()
    stale_listing = CompetitorListing(
        competitor_id="example_sk",
        brand="Knipex",
        ean="4003773022022",
        title="Old Cobra",
        price_eur=Decimal("19.99"),
        currency="EUR",
        in_stock=True,
        url="https://example.test/old",
        scraped_at=datetime.now(UTC) - timedelta(hours=orchestrator.CACHE_MAX_AGE_HOURS + 1),
    )
    session.add(stale_listing)
    session.flush()
    session.add(
        ProductMatch(
            listing_id=stale_listing.id,
            product_id=product.id,
            match_type="exact_ean",
            confidence=Decimal("1.00"),
            status="approved",
        )
    )
    session.commit()

    rows = orchestrator._latest_competitor_listings(product.id, session)

    assert rows == []


def test_latest_tz_listing_returns_approved_product_match() -> None:
    session = _session()
    product = Product(
        sku="TZ-Cobra-250",
        brand="Knipex",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    tz_listing = CompetitorListing(
        competitor_id="toolzone_sk",
        brand="Knipex",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
        price_eur=Decimal("22.05"),
        currency="EUR",
        url="https://toolzone.test/cobra",
        scraped_at=datetime.now(UTC),
    )
    session.add_all([product, tz_listing])
    session.flush()
    session.add(ProductMatch(
        listing_id=tz_listing.id, product_id=product.id,
        match_type="exact_ean", confidence=Decimal("1.00"), status="approved",
    ))
    session.commit()

    found = orchestrator._latest_tz_listing(product.id, session)

    assert found is not None
    assert found.id == tz_listing.id


def test_latest_competitor_listings_returns_approved_product_matches() -> None:
    session = _session()
    product = Product(
        sku="TZ-Cobra-250",
        brand="Knipex",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    listing = CompetitorListing(
        competitor_id="ahprofi_sk",
        brand="Knipex",
        ean="4003773022022",
        title="SIKO kliešte Cobra 250 mm",
        price_eur=Decimal("21.50"),
        currency="EUR",
        url="https://ahprofi.test/cobra",
        scraped_at=datetime.now(UTC),
    )
    session.add_all([product, listing])
    session.flush()
    session.add(ProductMatch(
        listing_id=listing.id, product_id=product.id,
        match_type="exact_ean", confidence=Decimal("1.00"), status="approved",
    ))
    session.commit()

    rows = orchestrator._latest_competitor_listings(product.id, session)

    assert [r.id for r in rows] == [listing.id]


def test_latest_competitor_listings_deduplicates_to_newest_per_competitor() -> None:
    session = _session()
    product = Product(
        sku="TZ-Cobra-250",
        brand="Knipex",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    old_listing = CompetitorListing(
        competitor_id="doktorkladivo_sk",
        brand="Knipex",
        ean="4003773022022",
        title="KNIPEX Cobra 8701250",
        price_eur=Decimal("22.10"),
        currency="EUR",
        url="https://doktorkladivo.test/cobra-old",
        scraped_at=datetime.now(UTC) - timedelta(days=2),
    )
    new_listing = CompetitorListing(
        competitor_id="doktorkladivo_sk",
        brand="Knipex",
        ean="4003773022022",
        title="KNIPEX Cobra 8701250",
        price_eur=Decimal("21.90"),
        currency="EUR",
        url="https://doktorkladivo.test/cobra-new",
        scraped_at=datetime.now(UTC),
    )
    session.add_all([product, old_listing, new_listing])
    session.flush()
    session.add_all([
        ProductMatch(listing_id=old_listing.id, product_id=product.id,
                     match_type="exact_ean", confidence=Decimal("1.00"), status="approved"),
        ProductMatch(listing_id=new_listing.id, product_id=product.id,
                     match_type="exact_ean", confidence=Decimal("1.00"), status="approved"),
    ])
    session.commit()

    rows = orchestrator._latest_competitor_listings(product.id, session)

    assert new_listing.id in [r.id for r in rows]


def test_search_product_db_only_finds_product_and_all_db_matches_by_ean() -> None:
    session = _session()
    product = Product(
        sku="TZ-Cobra-250",
        brand="Knipex",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    toolzone_listing = CompetitorListing(
        competitor_id="toolzone_sk",
        brand="Knipex",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
        price_eur=Decimal("22.05"),
        currency="EUR",
        scraped_at=datetime.now(UTC),
        url="https://toolzone.test/cobra",
    )
    competitor_one = CompetitorListing(
        competitor_id="ahprofi_sk",
        brand="Knipex",
        ean="4003773022022",
        title="KNIPEX Cobra 250",
        price_eur=Decimal("21.50"),
        currency="EUR",
        scraped_at=datetime.now(UTC),
        url="https://ahprofi.test/cobra",
    )
    competitor_two = CompetitorListing(
        competitor_id="doktorkladivo_sk",
        brand="Knipex",
        ean="4003773022022",
        title="KNIPEX Cobra 250",
        price_eur=Decimal("23.40"),
        currency="EUR",
        scraped_at=datetime.now(UTC) - timedelta(days=90),
        url="https://doktorkladivo.test/cobra",
    )
    session.add_all([product, toolzone_listing, competitor_one, competitor_two])
    session.flush()
    session.add_all([
        ProductMatch(
            listing_id=toolzone_listing.id,
            product_id=product.id,
            match_type="exact_ean",
            confidence=Decimal("1.00"),
            status="approved",
        ),
        ProductMatch(
            listing_id=competitor_one.id,
            product_id=product.id,
            match_type="exact_ean",
            confidence=Decimal("1.00"),
            status="approved",
        ),
        ProductMatch(
            listing_id=competitor_two.id,
            product_id=product.id,
            match_type="exact_ean",
            confidence=Decimal("1.00"),
            status="approved",
        ),
    ])
    session.commit()

    result = orchestrator.search_product_db_only("4003773022022", session)

    assert result.product is not None
    assert result.product.id == product.id
    assert result.tz_listing is not None
    assert result.tz_listing.id == toolzone_listing.id
    assert {row.id for row in result.competitor_hits} == {
        competitor_one.id,
        competitor_two.id,
    }
    assert result.from_cache is True


def test_search_product_db_only_can_start_from_competitor_listing_ean() -> None:
    session = _session()
    product = Product(
        sku="derived-ean-4003773022022",
        brand="Knipex",
        ean=None,
        title="Derived KNIPEX Cobra product",
    )
    listing = CompetitorListing(
        competitor_id="ahprofi_sk",
        brand="Knipex",
        ean="4003773022022",
        title="KNIPEX Cobra 250",
        price_eur=Decimal("21.50"),
        currency="EUR",
        scraped_at=datetime.now(UTC),
        url="https://ahprofi.test/cobra",
    )
    session.add_all([product, listing])
    session.flush()
    session.add(
        ProductMatch(
            listing_id=listing.id,
            product_id=product.id,
            match_type="exact_ean",
            confidence=Decimal("1.00"),
            status="approved",
        )
    )
    session.commit()

    result = orchestrator.search_product_db_only("4003773022022", session)

    assert result.product is not None
    assert result.product.id == product.id
    assert [row.id for row in result.competitor_hits] == [listing.id]
