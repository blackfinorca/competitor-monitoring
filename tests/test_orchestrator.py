from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from agnaradie_pricing import orchestrator
from agnaradie_pricing.db.models import (
    Base,
    ClusterMember,
    CompetitorListing,
    Product,
    ProductCluster,
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
    session.add(
        ProductMatch(
            ag_product_id=tz_product.id,
            competitor_id="toolzone_sk",
            competitor_sku="4003773022022",
            match_type="exact_ean",
            confidence=Decimal("1.00"),
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
        competitor_sku="old",
        brand="Knipex",
        mpn="87-01-250",
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
            ag_product_id=product.id,
            competitor_id="example_sk",
            competitor_sku="old",
            match_type="exact_ean",
            confidence=Decimal("1.00"),
        )
    )
    session.commit()

    rows = orchestrator._latest_competitor_listings(product.id, session)

    assert rows == []


def test_latest_tz_listing_falls_back_to_approved_ean_cluster() -> None:
    session = _session()
    product = Product(
        sku="AG-KNIPEX-8701250",
        brand="KNIPEX",
        mpn="87-01-250",
        ean="4003773022022",
        title="KNIPEX Klieste instalaterske Cobra 8701250",
    )
    tz_listing = CompetitorListing(
        competitor_id="toolzone_sk",
        competitor_sku="4003773022022",
        brand="Knipex",
        mpn="E7455930250",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
        price_eur=Decimal("22.05"),
        currency="EUR",
        in_stock=True,
        url="https://toolzone.test/cobra",
        scraped_at=datetime.now(UTC),
    )
    session.add_all([product, tz_listing])
    session.flush()
    cluster = ProductCluster(
        ean="4003773022022",
        cluster_method="ean",
        representative_brand="Knipex",
        representative_title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    session.add(cluster)
    session.flush()
    session.add(
        ClusterMember(
            cluster_id=cluster.id,
            listing_id=tz_listing.id,
            match_method="ean",
            status="approved",
        )
    )
    session.commit()

    found = orchestrator._latest_tz_listing(product.id, session)

    assert found is not None
    assert found.id == tz_listing.id


def test_latest_competitor_listings_reads_approved_ean_cluster_members() -> None:
    session = _session()
    product = Product(
        sku="TZ-Cobra-250",
        brand="Knipex",
        mpn="87-01-250",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    competitor_listing = CompetitorListing(
        competitor_id="ahprofi_sk",
        competitor_sku="8701250",
        brand="Knipex",
        mpn="87-01-250",
        ean="4003773022022",
        title="SIKO kliešte Cobra - 250 mm - 8701250",
        price_eur=Decimal("21.50"),
        currency="EUR",
        in_stock=True,
        url="https://ahprofi.test/cobra",
        scraped_at=datetime.now(UTC),
    )
    session.add_all([product, competitor_listing])
    session.flush()
    cluster = ProductCluster(
        ean="4003773022022",
        cluster_method="ean",
        representative_brand="Knipex",
        representative_title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    session.add(cluster)
    session.flush()
    session.add(
        ClusterMember(
            cluster_id=cluster.id,
            listing_id=competitor_listing.id,
            match_method="ean",
            status="approved",
        )
    )
    session.commit()

    rows = orchestrator._latest_competitor_listings(product.id, session)

    assert [row.id for row in rows] == [competitor_listing.id]


def test_latest_competitor_listings_keeps_multiple_approved_rows_from_same_competitor() -> None:
    session = _session()
    product = Product(
        sku="TZ-Cobra-250",
        brand="Knipex",
        mpn="87-01-250",
        ean="4003773022022",
        title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    first_listing = CompetitorListing(
        competitor_id="doktorkladivo_sk",
        competitor_sku="dk-old",
        brand="Knipex",
        mpn="87-01-250",
        ean="4003773022022",
        title="KNIPEX Kliešte inštalatérske Cobra 8701250",
        price_eur=Decimal("22.10"),
        currency="EUR",
        in_stock=True,
        url="https://doktorkladivo.test/cobra-old",
        scraped_at=datetime.now(UTC) - timedelta(days=2),
    )
    second_listing = CompetitorListing(
        competitor_id="doktorkladivo_sk",
        competitor_sku="dk-new",
        brand="Knipex",
        mpn="87-01-250",
        ean="4003773022022",
        title="KNIPEX Kliešte inštalatérske Cobra 8701250",
        price_eur=Decimal("21.90"),
        currency="EUR",
        in_stock=True,
        url="https://doktorkladivo.test/cobra-new",
        scraped_at=datetime.now(UTC),
    )
    session.add_all([product, first_listing, second_listing])
    session.flush()
    cluster = ProductCluster(
        ean="4003773022022",
        cluster_method="ean",
        representative_brand="Knipex",
        representative_title="Kliešte SIKO Cobra 250 mm KNIPEX",
    )
    session.add(cluster)
    session.flush()
    session.add_all([
        ClusterMember(
            cluster_id=cluster.id,
            listing_id=first_listing.id,
            match_method="ean",
            status="approved",
        ),
        ClusterMember(
            cluster_id=cluster.id,
            listing_id=second_listing.id,
            match_method="ean",
            status="approved",
        ),
    ])
    session.commit()

    rows = orchestrator._latest_competitor_listings(product.id, session)

    assert [row.id for row in rows] == [second_listing.id, first_listing.id]
