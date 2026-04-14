from agnaradie_pricing.db.models import (
    Base,
    CompetitorListing,
    PricingSnapshot,
    Product,
    ProductMatch,
    Recommendation,
)


def test_schema_defines_expected_tables() -> None:
    assert set(Base.metadata.tables) >= {
        "products",
        "competitor_listings",
        "product_matches",
        "pricing_snapshot",
        "recommendations",
    }


def test_product_has_required_indexes_and_unique_sku() -> None:
    assert Product.__table__.c.sku.unique is True
    assert {index.name for index in Product.__table__.indexes} >= {
        "idx_products_brand_mpn",
        "idx_products_ean",
    }


def test_competitor_listing_indexes_support_scrape_and_match_queries() -> None:
    assert {index.name for index in CompetitorListing.__table__.indexes} >= {
        "idx_cl_competitor_scraped",
        "idx_cl_brand_mpn",
    }


def test_match_snapshot_and_recommendation_tables_have_uniqueness_constraints() -> None:
    assert ProductMatch.__table__.name == "product_matches"
    assert PricingSnapshot.__table__.name == "pricing_snapshot"
    assert Recommendation.__table__.name == "recommendations"
    assert ProductMatch.__table__.constraints
    assert PricingSnapshot.__table__.constraints

