from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from agnaradie_pricing.db.models import Base, CompetitorListing as CompetitorListingRow
from agnaradie_pricing.scrapers.base import CompetitorListing
from agnaradie_pricing.scrapers.persistence import save_competitor_listings


def test_save_competitor_listings_inserts_rows() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    listing = CompetitorListing(
        competitor_id="doktorkladivo_sk",
        competitor_sku="2808",
        brand="KNIPEX",
        mpn="8701250",
        ean="4003773022022",
        title="KNIPEX Kliešte inštalatérske Cobra 8701250",
        price_eur=28.61,
        currency="EUR",
        in_stock=True,
        url="https://www.doktorkladivo.sk/knipex-klieste-instalaterske-cobra-8701250-p2808/",
        scraped_at=datetime(2026, 4, 12, tzinfo=UTC),
    )

    with Session(engine) as session:
        save_competitor_listings(session, [listing])
        session.commit()
        rows = session.scalars(select(CompetitorListingRow)).all()

    assert len(rows) == 1
    assert rows[0].competitor_id == "doktorkladivo_sk"
    assert rows[0].competitor_sku == "2808"
    assert rows[0].price_eur == Decimal("28.61")


def test_save_competitor_listings_backfills_missing_identifiers_on_conflict() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    initial = CompetitorListing(
        competitor_id="rebiop_sk",
        competitor_sku=None,
        brand=None,
        mpn=None,
        ean=None,
        title="Blankovací nôž so slzou BK-01",
        price_eur=30.79,
        currency="EUR",
        in_stock=True,
        url="https://www.rebiop.sk/detail/4853/blankovaci-noz-so-slzou-bk-01",
        scraped_at=datetime(2026, 4, 22, tzinfo=UTC),
    )
    enriched = CompetitorListing(
        competitor_id="rebiop_sk",
        competitor_sku="3.40010",
        brand="BAUPRO",
        mpn="BK-01",
        ean="8585033303677",
        title="Blankovací nôž so slzou BK-01",
        price_eur=30.79,
        currency="EUR",
        in_stock=True,
        url="https://www.rebiop.sk/detail/4853/blankovaci-noz-so-slzou-bk-01",
        scraped_at=datetime(2026, 4, 24, tzinfo=UTC),
    )

    with Session(engine) as session:
        save_competitor_listings(session, [initial])
        session.commit()
        save_competitor_listings(session, [enriched])
        session.commit()
        row = session.scalar(select(CompetitorListingRow))

    assert row is not None
    assert row.ean == "8585033303677"
    assert row.competitor_sku == "3.40010"
    assert row.brand == "BAUPRO"
    assert row.mpn == "BK-01"
    assert row.price_eur == Decimal("30.79")
