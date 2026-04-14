from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from agnaradie_pricing.catalogue.ingest import ingest_catalogue_csv, load_catalogue_csv
from agnaradie_pricing.db.models import Base, Product


def test_load_catalogue_csv_parses_required_columns() -> None:
    rows = load_catalogue_csv(Path("tests/fixtures/ag_catalogue.csv"))

    assert len(rows) == 2
    assert rows[0].sku == "AG-KNIPEX-8701250"
    assert rows[0].price_eur == Decimal("31.90")
    assert rows[0].cost_eur == Decimal("20.10")
    assert rows[1].ean is None
    assert rows[1].stock == 0


def test_ingest_catalogue_csv_upserts_products() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        count = ingest_catalogue_csv(session, Path("tests/fixtures/ag_catalogue.csv"))
        session.commit()
        products = session.scalars(select(Product).order_by(Product.sku)).all()

    assert count == 2
    assert [product.sku for product in products] == [
        "AG-BOSCH-GBH18V21",
        "AG-KNIPEX-8701250",
    ]
    assert products[0].cost_eur is None

    with Session(engine) as session:
        update_path = Path("tests/fixtures/ag_catalogue.csv")
        ingest_catalogue_csv(session, update_path)
        session.commit()
        assert len(session.scalars(select(Product)).all()) == 2

