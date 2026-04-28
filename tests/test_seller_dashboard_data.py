from __future__ import annotations

import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

_DASHBOARD = Path(__file__).resolve().parent.parent / "dashboard"
if str(_DASHBOARD) not in sys.path:
    sys.path.insert(0, str(_DASHBOARD))

from agnaradie_pricing.db.models import (  # noqa: E402
    Base,
    CompetitorListing,
    Product,
    ProductMatch,
)

from seller_dashboard_data import load_seller_dashboard_data  # noqa: E402


def _make_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _add_listing(
    session,
    *,
    competitor_id: str,
    title: str = "Listing",
    price: Decimal | None = Decimal("10.00"),
    url: str,
) -> CompetitorListing:
    listing = CompetitorListing(
        competitor_id=competitor_id,
        title=title,
        price_eur=price,
        currency="EUR",
        url=url,
    )
    session.add(listing)
    session.flush()
    return listing


def _add_product(session, *, ean: str | None, title: str) -> Product:
    product = Product(ean=ean, title=title, source="derived")
    session.add(product)
    session.flush()
    return product


def _approve(session, product: Product, listing: CompetitorListing) -> None:
    session.add(
        ProductMatch(
            listing_id=listing.id,
            product_id=product.id,
            match_type="exact_ean",
            confidence=Decimal("1.0"),
            status="approved",
        )
    )
    session.flush()


def test_happy_path_aggregates_offers_sellers_and_clusters() -> None:
    factory = _make_factory()
    with factory() as session:
        p1 = _add_product(session, ean="1111111111111", title="Cluster One")
        p2 = _add_product(session, ean=None, title="Cluster Two")
        p3 = _add_product(session, ean=None, title="Cluster Three")
        p4 = _add_product(session, ean="", title="Cluster Four")

        l1 = _add_listing(session, competitor_id="alpha", price=Decimal("10.00"), url="u1")
        l2 = _add_listing(session, competitor_id="beta",  price=Decimal("11.00"), url="u2")
        l3 = _add_listing(session, competitor_id="gamma", price=Decimal("12.00"), url="u3")
        l4 = _add_listing(session, competitor_id="alpha", price=Decimal("20.00"), url="u4")
        l5 = _add_listing(session, competitor_id="beta",  price=Decimal("21.00"), url="u5")
        l6 = _add_listing(session, competitor_id="alpha", price=Decimal("30.00"), url="u6")
        l7 = _add_listing(session, competitor_id="alpha", price=Decimal("40.00"), url="u7")

        _approve(session, p1, l1)
        _approve(session, p1, l2)
        _approve(session, p1, l3)
        _approve(session, p2, l4)
        _approve(session, p2, l5)
        _approve(session, p3, l6)
        _approve(session, p4, l7)
        session.commit()

        p4_id = p4.id

    result = load_seller_dashboard_data(factory)

    assert result["snapshot_date"] == date.today().isoformat()
    assert result["offers_total"] == 7
    assert result["eans_total"] == 4
    assert result["sellers_total"] == 3

    triples = {(o["e"], o["s"], o["t"]) for o in result["offers"]}
    assert ("1111111111111", "alpha", 10.0) in triples
    assert ("1111111111111", "beta",  11.0) in triples
    assert ("1111111111111", "gamma", 12.0) in triples

    by_seller: dict[str, int] = {}
    for o in result["offers"]:
        by_seller[o["s"]] = by_seller.get(o["s"], 0) + 1
    assert by_seller == {"alpha": 4, "beta": 2, "gamma": 1}

    alpha_eans = {o["e"] for o in result["offers"] if o["s"] == "alpha"}
    assert "1111111111111" in alpha_eans
    assert f"cluster:{p4_id}" in alpha_eans

    assert result["top_sellers"][:3] == ["alpha", "beta", "gamma"]
    assert result["all_sellers"] == ["alpha", "beta", "gamma"]
    assert result["seller_stats"]["alpha"] == {"offers": 4, "skus": 4}
    assert result["seller_stats"]["beta"]  == {"offers": 2, "skus": 2}
    assert result["seller_stats"]["gamma"] == {"offers": 1, "skus": 1}

    for o in result["offers"]:
        assert o["e"] in result["titles"]
    assert result["titles"]["1111111111111"] == "Cluster One"
    assert result["titles"][f"cluster:{p4_id}"] == "Cluster Four"

    for o in result["offers"]:
        assert o["d"] is None
        assert o["p"] == o["t"]


def test_excludes_match_with_pending_status() -> None:
    factory = _make_factory()
    with factory() as session:
        product = _add_product(session, ean="2222222222222", title="Pending Only")
        listing = _add_listing(session, competitor_id="solo", price=Decimal("5.00"), url="u-pend")
        session.add(
            ProductMatch(
                listing_id=listing.id,
                product_id=product.id,
                match_type="vector_llm",
                confidence=Decimal("0.88"),
                status="pending",
            )
        )
        session.commit()

    result = load_seller_dashboard_data(factory)

    assert result["offers"] == []
    assert result["offers_total"] == 0
    assert result["sellers_total"] == 0
    assert result["eans_total"] == 0


def test_min_price_aggregation_picks_lower_value() -> None:
    factory = _make_factory()
    with factory() as session:
        product = _add_product(session, ean="3333333333333", title="Min Cluster")
        l_high = _add_listing(session, competitor_id="acme", price=Decimal("12.00"), url="hi")
        l_low  = _add_listing(session, competitor_id="acme", price=Decimal("9.00"),  url="lo")
        _approve(session, product, l_high)
        _approve(session, product, l_low)
        session.commit()

    result = load_seller_dashboard_data(factory)

    assert len(result["offers"]) == 1
    offer = result["offers"][0]
    assert offer["e"] == "3333333333333"
    assert offer["s"] == "acme"
    assert offer["t"] == 9.0
    assert offer["p"] == 9.0
    assert result["seller_stats"]["acme"] == {"offers": 1, "skus": 1}


def test_title_truncated_to_120_chars() -> None:
    factory = _make_factory()
    long_title = "x" * 200
    with factory() as session:
        product = _add_product(session, ean="4444444444444", title=long_title)
        listing = _add_listing(session, competitor_id="seller", price=Decimal("5.00"), url="u-long")
        _approve(session, product, listing)
        session.commit()

    result = load_seller_dashboard_data(factory)

    assert "4444444444444" in result["titles"]
    assert len(result["titles"]["4444444444444"]) == 120
    assert result["titles"]["4444444444444"] == "x" * 120
