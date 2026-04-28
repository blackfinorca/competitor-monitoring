from __future__ import annotations

import math
from decimal import Decimal

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from agnaradie_pricing.db.models import Base, ClusterMember
from agnaradie_pricing.matching import new_matching


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


class _StaticVectorIndex:
    backend_description = "test-static"

    def __init__(self, listings: list[dict]) -> None:
        self._vectors = [
            listing["_vector"]
            for listing in listings
        ]


def _listing(listing_id: int, title: str, vector: list[float]) -> dict:
    return {
        "id": listing_id,
        "competitor_id": f"competitor_{listing_id}",
        "brand": "Knipex",
        "mpn": None,
        "ean": None,
        "title": title,
        "_vector": vector,
    }


def test_fuzzy_similarity_at_threshold_is_auto_approved(monkeypatch) -> None:
    Session = _session_factory()
    similarity = 0.960
    listings = [
        _listing(1, "Knipex cobra pliers", [1.0, 0.0]),
        _listing(2, "Knipex cobra pliers 250", [similarity, math.sqrt(1 - similarity ** 2)]),
    ]

    monkeypatch.setattr(new_matching, "TitleVectorIndex", _StaticVectorIndex)
    monkeypatch.setattr(
        new_matching,
        "find_best_llm_match",
        lambda _orphan, candidates, *, llm_client: (candidates[0], ("llm", 0.10)),
    )

    with Session() as session:
        stats = new_matching._phase_fuzzy(session, listings, llm_client=object())
        members = session.query(ClusterMember).order_by(ClusterMember.listing_id).all()

    assert stats["fuzzy_approved"] == 1
    assert stats["fuzzy_pending"] == 0
    assert [member.status for member in members] == ["approved", "approved"]
    assert all(member.similarity == Decimal("0.960") for member in members)


def test_fuzzy_similarity_below_threshold_stays_pending_even_with_high_llm_confidence(monkeypatch) -> None:
    Session = _session_factory()
    similarity = 0.959
    listings = [
        _listing(1, "Knipex cobra pliers", [1.0, 0.0]),
        _listing(2, "Knipex cobra pliers 250", [similarity, math.sqrt(1 - similarity ** 2)]),
    ]

    monkeypatch.setattr(new_matching, "TitleVectorIndex", _StaticVectorIndex)
    monkeypatch.setattr(
        new_matching,
        "find_best_llm_match",
        lambda _orphan, candidates, *, llm_client: (candidates[0], ("llm", 0.99)),
    )

    with Session() as session:
        stats = new_matching._phase_fuzzy(session, listings, llm_client=object())
        members = session.query(ClusterMember).order_by(ClusterMember.listing_id).all()

    assert stats["fuzzy_approved"] == 0
    assert stats["fuzzy_pending"] == 1
    assert [member.status for member in members] == ["pending", "pending"]
    assert all(member.similarity == Decimal("0.959") for member in members)


def test_reset_all_matches_clears_new_and_legacy_match_tables() -> None:
    Session = _session_factory()
    with Session() as session:
        session.execute(
            text(
                """
                INSERT INTO product_clusters (id, ean, cluster_method)
                VALUES (1, '4003773022022', 'ean')
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO cluster_members (id, cluster_id, listing_id, match_method, status)
                VALUES (1, 1, 10, 'ean', 'approved')
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO listing_matches (
                    id, toolzone_listing_id, competitor_listing_id, match_type, confidence
                )
                VALUES (1, 10, 11, 'exact_ean', 1.0)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO product_matches (
                    id, ag_product_id, competitor_id, competitor_sku, match_type, confidence,
                    verified_by_human
                )
                VALUES (1, NULL, 'demo', 'sku-1', 'exact_ean', 1.0, 0)
                """
            )
        )
        session.commit()

        new_matching.reset_all_matches(session)

        counts = {
            table_name: session.scalar(text(f"SELECT COUNT(*) FROM {table_name}"))
            for table_name in (
                "cluster_members",
                "product_clusters",
                "listing_matches",
                "product_matches",
            )
        }

    assert counts == {
        "cluster_members": 0,
        "product_clusters": 0,
        "listing_matches": 0,
        "product_matches": 0,
    }
