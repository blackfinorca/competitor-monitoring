from __future__ import annotations

from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from agnaradie_pricing.db.models import Base, CompetitorListing, Product, ProductMatch
from agnaradie_pricing.matching import pipeline


class _ExplodingVectorIndex:
    def __init__(self, *_args, **_kwargs) -> None:
        raise AssertionError("no-brand orphans should not build a vector index")


class _NoCandidateVectorIndex:
    def __init__(self, products, **_kwargs) -> None:
        self._vectors = [[1.0, 0.0], [0.0, 1.0]][: len(products)]


class _UnusedLlm:
    def complete(self, _prompt: str) -> str:
        raise AssertionError("LLM should not be called without vector candidates")


def _session():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _listing(
    *,
    title: str,
    brand: str | None,
    url: str,
    mpn: str | None = None,
    ean: str | None = None,
) -> CompetitorListing:
    return CompetitorListing(
        competitor_id="test_competitor",
        brand=brand,
        mpn=mpn,
        ean=ean,
        title=title,
        price_eur=Decimal("10.00"),
        currency="EUR",
        url=url,
    )


def test_run_matching_skips_no_brand_orphans_before_vector_index(monkeypatch) -> None:
    session = _session()
    session.add_all(
        [
            Product(sku="p1", brand=None, title="Unrelated catalogue item one"),
            Product(sku="p2", brand=None, title="Unrelated catalogue item two"),
            _listing(
                title="Unbranded orphan without ean",
                brand=None,
                url="https://example.test/no-brand",
            ),
        ]
    )
    session.commit()

    monkeypatch.setattr(pipeline, "TitleVectorIndex", _ExplodingVectorIndex)

    counts = pipeline.run_matching(session, llm_client=_UnusedLlm())

    assert counts["skipped"] == 1
    assert counts["vector_llm"] == 0
    assert counts["pending"] == 0


def test_run_matching_progress_counts_branded_orphans_without_candidates(
    monkeypatch,
    capsys,
) -> None:
    session = _session()
    session.add_all(
        [
            Product(sku="p1", brand="Acme", title="Alpha catalogue product"),
            _listing(
                title="Beta competitor listing",
                brand="Acme",
                url="https://example.test/branded",
            ),
        ]
    )
    session.commit()

    monkeypatch.setattr(pipeline, "TitleVectorIndex", _NoCandidateVectorIndex)

    counts = pipeline.run_matching(session, llm_client=_UnusedLlm())

    captured = capsys.readouterr()
    assert counts["skipped"] == 1
    assert "[phase-2] brand done  'ACME'  done=1/1" in captured.out


def test_run_matching_llm_only_skips_exact_and_derived_paths(monkeypatch) -> None:
    session = _session()
    session.add_all(
        [
            Product(
                sku="p1",
                brand=None,
                ean="4003773022022",
                title="Known product with same EAN",
            ),
            _listing(
                title="No-brand listing with same EAN",
                brand=None,
                ean="4003773022022",
                url="https://example.test/ean",
            ),
        ]
    )
    session.commit()

    monkeypatch.setattr(pipeline, "TitleVectorIndex", _ExplodingVectorIndex)

    counts = pipeline.run_matching(
        session,
        llm_client=_UnusedLlm(),
        llm_only=True,
    )

    assert counts["exact"] == 0
    assert counts["derived"] == 0
    assert counts["skipped"] == 1
    assert session.scalars(select(ProductMatch)).all() == []
