"""Data-loading layer for the Product Overview / Seller dashboard.

Exposes :func:`load_seller_dashboard_data`, which reads approved product_matches
and returns an aggregated payload suitable for the Streamlit Product Overview page.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from agnaradie_pricing.db.session import make_session_factory  # noqa: E402
from agnaradie_pricing.settings import Settings  # noqa: E402


_SQL = text(
    """
    SELECT p.id                     AS cluster_id,
           p.ean                    AS ean,
           p.title                  AS representative_title,
           cl.competitor_id         AS competitor_id,
           cl.price_eur             AS price_eur
    FROM   product_matches pm
    JOIN   products p  ON p.id  = pm.product_id
    JOIN   competitor_listings cl ON cl.id = pm.listing_id
    WHERE  pm.status = 'approved'
    """
)


def _default_factory() -> sessionmaker[Session]:
    return make_session_factory(Settings())


def load_seller_dashboard_data(
    session_factory: Callable[[], Session] | None = None,
) -> dict[str, Any]:
    factory = session_factory if session_factory is not None else _default_factory()

    with factory() as session:
        rows = session.execute(_SQL).fetchall()

    columns = ["cluster_id", "ean", "representative_title", "competitor_id", "price_eur"]
    df = pd.DataFrame(rows, columns=columns)

    snapshot_date = date.today().isoformat()

    if df.empty:
        return {
            "snapshot_date": snapshot_date,
            "eans_total": 0,
            "sellers_total": 0,
            "offers_total": 0,
            "top_sellers": [],
            "all_sellers": [],
            "seller_stats": {},
            "titles": {},
            "offers": [],
        }

    df["price_eur"] = pd.to_numeric(df["price_eur"], errors="coerce")

    def _ean_key(row: pd.Series) -> str:
        ean = row["ean"]
        if ean is None or (isinstance(ean, float) and pd.isna(ean)) or str(ean).strip() == "":
            return f"cluster:{int(row['cluster_id'])}"
        return str(ean)

    df["e"] = df.apply(_ean_key, axis=1)

    grouped = (
        df.dropna(subset=["price_eur"])
        .groupby(["cluster_id", "e", "competitor_id"], as_index=False)["price_eur"]
        .min()
    )

    offers: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        price = float(row["price_eur"])
        offers.append(
            {
                "e": row["e"],
                "s": row["competitor_id"],
                "p": price,
                "d": None,
                "t": price,
            }
        )

    titles: dict[str, str] = {}
    for e_key, sub in df.groupby("e"):
        title_val = None
        for candidate in sub["representative_title"]:
            if candidate is not None and not (isinstance(candidate, float) and pd.isna(candidate)):
                title_val = str(candidate)
                break
        titles[e_key] = (title_val or "")[:120]

    competitor_offer_counts: dict[str, int] = {}
    competitor_skus: dict[str, set[Any]] = {}
    for offer in offers:
        competitor_offer_counts[offer["s"]] = competitor_offer_counts.get(offer["s"], 0) + 1
    for _, row in grouped.iterrows():
        competitor_skus.setdefault(row["competitor_id"], set()).add(row["cluster_id"])

    seller_stats = {
        cid: {"offers": competitor_offer_counts[cid], "skus": len(competitor_skus[cid])}
        for cid in competitor_offer_counts
    }

    all_sellers = sorted(competitor_offer_counts.keys())
    top_sellers = [
        cid
        for cid, _ in sorted(
            competitor_offer_counts.items(), key=lambda kv: (-kv[1], kv[0])
        )
    ][:10]

    eans_total = int(grouped["e"].nunique()) if not grouped.empty else 0
    sellers_total = len(all_sellers)
    offers_total = len(offers)

    return {
        "snapshot_date": snapshot_date,
        "eans_total": eans_total,
        "sellers_total": sellers_total,
        "offers_total": offers_total,
        "top_sellers": top_sellers,
        "all_sellers": all_sellers,
        "seller_stats": seller_stats,
        "titles": titles,
        "offers": offers,
    }
