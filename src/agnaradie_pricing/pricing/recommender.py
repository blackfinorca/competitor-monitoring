"""Playbook-based pricing recommender.

Reads today's PricingSnapshot rows, applies rule thresholds from
config/playbooks.yaml, and writes Recommendation rows.
"""

from __future__ import annotations

from datetime import date, datetime, UTC
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from agnaradie_pricing.db.models import PricingSnapshot, Recommendation
from agnaradie_pricing.settings import load_playbooks


def classify_playbook(
    snapshot: PricingSnapshot,
    previous_snapshot: PricingSnapshot | None,
    thresholds: dict,
) -> str | None:
    """Return the playbook action for a snapshot, or None if no rule fires."""
    ag = snapshot.ag_price
    median = snapshot.median_price
    min_price = snapshot.min_price
    n = snapshot.competitor_count or 0

    # investigate — competitor price moved >20% day-on-day
    if (
        previous_snapshot is not None
        and previous_snapshot.median_price is not None
        and median is not None
    ):
        inv_thresh = thresholds.get("investigate", {}).get("min_day_on_day_move", 0.20)
        prev_med = float(previous_snapshot.median_price)
        if prev_med > 0:
            dod_move = abs(float(median) - prev_med) / prev_med
            if dod_move >= inv_thresh:
                return "investigate"

    if ag is None or median is None or min_price is None:
        return None

    ag_f = float(ag)
    median_f = float(median)
    min_f = float(min_price)

    # raise — AG is cheapest by >8%, margin headroom exists
    raise_cfg = thresholds.get("raise", {})
    if min_f > 0:
        gap_below_next = (min_f - ag_f) / min_f
        if gap_below_next >= raise_cfg.get("min_gap_below_next", 0.08):
            return "raise"

    # drop — AG >15% above median, at least 2 competitors
    drop_cfg = thresholds.get("drop", {})
    if median_f > 0:
        gap_above_median = (ag_f - median_f) / median_f
        if (
            gap_above_median >= drop_cfg.get("min_gap_above_median", 0.15)
            and n >= drop_cfg.get("min_competitors", 2)
        ):
            return "drop"

    # hold — AG within ±5% of median
    hold_cfg = thresholds.get("hold", {})
    if median_f > 0:
        gap_to_median = abs(ag_f - median_f) / median_f
        if gap_to_median <= hold_cfg.get("max_gap_to_median", 0.05):
            return "hold"

    return None


def suggested_price(playbook: str, snapshot: PricingSnapshot) -> Decimal | None:
    """Simple heuristic for a suggested new price."""
    ag = snapshot.ag_price
    median = snapshot.median_price
    min_price = snapshot.min_price

    if playbook == "raise" and min_price is not None:
        # Undercut next-cheapest by 1%
        return (min_price * Decimal("0.99")).quantize(Decimal("0.01"))
    if playbook == "drop" and median is not None:
        # Price to median
        return median.quantize(Decimal("0.01"))
    if playbook == "hold":
        return ag
    return ag


def build_recommendations(
    session: Session,
    snapshot_date: date | None = None,
    playbooks_path: Path = Path("config/playbooks.yaml"),
) -> int:
    today = snapshot_date or datetime.now(UTC).date()
    thresholds = load_playbooks(playbooks_path)
    written = 0

    snapshots = session.scalars(
        select(PricingSnapshot).where(PricingSnapshot.snapshot_date == today)
    ).all()

    # Index previous snapshots by product_id for day-on-day comparison.
    # Subquery fetches only the single most-recent date per product instead of
    # loading the full history into Python for deduplication.
    prev_snapshots: dict[int | None, PricingSnapshot] = {}
    if snapshots:
        from sqlalchemy import func as sqlfunc

        product_ids = [s.ag_product_id for s in snapshots if s.ag_product_id is not None]
        if product_ids:
            latest_dates_sq = (
                select(
                    PricingSnapshot.ag_product_id,
                    sqlfunc.max(PricingSnapshot.snapshot_date).label("max_date"),
                )
                .where(
                    PricingSnapshot.snapshot_date < today,
                    PricingSnapshot.ag_product_id.in_(product_ids),
                )
                .group_by(PricingSnapshot.ag_product_id)
                .subquery()
            )
            prev_rows = session.scalars(
                select(PricingSnapshot).join(
                    latest_dates_sq,
                    (PricingSnapshot.ag_product_id == latest_dates_sq.c.ag_product_id)
                    & (PricingSnapshot.snapshot_date == latest_dates_sq.c.max_date),
                )
            ).all()
            for row in prev_rows:
                prev_snapshots[row.ag_product_id] = row

    for snap in snapshots:
        prev = prev_snapshots.get(snap.ag_product_id)
        action = classify_playbook(snap, prev, thresholds)
        if action is None:
            continue

        # Upsert recommendation (one per product per day)
        existing = session.scalars(
            select(Recommendation).where(
                Recommendation.ag_product_id == snap.ag_product_id,
                Recommendation.snapshot_date == today,
            )
        ).first()

        if existing is None:
            existing = Recommendation(
                ag_product_id=snap.ag_product_id,
                snapshot_date=today,
            )
            session.add(existing)

        existing.playbook = action
        existing.current_price = snap.ag_price
        existing.suggested_price = suggested_price(action, snap)
        existing.rationale = _build_rationale(action, snap, prev)
        existing.status = "pending"
        written += 1

    return written


def _build_rationale(
    action: str,
    snap: PricingSnapshot,
    prev: PricingSnapshot | None,
) -> str:
    ag = snap.ag_price
    median = snap.median_price
    min_p = snap.min_price
    cheapest = snap.cheapest_competitor or "competitor"
    n = snap.competitor_count or 0

    if action == "raise":
        gap = (float(min_p) - float(ag)) / float(min_p) * 100 if min_p else 0
        return (
            f"ToolZone is {gap:.1f}% cheaper than the next competitor ({cheapest}). "
            f"Margin headroom allows a price increase up to {min_p} EUR."
        )
    if action == "drop":
        gap = (float(ag) - float(median)) / float(median) * 100 if median else 0
        return (
            f"ToolZone is {gap:.1f}% above the market median ({median} EUR) "
            f"across {n} competitors. A price reduction is recommended."
        )
    if action == "hold":
        gap = abs(float(ag) - float(median)) / float(median) * 100 if median else 0
        return (
            f"ToolZone price is within {gap:.1f}% of the market median ({median} EUR). "
            f"No change needed."
        )
    if action == "investigate":
        prev_med = prev.median_price if prev else None
        move = (
            abs(float(median) - float(prev_med)) / float(prev_med) * 100
            if prev_med
            else 0
        )
        return (
            f"Market median moved {move:.1f}% day-on-day "
            f"(from {prev_med} to {median} EUR). Investigate competitor pricing."
        )
    return ""
