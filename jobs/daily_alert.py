"""Daily alert dispatch entrypoint.

Reads today's Recommendation rows with playbook = 'investigate' (or any
playbook, depending on config) and posts a Slack message via an incoming
webhook.  The webhook URL is read from the ALERT_WEBHOOK_URL environment
variable (or .env via pydantic-settings).

Slack payload format (Block Kit):
  - Header: "AG Pricing Alerts — {date}"
  - One section per recommendation:
      "🔍 *{sku}* · {title}
       AG {current_price} € · Median {median} € · {rationale}"

Nothing is sent if ALERT_WEBHOOK_URL is not set or if there are no alerts.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from __future__ import annotations

import json
import logging
from datetime import date, datetime, UTC
from decimal import Decimal
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from agnaradie_pricing.db.models import Product, Recommendation, PricingSnapshot
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.settings import Settings

logger = logging.getLogger(__name__)

# Playbooks that always generate an alert (regardless of severity config)
ALERT_PLAYBOOKS = {"investigate"}


def _format_price(v: Decimal | None) -> str:
    if v is None:
        return "N/A"
    return f"{float(v):.2f} €"


def _build_blocks(alerts: list[dict], alert_date: date) -> list[dict]:
    """Build Slack Block Kit blocks for the alert payload."""
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"AG Pricing Alerts — {alert_date.isoformat()}",
            },
        },
        {"type": "divider"},
    ]

    for a in alerts:
        icon = {
            "investigate": ":mag:",
            "raise": ":arrow_up:",
            "drop": ":arrow_down:",
            "hold": ":white_check_mark:",
        }.get(a["playbook"], ":bell:")

        text = (
            f"{icon} *{a['sku']}*  ·  {a['title']}\n"
            f"AG {_format_price(a['current_price'])}  ·  "
            f"Median {_format_price(a['median_price'])}  ·  "
            f"Suggested {_format_price(a['suggested_price'])}\n"
            f"_{a['rationale']}_"
        )
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": text},
            }
        )

    blocks.append({"type": "divider"})
    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Total alerts: {len(alerts)}",
                }
            ],
        }
    )
    return blocks


def _collect_alerts(
    session: Session,
    alert_date: date,
    playbooks: set[str] = ALERT_PLAYBOOKS,
) -> list[dict]:
    """Return a list of alert dicts for today's matching recommendations."""
    rows = session.scalars(
        select(Recommendation).where(
            Recommendation.snapshot_date == alert_date,
            Recommendation.playbook.in_(playbooks),
            Recommendation.status == "pending",
        )
    ).all()

    if not rows:
        return []

    # Batch-load products and snapshots for the involved product IDs
    product_ids = [r.ag_product_id for r in rows if r.ag_product_id is not None]

    products = {
        p.id: p
        for p in session.scalars(
            select(Product).where(Product.id.in_(product_ids))
        ).all()
    }

    snapshots = {
        s.ag_product_id: s
        for s in session.scalars(
            select(PricingSnapshot).where(
                PricingSnapshot.ag_product_id.in_(product_ids),
                PricingSnapshot.snapshot_date == alert_date,
            )
        ).all()
    }

    alerts: list[dict] = []
    for rec in rows:
        prod = products.get(rec.ag_product_id)
        snap = snapshots.get(rec.ag_product_id)
        alerts.append(
            {
                "sku": prod.sku if prod else str(rec.ag_product_id),
                "title": (prod.title[:60] + "…") if prod and len(prod.title) > 60 else (prod.title if prod else ""),
                "playbook": rec.playbook,
                "current_price": rec.current_price,
                "suggested_price": rec.suggested_price,
                "median_price": snap.median_price if snap else None,
                "rationale": rec.rationale or "",
            }
        )

    return alerts


def post_slack_alerts(
    webhook_url: str,
    alerts: list[dict],
    alert_date: date,
    http_client: httpx.Client | None = None,
) -> bool:
    """POST a Slack message. Returns True on success."""
    blocks = _build_blocks(alerts, alert_date)
    payload = {"blocks": blocks}

    client = http_client or httpx.Client(timeout=10.0)
    try:
        response = client.post(
            webhook_url,
            content=json.dumps(payload),
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        logger.info("Slack alert posted (%d alerts)", len(alerts))
        return True
    except httpx.HTTPError as exc:
        logger.error("Failed to post Slack alert: %s", exc)
        return False


def main(
    alert_date: date | None = None,
    playbooks: set[str] | None = None,
) -> int:
    """Dispatch alerts for the given date. Returns the number of alerts sent."""
    today = alert_date or datetime.now(UTC).date()
    active_playbooks = playbooks or ALERT_PLAYBOOKS

    settings = Settings()
    webhook_url = settings.alert_webhook_url
    if not webhook_url:
        logger.info("ALERT_WEBHOOK_URL not set — skipping alert dispatch")
        return 0

    factory = make_session_factory(settings)
    with factory() as session:
        alerts = _collect_alerts(session, today, active_playbooks)

    if not alerts:
        logger.info("No alerts to dispatch for %s", today)
        return 0

    success = post_slack_alerts(webhook_url, alerts, today)
    return len(alerts) if success else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    n = main()
    print(f"  Alerts dispatched: {n}")
