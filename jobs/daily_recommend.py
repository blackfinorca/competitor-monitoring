"""Daily recommendation entrypoint.

1. Builds pricing snapshots from today's matched listings.
2. Runs the playbook classifier and writes Recommendation rows.
"""

import logging
from datetime import date, datetime, UTC

from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.pricing.snapshot import build_snapshots
from agnaradie_pricing.pricing.recommender import build_recommendations
from agnaradie_pricing.settings import Settings

logger = logging.getLogger(__name__)


def main(snapshot_date: date | None = None) -> dict[str, int]:
    today = snapshot_date or datetime.now(UTC).date()
    settings = Settings()
    factory = make_session_factory(settings)

    with factory() as session:
        n_snapshots = build_snapshots(session, today)
        session.commit()
        logger.info("Built %d pricing snapshots for %s", n_snapshots, today)

        n_recs = build_recommendations(session, today)
        session.commit()
        logger.info("Created %d recommendations for %s", n_recs, today)

    return {"snapshots": n_snapshots, "recommendations": n_recs}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = main()
    print(f"  Snapshots:       {result['snapshots']}")
    print(f"  Recommendations: {result['recommendations']}")
