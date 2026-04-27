"""Run the EAN-led product clustering pipeline.

Examples
--------
    # Incremental run (EAN + LLM fuzzy fallback)
    python jobs/run_new_matching.py

    # EAN clustering only, no LLM calls
    python jobs/run_new_matching.py --no-llm

    # Rebuild from scratch (drops cluster_members + product_clusters first)
    python jobs/run_new_matching.py --force
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from agnaradie_pricing.db.models import Base
from agnaradie_pricing.db.session import make_engine, make_session_factory
from agnaradie_pricing.matching.new_matching import run_new_matching
from agnaradie_pricing.settings import Settings


def main(*, force: bool, use_llm: bool) -> dict:
    settings = Settings()
    Base.metadata.create_all(make_engine(settings))
    factory = make_session_factory(settings)
    return run_new_matching(factory, settings, force=force, use_llm=use_llm)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Drop existing clusters before running.",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Skip the vector + LLM fuzzy phase.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    counters = main(force=args.force, use_llm=not args.no_llm)
    print(
        "\n=== Done ===\n"
        f"  EAN clusters created : {counters.get('ean_clusters_created', 0)}\n"
        f"  EAN members added    : {counters.get('ean_members_added', 0)}\n"
        f"  Fuzzy attempted      : {counters.get('fuzzy_pairs_attempted', 0)}\n"
        f"  Fuzzy approved       : {counters.get('fuzzy_approved', 0)}\n"
        f"  Fuzzy pending review : {counters.get('fuzzy_pending', 0)}"
    )
