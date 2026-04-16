"""End-to-end pipeline runner.

Runs the full pricing pipeline in order:
  1. Ingest      — load AG catalogue CSV into DB
  2. Scrape      — pull competitor prices
  3. Match       — link competitor listings to AG products
  4. Recommend   — build pricing snapshots + playbook recommendations
  5. Alert       — dispatch Slack alerts for flagged products
  6. Export      — write reports/prices_YYYY-MM-DD.csv

Modes
-----
    python run_pipeline.py --full
        Run every step, scrape ALL competitors (including toolzone.sk).

    python run_pipeline.py --skip-toolzone
        Same as --full but skip toolzone.sk during the scrape step.
        Use this for routine daily runs where ToolZone data is already fresh.

Optional flags
--------------
    --llm               Enable LLM fuzzy matching layer (Layer 6) during match step.
    --no-ingest         Skip catalogue ingestion (DB already up-to-date).
    --no-alert          Skip Slack alert dispatch.
    --no-export         Skip CSV export.
    --catalogue PATH    Path to AG catalogue CSV (default: data/ag_catalogue.csv).
    --output PATH       Output CSV path (default: reports/prices_YYYY-MM-DD.csv).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap sys.path so sub-modules resolve without installing the package
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT / "src"))

from agnaradie_pricing.settings import load_competitors

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _step(name: str) -> None:
    print(f"\n{'='*60}")
    print(f"  STEP: {name}")
    print(f"{'='*60}")


def _elapsed(t0: float) -> str:
    s = time.monotonic() - t0
    m, s = divmod(int(s), 60)
    return f"{m}m {s}s" if m else f"{s}s"


# ---------------------------------------------------------------------------
# Step implementations (thin wrappers around job modules)
# ---------------------------------------------------------------------------

def step_ingest(catalogue_path: Path) -> None:
    _step("1 / 6  Catalogue ingestion")
    from jobs.daily_ingest import main as ingest_main
    count = ingest_main(path=catalogue_path)
    print(f"  Ingested: {count} rows")


def step_scrape(skip_competitors: set[str]) -> None:
    _step("2 / 6  Competitor scraping")
    from jobs.daily_scrape import main as scrape_main

    all_competitors = load_competitors()
    run_ids = [c["id"] for c in all_competitors if c["id"] not in skip_competitors]

    if skip_competitors:
        print(f"  Skipping: {sorted(skip_competitors)}")
    print(f"  Scraping: {run_ids}")

    counts = scrape_main(only=run_ids)
    total = sum(counts.values())
    for cid, n in sorted(counts.items()):
        print(f"    {cid}: {n} listings")
    print(f"  Total listings scraped: {total}")


def step_match(enable_llm: bool) -> None:
    _step("3 / 6  Product matching")
    from jobs.daily_match import main as match_main

    argv = ["--llm"] if enable_llm else []
    counts = match_main(argv=argv)
    print(f"  Deterministic:   {counts['matched']}")
    print(f"  LLM fuzzy:       {counts['llm_matched']}")
    print(f"  Skipped:         {counts['skipped']}")
    print(f"  Already matched: {counts['already_matched']}")


def step_recommend() -> None:
    _step("4 / 6  Pricing recommendations")
    from jobs.daily_recommend import main as recommend_main

    counts = recommend_main()
    print(f"  Snapshots:       {counts['snapshots']}")
    print(f"  Recommendations: {counts['recommendations']}")


def step_alert() -> None:
    _step("5 / 6  Slack alerts")
    from jobs.daily_alert import main as alert_main

    n = alert_main()
    print(f"  Alerts dispatched: {n}")


def step_export(output: Path | None) -> None:
    _step("6 / 6  CSV export")
    from jobs.export_prices import main as export_main

    argv = []
    if output:
        argv += ["--output", str(output)]

    path = export_main(argv=argv)
    print(f"  Saved: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the end-to-end AG Naradie pricing pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py --full
  python run_pipeline.py --skip-toolzone
  python run_pipeline.py --full --llm
  python run_pipeline.py --skip-toolzone --no-ingest
""",
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--full",
        action="store_true",
        help="Run full pipeline, scrape ALL competitors including toolzone.sk.",
    )
    mode.add_argument(
        "--skip-toolzone",
        action="store_true",
        dest="skip_toolzone",
        help="Run full pipeline but skip toolzone.sk during scraping.",
    )

    parser.add_argument(
        "--llm",
        action="store_true",
        help="Enable LLM fuzzy matching layer during the match step.",
    )
    parser.add_argument(
        "--no-ingest",
        action="store_true",
        dest="no_ingest",
        help="Skip catalogue ingestion (use when DB is already up-to-date).",
    )
    parser.add_argument(
        "--no-alert",
        action="store_true",
        dest="no_alert",
        help="Skip Slack alert dispatch.",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        dest="no_export",
        help="Skip CSV export.",
    )
    parser.add_argument(
        "--catalogue",
        type=Path,
        default=Path("data/ag_catalogue.csv"),
        metavar="PATH",
        help="Path to AG catalogue CSV (default: data/ag_catalogue.csv).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        metavar="PATH",
        help="Output CSV path (default: reports/prices_YYYY-MM-DD.csv).",
    )

    args = parser.parse_args(argv)

    # Determine which competitors to skip during scraping
    skip_in_scrape: set[str] = set()
    if args.skip_toolzone:
        # Skip all competitors marked as own_store
        all_competitors = load_competitors()
        skip_in_scrape = {c["id"] for c in all_competitors if c.get("own_store")}

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    t_total = time.monotonic()
    print("\nAG Naradie Pricing Pipeline")
    print(f"Mode: {'--full' if args.full else '--skip-toolzone'}")
    if args.llm:
        print("LLM layer: enabled")

    try:
        if not args.no_ingest:
            t = time.monotonic()
            step_ingest(args.catalogue)
            print(f"  [{_elapsed(t)}]")

        t = time.monotonic()
        step_scrape(skip_in_scrape)
        print(f"  [{_elapsed(t)}]")

        t = time.monotonic()
        step_match(args.llm)
        print(f"  [{_elapsed(t)}]")

        t = time.monotonic()
        step_recommend()
        print(f"  [{_elapsed(t)}]")

        if not args.no_alert:
            t = time.monotonic()
            step_alert()
            print(f"  [{_elapsed(t)}]")

        if not args.no_export:
            t = time.monotonic()
            step_export(args.output)
            print(f"  [{_elapsed(t)}]")

    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)

    print(f"\nPipeline complete. Total time: {_elapsed(t_total)}")


if __name__ == "__main__":
    main()
