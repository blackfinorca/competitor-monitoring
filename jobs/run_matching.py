"""Run the unified product matching pipeline.

Usage
-----
    python jobs/run_matching.py              # match only new unmatched listings
    python jobs/run_matching.py --force      # re-match everything from scratch
    python jobs/run_matching.py --no-llm     # skip vector+LLM phase
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

import os
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.settings import Settings
from agnaradie_pricing.matching.pipeline import run_matching


def _get_llm_client():
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    from agnaradie_pricing.matching.llm_matcher import OpenAIClient
    return OpenAIClient(api_key=api_key, model=model)


def main() -> None:
    parser = argparse.ArgumentParser(description="Unified product matching pipeline")
    parser.add_argument("--force", action="store_true",
                        help="Re-match listings that already have a match")
    parser.add_argument("--no-llm", action="store_true",
                        help="Skip vector+LLM phase (deterministic + regex only)")
    args = parser.parse_args()

    settings = Settings()
    factory = make_session_factory(settings)

    llm_client = None if args.no_llm else _get_llm_client()
    if llm_client is None and not args.no_llm:
        print("[run_matching] OPENAI_API_KEY not set — running without LLM phase")

    with factory() as session:
        counts = run_matching(session, llm_client=llm_client, force=args.force)

    print("\n[run_matching] summary:")
    for key, val in counts.items():
        print(f"  {key:<15} {val}")


if __name__ == "__main__":
    main()
