"""Run the unified product matching pipeline.

Usage
-----
    python jobs/run_matching.py              # full run: EAN cluster → regex → LLM
    python jobs/run_matching.py --force      # wipe all matches and re-run from scratch
    python jobs/run_matching.py --no-llm     # EAN cluster + regex only (no API calls)
    python jobs/run_matching.py --resume     # continue LLM phase after a crash —
                                             # skips phases 0/1, picks up unmatched listings
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
                        help="Wipe all matches and re-run everything from scratch")
    parser.add_argument("--no-llm", action="store_true",
                        help="Skip vector+LLM phase (EAN cluster + regex only)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume LLM phase after a crash — skips EAN/regex phases, "
                             "processes only listings not yet matched")
    args = parser.parse_args()

    if args.no_llm and args.resume:
        parser.error("--no-llm and --resume cannot be used together")
    if args.force and args.resume:
        parser.error("--force and --resume cannot be used together")

    settings = Settings()
    factory = make_session_factory(settings)

    llm_client = None if args.no_llm else _get_llm_client()
    if llm_client is None and not args.no_llm:
        if args.resume:
            parser.error("--resume requires OPENAI_API_KEY")
        print("[run_matching] OPENAI_API_KEY not set — running without LLM phase")

    with factory() as session:
        counts = run_matching(
            session,
            llm_client=llm_client,
            force=args.force,
            llm_only=args.resume,  # resume = skip phases 0/1, run LLM on remaining orphans
        )

    print("\n[run_matching] summary:")
    for key, val in counts.items():
        print(f"  {key:<15} {val}")


if __name__ == "__main__":
    main()
