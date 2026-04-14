#!/usr/bin/env python3
"""Manual competitor inspection helper."""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agnaradie_pricing.scrapers.inspection import inspect_competitor


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="Competitor base URL, for example https://example.sk")
    args = parser.parse_args()

    report = inspect_competitor(args.url)
    print(f"Base URL: {report.base_url}")
    print(f"robots.txt: {_format_probe(report.robots_txt)}")
    if report.sitemaps:
        print("Sitemaps:")
        for sitemap in report.sitemaps:
            print(f"  - {sitemap}")
    if report.sitemap_probe:
        print(f"First sitemap probe: {_format_probe(report.sitemap_probe)}")
    print("Heureka feed probes:")
    for probe in report.feed_probes:
        print(f"  - {_format_probe(probe)}")
    print(f"Heureka feed: {report.heureka_feed_url or 'not found'}")


def _format_probe(probe) -> str:
    if probe.status_code is None:
        return f"{probe.url} ERROR {probe.error}"
    return f"{probe.url} HTTP {probe.status_code}"


if __name__ == "__main__":
    main()
