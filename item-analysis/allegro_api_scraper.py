"""Allegro API scraper scaffold.

This module is intentionally minimal for now.
We will extend it with endpoint discovery, authentication, pagination,
and persistence in follow-up changes.
"""

from __future__ import annotations


class AllegroApiScraper:
    """Starter class for future Allegro API-based scraping."""

    def __init__(self) -> None:
        self.base_url = "https://api.allegro.pl"

    def run(self) -> list[dict]:
        """Run scraper and return normalized records.

        Placeholder implementation until API integration is added.
        """
        return []


if __name__ == "__main__":
    scraper = AllegroApiScraper()
    records = scraper.run()
    print(f"Allegro API scraper scaffold ready. Records fetched: {len(records)}")
