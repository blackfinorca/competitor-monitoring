"""Competitor site probing helpers."""

from dataclasses import dataclass
from urllib.parse import urljoin

import httpx


HEUREKA_FEED_PATHS = (
    "/heureka.xml",
    "/heureka-feed.xml",
    "/heureka/export.xml",
    "/feed/heureka.xml",
)


@dataclass(frozen=True)
class ProbeResult:
    url: str
    status_code: int | None
    content_type: str | None = None
    text: str | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status_code is not None and 200 <= self.status_code < 300


@dataclass(frozen=True)
class CompetitorInspectionReport:
    base_url: str
    robots_txt: ProbeResult
    sitemaps: list[str]
    sitemap_probe: ProbeResult | None
    feed_probes: list[ProbeResult]
    heureka_feed_url: str | None


def inspect_competitor(
    base_url: str,
    http_client: httpx.Client | None = None,
) -> CompetitorInspectionReport:
    base_url = base_url.rstrip("/")
    owns_client = http_client is None
    client = http_client or httpx.Client(timeout=10.0, follow_redirects=True)
    try:
        robots = _probe(client, urljoin(base_url + "/", "robots.txt"))
        sitemaps = _extract_sitemaps(robots)
        sitemap_probe = _probe(client, sitemaps[0]) if sitemaps else None
        feed_probes = [
            _probe(client, urljoin(base_url, path)) for path in HEUREKA_FEED_PATHS
        ]
        feed_url = next((probe.url for probe in feed_probes if _looks_like_xml(probe)), None)
        return CompetitorInspectionReport(
            base_url=base_url,
            robots_txt=robots,
            sitemaps=sitemaps,
            sitemap_probe=sitemap_probe,
            feed_probes=feed_probes,
            heureka_feed_url=feed_url,
        )
    finally:
        if owns_client:
            client.close()


def _probe(client: httpx.Client, url: str) -> ProbeResult:
    try:
        response = client.get(url)
    except httpx.HTTPError as exc:
        return ProbeResult(url=url, status_code=None, error=str(exc))
    return ProbeResult(
        url=str(response.url),
        status_code=response.status_code,
        content_type=response.headers.get("content-type"),
        text=response.text,
    )


def _extract_sitemaps(robots: ProbeResult) -> list[str]:
    if not robots.ok or robots.text is None:
        return []
    sitemaps = []
    for line in robots.text.splitlines():
        key, _, value = line.partition(":")
        if key.strip().lower() == "sitemap" and value.strip():
            sitemaps.append(value.strip())
    return sitemaps


def _looks_like_xml(probe: ProbeResult) -> bool:
    return probe.ok and probe.url.endswith(".xml")
