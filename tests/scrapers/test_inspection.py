import httpx

from agnaradie_pricing.scrapers.inspection import inspect_competitor


def test_inspect_competitor_reports_robots_sitemap_and_feed() -> None:
    base_url = "https://example.sk"
    responses = {
        f"{base_url}/robots.txt": httpx.Response(
            200, text="User-agent: *\nSitemap: https://example.sk/sitemap.xml\n"
        ),
        f"{base_url}/sitemap.xml": httpx.Response(200, text="<xml />"),
        f"{base_url}/heureka.xml": httpx.Response(200, text="<SHOP></SHOP>"),
    }
    transport = httpx.MockTransport(
        lambda request: responses.get(str(request.url), httpx.Response(404))
    )

    report = inspect_competitor(base_url, http_client=httpx.Client(transport=transport))

    assert report.base_url == base_url
    assert report.robots_txt.status_code == 200
    assert report.sitemaps == ["https://example.sk/sitemap.xml"]
    assert report.heureka_feed_url == "https://example.sk/heureka.xml"
