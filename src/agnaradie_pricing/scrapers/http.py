"""Shared HTTP client factory for all scrapers.

Goals
-----
- Look like a real browser (User-Agent, Accept headers, language).
- Enforce per-scraper rate limiting with random jitter so requests are not
  perfectly periodic (a common bot-detection signal).
- Handle 429 / 503 responses with exponential back-off and retry.
- Keep a persistent session (cookies, connection pooling) per scraper instance.

Usage
-----
    from agnaradie_pricing.scrapers.http import make_client, polite_get

    client = make_client()          # use in scraper __init__
    resp   = polite_get(client, url, min_rps=1)   # use instead of client.get()
"""

from __future__ import annotations

import random
import time
from typing import Any

import httpx

# A recent Chrome UA on macOS — realistic and widely seen
_DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_DEFAULT_HEADERS = {
    "User-Agent": _DEFAULT_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "sk-SK,sk;q=0.9,cs;q=0.8,en-US;q=0.7,en;q=0.6",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

# Back-off: seconds to wait after a 429/503 before retrying
_BACKOFF_SCHEDULE = [5, 15, 60]  # 3 attempts, then give up


def make_client(
    timeout: float = 12.0,
    extra_headers: dict[str, str] | None = None,
) -> httpx.Client:
    """Return a session-persistent httpx.Client that looks like a browser."""
    headers = {**_DEFAULT_HEADERS, **(extra_headers or {})}
    return httpx.Client(
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
        http2=False,  # most Slovak shops don't use HTTP/2; keep it simple
    )


def polite_get(
    client: httpx.Client,
    url: str,
    *,
    min_rps: float = 1.0,
    jitter: float = 0.4,
    referer: str | None = None,
    **kwargs: Any,
) -> httpx.Response:
    """GET a URL with rate-limiting, jitter, and automatic back-off on 429/503.

    Parameters
    ----------
    client      Persistent httpx.Client (maintains cookies between calls).
    url         Target URL.
    min_rps     Maximum request rate in requests-per-second (default 1 rps).
    jitter      Random extra delay added on top of the base interval, in seconds
                (uniform distribution 0 … jitter). Default 0.4 s.
    referer     Optional Referer header to set for this request.
    **kwargs    Passed straight through to client.get().
    """
    base_delay = 1.0 / max(min_rps, 0.1)
    sleep_for = base_delay + random.uniform(0, jitter)
    time.sleep(sleep_for)

    headers = {}
    if referer:
        headers["Referer"] = referer

    last_exc: Exception | None = None
    for attempt, backoff in enumerate([0] + _BACKOFF_SCHEDULE):
        if backoff:
            time.sleep(backoff + random.uniform(0, 2))
        try:
            response = client.get(url, headers=headers or None, **kwargs)
            if response.status_code == 429 or response.status_code == 503:
                if attempt < len(_BACKOFF_SCHEDULE):
                    continue   # retry after backoff
                response.raise_for_status()  # give up, raise
            return response
        except httpx.TimeoutException as exc:
            last_exc = exc
            if attempt >= len(_BACKOFF_SCHEDULE):
                raise
        except httpx.HTTPStatusError:
            raise

    raise last_exc  # type: ignore[misc]
