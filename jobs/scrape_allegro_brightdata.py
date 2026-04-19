"""Scrape Allegro.sk offers via Bright Data Scraping Browser.

No local Chrome needed — Bright Data hosts the browser and handles
Cloudflare / DataDome bot protection automatically.

Setup:
    Set BRD_CUSTOMER_ID and BRD_API_KEY in your .env file.
    (Customer ID is in Bright Data dashboard → Account → Customer ID)

Usage:
    python jobs/scrape_allegro_brightdata.py --limit 10
    python jobs/scrape_allegro_brightdata.py --rows 2484-4979
    python jobs/scrape_allegro_brightdata.py --resume
    python jobs/scrape_allegro_brightdata.py            # full run
"""

import argparse
import asyncio
import csv
import logging
import os
import random
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

log = logging.getLogger(__name__)

_DEFAULT_INPUT = "data/allegro_eans.csv"
_DEFAULT_OUTPUT = "data/allegro_offers.csv"
_BASE_URL = "https://allegro.sk"
_FIELDNAMES = ["ean", "title", "seller", "seller_url", "price_eur", "delivery_eur", "scraped_at"]

_EXTRACT_JS = """() => {
    return [...document.querySelectorAll("article")].map(a => {
        const text = a.innerText || "";
        const sellerM = text.match(/Predajca:\\s*([^\\n|]+)/);
        const priceM  = text.match(/(\\d+[,.]\\d{2})\\s*\\u20ac/);
        const delivM  = text.match(/(\\d+[,.]\\d{2})\\s*\\u20ac\\s*s doru/);
        const links   = [...a.querySelectorAll("a")].filter(
            l => l.href.includes("/obchod/") || l.href.includes("/uzivatel/")
        );
        const titleEl = a.querySelector("h2, h3");
        return {
            title:        titleEl ? titleEl.innerText.trim() : "",
            seller:       sellerM ? sellerM[1].trim() : "",
            price_eur:    priceM  ? priceM[1].replace(",", ".") : null,
            delivery_eur: delivM  ? delivM[1].replace(",", ".") : null,
            seller_url:   links[0] ? links[0].href : ""
        };
    });
}"""


def _brd_ws_endpoint() -> str:
    customer_id = os.getenv("BRD_CUSTOMER_ID", "")
    api_key = os.getenv("BRD_API_KEY", "")
    zone = os.getenv("BRD_ZONE", "scraping_browser")
    if not customer_id or customer_id == "YOUR_CUSTOMER_ID":
        raise RuntimeError(
            "Set BRD_CUSTOMER_ID in your .env file.\n"
            "Find it in Bright Data dashboard → Account → Customer ID."
        )
    if not api_key:
        raise RuntimeError("Set BRD_API_KEY in your .env file.")
    endpoint = f"wss://brd-customer-{customer_id}-zone-{zone}:{api_key}@brd.superproxy.io:9222"
    print(f"Connecting to: {endpoint.split('@')[1]}")  # log host without credentials
    return endpoint


async def scrape_ean(page, ean: str) -> list[dict]:
    try:
        await page.goto(
            f"{_BASE_URL}/vyhladavanie?string={ean}",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
    except Exception as e:
        log.debug("search timeout for %s: %s", ean, e)
        return []

    try:
        product_links = await page.eval_on_selector_all(
            'a[href*="/produkt/"]', "els => els.map(e => e.href)"
        )
    except Exception:
        return []

    if not product_links:
        return []

    product_url = product_links[0].split("?")[0] + "#inne-oferty-produktu"
    try:
        await page.goto(product_url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_selector("article", timeout=15_000)
    except Exception as e:
        log.debug("product page timeout for %s: %s", ean, e)
        return []

    try:
        raw: list[dict] = await page.evaluate(_EXTRACT_JS)
    except Exception as e:
        log.debug("extraction error for %s: %s", ean, e)
        return []

    scraped_at = datetime.now(UTC).isoformat()
    return [
        {
            "ean": ean,
            "title": o["title"],
            "seller": o["seller"],
            "seller_url": o["seller_url"],
            "price_eur": o["price_eur"],
            "delivery_eur": o["delivery_eur"],
            "scraped_at": scraped_at,
        }
        for o in raw
        if o.get("seller")
    ]


async def run(
    input_path: str,
    output_path: str,
    limit: int,
    concurrency: int,
    resume: bool,
    row_start: int | None = None,
    row_end: int | None = None,
) -> int:
    ws_endpoint = _brd_ws_endpoint()

    with open(input_path, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    eans = [r["ean"] for r in all_rows]

    if row_start is not None or row_end is not None:
        lo = (row_start - 2) if row_start is not None else 0
        hi = (row_end - 1) if row_end is not None else len(eans)
        eans = eans[max(lo, 0):hi]
        print(f"Row range {row_start}–{row_end}: {len(eans)} EANs selected")

    if limit:
        eans = eans[:limit]

    if resume and Path(output_path).exists():
        with open(output_path, newline="", encoding="utf-8") as f:
            done = {r["ean"] for r in csv.DictReader(f)}
        before = len(eans)
        eans = [e for e in eans if e not in done]
        print(f"Resume: skipping {before - len(eans)} done, {len(eans)} remaining")

    print(f"Scraping {len(eans)} EANs  concurrency={concurrency}  via Bright Data Scraping Browser")

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    append = resume and out_path.exists()
    out_file = open(output_path, "a" if append else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_file, fieldnames=_FIELDNAMES)
    if not append:
        writer.writeheader()

    total_offers = 0
    not_found = 0
    semaphore = asyncio.Semaphore(concurrency)

    async with async_playwright() as pw:
        browser = await pw.chromium.connect_over_cdp(ws_endpoint)
        ctx = browser.contexts[0] if browser.contexts else await browser.new_context()

        async def process(ean: str, idx: int) -> None:
            nonlocal total_offers, not_found
            async with semaphore:
                page = await ctx.new_page()
                try:
                    offers = await scrape_ean(page, ean)
                    if offers:
                        for o in offers:
                            writer.writerow(o)
                        out_file.flush()
                        total_offers += len(offers)
                        print(f"[{idx+1}/{len(eans)}] {ean}  → {len(offers)} offers")
                    else:
                        not_found += 1
                        print(f"[{idx+1}/{len(eans)}] {ean}  → not found")
                except Exception as e:
                    log.warning("error on %s: %s", ean, e)
                    not_found += 1
                finally:
                    await page.close()

        try:
            await asyncio.gather(*[process(e, i) for i, e in enumerate(eans)])
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\nInterrupted — saving progress.")

        try:
            await browser.close()
        except Exception:
            pass

    out_file.close()
    print(f"\nDone. {total_offers} offers from {len(eans) - not_found}/{len(eans)} EANs.")
    return 0


def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Scrape Allegro.sk via Bright Data Scraping Browser")
    parser.add_argument("--input", default=_DEFAULT_INPUT)
    parser.add_argument("--output", default=_DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=0, help="Limit EANs (0 = all)")
    parser.add_argument("--rows", metavar="START-END",
                        help="Excel row range, e.g. 2484-4979 (header=row 1, data from row 2)")
    parser.add_argument("--concurrency", type=int, default=5, help="Parallel browser tabs")
    parser.add_argument("--resume", action="store_true", help="Skip already-scraped EANs")
    args = parser.parse_args()

    row_start = row_end = None
    if args.rows:
        parts = args.rows.split("-")
        row_start, row_end = int(parts[0]), int(parts[1])

    sys.exit(asyncio.run(run(
        input_path=args.input,
        output_path=args.output,
        limit=args.limit,
        concurrency=args.concurrency,
        resume=args.resume,
        row_start=row_start,
        row_end=row_end,
    )))


if __name__ == "__main__":
    main()
