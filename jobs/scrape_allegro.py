"""Scrape Allegro.sk offers for each EAN in the input CSV.

Allegro.sk is behind Cloudflare bot protection, so we connect to a running
Chrome instance via CDP (Chrome DevTools Protocol) instead of launching a
new headless browser.

HOW TO USE
----------
1. Launch Chrome with remote debugging:
     open -na "Google Chrome" --args --remote-debugging-port=9222 \
       --user-data-dir=/tmp/allegro-chrome
   (or use the --launch-chrome flag below)

2. Run the scraper:
     python jobs/scrape_allegro.py --limit 3              # test: 3 EANs
     python jobs/scrape_allegro.py --limit 50             # validate: 50 EANs
     python jobs/scrape_allegro.py                        # full run (~24k EANs)

For each EAN the scraper:
  1. Searches allegro.sk/vyhladavanie?string={EAN} → finds /produkt/ URL
  2. Loads the product page with #inne-oferty-produktu → waits for offer articles
  3. Extracts: title, seller, price_eur, delivery_eur per offer
"""

import argparse
import asyncio
import csv
import json
import logging
import random
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

from playwright.async_api import async_playwright

log = logging.getLogger(__name__)

_DEFAULT_INPUT = "data/allegro_eans.csv"
_DEFAULT_OUTPUT = "data/allegro_offers.csv"
_CDP_URL = "http://localhost:9222"
_BASE_URL = "https://allegro.sk"
_FIELDNAMES = ["ean", "title", "seller", "seller_url", "price_eur", "delivery_eur", "scraped_at"]
_COOKIE_FILE = "data/allegro_cookies.json"

# JS to extract all offer articles from the product page
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


async def scrape_ean(page, ean: str) -> list[dict]:
    # 4–7s pause before each scrape (simulates reading/thinking)
    pre = random.uniform(4.0, 7.0)
    print(f"  pre-scrape {pre:.1f}s", flush=True)
    await asyncio.sleep(pre)

    try:
        await page.goto(
            f"{_BASE_URL}/vyhladavanie?string={quote(ean)}",
            wait_until="domcontentloaded",
            timeout=20_000,
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
        await page.goto(product_url, wait_until="domcontentloaded", timeout=20_000)
        await page.wait_for_selector("article", timeout=12_000)
    except Exception as e:
        log.debug("product page timeout for %s: %s", ean, e)
        return []

    try:
        raw: list[dict] = await page.evaluate(_EXTRACT_JS)
    except Exception as e:
        log.debug("extraction error for %s: %s", ean, e)
        return []

    scraped_at = datetime.now(UTC).isoformat()
    offers = [
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

    # 2–5s pause after scrape
    post = random.uniform(2.0, 5.0)
    print(f"  post-scrape {post:.1f}s", flush=True)
    await asyncio.sleep(post)

    return offers


def _launch_chrome(port: int) -> subprocess.Popen:
    import shutil
    chrome_path = shutil.which("google-chrome") or shutil.which("chromium")
    if sys.platform == "darwin":
        chrome_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if not chrome_path:
        raise RuntimeError("Chrome not found. Launch manually with --remote-debugging-port")
    return subprocess.Popen([
        chrome_path,
        f"--remote-debugging-port={port}",
        "--user-data-dir=/tmp/allegro-scraper-chrome",
        "--disable-blink-features=AutomationControlled",
        "--no-first-run",
        "--no-default-browser-check",
        _BASE_URL,
    ])


async def run(
    input_path: str,
    output_path: str,
    limit: int,
    concurrency: int,
    delay: float,
    cdp_url: str,
    resume: bool,
    row_start: int | None = None,
    row_end: int | None = None,
) -> int:
    with open(input_path, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    eans = [r["ean"] for r in all_rows]

    # Row range (1-based, matching Excel row numbers; header = row 1, data starts at row 2)
    if row_start is not None or row_end is not None:
        lo = (row_start - 2) if row_start is not None else 0          # row 2 → index 0
        hi = (row_end  - 1) if row_end   is not None else len(eans)   # row N → index N-1 (exclusive)
        eans = eans[max(lo, 0):hi]
        print(f"Row range {row_start}–{row_end}: {len(eans)} EANs selected")

    if limit:
        eans = eans[:limit]

    # Resume: skip EANs already in output
    if resume and Path(output_path).exists():
        with open(output_path, newline="", encoding="utf-8") as f:
            done = {r["ean"] for r in csv.DictReader(f)}
        before = len(eans)
        eans = [e for e in eans if e not in done]
        log.info("Resume: skipping %d already-scraped EANs, %d remaining", before - len(eans), len(eans))

    print(f"Scraping {len(eans)} EANs  concurrency={concurrency}  CDP={cdp_url}")

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    append = resume and out_path.exists()
    out_file = open(output_path, "a" if append else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_file, fieldnames=_FIELDNAMES)
    if not append:
        writer.writeheader()

    total_offers = 0
    not_found = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.connect_over_cdp(cdp_url)
        ctx = browser.contexts[0]

        cookie_path = Path(_COOKIE_FILE)
        if cookie_path.exists():
            try:
                saved = json.loads(cookie_path.read_text())
                await ctx.add_cookies(saved)
                print(f"Loaded {len(saved)} cookies from {_COOKIE_FILE}")
            except (json.JSONDecodeError, Exception) as e:
                log.warning("Could not load cookies from %s: %s", _COOKIE_FILE, e)

        print("\nChrome connected. Solve any Cloudflare challenge in the browser window,")
        print("then press Enter to start scraping (or just Enter if cookies restored session)...")
        await asyncio.get_event_loop().run_in_executor(None, input)

        # Save cookies so next session skips the Cloudflare prompt
        cookies = await ctx.cookies()
        cookie_path.parent.mkdir(parents=True, exist_ok=True)
        cookie_path.write_text(json.dumps(cookies))
        print(f"Saved {len(cookies)} cookies to {_COOKIE_FILE}")

        next_long_pause = random.randint(8, 15)
        try:
            for idx, ean in enumerate(eans):
                # Long human-like break every 8–15 scrapes
                if idx > 0 and idx % next_long_pause == 0:
                    pause = random.uniform(20.0, 30.0)
                    print(f"\n--- long pause {pause:.1f}s after {idx} scrapes ---\n", flush=True)
                    await asyncio.sleep(pause)
                    next_long_pause = random.randint(8, 15)

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
                finally:
                    await page.close()
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

    parser = argparse.ArgumentParser(description="Scrape Allegro.sk offers by EAN via CDP")
    parser.add_argument("--input", default=_DEFAULT_INPUT)
    parser.add_argument("--output", default=_DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=0, help="Limit EANs (0 = all)")
    parser.add_argument("--rows", metavar="START-END",
                        help="Excel row range to process, e.g. 2484-4979 (header=row 1, data from row 2)")
    parser.add_argument("--concurrency", type=int, default=3, help="Parallel browser tabs")
    parser.add_argument("--delay", type=float, default=3.0, help="Base delay between EANs (s), +0.5–2.5s random")
    parser.add_argument("--cdp", default=_CDP_URL, help="Chrome CDP URL")
    parser.add_argument("--resume", action="store_true", help="Skip already-scraped EANs")
    parser.add_argument("--launch-chrome", action="store_true", help="Launch Chrome automatically")
    args = parser.parse_args()

    row_start = row_end = None
    if args.rows:
        parts = args.rows.split("-")
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            parser.error(f"--rows must be START-END with positive integers, got: {args.rows!r}")
        row_start, row_end = int(parts[0]), int(parts[1])

    chrome_proc = None
    if args.launch_chrome:
        port = int(args.cdp.split(":")[-1])
        chrome_proc = _launch_chrome(port)
        time.sleep(3)  # wait for Chrome to start

    try:
        sys.exit(asyncio.run(run(
            input_path=args.input,
            output_path=args.output,
            limit=args.limit,
            concurrency=args.concurrency,
            delay=args.delay,
            cdp_url=args.cdp,
            resume=args.resume,
            row_start=row_start,
            row_end=row_end,
        )))
    finally:
        if chrome_proc:
            chrome_proc.terminate()


if __name__ == "__main__":
    main()
