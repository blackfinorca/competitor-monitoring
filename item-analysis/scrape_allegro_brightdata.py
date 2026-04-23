"""Scrape Allegro.sk offers via Bright Data Residential Proxy.

Playwright runs locally but all traffic is routed through Bright Data's
residential proxy network, giving real residential IPs per request.

Setup (.env):
    BRD_PROXY_HOST=brd.superproxy.io
    BRD_PROXY_PORT=33335
    BRD_PROXY_USER=brd-customer-hl_67784f44-zone-allegro_residential_proxy
    BRD_PROXY_PASS=yourpassword
    BRD_PROXY_IGNORE_HTTPS_ERRORS=true

Usage:
    python item-analysis/scrape_allegro_brightdata.py --limit 10
    python item-analysis/scrape_allegro_brightdata.py --rows 2484-4979
    python item-analysis/scrape_allegro_brightdata.py --resume
    python item-analysis/scrape_allegro_brightdata.py            # full run
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

_DEFAULT_INPUT = "item-analysis/allegro_eans.csv"
_DEFAULT_OUTPUT = "item-analysis/allegro_offers.csv"
_BASE_URL = "https://allegro.sk"
_FIELDNAMES = ["ean", "title", "seller", "seller_url", "price_eur", "delivery_eur", "box_price_eur", "scraped_at"]

# JS to extract all offer articles from the product page.
# Skips articles that live inside "Najrýchlejšie" or "Najlacnejšie" sections.
_EXTRACT_JS = """() => {
    const excluded = new Set();
    for (const h of document.querySelectorAll("h2, h3, h4")) {
        const t = h.innerText || "";
        if (!t.includes("Najr") && !t.includes("Najla")) continue;
        let node = h.parentElement;
        for (let i = 0; i < 8; i++) {
            if (!node || node === document.body) break;
            const arts = node.querySelectorAll("article");
            if (arts.length) { arts.forEach(a => excluded.add(a)); break; }
            node = node.parentElement;
        }
    }

    return [...document.querySelectorAll("article")]
        .filter(a => !excluded.has(a))
        .map(a => {
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

_BOX_PRICE_JS = """() => {
    const meta = document.querySelector('meta[itemprop="price"]');
    if (meta) { const v = meta.getAttribute("content"); if (v) return v; }

    const buyBtnSels = ['button', 'a'];
    for (const tag of buyBtnSels) {
        for (const el of document.querySelectorAll(tag)) {
            const t = el.innerText || "";
            if (!t.includes("Pridať do košíka") && !t.includes("Kúpiť teraz")) continue;
            let node = el.parentElement;
            for (let i = 0; i < 10; i++) {
                if (!node || node === document.body) break;
                const m = (node.innerText || "").match(/(\\d+[,.]\\d{2})\\s*\\u20ac/);
                if (m) return m[1].replace(",", ".");
                node = node.parentElement;
            }
        }
    }

    for (const sec of document.querySelectorAll("section, [role='region'], aside")) {
        const t = sec.innerText || "";
        if (t.includes("Podmienky ponuky")) {
            const m = t.match(/(\\d+[,.]\\d{2})\\s*\\u20ac/);
            if (m) return m[1].replace(",", ".");
        }
    }

    const offerPrice = document.querySelector('[itemprop="offers"] [itemprop="price"]');
    if (offerPrice) return offerPrice.getAttribute("content") || offerPrice.innerText.trim();

    const m2 = document.body.innerText.slice(0, 3000).match(/(\\d+[,.]\\d{2})\\s*\\u20ac/);
    return m2 ? m2[1].replace(",", ".") : null;
}"""


def _brd_proxy_config() -> dict:
    host = os.getenv("BRD_PROXY_HOST", "brd.superproxy.io")
    port = os.getenv("BRD_PROXY_PORT", "33335")
    user = os.getenv("BRD_PROXY_USER", "")
    password = os.getenv("BRD_PROXY_PASS", "")
    if not user or not password:
        raise RuntimeError(
            "Set BRD_PROXY_USER and BRD_PROXY_PASS in your .env file.\n"
            "Example:\n"
            "  BRD_PROXY_USER=brd-customer-hl_xxxxx-zone-allegro_residential_proxy\n"
            "  BRD_PROXY_PASS=yourpassword"
        )
    print(f"Proxy: {host}:{port} (user: {user.split(':')[0]})")
    return {"server": f"http://{host}:{port}", "username": user, "password": password}


def _ignore_https_errors() -> bool:
    return os.getenv("BRD_PROXY_IGNORE_HTTPS_ERRORS", "true").strip().lower() not in {"0", "false", "no", "off"}


async def scrape_ean(page, ean: str, debug: bool = False) -> list[dict]:
    try:
        await page.goto(
            f"{_BASE_URL}/vyhladavanie?string={ean}",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
    except Exception as e:
        log.warning("search timeout for %s: %s", ean, e)
        return []

    current_url = page.url
    if debug:
        title = await page.title()
        print(f"  [debug] search page: url={current_url!r} title={title!r}", flush=True)

    if "/produkt/" in current_url:
        product_url = current_url.split("?")[0] + "#inne-oferty-produktu"
    else:
        # Product links are JS-rendered — wait before querying
        try:
            await page.wait_for_selector('a[href*="/produkt/"]', timeout=10_000)
        except Exception:
            pass

        try:
            product_links = await page.eval_on_selector_all(
                'a[href*="/produkt/"]', "els => els.map(e => e.href)"
            )
        except Exception:
            return []

        if not product_links:
            try:
                await page.wait_for_selector('a[href*="/oferta/"]', timeout=5_000)
                product_links = await page.eval_on_selector_all(
                    'a[href*="/oferta/"]', "els => els.map(e => e.href)"
                )
            except Exception:
                product_links = []

        if not product_links:
            if debug:
                print(f"  [debug] no product/oferta links found for {ean}", flush=True)
            return []

        product_url = product_links[0].split("?")[0] + "#inne-oferty-produktu"

    if "/oferta/" in product_url and "/produkt/" not in product_url:
        try:
            await page.goto(product_url.split("#")[0], wait_until="domcontentloaded", timeout=30_000)
            parent_links = await page.eval_on_selector_all(
                'a[href*="/produkt/"]', "els => els.map(e => e.href)"
            )
            if parent_links:
                product_url = parent_links[0].split("?")[0] + "#inne-oferty-produktu"
        except Exception as e:
            log.debug("oferta redirect for %s: %s", ean, e)

    try:
        await page.goto(product_url, wait_until="domcontentloaded", timeout=30_000)

        if debug:
            title = await page.title()
            print(f"  [debug] product page: url={page.url!r} title={title!r}", flush=True)

        # Click "Všetky ponuky" button to expand the full offer list
        try:
            btn = await page.query_selector(
                'button:has-text("ponuky"), a:has-text("ponuky"), '
                'button:has-text("Všetky"), a:has-text("Všetky")'
            )
            if btn:
                await btn.click()
                await asyncio.sleep(1.5)
        except Exception:
            pass

        # Wait for offer articles — reviews appear first, only offers have "Predajca:"
        await page.wait_for_function(
            "() => [...document.querySelectorAll('article')].some(a => a.innerText.includes('Predajca:'))",
            timeout=20_000,
        )
    except Exception as e:
        if debug:
            title = await page.title()
            print(f"  [debug] product page FAILED for {ean}: url={page.url!r} title={title!r} err={e}", flush=True)
        else:
            log.warning("product page timeout for %s: %s", ean, e)
        return []

    try:
        raw: list[dict] = await page.evaluate(_EXTRACT_JS)
    except Exception as e:
        log.debug("extraction error for %s: %s", ean, e)
        return []

    try:
        box_price = await page.evaluate(_BOX_PRICE_JS)
    except Exception:
        box_price = None

    scraped_at = datetime.now(UTC).isoformat()
    return [
        {
            "ean": ean,
            "title": o["title"],
            "seller": o["seller"],
            "seller_url": o["seller_url"],
            "price_eur": o["price_eur"],
            "delivery_eur": o["delivery_eur"],
            "box_price_eur": box_price,
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
    skip_found: bool = False,
    eans_filter: list[str] | None = None,
    debug: bool = False,
    row_start: int | None = None,
    row_end: int | None = None,
) -> int:
    proxy = _brd_proxy_config()

    with open(input_path, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    eans = [r["ean"] for r in all_rows]

    if eans_filter:
        eans = [e for e in eans if e in set(eans_filter)]
        print(f"--eans filter: {len(eans)} EANs selected")

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
        print(f"--resume: skipping {before - len(eans)} already-scraped EANs, {len(eans)} remaining")

    if skip_found and Path(output_path).exists():
        with open(output_path, newline="", encoding="utf-8") as f:
            found = {r["ean"] for r in csv.DictReader(f) if r.get("seller")}
        before = len(eans)
        eans = [e for e in eans if e not in found]
        print(f"--skip-found: skipping {before - len(eans)} EANs with existing offers, {len(eans)} remaining")

    print(f"Scraping {len(eans)} EANs  concurrency={concurrency}  via Bright Data Residential Proxy")

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    append = (resume or skip_found) and out_path.exists()
    out_file = open(output_path, "a" if append else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_file, fieldnames=_FIELDNAMES)
    if not append:
        writer.writeheader()

    counters = {"total": 0, "not_found": 0}
    write_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(concurrency)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(proxy=proxy, ignore_https_errors=_ignore_https_errors())

        async def process(ean: str, idx: int) -> None:
            async with semaphore:
                page = await ctx.new_page()
                try:
                    offers = await scrape_ean(page, ean, debug=debug)
                    async with write_lock:
                        if offers:
                            for o in offers:
                                writer.writerow(o)
                            out_file.flush()
                            counters["total"] += len(offers)
                            print(f"[{idx+1}/{len(eans)}] {ean}  → {len(offers)} offers", flush=True)
                        else:
                            counters["not_found"] += 1
                            print(f"[{idx+1}/{len(eans)}] {ean}  → not found", flush=True)
                except Exception as e:
                    log.warning("error on %s: %s", ean, e)
                    async with write_lock:
                        counters["not_found"] += 1
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
    total_offers = counters["total"]
    not_found = counters["not_found"]
    print(f"\nDone. {total_offers} offers from {len(eans) - not_found}/{len(eans)} EANs.")

    _rebuild_wide_excel(output_path)
    return 0


def _rebuild_wide_excel(offers_csv: str) -> None:
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parent))
    try:
        from export_allegro_offers import main as export_main
        wide_path = "item-analysis/allegro_offers_wide.xlsx"
        print(f"\nRebuilding {wide_path} …")
        export_main(
            input_path=offers_csv,
            output_path=wide_path,
            ref_path="item-analysis/Allegro zalistované položky 42026.xlsx",
            ref_sheet="export(1)",
            ref_ean_col="products_ean",
            ref_price_col="price",
            ref_label="KUTILOVO Price",
        )
    except Exception as e:
        print(f"Warning: could not rebuild wide Excel: {e}")


def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Scrape Allegro.sk via Bright Data Scraping Browser")
    parser.add_argument("--input", default=_DEFAULT_INPUT)
    parser.add_argument("--output", default=_DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=0, help="Limit EANs (0 = all)")
    parser.add_argument("--rows", metavar="START-END",
                        help="Excel row range, e.g. 2484-4979 (header=row 1, data from row 2)")
    parser.add_argument("--concurrency", type=int, default=5, help="Parallel browser tabs")
    parser.add_argument("--eans", help="Comma-separated EANs to scrape, e.g. '3253561947490,8590804097587'")
    parser.add_argument("--resume", action="store_true", help="Skip EANs already in output (any row)")
    parser.add_argument("--skip-found", action="store_true",
                        help="Skip EANs that already have ≥1 offer; retries 'not found' ones")
    parser.add_argument("--debug", action="store_true", help="Print page URL/title on each step to diagnose failures")
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
        skip_found=args.skip_found,
        eans_filter=[e.strip() for e in args.eans.split(",")] if args.eans else None,
        debug=args.debug,
        row_start=row_start,
        row_end=row_end,
    )))


if __name__ == "__main__":
    main()
