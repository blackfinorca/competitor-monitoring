"""Scrape Allegro.sk offers for each EAN in the input CSV.

Allegro.sk is behind Cloudflare bot protection, so we connect to a running
Chrome instance via CDP (Chrome DevTools Protocol) instead of launching a
new headless browser.

HOW TO USE
----------
1. Launch 1–3 Chrome windows with remote debugging (different ports, different profiles).
   --enable-automation is required so Chrome accepts CDP browser-management commands:
     open -na "Google Chrome" --args --remote-debugging-port=9222 \
       --user-data-dir=/tmp/allegro-chrome-1 --enable-automation https://allegro.sk
     open -na "Google Chrome" --args --remote-debugging-port=9223 \
       --user-data-dir=/tmp/allegro-chrome-2 --enable-automation https://allegro.sk

2. Run the scraper:
     python item-analysis/scrape_allegro.py --cdp 9222,9223 --limit 10   # 2 browsers, 10 EANs
     python item-analysis/scrape_allegro.py --cdp 9222                    # single browser
     python item-analysis/scrape_allegro.py                               # full run, single browser

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
import re
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from playwright.async_api import async_playwright

log = logging.getLogger(__name__)

_DEFAULT_INPUT = "item-analysis/allegro_eans.csv"
_DEFAULT_OUTPUT = "item-analysis/allegro_offers.csv"
_CDP_URL = "http://localhost:9222"
_BASE_URL = "https://allegro.sk"
_FIELDNAMES = ["ean", "title", "seller", "seller_url", "price_eur", "delivery_eur", "box_price_eur", "scraped_at"]
_COOKIE_FILE = "item-analysis/allegro_cookies.json"

# JS to extract all offer articles from the product page.
# Skips articles that live inside "Najrýchlejšie" or "Najlacnejšie" sections.
_EXTRACT_JS = """() => {
    // Mark articles that belong to excluded sections (Najrýchlejšie / Najlacnejšie)
    const excluded = new Set();
    for (const h of document.querySelectorAll("h2, h3, h4")) {
        const t = h.innerText || "";
        if (!t.includes("Najr") && !t.includes("Najla")) continue;
        // Walk up to the nearest ancestor that contains articles
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

# JS to extract the main "box price" — the price shown in the offer conditions
# panel on the right side of the product page (what the customer sees first).
_BOX_PRICE_JS = """() => {
    // 1. schema.org meta tag — most reliable, set to the selected offer price
    const meta = document.querySelector('meta[itemprop="price"]');
    if (meta) { const v = meta.getAttribute("content"); if (v) return v; }

    // 2. Allegro buy-box: the price block that sits above the "Pridať do košíka" button.
    //    Walk all elements that contain "Pridať do košíka" or "Kúpiť teraz" and find
    //    the nearest ancestor that also contains a prominent EUR price.
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

    // 3. "Podmienky ponuky" section
    for (const sec of document.querySelectorAll("section, [role='region'], aside")) {
        const t = sec.innerText || "";
        if (t.includes("Podmienky ponuky")) {
            const m = t.match(/(\\d+[,.]\\d{2})\\s*\\u20ac/);
            if (m) return m[1].replace(",", ".");
        }
    }

    // 4. itemprop offer price
    const offerPrice = document.querySelector('[itemprop="offers"] [itemprop="price"]');
    if (offerPrice) return offerPrice.getAttribute("content") || offerPrice.innerText.trim();

    // 5. fallback: first EUR price in the top 3000 chars of page text
    const m2 = document.body.innerText.slice(0, 3000).match(/(\\d+[,.]\\d{2})\\s*\\u20ac/);
    return m2 ? m2[1].replace(",", ".") : null;
}"""


def _looks_like_all_offers_label(text: str) -> bool:
    normalized = re.sub(r"\s+", " ", text).strip().casefold()
    return "všetky" in normalized and "ponuky" in normalized


async def _click_all_offers_control(page) -> bool:
    controls = page.locator("button, a")
    count = await controls.count()
    for idx in range(count):
        control = controls.nth(idx)
        text = await control.inner_text()
        if not _looks_like_all_offers_label(text):
            continue
        await control.click()
        return True
    return False


async def scrape_ean(page, ean: str) -> list[dict]:
    try:
        await page.goto(
            f"{_BASE_URL}/vyhladavanie?string={ean}",
            wait_until="domcontentloaded",
            timeout=20_000,
        )
    except Exception as e:
        log.debug("search timeout for %s: %s", ean, e)
        return []

    current_url = page.url
    if "/produkt/" in current_url:
        product_url = current_url.split("?")[0] + "#inne-oferty-produktu"
    else:
        # Product links are rendered by JS — wait for them before querying
        try:
            await page.wait_for_selector('a[href*="/produkt/"]', timeout=10_000)
        except Exception:
            pass  # fall through to oferta/ fallback below

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
            return []

        product_url = product_links[0].split("?")[0] + "#inne-oferty-produktu"

    if "/oferta/" in product_url and "/produkt/" not in product_url:
        try:
            await page.goto(product_url.split("#")[0], wait_until="domcontentloaded", timeout=20_000)
            parent_links = await page.eval_on_selector_all(
                'a[href*="/produkt/"]', "els => els.map(e => e.href)"
            )
            if parent_links:
                product_url = parent_links[0].split("?")[0] + "#inne-oferty-produktu"
        except Exception as e:
            log.debug("oferta redirect for %s: %s", ean, e)

    try:
        await page.goto(product_url, wait_until="domcontentloaded", timeout=20_000)

        # Click only the "Všetky ponuky" control, not other "Všetky ..." sections.
        try:
            clicked = await _click_all_offers_control(page)
            if clicked:
                await asyncio.sleep(1.5)
        except Exception:
            pass

        # Reviews appear first — wait specifically for offer articles which contain "Predajca:"
        await page.wait_for_function(
            "() => [...document.querySelectorAll('article')].some(a => a.innerText.includes('Predajca:'))",
            timeout=20_000,
        )
    except Exception as e:
        log.debug("product page timeout for %s: %s", ean, e)
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
    offers = [
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

    return offers


def _worker_startup_stagger_seconds(worker_id: int) -> float:
    return float(max(worker_id, 0))


async def _worker(
    worker_id: int,
    page,
    queue: asyncio.Queue,
    writer: "csv.DictWriter",
    out_file,
    write_lock: asyncio.Lock,
    counters: dict,
    total: int,
    output_path: str,
    started_at: float,
) -> None:
    next_long_pause = random.randint(8, 15)
    idx = 0
    startup_stagger = _worker_startup_stagger_seconds(worker_id)
    if startup_stagger > 0:
        print(f"[W{worker_id}] startup stagger {startup_stagger:.1f}s", flush=True)
        await asyncio.sleep(startup_stagger)
    while True:
        try:
            ean = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        offers = await scrape_ean(page, ean)

        async with write_lock:
            done = counters["done"] + 1
            counters["done"] = done
            if offers:
                _persist_offers_batch(offers, writer, out_file, output_path)
                counters["total"] += len(offers)
                counters["scraped_eans"] += 1
                print(f"[W{worker_id}] [{done}/{total}] {ean}  → {len(offers)} offers", flush=True)
            else:
                counters["not_found"] += 1
                print(f"[W{worker_id}] [{done}/{total}] {ean}  → not found", flush=True)

        queue.task_done()
        idx += 1

        post = random.uniform(2.0, 5.0)
        print(f"[W{worker_id}] rest {post:.1f}s", flush=True)
        await asyncio.sleep(post)

        if idx > 0 and idx % next_long_pause == 0:
            pause = random.uniform(20.0, 30.0)
            elapsed_seconds = time.monotonic() - started_at
            print(
                f"[W{worker_id}] {_format_progress_snapshot(counters=counters, total=total, elapsed_seconds=elapsed_seconds)}",
                flush=True,
            )
            print(f"[W{worker_id}] long pause {pause:.1f}s", flush=True)
            await asyncio.sleep(pause)
            next_long_pause = random.randint(8, 15)


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
        "--enable-automation",           # required for CDP browser-management commands
        "--disable-blink-features=AutomationControlled",  # hides navigator.webdriver in JS
        "--no-first-run",
        "--no-default-browser-check",
        _BASE_URL,
    ])


def _parse_cdp_urls(cdp_arg: str) -> list[str]:
    """Accept 'port,port,...' or 'http://host:port,...' or a mix."""
    urls = []
    for part in cdp_arg.split(","):
        part = part.strip()
        if part.startswith("http"):
            urls.append(part)
        else:
            urls.append(f"http://localhost:{part}")
    return urls


def _persist_offers_batch(
    offers: list[dict],
    writer: "csv.DictWriter",
    out_file,
    output_path: str | None = None,
) -> None:
    for offer in offers:
        writer.writerow(offer)
    out_file.flush()


def _finalize_output(
    out_file,
    output_path: str,
    rebuild_excel=None,
) -> None:
    if rebuild_excel is None:
        rebuild_excel = _rebuild_wide_excel
    out_file.flush()
    rebuild_excel(output_path)


def _filter_eans_for_existing_output(
    eans: list[str],
    existing_rows: list[dict[str, str]],
    *,
    resume: bool,
    skip_found: bool,
) -> list[str]:
    remaining = eans

    if resume:
        done = {row["ean"] for row in existing_rows}
        remaining = [ean for ean in remaining if ean not in done]

    if skip_found:
        found = {row["ean"] for row in existing_rows if row.get("seller")}
        remaining = [ean for ean in remaining if ean not in found]

    return remaining


def _should_append_output(*, resume: bool, skip_found: bool, output_exists: bool) -> bool:
    return output_exists and (resume or skip_found)


def _format_mmss(seconds: float) -> str:
    total_seconds = max(0, int(round(seconds)))
    minutes, secs = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _format_progress_snapshot(*, counters: dict, total: int, elapsed_seconds: float) -> str:
    done = counters["done"]
    scraped_eans = counters["scraped_eans"]
    not_found = counters["not_found"]
    missing = max(total - done, 0)

    if done > 0 and elapsed_seconds > 0:
        rate = done / elapsed_seconds
        eta = _format_mmss(missing / rate) if rate > 0 else "--:--"
    else:
        eta = "--:--"

    return (
        f"progress: scraped={scraped_eans} "
        f"not_found={not_found} missing={missing} "
        f"elapsed={_format_mmss(elapsed_seconds)} eta={eta}"
    )


async def run(
    input_path: str,
    output_path: str,
    limit: int,
    concurrency: int,
    delay: float,
    cdp_url: str,
    resume: bool,
    skip_found: bool = False,
    eans_filter: list[str] | None = None,
    row_start: int | None = None,
    row_end: int | None = None,
) -> int:
    with open(input_path, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    eans = [r["ean"] for r in all_rows]

    if eans_filter:
        eans = [e for e in eans if e in set(eans_filter)]
        print(f"--eans filter: {len(eans)} EANs selected")

    # Row range (1-based, matching Excel row numbers; header = row 1, data starts at row 2)
    if row_start is not None or row_end is not None:
        lo = (row_start - 2) if row_start is not None else 0          # row 2 → index 0
        hi = (row_end  - 1) if row_end   is not None else len(eans)   # row N → index N-1 (exclusive)
        eans = eans[max(lo, 0):hi]
        print(f"Row range {row_start}–{row_end}: {len(eans)} EANs selected")

    if limit:
        eans = eans[:limit]

    # Resume: skip EANs already in output (any row = skip)
    output_exists = Path(output_path).exists()
    existing_rows: list[dict[str, str]] = []
    if output_exists and (resume or skip_found):
        with open(output_path, newline="", encoding="utf-8") as f:
            existing_rows = list(csv.DictReader(f))

    if resume:
        before = len(eans)
        eans = _filter_eans_for_existing_output(eans, existing_rows, resume=True, skip_found=False)
        print(f"--resume: skipping {before - len(eans)} already-scraped EANs, {len(eans)} remaining")

    if skip_found:
        before = len(eans)
        eans = _filter_eans_for_existing_output(eans, existing_rows, resume=False, skip_found=True)
        print(f"--skip-found: skipping {before - len(eans)} EANs with existing offers, {len(eans)} remaining")

    cdp_urls = _parse_cdp_urls(cdp_url)
    n_workers = len(cdp_urls)
    print(f"Scraping {len(eans)} EANs  workers={n_workers}  CDP={cdp_urls}")

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    append = _should_append_output(resume=resume, skip_found=skip_found, output_exists=out_path.exists())
    out_file = open(output_path, "a" if append else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_file, fieldnames=_FIELDNAMES)
    if not append:
        writer.writeheader()

    cookie_path = Path(_COOKIE_FILE)
    started_at = time.monotonic()

    async with async_playwright() as pw:
        # Connect to each Chrome and load cookies
        browsers, contexts, pages = [], [], []
        for url in cdp_urls:
            browser = await pw.chromium.connect_over_cdp(url)
            ctx = browser.contexts[0]
            if cookie_path.exists():
                saved = json.loads(cookie_path.read_text())
                await ctx.add_cookies(saved)
            page = ctx.pages[0] if ctx.pages else await ctx.new_page()
            browsers.append(browser)
            contexts.append(ctx)
            pages.append(page)

        if cookie_path.exists():
            print(f"Loaded cookies into {n_workers} browser(s) from {_COOKIE_FILE}")

        print(f"\nConnected to {n_workers} Chrome window(s).")
        print("Solve any Cloudflare challenge in ALL windows if needed, then press Enter...")
        await asyncio.get_event_loop().run_in_executor(None, input)

        # Save cookies from first context (all share same cf_clearance)
        cookies = await contexts[0].cookies()
        cookie_path.parent.mkdir(parents=True, exist_ok=True)
        cookie_path.write_text(json.dumps(cookies))
        print(f"Saved {len(cookies)} cookies to {_COOKIE_FILE}")

        # Fill the work queue
        queue: asyncio.Queue = asyncio.Queue()
        for ean in eans:
            queue.put_nowait(ean)

        write_lock = asyncio.Lock()
        counters = {"total": 0, "not_found": 0, "done": 0, "scraped_eans": 0}

        tasks = [
            asyncio.create_task(
                _worker(
                    i,
                    pages[i],
                    queue,
                    writer,
                    out_file,
                    write_lock,
                    counters,
                    len(eans),
                    output_path,
                    started_at,
                )
            )
            for i in range(n_workers)
        ]
        try:
            await asyncio.gather(*tasks)
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\nInterrupted — saving progress.")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        for browser in browsers:
            try:
                await browser.close()
            except Exception:
                pass

    _finalize_output(out_file, output_path)
    out_file.close()
    total_offers = counters["total"]
    not_found = counters["not_found"]
    print(f"\nDone. {total_offers} offers from {len(eans) - not_found}/{len(eans)} EANs.")
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

    parser = argparse.ArgumentParser(description="Scrape Allegro.sk offers by EAN via CDP")
    parser.add_argument("--input", default=_DEFAULT_INPUT)
    parser.add_argument("--output", default=_DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=0, help="Limit EANs (0 = all)")
    parser.add_argument("--rows", metavar="START-END",
                        help="Excel row range to process, e.g. 2484-4979 (header=row 1, data from row 2)")
    parser.add_argument("--concurrency", type=int, default=1, help="Unused (workers derived from --cdp)")
    parser.add_argument("--delay", type=float, default=3.0, help="Base delay between EANs (s), +0.5–2.5s random")
    parser.add_argument("--cdp", default="9222",
                        help="CDP port(s) or URL(s), comma-separated. e.g. '9222,9223' or 'http://localhost:9222'")
    parser.add_argument("--eans", help="Comma-separated EANs to scrape, e.g. '3253561947490,8590804097587'")
    parser.add_argument("--resume", action="store_true", help="Skip EANs already in output (any row)")
    parser.add_argument("--skip-found", action="store_true",
                        help="Skip EANs that already have ≥1 offer; retries 'not found' ones")
    parser.add_argument("--launch-chrome", action="store_true", help="Launch Chrome automatically on port 9222")
    args = parser.parse_args()

    row_start = row_end = None
    if args.rows:
        parts = args.rows.split("-")
        row_start, row_end = int(parts[0]), int(parts[1])

    chrome_proc = None
    if args.launch_chrome:
        chrome_proc = _launch_chrome(9222)
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
            skip_found=args.skip_found,
            eans_filter=[e.strip() for e in args.eans.split(",")] if args.eans else None,
            row_start=row_start,
            row_end=row_end,
        )))
    finally:
        if chrome_proc:
            chrome_proc.terminate()


if __name__ == "__main__":
    main()
