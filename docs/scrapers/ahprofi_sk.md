# AH Profi (ahprofi.sk) Scraping Knowledge Base

> Custom Slovak hardware platform with no Heureka feed. Catalogue is discovered
> via XML sitemap; product detail pages are static HTML with microdata + OG tags.

Source: [src/agnaradie_pricing/scrapers/ahprofi.py](../../src/agnaradie_pricing/scrapers/ahprofi.py)
Config: `ahprofi_sk` in [config/competitors.yaml](../../config/competitors.yaml) — `rate_limit_rps: 1`, `workers: 3`.

---

## 1. Site Architecture

### Domain & URL Patterns

| Purpose | Pattern | Example |
|---|---|---|
| Sitemap index | `/sitemap` | https://www.ahprofi.sk/sitemap |
| Sitemap product page | `/sitemap?products=true&page={N}` | `…&page=1` |
| Product detail | `/{slug}` (single segment) | `/knipex-cobra-87-01-250` |
| Search | `/vysledky-vyhladavania?search_keyword={q}` | `…?search_keyword=8701250` |
| Brand index | `/{brand-slug}` | `/knipex` |

### Discovery flow

```
GET /sitemap                       → finds N (~11) sub-sitemap pages
  → GET /sitemap?products=true&page=N  → ~1 000 product URLs each
    → GET each product URL         → parse static microdata
```

~11 000 products total as of 2026-04.

### Search redirect rule

A successful exact-MPN search **redirects** straight to the product page; staying
on `/vysledky-vyhladavania` after the request means no exact match. The scraper
treats the marker `"vysledky-vyhladavania"` in `final_url` as "miss".

---

## 2. Bot Protection

n/a — no Cloudflare, no JS challenge, no CAPTCHA, no rate-limit header. Standard
`httpx` clients with a polite UA work indefinitely. `polite_get()` sets
`User-Agent` and `Referer`.

---

## 3. Page Rendering Behaviour

- Sitemap pages: pure XML, regex-parseable.
- Search and product pages: server-rendered HTML; no JS hydration is required
  for the fields we want.

---

## 4. Data Extraction

Detail page is parsed by regex against static HTML in
[`_parse_product_page`](../../src/agnaradie_pricing/scrapers/ahprofi.py#L172):

| Field | Source |
|---|---|
| `title` | `<meta property="og:title" content="…">` — strip the `" \| ahprofi.sk"` suffix |
| `mpn` / `competitor_sku` | `itemprop="productID">…<` |
| `ean` | `itemprop="gtin13">…<` |
| `price_eur` | `itemprop="price" content="N.NN"` |
| `in_stock` | `itemprop="availability" href="…/InStock"` (true) vs `…/OutOfStock` (false) |
| `brand` | `<div id="product-codes">…<span>Výrobca&nbsp;</span>…<a>BRAND</a>` |

The brand regex is **anchored** to the `Výrobca` label inside `#product-codes` —
do not loosen it; AH Profi reuses the word "Brand" in nav. Brand extraction was
added in commit `a0f0e05` after backfill testing.

### Search MPN normalisation

AH Profi indexes MPNs in **condensed** form ("8701250") rather than canonical
("87-01-250"). `search_by_mpn` strips `-`, `_`, `.`, whitespace before querying.
EAN search also goes through `_search_and_parse` and works as-is.

---

## 5. Anti-Detection Timing

Per-thread `polite_get(min_rps=1.0)` from
[scrapers/http.py](../../src/agnaradie_pricing/scrapers/http.py) — sleeps if the
last request was sent <1 s ago on the same client. No long breaks needed.

---

## 6. Parallelism

`run_daily_iter` uses `parallel_map(workers=3)` over each sitemap page's URL
batch. Each worker holds a thread-local `httpx.Client` from
`get_thread_client()`, so the 1 RPS cap is per worker → ~3 req/s aggregate.
After yielding a batch, the next sitemap page is fetched serially before the
next parallel batch.

Workers run concurrently with a small startup stagger configured at the job
level (see [jobs/daily_scrape.py](../../jobs/daily_scrape.py)).

---

## 7. Output Schema

Emits `CompetitorListing` rows with `competitor_id="ahprofi_sk"`. The
persistence layer dedupes on `(competitor_id, url)`; reruns refresh price / title
/ stock and backfill `brand` / `ean` / `mpn` if previously null
([`save_competitor_listings`](../../src/agnaradie_pricing/scrapers/persistence.py)).

---

## 8. Known Pitfalls & Fixes

| Problem | Root Cause | Fix |
|---|---|---|
| `brand=None` on every row | parser only emitted title/price/EAN, ignored the `Výrobca` block | Added `_BRAND_RE` anchored to `#product-codes > span "Výrobca"` (commit `a0f0e05`) |
| `search_by_mpn` returned no hit for MPNs containing dashes | site indexes condensed MPN | `re.sub(r"[\-._\s]+", "", mpn)` before query |
| Empty results from `/sitemap` if encoded `&amp;` was not handled | XML escape leaked into the URL pattern | `_SITEMAP_PAGE_RE` accepts both `&` and `&amp;` |
| Price came back `None` because content attr had thousands separator | site uses `.` as decimal everywhere — false alarm | leave the parser as `[\d.]+` |

---

## 9. Transferable Patterns

- **Sitemap-driven catalogue discovery** — when a site has no feed, look for
  `/sitemap` or `/sitemap.xml`. AH Profi splits products across `?page=N` for
  scaling.
- **Static microdata is gold** — when present, prefer `itemprop=…` over scraping
  the rendered list page.
- **Condensed-vs-hyphenated MPN** — Slovak hardware sites often store MPNs
  without separators. Normalise on the wire, keep canonical form in the DB.
- **OG title vs `<title>`** — `og:title` is cleaner (no site-name suffix), but
  always strip the trailing `" | brand.sk"` defensively.
