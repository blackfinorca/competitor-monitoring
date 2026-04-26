# Doktor Kladivo (doktorkladivo.sk) Scraping Knowledge Base

> Custom Shoptet-derived Slovak hardware platform. Catalogue is crawled from the
> top-level "Náradie" category by paginated offset; detail pages carry inline
> JS dataLayer + a custom `<bs-grid-item class="ean value">` web component for
> the EAN.

Source: [src/agnaradie_pricing/scrapers/doktorkladivo.py](../../src/agnaradie_pricing/scrapers/doktorkladivo.py)
Config: `doktorkladivo_sk` — `rate_limit_rps: 1`, `workers: 4`.

---

## 1. Site Architecture

### Domain & URL Patterns

| Purpose | Pattern | Example |
|---|---|---|
| Catalogue root | `/naradie-c1006/` | https://www.doktorkladivo.sk/naradie-c1006/ |
| Catalogue offset | `/naradie-c1006/?f={offset}` | `…/?f=24` (page 2) |
| Product detail | `/{slug}-p{N}/?cid=1006` | `/knipex-87-01-250-p1234/?cid=1006` |
| Search | `/hladat/?q={q}` | `…?q=knipex+87+01+250` |

### Discovery flow

```
GET /naradie-c1006/?f=0          → 24 products
  → GET /naradie-c1006/?f=24    → next 24
    → … until a page returns no product URLs (loop break)
```

~10 000 products in the catalogue (2026-04). Pagination step is **24 items per
page**, controlled via `page_size` config.

---

## 2. Bot Protection

n/a — no Cloudflare or rate-limit headers. Polite UA + 1 RPS works.

---

## 3. Page Rendering Behaviour

- Category pages are server-rendered HTML with anchor tags to every product card.
- Product detail pages contain an inline JavaScript `dataLayer` push with
  product fields — the canonical source for brand/MPN/price.
- The EAN is inside a custom web component `<bs-grid-item class="ean value">`,
  unrendered without a browser, but its `<span>` body is plain text and regex
  works.

---

## 4. Data Extraction

### Category page

[`_extract_product_paths`](../../src/agnaradie_pricing/scrapers/doktorkladivo.py#L184)
matches `href="/{slug}-p{N}/[…]"`. Dedupe key strips the query string so the
same product behind different `?cid=` values appears once; the original href
(with `?cid=`) is retained for fetching.

### Detail page ([`_parse_product_page`](../../src/agnaradie_pricing/scrapers/doktorkladivo.py#L201))

| Field | Source |
|---|---|
| `title` | `<h1>` text |
| `mpn` | `"product_code":"…"` (inline JS) |
| `brand` | `"product_brand":"…"` (inline JS) |
| `ean` | `<bs-grid-item class="ean value"><span>NNNNNNNNNNNNN</span>` |
| `price_eur` | `"price":N.NN,"priceCurrency":"EUR"` |
| `in_stock` | `"availability":"https://schema.org/InStock"` (true) vs `OutOfStock` (false) |
| `competitor_sku` | `"ecomm_prodid":"…"`, fallback to `-p(\d+)/` from URL |

### Search fallback

[`search_by_mpn`](../../src/agnaradie_pricing/scrapers/doktorkladivo.py#L153)
spaces out separators (`-` → space) and delegates to
[`_SearchDelegate`](../../src/agnaradie_pricing/scrapers/doktorkladivo.py#L170)
— a thin subclass of [`ShoptetGenericScraper`](../../src/agnaradie_pricing/scrapers/shoptet_generic.py)
that handles the `/hladat/?q=…` flow.

---

## 5. Anti-Detection Timing

`polite_get(min_rps=1.0)` per thread; no breaks needed.

---

## 6. Parallelism

`run_daily_iter` interleaves pagination with parallel scraping:

```
fetch listing page (24 URLs) [serial on main client]
  → parallel_map(workers=4) over those 24 URLs [thread-local clients]
    → yield all 24 → DB save batch
      → next listing offset
```

This means the DB starts saving after ~30 s rather than after the full ~10 k
crawl finishes — a key win when the run is interrupted.

---

## 7. Output Schema

`competitor_id="doktorkladivo_sk"`. Brand and MPN are usually populated; EAN
present when the source page has it. Persisted via the standard URL upsert.

---

## 8. Known Pitfalls & Fixes

| Problem | Root Cause | Fix |
|---|---|---|
| EAN missing from regex search | DOM uses a custom web component, not standard markup | regex against the literal `class="ean value">…<span>` chain |
| MPN encoded with hyphens vs spaces between site and our DB | Doktor Kladivo's search tokeniser splits on non-alnum | normalise hyphens to spaces in `search_by_mpn` |
| Pagination overrun | site returns the last page repeatedly past the end | break when `_extract_product_paths` returns empty |
| Duplicate scraping for products in multiple categories | same product reachable via different `?cid=` | dedupe by `path.split("?")[0]` while keeping the qs for fetching |

---

## 9. Transferable Patterns

- **`run_daily_iter` interleaving** — yield every page-batch immediately so the
  DB starts filling before the full crawl is done. Used here, in
  [naradieshop](naradieshop_sk.md), and in
  [ahprofi](ahprofi_sk.md).
- **dataLayer mining** — most Shoptet/PrestaShop/WooCommerce stores push a
  `dataLayer` blob with `product_code`, `product_brand`, `price`. Always check
  the inline JS before scraping the rendered DOM.
- **Schema.org availability strings** — `availability:"https://schema.org/InStock|OutOfStock|PreOrder"`
  — common across many sites. Match the suffix word, not the URL.
- **Thin delegate subclasses** — when one scraper needs another's search logic
  but not its full crawl, subclass `ShoptetGenericScraper` for the search alone.
