# NaradieShop (naradieshop.sk) Scraping Knowledge Base

> ThirtyBees / PrestaShop-derived store. No Heureka feed. Catalogue discovered
> via XML sitemap → categories → paginated listing pages → JSON-LD on detail.

Source: [src/agnaradie_pricing/scrapers/naradieshop.py](../../src/agnaradie_pricing/scrapers/naradieshop.py)
Config: `naradieshop_sk` — `rate_limit_rps: 1`, `workers: 3`.

---

## 1. Site Architecture

### Domain & URL Patterns

| Purpose | Pattern | Example |
|---|---|---|
| Sitemap | `/sitemap.xml` | https://naradieshop.sk/sitemap.xml |
| Category | `/{slug}` (path depth = 1) | `/skrutkovace-43` |
| Category page N | `/{slug}?p=N` | `…?p=2` |
| Product detail | `/{cat-slug}/{product-slug}` | `/skrutkovace-43/wera-867-1-z-…` |
| Search | `/vyhladavanie?search_query=…` | `…?search_query=knipex+8701250` |

### Discovery flow

```
GET /sitemap.xml                       → all <loc> URLs
  → keep only category URLs (path depth == 1, "host/slug")
    → paginate ?p=1, ?p=2 … until pagination_next is .disabled
      → each card → product URL → JSON-LD detail
```

The category filter is `u.rstrip("/").count("/") == 3` (counts `https:/` slashes
+ the trailing one). Detail URLs are deeper and excluded automatically.

---

## 2. Bot Protection

n/a — no Cloudflare, no rate-limit headers. Polite UA + 1 RPS per worker is
sufficient.

---

## 3. Page Rendering Behaviour

- Sitemap: pure XML.
- Category listing pages: server-rendered HTML (no JS hydration needed for
  the cards).
- **Detail pages: JSON-LD** — preferred over HTML scraping because the structure
  is stable.

---

## 4. Data Extraction

### Listing page (used only by `search_by_query`)

[`_NaradieShopParser`](../../src/agnaradie_pricing/scrapers/naradieshop.py#L313)
walks `<ul id="catprod-list">`. ThirtyBees omits closing `</li>` tags, so item
boundaries are detected by the **start** of the next `ajax_block_product`
element OR the `_tag_depth < _item_depth` invariant in `handle_endtag`.

| Field | Source |
|---|---|
| `title` | `<a class="product-name">` text |
| `href` | same `<a>` href, query-stripped via `_clean_url` |
| `price_raw` | `<span class="price">` (excl. `old-price`) |
| `in_stock` | `<div class="quantity-cat-spec">` text — "skladom"/"posledn" → True; "nie je"/"vypred" → False |

### Detail page (JSON-LD)

[`_parse_detail_page`](../../src/agnaradie_pricing/scrapers/naradieshop.py#L202)
finds the first `<script type="application/ld+json">` whose payload is
`@type: "Product"` (handles both single-object and list payloads).

| Field | JSON-LD path |
|---|---|
| `title` | `name` |
| `ean` | `gtin13` → `gtin8` → `gtin` (first non-empty) |
| `brand` | `brand.name` (or `brand` if string) |
| `price_eur` | `offers.price` |
| `in_stock` | `offers.availability` contains `"InStock"` |
| `competitor_sku` | `sku` |

`mpn` is intentionally `None` — NaradieShop does not publish MPN; matching falls
back to EAN/title.

### Listing card filtering

`quantity-cat-cat-ext-out` cards = "Na externom sklade" products — their detail
URLs return 404. `_extract_listing_urls` drops these before requesting.

### Pagination end detection

`_NEXT_DISABLED_RE` matches `pagination_next … class="…disabled"` — when present,
or when a page yields zero new product URLs, the loop breaks.

---

## 5. Anti-Detection Timing

`polite_get(min_rps=1.0)` per thread. No long breaks needed for a ~20 k product
catalogue.

---

## 6. Parallelism

- Sitemap fetch: serial on the main client.
- Per category page: `parallel_map(workers=3)` over the page's product URLs.
- Each worker uses `get_thread_client()` so the 1 RPS cap is per worker.

`seen_product_urls: set[str]` deduplicates across categories — products that
live in multiple categories are scraped exactly once.

---

## 7. Output Schema

`competitor_id="naradieshop_sk"`. `mpn=None` (always). EAN comes from JSON-LD;
brand comes from `brand.name`.

---

## 8. Known Pitfalls & Fixes

| Problem | Root Cause | Fix |
|---|---|---|
| Pagination loops past the last page | `?p=N` returns the same content past the end | break on `pagination_next … disabled` AND on empty product list |
| Same product scraped multiple times | products linked from several categories | `seen_product_urls: set` dedupe across categories |
| 404 on some cards | "Na externom sklade" listings have dead detail URLs | drop cards containing `quantity-cat-ext-out` |
| Listing parser drops items | ThirtyBees omits `</li>` | `_NaradieShopParser` uses tag-depth tracking + start-of-next-card finalisation |
| `search_by_query` returns title only (no EAN) | search-result cards don't carry JSON-LD | follow up with [`enrich_from_detail_page`](../../src/agnaradie_pricing/scrapers/detail.py) on the cleaned URL |
| Price parsing fails on `1\xa0234,56 €` | non-breaking space + comma | `_parse_price` strips `\xa0`, `€`, `EUR`, then comma → dot |

---

## 9. Transferable Patterns

- **JSON-LD first** — when a site exposes `@type: "Product"`, use it. Far less
  brittle than CSS/regex over rendered markup.
- **Sitemap-driven category enumeration** — extract a unique-depth filter from
  the sitemap URL pattern when the site offers no API.
- **PrestaShop / ThirtyBees quirks** — search-result cards omit `</li>` closing
  tags; rely on tag-depth or start-of-next-item to finalise records.
- **`enrich_from_detail_page`** — generic helper that takes a thin search-result
  listing and re-fetches the product page for full data. Handy for any
  site whose listing card is intentionally minimal.
