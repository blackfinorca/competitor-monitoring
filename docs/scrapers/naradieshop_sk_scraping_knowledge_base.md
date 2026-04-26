# NaradieShop Scraping Knowledge Base

> Reference document for scraping `naradieshop_sk` (`naradieshop.sk`).

---

## 1. Site Architecture

NaradieShop runs on ThirtyBees / PrestaShop. Product detail pages expose useful JSON-LD; category pages are server-rendered enough to extract product URLs.

| Purpose | Pattern | Notes |
|---|---|---|
| Base URL | `https://naradieshop.sk` | Code uses non-`www` base for crawling |
| Sitemap | `/sitemap.xml` | Used to discover one-segment category URLs |
| Category page | `/{category}` | First page |
| Pagination | `/{category}?p={page}` | Continues until disabled next link |
| Search fallback | `/vyhladavanie?search_query={query}` | Search result is enriched from detail page |

### Current Crawl Flow

```text
GET /sitemap.xml
    -> select URLs with one path segment as categories
For each category:
    GET category, category?p=2, ...
    -> extract product URLs from product cards
    -> skip external-stock cards
    -> fetch detail pages in parallel
    -> parse Product JSON-LD
```

---

## 2. Data Extraction

### Category Listing

Product cards are matched by `ajax_block_product`. The product URL comes from `a.product-name`.

Cards containing `quantity-cat-ext-out` are skipped because their detail pages return 404.

### Product Detail JSON-LD

| Field | Source | Notes |
|---|---|---|
| `title` | Product `name` | Required |
| `ean` | `gtin13`, `gtin8`, or `gtin` | Priority order |
| `brand` | `brand.name` or `brand` string | Detail page only |
| `competitor_sku` | Product `sku` | Optional |
| `price_eur` | `offers.price` | Required |
| `in_stock` | `offers.availability` | `InStock` substring -> true |

---

## 3. Search Fallback

`search_by_query()` parses the first result under `<ul id="catprod-list">`.

Search cards provide:

| Field | Source |
|---|---|
| `title` | `a.product-name` |
| `url` | cleaned product URL |
| `price_eur` | `span.price`, European decimal format |
| `in_stock` | `quantity-cat-spec` text |

The initial search listing usually lacks EAN/brand/MPN, so it is passed through `enrich_from_detail_page()`.

---

## 4. Operational Notes

| Setting | Value |
|---|---|
| `competitor_id` | `naradieshop_sk` |
| Default RPS | `1` per worker |
| Config workers | `3` |
| Feed support | None |
| Daily mode | Sitemap category crawl + detail JSON-LD |

Run only this competitor:

```bash
./.venv/bin/python jobs/daily_scrape.py --only naradieshop_sk
```

---

## 5. Known Pitfalls

| Problem | Root Cause | Fix |
|---|---|---|
| Detail 404s for visible listing cards | External-stock products | Skip cards with `quantity-cat-ext-out` |
| Search URL contains tracking params | Search result href includes query data | Clean URL to scheme/netloc/path only |
| Missing identifiers on search cards | Search cards are summary HTML | Enrich from product detail JSON-LD |

