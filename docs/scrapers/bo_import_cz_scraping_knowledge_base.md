# BO-Import Scraping Knowledge Base

> Reference document for scraping `bo_import_cz` (`bo-import.cz`).

---

## 1. Site Architecture

BO-Import is an authorised Czech KNIPEX distributor running on BSSHOP. The scraper first probes feeds, then falls back to brand-page crawling.

| Purpose | Pattern | Notes |
|---|---|---|
| Base URL | `https://www.bo-import.cz` | Configured competitor URL |
| Feed probes | Standard Heureka/Zboží paths | Used first if available |
| Brand page | `/{brand_slug}/` | Brand slug derived from AG catalogue brand |
| Pagination | `/{brand_slug}/?f={offset}` | Offset step 30 |
| Search | `/search/?search={query}` | Returns product paths |
| Product detail | `/{slug}-p{id}/` | JSON-LD source |

### Current Crawl Flow

```text
Probe feed paths
if no feed:
    derive unique brand slugs from AG catalogue
    GET /{brand_slug}/?f=N
    collect product paths
    stop if page cycles over already-seen URLs
    scrape product pages in parallel
```

---

## 2. Data Extraction

Product pages are parsed from JSON-LD `Product`.

| Field | Source | Notes |
|---|---|---|
| `title` | `name` | Required |
| `competitor_sku` | `sku` | Example `KNI-8701300` |
| `mpn` | `sku` with manufacturer prefix stripped | `KNI-8701300` -> `8701300` |
| `ean` | `gtin13`, fallback `gtin` | EAN-shaped only |
| `brand` | `brand.name` / `brand` string | Optional |
| `price_eur` | `offers.price` | CZK converted to EUR unless already EUR |
| `in_stock` | `offers.availability` | `InStock` / `OutOfStock` |
| `url` | `offers.url`, fallback fetched URL | Stored row URL |

---

## 3. Search Fallback

`search_by_mpn()` scans the brand page first and matches normalised MPN. If that fails, `search_by_query()` calls `/search/?search={query}` and returns the first detail-page parse.

---

## 4. Operational Notes

| Setting | Value |
|---|---|
| `competitor_id` | `bo_import_cz` |
| Default RPS | `2` per worker |
| Config workers | `4` |
| Page size | `30` |
| Currency | CZK source, stored as EUR using fixed `25.0` |

Run only this competitor:

```bash
./.venv/bin/python jobs/daily_scrape.py --only bo_import_cz
```

---

## 5. Known Pitfalls

| Problem | Root Cause | Fix |
|---|---|---|
| Out-of-range pagination repeats last page | BSSHOP may not return 404 | Stop when all product URLs were already seen |
| JSON-LD `mpn` empty | Site stores usable code in `sku` | Strip prefix from `sku` |
| Czech prices | Source currency is usually CZK | Convert before storing |

