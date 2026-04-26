# Doktor Kladivo Scraping Knowledge Base

> Reference document for scraping `doktorkladivo_sk` (`doktorkladivo.sk`).

---

## 1. Site Architecture

Doktor Kladivo runs on a custom Shoptet-derived platform. Product listing pages and detail pages are static enough for `httpx`; no browser automation is required.

| Purpose | Pattern | Notes |
|---|---|---|
| Base URL | `https://www.doktorkladivo.sk` | Configured competitor URL |
| Full catalogue category | `/naradie-c1006/` | Top-level tools category |
| Pagination | `/naradie-c1006/?f={offset}` | `f=0,24,48,...`; 24 products per page |
| Product detail | `/{slug}-p{id}/` | Listing links may include `?cid=1006` |
| Search fallback | `/hladat/?q={query}` | Delegated to generic Shoptet parsing/enrichment |

### Current Crawl Flow

```text
daily scrape -> /naradie-c1006/?f=N
    -> extract /slug-pNNNN/ product paths
    -> fetch each product detail page in parallel
    -> parse inline JS/web component fields
```

---

## 2. Data Extraction

### Category Page

Product links are extracted from hrefs matching:

```regex
href="(/[^"]+\-p\d+/[^"]*)"
```

The scraper strips query strings for deduplication but keeps the original link for fetching.

### Product Detail Page

| Field | Source | Notes |
|---|---|---|
| `title` | `<h1>` text | Required |
| `mpn` | inline JS `"product_code":"..."` | Manufacturer product code |
| `brand` | inline JS `"product_brand":"..."` | Usually reliable |
| `ean` | `<bs-grid-item class="ean value"><span>...</span>` | 8-13 digit regex |
| `price_eur` | inline JS `"price":N,"priceCurrency":"EUR"` | Required |
| `in_stock` | schema availability URL | `InStock` -> true |
| `competitor_sku` | `"ecomm_prodid"` or URL `-p{id}/` | Internal product ID |

---

## 3. Search Fallback

`search_by_mpn(brand, mpn)` normalises separators in the MPN to spaces and calls:

```text
GET /hladat/?q={brand} {mpn_spaced}
```

Search parsing is delegated through `ShoptetGenericScraper`, then enriched from the detail page JSON-LD to fill missing EAN, MPN, and brand.

---

## 4. Operational Notes

| Setting | Value |
|---|---|
| `competitor_id` | `doktorkladivo_sk` |
| Default RPS | `1` per worker |
| Config workers | `4` |
| Feed support | None in dedicated scraper |
| Daily mode | Full catalogue crawl |

Run only this competitor:

```bash
./.venv/bin/python jobs/daily_scrape.py --only doktorkladivo_sk
```

---

## 5. Known Pitfalls

| Problem | Root Cause | Fix |
|---|---|---|
| Duplicate product URLs | Category links can include query strings | Deduplicate by path without query |
| Missing search identifiers | Search result cards may lack EAN/brand | Enrich from detail page JSON-LD |
| Missing price | Product page lacks expected inline EUR price | Skip listing; price is required |

