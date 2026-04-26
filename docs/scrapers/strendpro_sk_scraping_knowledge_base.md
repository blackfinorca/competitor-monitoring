# Strendpro Scraping Knowledge Base

> Reference document for scraping `strendpro_sk` (`strendpro.sk`).

---

## 1. Site Architecture

Strendpro is crawled as a full catalogue through category pages and static product detail pages.

| Purpose | Pattern | Notes |
|---|---|---|
| Base URL | `https://www.strendpro.sk` | Configured competitor URL |
| Category links | Paths containing `/c/{id}/` | Extracted from homepage |
| Product links | Paths containing `/p/{id}/` | Extracted from category pages |
| Pagination | `<link rel="next" href="...">` | Followed until absent |

### Current Crawl Flow

```text
GET homepage
    -> collect /c/{id}/ category URLs
For each category:
    GET category and rel=next pages
    -> collect /p/{id}/ product URLs
    -> scrape each product detail page
```

---

## 2. Data Extraction

Product details combine JSON-LD and parameter rows.

| Field | Source | Notes |
|---|---|---|
| `title` | Product JSON-LD `name` | Required |
| `brand` | Product JSON-LD `brand.name` / `brand` | Optional |
| `price_eur` | JSON-LD `offers.price` | Required |
| `in_stock` | JSON-LD `offers.availability` | `InStock` / `OutOfStock` |
| `competitor_sku` | Parameter label `Kat. číslo` / `Kat cislo`; fallback JSON-LD `model` | Optional |
| `ean` | Parameter label `EAN kód` / `EAN`; fallback JSON-LD `gtin13` | Optional |

Parameter labels are normalised by removing accents and collapsing whitespace, so `EAN kód` becomes `ean kod`.

---

## 3. Search Fallback

`search_by_mpn()` and `search_by_query()` currently return `None`. Strendpro is handled as a full-catalogue crawler.

---

## 4. Operational Notes

| Setting | Value |
|---|---|
| `competitor_id` | `strendpro_sk` |
| Default RPS | `1` |
| Config workers | `1` |
| Daily mode | Homepage category crawl + product details |
| Limit env | `STRENDPRO_MAX_PRODUCTS` |

Run only this competitor:

```bash
./.venv/bin/python jobs/daily_scrape.py --only strendpro_sk
```

---

## 5. Known Pitfalls

| Problem | Root Cause | Fix |
|---|---|---|
| Duplicate products across categories | Same product can appear in multiple categories | Track `seen_products` by normalised product URL |
| Missing EAN in JSON-LD | Barcode may appear only in parameter rows | Parse parameter table as primary EAN source |
| Pagination not obvious in body | Site exposes next page via link tag | Use `<link rel="next">` |

