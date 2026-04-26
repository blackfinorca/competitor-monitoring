# AGI Scraping Knowledge Base

> Reference document for scraping `agi_sk` (`agi.sk`).

---

## 1. Site Architecture

AGI is a Slovak tool and hardware distributor running on the rshop platform. Product/category links are absolute URLs in HTML, and product detail pages expose JSON-LD.

| Purpose | Pattern | Notes |
|---|---|---|
| Base URL | `https://www.agi.sk` | Configured competitor URL |
| Manufacturers page | `/vyrobcovia` | Maps brand names to category URLs |
| Manufacturer category | `/{slug}-c{id}` | Resolved from manufacturer page |
| Category pagination | `/{slug}-c{id}?page={n}` | 12 products per page |
| Product detail | `/{slug}-p{id}` | JSON-LD source |
| Search | `/vyhladavanie?search={query}` | First product result is scraped |

### Current Crawl Flow

```text
Probe feed paths
if no feed:
    read unique brands from AG catalogue
    resolve each brand on /vyrobcovia
    paginate manufacturer category
    scrape product pages in parallel
```

---

## 2. Data Extraction

Product details are parsed from JSON-LD `Product`.

| Field | Source | Notes |
|---|---|---|
| `title` | `name` | Required |
| `competitor_sku` | `sku` | Internal AGI integer ID |
| `mpn` | JSON-LD `mpn` | EDE/manufacturer code; EAN is more reliable for matching |
| `ean` | `gtin13`, `gtin8`, or `gtin` | EAN-shaped only |
| `brand` | `brand.name` / `brand` | If missing or `EDE`, fallback to manufacturer slug display name |
| `price_eur` | `offers.price` | EUR |
| `in_stock` | `offers.availability` | `InStock` / `OutOfStock` |
| `url` | `offers.url`, fallback fetched URL | Stored row URL |

---

## 3. Search Fallback

`search_by_query()` calls:

```text
GET /vyhladavanie?search={query}
```

It extracts product URLs from the HTML and scrapes the first product detail page.

---

## 4. Operational Notes

| Setting | Value |
|---|---|
| `competitor_id` | `agi_sk` |
| Default RPS | `2` per worker |
| Config workers | `4` |
| Page size | `12` |
| Daily mode | Feed if found; otherwise manufacturer category crawl |

Run only this competitor:

```bash
./.venv/bin/python jobs/daily_scrape.py --only agi_sk
```

---

## 5. Known Pitfalls

| Problem | Root Cause | Fix |
|---|---|---|
| Brand `EDE` in JSON-LD | Distributor appears as brand on some pages | Fallback to manufacturer category brand |
| Brand slug not found | `/vyrobcovia` matching is exact/prefix/substring only | Check manufacturer page naming |
| MPN not best match key | `mpn` can be EDE article number | Prefer EAN when available |

