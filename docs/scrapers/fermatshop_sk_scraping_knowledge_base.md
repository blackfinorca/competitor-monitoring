# Fermatshop Scraping Knowledge Base

> Reference document for scraping `fermatshop_sk` (`fermatshop.sk`).

---

## 1. Site Architecture

Fermatshop is a static HTML catalogue scrape backed by `https://www.fermatshop.sk`. It does not expose reliable EANs on product pages.

| Purpose | Pattern | Notes |
|---|---|---|
| Base URL | `https://www.fermatshop.sk` | Configured competitor URL |
| Sitemap | `/sitemap.xml` | Source of candidate product URLs |
| Product detail | `/{category}/{product}/` | Two path segments |

### Current Crawl Flow

```text
GET /sitemap.xml
    -> parse <loc> URLs
    -> keep only /category/product/ paths
    -> filter account/cart/search pages
GET each product page
    -> parse static HTML fields
```

---

## 2. Data Extraction

| Field | Source | Notes |
|---|---|---|
| `title` | `<h1 class="flypage-h1">` | Required |
| `brand` | `<span class="manu_name">` | Manufacturer name |
| `competitor_sku` | `.flypage_sku .product_sku_value` | Product code |
| `ean` | Synthetic `NOEAN-{product_code}` | Site does not publish real EANs |
| `price_eur` | `#product-detail-price-value` | European price text |
| `in_stock` | `.shop_product_availability_value` | Text contains `Na sklade` / `Skladom` |

---

## 3. Search Fallback

`search_by_mpn()` and `search_by_query()` currently return `None`. Fermatshop is treated as a full-catalogue rerun/backfill scraper, not a live lookup scraper.

---

## 4. Operational Notes

| Setting | Value |
|---|---|
| `competitor_id` | `fermatshop_sk` |
| Default RPS | `1` |
| Config workers | `1` |
| Feed support | Probes feeds, but full-catalog mode uses sitemap |
| Daily mode | Sitemap full catalogue crawl |
| Limit env | `FERMATSHOP_MAX_PRODUCTS` or legacy `FERANT_MAX_PRODUCTS` |

Run only this competitor:

```bash
./.venv/bin/python jobs/daily_scrape.py --only fermatshop_sk
```

---

## 5. Known Pitfalls

| Problem | Root Cause | Fix |
|---|---|---|
| No real EAN | Product pages do not publish barcode data | Emit `NOEAN-{product_code}` and do not use EAN-based matching/backfill |
| Non-product sitemap URLs | Sitemap includes account/cart/search pages | Keep only two-segment product paths and explicit exclusions |
| Historical missing brands | Older rows may predate brand extraction | Rerun daily scrape; URL upsert backfills brand |

