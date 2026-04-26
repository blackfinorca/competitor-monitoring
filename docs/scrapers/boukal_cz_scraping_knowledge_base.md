# Boukal Scraping Knowledge Base

> Reference document for scraping `boukal_cz` (`boukal.cz`).

---

## 1. Site Architecture

Boukal runs on a custom PHP / m1web (K2/Joomla) e-commerce platform. Product pages and brand pages are static enough for HTTP-only scraping.

| Purpose | Pattern | Notes |
|---|---|---|
| Base URL | `https://www.boukal.cz` | Configured competitor URL |
| Feed probes | Standard Heureka/Zboží paths | Used first if available |
| Brand page | `/{brand_slug}?p={page}` | Brand slug derived from AG catalogue brand |
| Product detail | `/{slug}-produkt` | Extracted from brand pages |

### Current Crawl Flow

```text
Probe Heureka/Zboží feeds
if no feed:
    group AG catalogue by brand slug
    paginate each /{brand_slug}?p=N
    collect product paths
    scrape product pages in parallel
```

---

## 2. Data Extraction

Product pages use a spec table plus microdata and GTM JSON.

| Field | Source | Notes |
|---|---|---|
| `competitor_sku` | Spec label `E-shop` | Fallback URL numeric ID |
| `mpn` | Spec label `Katalog` | Manufacturer/catalogue code |
| `ean` | Spec label `EAN` | Real barcode |
| `price_eur` | `itemprop="price" content="..."` | CZK converted to EUR at fixed `25.0` |
| `in_stock` | `itemprop="availability"` href | `InStock` substring -> true |
| `brand` | GTM `m4detail.items[0].item_brand` | Optional |
| `title` | GTM `m4detail.items[0].item_name` | Fallback MPN/URL |

---

## 3. Search Fallback

`search_by_mpn(brand, mpn)`:

1. Convert brand to slug.
2. Paginate brand pages.
3. Open each product page.
4. Compare normalised MPN by stripping spaces/hyphens.
5. Fallback to `search_by_query()`.

`search_by_query()` opens first-page brand products and scores by token overlap in title/MPN.

---

## 4. Operational Notes

| Setting | Value |
|---|---|
| `competitor_id` | `boukal_cz` |
| Default RPS | `3` per worker |
| Config workers | `4` |
| Currency | CZK source, stored as EUR using fixed conversion |
| Daily mode | Feed if found; otherwise brand-page crawl |

Run only this competitor:

```bash
./.venv/bin/python jobs/daily_scrape.py --only boukal_cz
```

---

## 5. Known Pitfalls

| Problem | Root Cause | Fix |
|---|---|---|
| CZK prices in source | Site is Czech | Convert to EUR with configured fixed rate |
| Brand page pagination stops silently | Last page hides next button | Detect active `k2pagNextAjax` without `k2hidden` |
| Product data split across structures | Specs, microdata, and GTM all needed | Combine all three sources |

