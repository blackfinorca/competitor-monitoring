# Rebiop Scraping Knowledge Base

> Reference document for scraping `rebiop_sk` (`rebiop.sk`).

---

## 1. Site Architecture

Rebiop runs on a custom DataSun e-commerce platform. Product data is static HTML. No reliable Heureka feed is known, but the scraper probes standard feed paths before crawling.

| Purpose | Pattern | Notes |
|---|---|---|
| Base URL | `https://www.rebiop.sk` | Configured competitor URL |
| Category URL | `/catalog/{id}/{slug}` | Homepage exposes top-level categories |
| Category pagination | `/catalog/{id}/{slug}/p/{n}` | 24 products per page |
| Product detail | `detail/{id}/{slug}` | Search/listing cards may append `/cat/{cat_id}` |
| Search fallback | `/search/products?q={query}` | May return detail HTML or search cards |

### Current Crawl Flow

```text
Probe feed paths
if no feed:
    GET homepage
    -> BFS category URLs
    -> extract product cards from category pages
    -> strip /cat/{id}
    -> fetch detail pages in parallel
```

---

## 2. Data Extraction

### Category Cards

Cards are scoped to:

```html
<div class="ctg-product-box" data-id="{sku}">
  <a href="detail/{id}/{slug}/cat/{cat_id}">
```

The card parser only provides summary fields. It intentionally leaves `brand=None`.

### Product Detail Fields

Detail pages are parsed from `<dt>/<dd>` pairs and `<h1>`.

| Field | Source | Notes |
|---|---|---|
| `title` | `<h1>` | Required |
| `ean` | `EAN kód:` or `EAN kód` | Real EAN when present |
| `competitor_sku` | `Kód:` or `Kód` | Internal DataSun code |
| `brand` | Explicit `Značka`, `Výrobca`, or `Vyrobca` field | Do not infer from meta/nav |
| `price_eur` | `Cena s DPH`, fallback `Cena bez DPH` | Required |
| `in_stock` | Detail stock text | `Skladom` and not `nie` -> true |

---

## 3. Search Fallback

`search_by_query()` order:

1. Try parsing the response as a direct detail page.
2. If not detail HTML, parse the first search-result card.
3. Strip trailing `/cat/{id}` from the card URL.
4. Fetch the detail URL and prefer the enriched detail result.
5. If detail fetch fails, return the card listing with cleaned URL.

This ordering is important because the detail page is where EAN and explicit brand fields live.

---

## 4. Operational Notes

| Setting | Value |
|---|---|
| `competitor_id` | `rebiop_sk` |
| Default RPS | `1` per worker |
| Config workers | `3` |
| Daily mode | Feed if found; otherwise BFS category crawl |

Run only this competitor:

```bash
./.venv/bin/python jobs/daily_scrape.py --only rebiop_sk
```

---

## 5. Known Pitfalls

| Problem | Root Cause | Fix |
|---|---|---|
| Brand-looking words in nav/meta | Site pages can contain generic brand/category text | Only trust explicit detail fields |
| Pagination loops on sidebar links | Non-product links can appear repeatedly | Extract only `.ctg-product-box` product links |
| Search card lacks identifiers | Search results are summary cards | Always refetch detail URL when possible |
| Some pages lack explicit brand | Site does not always expose manufacturer | Persistence can backfill from known real EANs |

