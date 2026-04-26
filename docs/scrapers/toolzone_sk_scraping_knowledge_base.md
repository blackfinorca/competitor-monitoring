# ToolZone Scraping Knowledge Base

> Reference document for scraping `toolzone_sk` (`toolzone.sk`). ToolZone is marked `own_store: true` in config but is still scraped as the reference catalogue source.

---

## 1. Site Architecture

ToolZone runs on iKeloc/Keloc. Listing pages are partly AJAX-rendered, but product detail pages expose JSON-LD and GTM data.

| Purpose | Pattern | Notes |
|---|---|---|
| Base URL | `https://www.toolzone.sk` | Configured competitor URL |
| Sitemap | `/sitemap.xml` | Main full-catalogue source |
| Product detail | `/produkt/{slug}.htm` | JSON-LD + GTM dataLayer |
| Search | `/vyhledavani/?search_query={query}` | Raw HTML may expose `/produkt/` links |
| Manufacturers index | `/vyrobci/` | Used by manufacturer scrape |
| Manufacturer page | `/vyrobce/{slug}/` | Paginated via `/katalog-strana{page}` |

### Current Crawl Flow

```text
GET /sitemap.xml
    -> collect /produkt/*.htm URLs
    -> optional brand_slug filtering
    -> scrape in chunks of 200
    -> parse product detail JSON-LD and GTM price
```

---

## 2. Data Extraction

### Product Detail JSON-LD

| Field | Source | Notes |
|---|---|---|
| `title` | JSON-LD `name` | Product title |
| `competitor_sku` | JSON-LD `sku` | Internal ToolZone SKU; may also look like EAN |
| `mpn` | JSON-LD `mpn` | Manufacturer part number |
| `ean` | `gtin13`, `gtin8`, `gtin`, or numeric `sku` | EAN-shaped only |
| `brand` | `brand.name` / `brand` | Product brand |
| `price_eur` | GTM EUR price preferred; JSON-LD fallback | JSON-LD can report CZK |
| `in_stock` | `offers.availability` | `InStock` / `OutOfStock` |
| `url` | `offers.url`, fallback page URL | Stored row URL |

### Price Handling

ToolZone JSON-LD sometimes reports CZK. The scraper first looks for GTM `currencyCode: "EUR"` and following `price`; if absent, it converts CZK to EUR at a rough fixed rate.

---

## 3. Search and Manufacturer Modes

`search_by_query()` calls `/vyhledavani/` and scans raw HTML for absolute `/produkt/` links. If the platform renders results only through AJAX, this fallback returns `None`.

Manufacturer scraping:

```text
GET /vyrobci/
    -> collect (display_name, slug)
GET /vyrobce/{slug}/, /vyrobce/{slug}/katalog-strana2, ...
    -> extract product URLs
    -> scrape details in parallel
```

---

## 4. Operational Notes

| Setting | Value |
|---|---|
| `competitor_id` | `toolzone_sk` |
| Default RPS | config says `4` per worker |
| Config workers | `16` |
| Daily mode | Sitemap full catalogue crawl |
| Batch/chunk size | 200 URLs before yielding |
| Ownership | `own_store: true` |

Run only this scraper:

```bash
./.venv/bin/python jobs/daily_scrape.py --only toolzone_sk
```

---

## 5. Known Pitfalls

| Problem | Root Cause | Fix |
|---|---|---|
| Search returns no product links | Results can be AJAX-rendered | Prefer sitemap/manufacturer detail crawling |
| JSON-LD price in CZK | Site exposes mixed currency sources | Prefer GTM EUR dataLayer price |
| Large sitemap | Around tens of thousands of product URLs | Use chunked yielding and optional `brand_slugs` filter |
| Reference store in reports | ToolZone is owned by AG | Exclude `own_store` from competitor benchmarks |

