# AH Profi Scraping Knowledge Base

> Reference document for scraping `ahprofi_sk` (`ahprofi.sk`).

---

## 1. Site Architecture

AH Profi is a custom Slovak platform. Category listing pages are JS-rendered, but product detail pages and sitemap pages expose enough static HTML for HTTP-only scraping.

| Purpose | Pattern | Notes |
|---|---|---|
| Base URL | `https://www.ahprofi.sk` | Configured competitor URL |
| Sitemap index | `/sitemap` | Contains product sitemap page numbers |
| Product sitemap page | `/sitemap?products=true&page={n}` | Contains product `<loc>` URLs |
| Search fallback | `/vysledky-vyhladavania?search_keyword={query}` | Redirects to detail page on exact hit |

### Current Crawl Flow

```text
GET /sitemap
    -> discover sitemap?products=true&page=N
GET each product sitemap page
    -> extract product URLs
GET each product detail page in parallel
    -> parse microdata / og tags / product codes block
```

---

## 2. Data Extraction

Product detail pages are parsed with targeted regexes.

| Field | Source | Notes |
|---|---|---|
| `title` | `og:title` content | Strip ` | ahprofi.sk` suffix |
| `competitor_sku` | `itemprop="productID"` | Same value is used as `mpn` |
| `mpn` | `itemprop="productID"` | AH Profi product code |
| `ean` | `itemprop="gtin13"` text | 8-13 digits |
| `brand` | `#product-codes` field labelled `Výrobca` | Anchored to explicit manufacturer field |
| `price_eur` | `itemprop="price" content="N.NN"` | Required |
| `in_stock` | `itemprop="availability"` URL | `InStock` substring -> true |

Brand example:

```html
<div id="product-codes">
  <span class="col col-5 label">Výrobca&nbsp;</span>
  <span class="col col-7 right"><a href="https://www.ahprofi.sk/knipex">Knipex</a></span>
</div>
```

---

## 3. Search Fallback

`search_by_mpn(brand, mpn)` strips separators from the MPN:

```text
87-01-250 -> 8701250
```

Then it calls `/vysledky-vyhladavania?search_keyword=8701250`.

If the final response URL still contains `vysledky-vyhladavania`, the scraper treats it as no exact match. If the server redirected to a product URL, it parses the returned product HTML.

---

## 4. Operational Notes

| Setting | Value |
|---|---|
| `competitor_id` | `ahprofi_sk` |
| Default RPS | `1` per worker |
| Config workers | `3` |
| Feed support | None |
| Daily mode | Sitemap full catalogue crawl |

Run only this competitor:

```bash
./.venv/bin/python jobs/daily_scrape.py --only ahprofi_sk
```

---

## 5. Known Pitfalls

| Problem | Root Cause | Fix |
|---|---|---|
| Category pages do not expose product cards statically | JS-rendered listing pages | Use sitemap pages for full crawl |
| Search page returned instead of detail page | No exact search redirect | Return `None` rather than parsing unrelated results |
| Brand false positives | Brand appears in navigation/title copy | Parse only the explicit `Výrobca` field inside `#product-codes` |

