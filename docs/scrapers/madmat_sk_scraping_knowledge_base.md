# Madmat Scraping Knowledge Base

> Reference document for scraping `madmat_sk` (`madmat.sk`).

---

## 1. Site Architecture

Madmat is currently handled by the generic Shoptet / Heureka feed path in `daily_scrape.py`. The scraper probes standard feed URLs and parses the first available XML feed.

| Purpose | Pattern | Notes |
|---|---|---|
| Base URL | `https://www.madmat.sk` | Configured competitor URL |
| Primary feed candidate | `/heureka.xml` | Listed as known feed competitor |
| Other probed feed paths | `/heureka-feed.xml`, `/heureka/export.xml`, `/feed/heureka.xml`, `/zbozi.xml`, `/export/heureka.xml`, `/export/zbozi.xml` | Shared probe list |

---

## 2. Feed Extraction

The feed parser reads `<SHOPITEM>` records.

| Field | Source tag | Notes |
|---|---|---|
| `title` | `PRODUCTNAME`, fallback `PRODUCT` | Required |
| `url` | `URL` | Required |
| `price_eur` | `PRICE_VAT` | Required; comma converted to dot |
| `competitor_sku` | `ITEM_ID` | Optional |
| `brand` | `MANUFACTURER` | Optional |
| `mpn` | `PRODUCTNO` | Optional |
| `ean` | `EAN` | Optional |
| `in_stock` | `DELIVERY_DATE` | `"0"` -> true |

---

## 3. Operational Notes

| Setting | Value |
|---|---|
| `competitor_id` | `madmat_sk` |
| Default RPS | `1` |
| Config workers | `1` |
| Scraper class | `ShoptetGenericScraper` via feed mode |
| Daily mode | XML feed |

Run only this competitor:

```bash
./.venv/bin/python jobs/daily_scrape.py --only madmat_sk
```

---

## 4. Known Pitfalls

| Problem | Root Cause | Fix |
|---|---|---|
| Feed URL changes | Feed location is not hard-coded | Keep probing `HEUREKA_FEED_PATHS` |
| Missing products | Feed is only as complete as exported feed | Inspect site/feed if counts drop |
| Missing stock precision | `DELIVERY_DATE` only maps immediate availability | Preserve `None` for unknown delivery values |

