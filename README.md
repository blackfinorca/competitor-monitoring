# AG Naradie — Competitor Monitoring

Price monitoring for AG Naradie / ToolZone. Scrapes competitor catalogues, matches products, generates pricing recommendations.

---

## Setup

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env          # set DATABASE_URL and OPENAI_API_KEY
scripts/bootstrap_db.sh
alembic upgrade head
```

After activating the virtualenv, run jobs with `python` or `./.venv/bin/python`.
The system `python3` on some machines may be older than the repo's Python 3.11 syntax.

---

## Pipeline Overview

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────────┐
│   SCRAPE    │ →  │    MATCH    │ →  │  RECOMMEND  │ →  │    ALERT     │
│             │    │             │    │             │    │              │
│ Fetch prices│    │ Link comp.  │    │ Build price │    │ Slack/webhook│
│ from all    │    │ listings to │    │ snapshots & │    │ for priority │
│ competitors │    │ ToolZone    │    │ actions     │    │ items        │
└─────────────┘    └─────────────┘    └─────────────┘    └──────────────┘
  daily_scrape       match_products     daily_recommend     daily_alert
  manufacturer_
  scrape
```

Two scraping modes:

```
Full catalogue mode          Manufacturer mode
──────────────────           ─────────────────
daily_scrape.py              daily_scrape.py --manufacturer knipex
  All competitors            manufacturer_scrape.py --manufacturer knipex
  All products                 Phase 1: ToolZone catalogue crawl
  MPN search / feeds           Phase 2: Catalogue competitors (parallel)
                               Phase 3: Search competitors vs TZ MPNs
```

`daily_scrape.py` saves listings to the DB in batches during the run and flushes
the pending buffer on shutdown/interrupt, so long scrapes keep their partial progress.
The current operational cadence is a **monthly** scrape + analytics cycle.

---

## Competitors

| ID | Name | Scrape method |
|----|------|--------------|
| `toolzone_sk` | ToolZone (own) | Sitemap full-catalogue crawl · JSON-LD |
| `madmat_sk` | Madmat | Heureka XML feed |
| `centrumnaradia_sk` | Centrum Naradia | Heureka XML feed |
| `fermatshop_sk` | Fermatshop | Sitemap full-catalogue crawl |
| `strendpro_sk` | Strendpro | Category + pagination full-catalogue crawl |
| `boukal_cz` | Boukal (CZ) | Feed discovery first · JS-rendered fallback |
| `bo_import_cz` | BO-Import (CZ) | Manufacturer-page crawl · JSON-LD · CZK→EUR |
| `agi_sk` | AGI | Manufacturer-page crawl · JSON-LD |
| `ahprofi_sk` | AH Profi | Search-by-MPN |
| `naradieshop_sk` | NaradieShop | Search-by-MPN |
| `doktorkladivo_sk` | Doktor Kladivo | Search-by-MPN |
| `rebiop_sk` | Rebiop | Search-by-MPN |

> **Search-by-MPN competitors** require ToolZone reference products to work in manufacturer mode — always include `toolzone_sk` in `--only` when using them.

---

## Scraping

### By manufacturer (recommended for focused work)

```bash
# All competitors, all Knipex products
python jobs/daily_scrape.py --manufacturer knipex

# Catalogue competitors only (have brand/manufacturer pages)
python jobs/daily_scrape.py --manufacturer knipex --only toolzone_sk boukal_cz bo_import_cz agi_sk

# One search competitor (needs toolzone_sk for MPN list)
python jobs/daily_scrape.py --manufacturer knipex --only toolzone_sk naradieshop_sk

# Custom brand display name (used in feed/search filtering)
python jobs/daily_scrape.py --manufacturer knipex --brand-name Knipex
```

### By competitor (full catalogue)

```bash
# All competitors, full catalogue
python jobs/daily_scrape.py

# One competitor only
python jobs/daily_scrape.py --only agi_sk

# Full-catalogue crawlers
python jobs/daily_scrape.py --only fermatshop_sk
python jobs/daily_scrape.py --only strendpro_sk

# Multiple competitors
python jobs/daily_scrape.py --only boukal_cz bo_import_cz madmat_sk fermatshop_sk

# Different catalogue file
python jobs/daily_scrape.py --catalogue data/my_catalogue.csv
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--manufacturer SLUG` | — | Switch to manufacturer mode (e.g. `knipex`) |
| `--brand-name NAME` | derived from slug | Brand display name for feed/search filtering |
| `--only ID [ID ...]` | all | Restrict to these competitor IDs |
| `--catalogue PATH` | `data/ag_catalogue.csv` | Catalogue CSV (full-catalogue mode only) |
| `--sequential` | off | Disable parallelism — useful for debugging |

---

## Matching

`jobs/match_products.py` links scraped competitor listings to ToolZone
reference listings and writes the results to `listing_matches`. Layers run in
order and the first hit wins.

```
Layer  Type                Trigger                              Confidence
─────  ──────────────────  ───────────────────────────────────  ──────────
  1    exact_ean           EAN barcode identical                  1.00
  2    exact_mpn           Brand + MPN both match                 1.00
  3    mpn_no_brand        MPN matches, listing has no brand      0.90
  4    regex_ean           EAN extracted from listing title       0.95
  5*   llm_fuzzy           vector top-40 + gpt-5-nano verify      0.85
       * opt-in via --llm flag, requires OPENAI_API_KEY
```

Notes:
- The current LLM matcher narrows candidates locally with multilingual vector
  search, then sends up to 40 ToolZone candidates to OpenAI for verification.
- Default OpenAI model is `gpt-5-nano`.
- ToolZone is the reference catalogue for `match_products.py`; `--only` limits
  the competitor side, not the ToolZone side.

### By manufacturer and/or competitor

```bash
# Match all Knipex listings (all competitors)
python jobs/match_products.py --manufacturer knipex

# Match one competitor only
python jobs/match_products.py --manufacturer knipex --only agi_sk

# Scrape fresh + match in one command
python jobs/match_products.py --manufacturer knipex --scrape

# Full pipeline: scrape → match → LLM fuzzy → report
python jobs/match_products.py --manufacturer knipex --scrape --llm

# LLM matching for one competitor
python jobs/match_products.py --manufacturer knipex --only agi_sk --llm

# Re-match already-matched listings (e.g. after model change)
python jobs/match_products.py --manufacturer knipex --force --llm
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--manufacturer BRAND` | all brands | Brand to match (e.g. `knipex`) |
| `--scrape` | off | Run scraping first before matching |
| `--llm` | off | Enable LLM fuzzy layer (requires `OPENAI_API_KEY`) |
| `--only ID [ID ...]` | all | Restrict to these competitor IDs |
| `--force` | off | Re-match listings that already have a match |
| `--no-report` | off | Skip the price-comparison table at the end |

### Legacy matching

`jobs/old_match_products.py` keeps the pre-vector LLM behavior with the same
flags and DB output as `jobs/match_products.py`. Use it when you want to
compare the current vector-backed matcher against the older
brand/title-token pre-filter.

```bash
python jobs/old_match_products.py --manufacturer knipex --llm
python jobs/old_match_products.py --only agi_sk boukal_cz --llm
```

---

## Common Workflows

```bash
# ── New manufacturer, first time ──────────────────────────────────────────
python jobs/match_products.py --manufacturer knipex --scrape --llm

# ── Refresh one competitor, then re-match ─────────────────────────────────
python jobs/daily_scrape.py --manufacturer knipex --only agi_sk
python jobs/match_products.py --manufacturer knipex --only agi_sk --llm

# ── Search competitor (needs ToolZone MPN list) ───────────────────────────
python jobs/daily_scrape.py --manufacturer knipex --only toolzone_sk naradieshop_sk
python jobs/match_products.py --manufacturer knipex --only naradieshop_sk --llm

# ── Full monthly run (all brands, all competitors) ────────────────────────
python jobs/daily_scrape.py
python jobs/match_products.py --llm
python jobs/daily_recommend.py
python jobs/daily_alert.py

# ── Compare current matcher vs legacy matcher ──────────────────────────────
python jobs/match_products.py --only agi_sk boukal_cz --llm
python jobs/old_match_products.py --only agi_sk boukal_cz --llm
```

---

## Other Jobs

### `daily_ingest.py` — Load AG catalogue
```bash
python jobs/daily_ingest.py                          # reads data/ag_catalogue.csv
```

### `daily_recommend.py` — Generate pricing recommendations
```bash
python jobs/daily_recommend.py                       # no flags
```

### `daily_alert.py` — Send Slack alerts
```bash
python jobs/daily_alert.py                           # silent if ALERT_WEBHOOK_URL not set
```

### `old_match_products.py` — Legacy ToolZone matcher
```bash
python jobs/old_match_products.py --manufacturer knipex --llm
python jobs/old_match_products.py --only agi_sk boukal_cz --llm
```

Uses the older LLM candidate selection path:
- same CLI flags as `match_products.py`
- same `listing_matches` output table
- brand/title-token pre-filter before LLM verification

### `export_manufacturer.py` — Export manufacturer comparison to Excel

Exports all ToolZone products for a manufacturer with matched competitor prices side by side.
One row per ToolZone product, one column group per competitor. Price differences are colour-coded.
Includes a **Summary** sheet with per-competitor match rate and average price diff.

```bash
# All competitors → reports/knipex_2026-04-17.xlsx
python jobs/export_manufacturer.py --manufacturer knipex

# Specific competitors only
python jobs/export_manufacturer.py --manufacturer knipex --only boukal_cz bo_import_cz agi_sk

# Custom output path
python jobs/export_manufacturer.py --manufacturer knipex --output reports/knipex-april.xlsx

# Only high-confidence matches (EAN/MPN)
python jobs/export_manufacturer.py --manufacturer knipex --min-confidence 0.90
```

| Flag | Default | Description |
|------|---------|-------------|
| `--manufacturer SLUG` | required | Brand to export (e.g. `knipex`) |
| `-o / --output PATH` | `reports/{manufacturer}_YYYY-MM-DD.xlsx` | Output file |
| `--only ID [ID ...]` | all | Include only these competitor IDs |
| `--min-confidence FLOAT` | `0.72` | Minimum match confidence to include |

**Colour coding (Diff% column):**
🟢 Green = competitor cheaper than ToolZone · 🔴 Orange = competitor more expensive · 🟡 Yellow = within ±1%

---

### `export_prices.py` — Export to CSV
```bash
python jobs/export_prices.py                         # → reports/prices_YYYY-MM-DD.csv
python jobs/export_prices.py --output april-2026.csv
python jobs/export_prices.py --only toolzone_sk boukal_cz
```

### `manufacturer_scrape.py` — Scrape one manufacturer (direct)
```bash
python jobs/manufacturer_scrape.py --manufacturer knipex
python jobs/manufacturer_scrape.py --list-manufacturers   # see all available slugs
```

### `enrich_allegro_eans.py` — Backfill missing AG EANs from Allegro

Reads an Allegro Excel export or normalized CSV, matches rows against AG
`products` with missing EANs, and writes confirmed EANs back into
`products.ean`.

```bash
# Default Excel input + LLM verification for ambiguous rows
python jobs/enrich_allegro_eans.py --input "item-analysis/Allegro zalistované položky 42026.xlsx" --llm

# Dry run + report only
python jobs/enrich_allegro_eans.py --dry-run --report reports/allegro_backfill_preview.csv

# Small validation batch
python jobs/enrich_allegro_eans.py --llm --limit 200 --batch-size 50
```

Behavior:
- reads `.xlsx` or normalized `.csv` input
- updates `products.ean` only when the current DB value is empty
- uses strict exact-title / vector checks before optional LLM fallback
- commits updates in batches and flushes buffered updates on interrupt
- uses up to 40 candidates for the LLM fallback and accepts confidence `>= 0.85`
- writes a per-row report to `reports/allegro_ean_backfill_YYYY-MM-DD.csv` by default

### Allegro sidecar scripts

The `item-analysis/` directory contains browser/Excel-based Allegro utilities
that are separate from the main backend pipeline:

```bash
# Normalise Allegro Excel export into CSV
python item-analysis/read_allegro_eans.py

# Load scraped Allegro offers into the DB
python item-analysis/load_allegro_offers.py
```

---

## Dashboard

```bash
streamlit run dashboard/app.py
```

Tabs: **Product Search** · **Price Compare** · **By Manufacturer** · **Coverage Health**

---

## Cron (monthly full run)

```cron
00 01 1 * *  cd /path/to/repo && .venv/bin/python jobs/daily_ingest.py
00 02 1 * *  cd /path/to/repo && .venv/bin/python jobs/daily_scrape.py
00 04 1 * *  cd /path/to/repo && .venv/bin/python jobs/match_products.py --llm
00 05 1 * *  cd /path/to/repo && .venv/bin/python jobs/daily_recommend.py
30 05 1 * *  cd /path/to/repo && .venv/bin/python jobs/daily_alert.py
```

---

## Adding a Competitor

1. Add entry to `config/competitors.yaml`
2. Run `scripts/inspect_competitor.py https://example.sk`
3. Prefer Heureka XML feed when available
4. Add tests under `tests/scrapers/`
