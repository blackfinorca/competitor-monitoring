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
  manufacturer_      daily_match
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

---

## Competitors

| ID | Name | Scrape method |
|----|------|--------------|
| `toolzone_sk` | ToolZone (own) | Manufacturer-page crawl · JSON-LD |
| `madmat_sk` | Madmat | Heureka XML feed |
| `centrumnaradia_sk` | Centrum Naradia | Heureka XML feed |
| `boukal_cz` | Boukal (CZ) | Brand-page pagination · JSON-LD |
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

# Multiple competitors
python jobs/daily_scrape.py --only boukal_cz bo_import_cz madmat_sk

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

Links scraped competitor listings to ToolZone reference products. Layers run in order — first match wins.

```
Layer  Type                Trigger                              Confidence
─────  ──────────────────  ───────────────────────────────────  ──────────
  1    exact_ean           EAN barcode identical                  1.00
  2    exact_mpn           Brand + MPN both match                 1.00
  3    mpn_no_brand        MPN matches, listing has no brand      0.90
  4    regex_ean_title     EAN-13 extracted from title            0.93
  5    regex_mpn_title     MPN from title + brand agrees          0.90
  6    regex_mpn_no_brand  MPN from title, brand absent           0.72–0.78
  7*   llm_fuzzy           gpt-4o-mini title/spec similarity      0.75–0.84
       * opt-in via --llm flag, requires OPENAI_API_KEY
```

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

# ── Full daily run (all brands, all competitors) ──────────────────────────
python jobs/daily_scrape.py
python jobs/match_products.py --llm
python jobs/daily_recommend.py
python jobs/daily_alert.py
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

### `daily_match.py` — Batch match (legacy)
```bash
python jobs/daily_match.py                           # deterministic layers only
python jobs/daily_match.py --llm                     # + LLM layer
python jobs/daily_match.py --llm --min-confidence 0.80
```

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

---

## Dashboard

```bash
streamlit run dashboard/app.py
```

Tabs: **Product Search** · **Price Compare** · **By Manufacturer** · **Coverage Health**

---

## Cron (overnight full run)

```cron
00 01 * * *  cd /path/to/repo && .venv/bin/python jobs/daily_ingest.py
00 02 * * *  cd /path/to/repo && .venv/bin/python jobs/daily_scrape.py
00 04 * * *  cd /path/to/repo && .venv/bin/python jobs/match_products.py --llm
00 05 * * *  cd /path/to/repo && .venv/bin/python jobs/daily_recommend.py
30 05 * * *  cd /path/to/repo && .venv/bin/python jobs/daily_alert.py
```

---

## Adding a Competitor

1. Add entry to `config/competitors.yaml`
2. Run `scripts/inspect_competitor.py https://example.sk`
3. Prefer Heureka XML feed when available
4. Add tests under `tests/scrapers/`
