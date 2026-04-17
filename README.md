# AG Naradie Pricing Agent

PoC competitor price monitoring agent for AG Naradie. The first round focuses on a small SKU subset, daily batch jobs, advisory recommendations, and a Streamlit dashboard.

## Setup

Requires Python 3.11+ and Postgres 16 with pgvector enabled.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
cp .env.example .env
```

Set `DATABASE_URL` and `OPENAI_API_KEY` in `.env`.

## Database

```bash
scripts/bootstrap_db.sh
alembic upgrade head
```

## Daily Pipeline

Run overnight in this order:

```cron
00 01 * * * cd /path/to/competitor-monitoring && .venv/bin/python jobs/daily_ingest.py
00 02 * * * cd /path/to/competitor-monitoring && .venv/bin/python jobs/daily_scrape.py
00 04 * * * cd /path/to/competitor-monitoring && .venv/bin/python jobs/daily_match.py
00 05 * * * cd /path/to/competitor-monitoring && .venv/bin/python jobs/daily_recommend.py
30 05 * * * cd /path/to/competitor-monitoring && .venv/bin/python jobs/daily_alert.py
```

---

## Job Reference

### `daily_ingest.py` — Load AG catalogue

Reads `data/ag_catalogue.csv` and upserts products into the database. No flags.

```bash
python jobs/daily_ingest.py
```

---

### `daily_scrape.py` — Scrape competitor prices

Fetches prices from all active competitor scrapers and saves listings to the database.

```bash
# Scrape all competitors
python jobs/daily_scrape.py

# Scrape specific competitors only
python jobs/daily_scrape.py --only boukal_cz madmat_sk

# Use a different catalogue file
python jobs/daily_scrape.py --catalogue data/my_catalogue.csv

# Debug: run one competitor at a time (no parallelism)
python jobs/daily_scrape.py --sequential --only ahprofi_sk
```

| Flag | Default | Description |
|------|---------|-------------|
| `--only ID [ID ...]` | all | Scrape only the listed competitor IDs |
| `--catalogue PATH` | `data/ag_catalogue.csv` | Path to the AG catalogue CSV |
| `--sequential` | off | Disable parallel scraping; run competitors one by one (useful for debugging) |

**Active competitor IDs:**
`toolzone_sk`, `madmat_sk`, `centrumnaradia_sk`, `boukal_cz`, `bo_import_cz`, `ahprofi_sk`, `naradieshop_sk`, `doktorkladivo_sk`, `rebiop_sk`, `agi_sk`

---

### `manufacturer_scrape.py` — Scrape all products for one manufacturer

Scrapes ToolZone + all enabled competitors for a specific manufacturer brand (e.g. Knipex, Wiha). Use this before `match_products.py` when you want fresh data.

```bash
# Scrape all competitors for Knipex
python jobs/manufacturer_scrape.py --manufacturer knipex

# Custom brand display name (used for feed/search filtering)
python jobs/manufacturer_scrape.py --manufacturer wiha --brand-name Wiha

# Scrape specific competitors only
python jobs/manufacturer_scrape.py --manufacturer knipex --only boukal_cz bo_import_cz

# Debug: run one competitor at a time
python jobs/manufacturer_scrape.py --manufacturer knipex --sequential

# List all available manufacturer slugs on ToolZone
python jobs/manufacturer_scrape.py --list-manufacturers
```

| Flag | Default | Description |
|------|---------|-------------|
| `--manufacturer SLUG` | required | Manufacturer slug as used in ToolZone URLs (e.g. `knipex`, `wiha`, `format`) |
| `--brand-name NAME` | derived from slug | Brand display name used in feed/search filtering |
| `--only ID [ID ...]` | all | Scrape only these competitor IDs |
| `--sequential` | off | Disable parallel execution; run competitors one by one |
| `--list-manufacturers` | — | Print all manufacturer slugs available on ToolZone and exit |

---

### `match_products.py` — Scrape → match → report pipeline

End-to-end pipeline for a single manufacturer: optionally scrapes fresh data, runs EAN matching, optionally runs LLM fuzzy matching, and prints a price-comparison report.

```bash
# EAN match only, print report
python jobs/match_products.py --manufacturer knipex

# Scrape fresh data first, then EAN + LLM match
python jobs/match_products.py --manufacturer knipex --scrape --llm

# LLM match without re-scraping
python jobs/match_products.py --manufacturer knipex --llm

# Restrict to specific competitors
python jobs/match_products.py --manufacturer knipex --only boukal_cz bo_import_cz

# Re-match listings that already have a match record
python jobs/match_products.py --manufacturer knipex --force --llm

# Match without printing the report at the end
python jobs/match_products.py --manufacturer knipex --llm --no-report
```

| Flag | Default | Description |
|------|---------|-------------|
| `--manufacturer BRAND` | all brands | Brand to scrape/match (e.g. `knipex`). Omit to run across all brands |
| `--scrape` | off | Run `manufacturer_scrape.py` before matching (requires `--manufacturer`) |
| `--llm` | off | Enable LLM fuzzy matching for listings without an EAN match (requires `OPENAI_API_KEY`) |
| `--only ID [ID ...]` | all | Restrict scraping and matching to these competitor IDs |
| `--force` | off | Re-match listings that already have a match record |
| `--no-report` | off | Skip the price-comparison table printed at the end |

LLM matches are saved immediately as they are found and printed to the terminal in real time. Requires `OPENAI_API_KEY` in `.env`. Model defaults to `gpt-4o-mini` (configurable via `OPENAI_MODEL`).

---

### `daily_match.py` — Match listings to AG products

Runs the deterministic + regex matching pipeline (layers 1–5) against all unmatched competitor listings. Optionally enables LLM-assisted fuzzy matching (layer 6).

```bash
# Deterministic matching only (layers 1–5)
python jobs/daily_match.py

# + LLM fuzzy layer (requires OPENAI_API_KEY in .env)
python jobs/daily_match.py --llm

# LLM with custom confidence threshold
python jobs/daily_match.py --llm --min-confidence 0.80
```

| Flag | Default | Description |
|------|---------|-------------|
| `--llm` | off | Enable LLM fuzzy matching for listings that deterministic layers couldn't match |
| `--min-confidence FLOAT` | `0.75` | Minimum confidence score to accept an LLM match (0.0–1.0) |

---

### `daily_recommend.py` — Generate pricing recommendations

Builds daily pricing snapshots and writes recommendation rows for review. No flags.

```bash
python jobs/daily_recommend.py
```

---

### `daily_alert.py` — Send Slack alerts

Posts high-priority recommendations to the Slack webhook defined in `ALERT_WEBHOOK_URL`. No flags. Silent if the env var is not set.

```bash
python jobs/daily_alert.py
```

---

### `export_prices.py` — Export price comparison to CSV

Produces one CSV row per AG product with competitor prices in columns. ToolZone is the reference column; all other competitors follow alphabetically. Output is UTF-8 with BOM (opens directly in Excel).

```bash
# Export all competitors to reports/prices_YYYY-MM-DD.csv
python jobs/export_prices.py

# Custom output path
python jobs/export_prices.py --output reports/april-2026.csv

# Export specific competitors only
python jobs/export_prices.py --only toolzone_sk boukal_cz madmat_sk
```

| Flag | Default | Description |
|------|---------|-------------|
| `-o / --output PATH` | `reports/prices_YYYY-MM-DD.csv` | Output file path |
| `--only ID [ID ...]` | all with matches | Include only these competitor IDs |

---

## Dashboard

```bash
streamlit run dashboard/app.py
```

---

## Adding Competitors

1. Add the competitor to `config/competitors.yaml`.
2. Run `scripts/inspect_competitor.py https://example.sk`.
3. Prefer a Heureka XML feed parser when a feed is available.
4. Add fixture-based tests under `tests/scrapers/`.
