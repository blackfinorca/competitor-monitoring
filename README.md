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

# Combine
python jobs/daily_scrape.py --only boukal_cz --catalogue data/my_catalogue.csv
```

| Flag | Default | Description |
|------|---------|-------------|
| `--only ID [ID ...]` | all | Scrape only the listed competitor IDs |
| `--catalogue PATH` | `data/ag_catalogue.csv` | Path to the AG catalogue CSV |

**Active competitor IDs:**
`doktorkladivo_sk`, `ahprofi_sk`, `naradieshop_sk`, `toolzone_sk`, `rebiop_sk`, `boukal_cz`, `madmat_sk`, `centrumnaradia_sk`

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
