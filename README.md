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

Set `DATABASE_URL` and `ANTHROPIC_API_KEY` in `.env`.

## Database

```bash
scripts/bootstrap_db.sh
alembic upgrade head
```

## Daily Jobs

The PoC pipeline is intended to run overnight in this order:

```cron
00 01 * * * cd /path/to/competitor-monitoring && .venv/bin/python jobs/daily_ingest.py
00 02 * * * cd /path/to/competitor-monitoring && .venv/bin/python jobs/daily_scrape.py
00 04 * * * cd /path/to/competitor-monitoring && .venv/bin/python jobs/daily_match.py
00 05 * * * cd /path/to/competitor-monitoring && .venv/bin/python jobs/daily_recommend.py
30 05 * * * cd /path/to/competitor-monitoring && .venv/bin/python jobs/daily_alert.py
```

## Dashboard

```bash
streamlit run dashboard/app.py
```

## Adding Competitors

1. Add the competitor to `config/competitors.yaml`.
2. Run `scripts/inspect_competitor.py https://example.sk`.
3. Prefer a Heureka XML feed parser when a feed is available.
4. Add fixture-based tests under `tests/scrapers/`.

