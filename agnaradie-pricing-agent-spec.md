# AG Náradie — Competitor Price Monitoring Agent

**Version:** 0.1 (first round, PoC)
**Audience:** Codex / coding agent
**Owner:** Michal
**Goal of this document:** give Codex enough spec to scaffold the project end-to-end, without ambiguity.

---

## 1. Project goal

Build an agent that monitors competitor prices for AG Náradie (Slovak B2B tool distributor, Trnava), matches competitor SKUs to AG Náradie's catalogue, and produces daily pricing recommendations (raise / hold / drop) with evidence.

First-round scope is a **PoC**: end-to-end pipeline on a small subset of SKUs, advisory-only output, single-VM deployment.

Out of scope for round 1:
- Auto-repricing back into AG's e-shop or ERP
- Multi-tenant or SaaS features
- Advanced ML (elasticity modelling, forecasting)
- User authentication on the dashboard

---

## 2. Non-goals / constraints

- Not a full-site crawler. Prefer Heureka XML feeds and targeted search over full catalogue crawl.
- Not real-time. Daily batch is fine.
- Not a repricing engine. Recommendations only, human-in-the-loop.
- Respect `robots.txt` and use conservative rate limits.

---

## 3. Competitor set (round 1)

All seven are direct online competitors. Treat as equal weight in this phase.

| id | name | url |
|---|---|---|
| doktorkladivo_sk | Doktor Kladivo | https://www.doktorkladivo.sk |
| ahprofi_sk | AH Profi | https://www.ahprofi.sk |
| naradieshop_sk | NaradieShop | https://www.naradieshop.sk |
| madmat_sk | Madmat | https://www.madmat.sk |
| centrumnaradia_sk | Centrum Náradia | https://www.centrumnaradia.sk |
| fermatshop_sk | Fermatshop | https://www.fermatshop.sk |
| toolzone_sk | ToolZone | https://www.toolzone.sk |

AG Náradie's own e-shop link will be provided separately and added to config as the canonical source.

---

## 4. Architecture

Five loosely coupled layers, one-direction data flow, shared Postgres DB.

```
[1] Catalogue ingestion  →  [2] Competitor scraping  →  [3] SKU matching
                                                              ↓
[5] Dashboard / alerts  ←  [4] Pricing analysis & recommendations
```

### Layer 1 — Catalogue ingestion (AG Náradie)
- Input: AG Náradie product export (CSV/feed/DB). Exact source TBD.
- Required fields: `sku`, `brand`, `mpn`, `ean` (if available), `title`, `category`, `price_eur`, `cost_eur` (if available), `stock`.
- Output: rows in `products` table. Refresh daily.

### Layer 2 — Competitor scraping
- One scraper per competitor, all implementing the same `CompetitorScraper` interface.
- Strategy preference order per competitor:
  1. Heureka XML feed (`/heureka.xml` or similar).
  2. Google Shopping feed.
  3. Search-by-MPN via the site's search endpoint.
  4. Category crawl (last resort).
- Output: rows in `competitor_listings` table.
- Rate limit: 1–2 req/sec per site, configurable. Run overnight.

### Layer 3 — SKU matching
Two-stage pipeline:
- **Stage A — deterministic**: `brand + MPN` (normalised uppercase, strip spaces/dashes/dots). Also try `EAN` if both sides have it. Confidence = 1.0.
- **Stage B — LLM fuzzy**: for unmatched competitor listings, generate top-20 candidates via embedding similarity on title+description, then ask Claude Haiku "is this the same product? return match + confidence".
- Store in `product_matches`. Anything with confidence < 0.85 goes to human review queue.

### Layer 4 — Pricing analysis & recommendations
- Daily job builds `pricing_snapshot` per SKU: min/median/max competitor price, who is cheapest, AG rank.
- Rule-based classifier assigns a **playbook**:
  - `raise` — AG cheapest by >8%, margin headroom exists.
  - `hold` — AG within ±5% of median.
  - `drop` — AG >15% above median on fast-moving SKU.
  - `investigate` — competitor price moved >20% day-on-day.
- LLM generates a short rationale per recommendation.
- Output: rows in `recommendations` (status: pending/approved/rejected).

### Layer 5 — Dashboard & alerts
- Streamlit app. Three views:
  - Recommendations queue (filter, approve/reject).
  - Price history per SKU (chart, all competitors overlaid).
  - Coverage health (match rates, per competitor, confidence breakdown).
- Email/Slack alert for `investigate` events.

---

## 5. Tech stack

| Concern | Choice |
|---|---|
| Language | Python 3.11+ |
| Package manager | uv or poetry |
| DB | Postgres 16 + pgvector |
| HTTP | httpx |
| HTML parsing | selectolax |
| Browser (fallback) | playwright |
| XML feeds | lxml |
| ORM / migrations | SQLAlchemy 2.x + Alembic |
| Embeddings | sentence-transformers multilingual (local) |
| LLM | Anthropic Claude — Haiku for matching, Sonnet for recommendation rationale |
| Orchestration | cron + Python entrypoints (upgrade to Prefect later) |
| Dashboard | Streamlit |
| Config | YAML (pydantic-settings for env vars) |
| Logging | structlog, JSON output |
| Tests | pytest, with httpx respx for scraper tests |

---

## 6. Repository structure

```
agnaradie-pricing-agent/
├── README.md
├── pyproject.toml
├── .env.example
├── config/
│   ├── competitors.yaml
│   └── playbooks.yaml
├── src/agnaradie_pricing/
│   ├── __init__.py
│   ├── settings.py             # pydantic-settings
│   ├── db/
│   │   ├── models.py
│   │   ├── session.py
│   │   └── migrations/         # alembic
│   ├── catalogue/
│   │   ├── ingest.py           # AG Náradie loader
│   │   └── normalise.py        # brand/MPN normalisation
│   ├── scrapers/
│   │   ├── base.py             # CompetitorScraper ABC
│   │   ├── shoptet_generic.py  # generic Shoptet parser
│   │   ├── heureka_feed.py     # XML feed parser (reusable)
│   │   ├── doktorkladivo.py
│   │   ├── ahprofi.py
│   │   ├── naradieshop.py
│   │   ├── madmat.py
│   │   ├── centrumnaradia.py
│   │   ├── ferant.py
│   │   └── toolzone.py
│   ├── matching/
│   │   ├── deterministic.py
│   │   ├── embeddings.py
│   │   └── llm_matcher.py
│   ├── pricing/
│   │   ├── snapshot.py
│   │   ├── recommender.py
│   │   └── rationale.py        # LLM explanation generation
│   ├── alerts/
│   │   └── dispatch.py
│   └── utils/
│       ├── http.py             # httpx client with retry + rate limit
│       └── text.py             # normalisation helpers
├── jobs/
│   ├── daily_ingest.py
│   ├── daily_scrape.py
│   ├── daily_match.py
│   ├── daily_recommend.py
│   └── daily_alert.py
├── dashboard/
│   └── app.py                  # Streamlit
├── tests/
│   ├── test_normalise.py
│   ├── test_matching.py
│   ├── scrapers/
│   │   └── fixtures/           # saved HTML/XML samples
│   └── test_recommender.py
└── scripts/
    ├── bootstrap_db.sh
    └── inspect_competitor.py   # manual site inspection helper
```

---

## 7. Database schema (first cut)

```sql
-- AG Náradie canonical catalogue
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    sku TEXT UNIQUE NOT NULL,
    brand TEXT,
    mpn TEXT,
    ean TEXT,
    title TEXT NOT NULL,
    category TEXT,
    price_eur NUMERIC(10,2),
    cost_eur NUMERIC(10,2),
    stock INT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_products_brand_mpn ON products (brand, mpn);
CREATE INDEX idx_products_ean ON products (ean);

-- Competitor listings (raw, per scrape)
CREATE TABLE competitor_listings (
    id BIGSERIAL PRIMARY KEY,
    competitor_id TEXT NOT NULL,
    competitor_sku TEXT,
    brand TEXT,
    mpn TEXT,
    ean TEXT,
    title TEXT NOT NULL,
    price_eur NUMERIC(10,2) NOT NULL,
    currency TEXT DEFAULT 'EUR',
    in_stock BOOLEAN,
    url TEXT,
    scraped_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_cl_competitor_scraped ON competitor_listings (competitor_id, scraped_at DESC);
CREATE INDEX idx_cl_brand_mpn ON competitor_listings (brand, mpn);

-- Matches between AG SKU and competitor listing
CREATE TABLE product_matches (
    id BIGSERIAL PRIMARY KEY,
    ag_product_id INT REFERENCES products(id),
    competitor_id TEXT NOT NULL,
    competitor_sku TEXT,
    match_type TEXT NOT NULL,         -- 'exact_ean' | 'exact_mpn' | 'llm_fuzzy'
    confidence NUMERIC(3,2) NOT NULL,
    verified_by_human BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ag_product_id, competitor_id, competitor_sku)
);

-- Daily pricing snapshot per SKU
CREATE TABLE pricing_snapshot (
    id BIGSERIAL PRIMARY KEY,
    ag_product_id INT REFERENCES products(id),
    snapshot_date DATE NOT NULL,
    ag_price NUMERIC(10,2),
    competitor_count INT,
    min_price NUMERIC(10,2),
    median_price NUMERIC(10,2),
    max_price NUMERIC(10,2),
    ag_rank INT,                      -- 1 = cheapest
    cheapest_competitor TEXT,
    UNIQUE (ag_product_id, snapshot_date)
);

-- Recommendations
CREATE TABLE recommendations (
    id BIGSERIAL PRIMARY KEY,
    ag_product_id INT REFERENCES products(id),
    snapshot_date DATE NOT NULL,
    playbook TEXT NOT NULL,           -- 'raise' | 'hold' | 'drop' | 'investigate'
    current_price NUMERIC(10,2),
    suggested_price NUMERIC(10,2),
    rationale TEXT,
    status TEXT DEFAULT 'pending',    -- 'pending' | 'approved' | 'rejected' | 'applied'
    created_at TIMESTAMPTZ DEFAULT NOW(),
    reviewed_at TIMESTAMPTZ,
    reviewer TEXT
);
```

---

## 8. Key interfaces

### 8.1 CompetitorScraper

```python
# src/agnaradie_pricing/scrapers/base.py
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

@dataclass
class CompetitorListing:
    competitor_id: str
    competitor_sku: str | None
    brand: str | None
    mpn: str | None
    ean: str | None
    title: str
    price_eur: float
    currency: str
    in_stock: bool | None
    url: str
    scraped_at: datetime

class CompetitorScraper(ABC):
    def __init__(self, config: dict):
        self.config = config
        self.competitor_id: str = config["id"]
        self.base_url: str = config["url"]

    @abstractmethod
    def discover_feed(self) -> str | None: ...

    @abstractmethod
    def fetch_feed(self, feed_url: str) -> list[CompetitorListing]: ...

    @abstractmethod
    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None: ...

    def run_daily(self, ag_catalogue: list[dict]) -> list[CompetitorListing]:
        feed_url = self.discover_feed()
        if feed_url:
            return self.fetch_feed(feed_url)
        results = []
        for sku in ag_catalogue:
            if sku.get("brand") and sku.get("mpn"):
                hit = self.search_by_mpn(sku["brand"], sku["mpn"])
                if hit:
                    results.append(hit)
        return results
```

### 8.2 Matcher

```python
def match_deterministic(product, listing) -> float | None:
    # Return 1.0 if EAN match, 1.0 if normalised brand+MPN match, else None
    ...

def match_llm(product, candidates: list[listing]) -> tuple[listing, float] | None:
    # Call Claude Haiku with candidate shortlist; return best match + confidence
    ...
```

### 8.3 Recommender playbook

```python
# config/playbooks.yaml keys load into these thresholds
PLAYBOOKS = {
    "raise":       {"min_gap_below_next": 0.08, "min_margin_after": 0.15},
    "hold":        {"max_gap_to_median":  0.05},
    "drop":        {"min_gap_above_median": 0.15, "min_competitors": 2},
    "investigate": {"min_day_on_day_move": 0.20},
}
```

---

## 9. Configuration

### `config/competitors.yaml`
```yaml
competitors:
  - id: doktorkladivo_sk
    name: Doktor Kladivo
    url: https://www.doktorkladivo.sk
    weight: 1.0
    rate_limit_rps: 1
  - id: ahprofi_sk
    name: AH Profi
    url: https://www.ahprofi.sk
    weight: 1.0
    rate_limit_rps: 1
  - id: naradieshop_sk
    name: NaradieShop
    url: https://www.naradieshop.sk
    weight: 1.0
    rate_limit_rps: 1
  - id: madmat_sk
    name: Madmat
    url: https://www.madmat.sk
    weight: 1.0
    rate_limit_rps: 1
  - id: centrumnaradia_sk
    name: Centrum Náradia
    url: https://www.centrumnaradia.sk
    weight: 1.0
    rate_limit_rps: 1
  - id: fermatshop_sk
    name: Fermatshop
    url: https://www.fermatshop.sk
    weight: 1.0
    rate_limit_rps: 1
  - id: toolzone_sk
    name: ToolZone
    url: https://www.toolzone.sk
    weight: 1.0
    rate_limit_rps: 1
```

### `.env.example`
```
DATABASE_URL=postgresql+psycopg://user:pass@localhost:5432/agnaradie
ANTHROPIC_API_KEY=sk-ant-...
LOG_LEVEL=INFO
ALERT_WEBHOOK_URL=
```

---

## 10. Build plan for Codex (round 1)

Execute in this order. Each task should produce runnable code with tests where noted.

1. **Bootstrap project**
   - Create repo skeleton per §6.
   - `pyproject.toml` with dependencies: httpx, selectolax, lxml, sqlalchemy, psycopg, alembic, pydantic-settings, structlog, streamlit, anthropic, sentence-transformers, pgvector, pytest, respx.
   - Add `README.md` with setup instructions, `.env.example`, `scripts/bootstrap_db.sh`.

2. **Database layer**
   - Implement SQLAlchemy models from §7.
   - First Alembic migration.
   - Simple session factory.

3. **Config loading**
   - Load `config/competitors.yaml` + `.env` via pydantic-settings.

4. **Scraper base + one generic implementation**
   - Implement `CompetitorScraper` ABC per §8.1.
   - Implement `HeurekaFeedMixin` that parses Heureka XML feed format.
   - Implement a `ShoptetGenericScraper` that inherits the mixin and handles search-by-MPN fallback for Shoptet-hosted sites.
   - Add `scripts/inspect_competitor.py`: given a competitor URL, probe for sitemap, Heureka feed, robots.txt, and print findings. Use this to decide per-site strategy.

5. **One concrete competitor end-to-end: `doktorkladivo_sk`**
   - Use inspect script to identify feed availability.
   - Implement the scraper (most likely subclass of `HeurekaFeedMixin` or `ShoptetGenericScraper`).
   - Write test with a saved fixture XML in `tests/scrapers/fixtures/`.
   - Persist listings into `competitor_listings`.

6. **Remaining six scrapers**
   - Repeat step 5 for the other six competitors. Reuse the generic parsers wherever possible.

7. **Catalogue ingestion stub**
   - For round 1, accept a CSV at `data/ag_catalogue.csv` with columns `sku,brand,mpn,ean,title,category,price_eur,cost_eur,stock`.
   - Loader writes into `products`. Real ERP integration later.

8. **Normalisation helpers**
   - `normalise_brand(s)`: uppercase, strip, map aliases ("KNIPEX" ↔ "Knipex GmbH").
   - `normalise_mpn(s)`: uppercase, strip spaces/dashes/dots.

9. **Deterministic matcher**
   - Match by EAN first, then `(brand, mpn)` normalised.
   - Persist to `product_matches` with `match_type='exact_ean'` or `'exact_mpn'`, confidence 1.0.
   - Target: achieve >50% match rate on round-1 catalogue.

10. **LLM fuzzy matcher**
    - Embed AG `title + brand + mpn` with local multilingual model, store vectors in pgvector.
    - For each unmatched competitor listing: retrieve top-20 nearest AG products, call Claude Haiku with the shortlist and return best match + confidence.
    - Persist as `match_type='llm_fuzzy'`, confidence from LLM (0.0–1.0).

11. **Pricing snapshot**
    - Daily job: for each AG product with ≥1 match, compute min/median/max competitor price, AG rank, cheapest competitor.
    - Write to `pricing_snapshot`.

12. **Recommender**
    - Apply playbook rules from §8.3.
    - For each classified SKU, call Claude Sonnet for a short rationale (1–3 sentences, Slovak or English).
    - Write to `recommendations`.

13. **Streamlit dashboard**
    - Page 1: Recommendations queue — table with filters, approve/reject buttons.
    - Page 2: SKU detail — price history chart, all competitors overlaid.
    - Page 3: Coverage health — match rate per competitor, confidence histogram.

14. **Alerts**
    - For `investigate` events, send a JSON payload to `ALERT_WEBHOOK_URL` (Slack-compatible).

15. **Orchestration**
    - Cron entries documented in README: ingest → scrape → match → recommend → alert, staggered overnight.

---

## 11. Definition of Done (round 1)

- All 7 competitor scrapers run daily without manual intervention.
- >70% overall match rate against the round-1 AG catalogue subset.
- `pricing_snapshot` populated for all matched SKUs.
- At least one recommendation of each playbook type (`raise`, `hold`, `drop`, `investigate`) visible in the dashboard.
- Tests green. Scraper tests use fixtures, no live network calls.
- README documents setup, cron schedule, and how to add a new competitor.

---

## 12. Open questions (resolve before or during round 1)

- AG Náradie e-shop URL and catalogue export format — Michal to provide.
- Whether AG Náradie's catalogue exposes `EAN` and `cost_eur`.
- Which of the seven competitors publish a Heureka XML feed (inspect script will answer this).
- Preferred language for recommendation rationale — Slovak or English.
- Whether reviewer identity should be captured (single-user PoC vs multi-user).

---

## 13. Risks & mitigations

- **Catalogue data quality** — if AG's brand/MPN fields are messy, matching degrades. Mitigation: normalisation layer + human review queue for low-confidence matches.
- **Competitor site changes** — scrapers break. Mitigation: prefer Heureka feeds (stable schema), monitor scraper error rates, alert on >10% drop in listings scraped.
- **LLM cost drift** — fuzzy matching at 60k SKU × 7 competitors is expensive. Mitigation: cache matches indefinitely (matches don't expire), batch calls, use Haiku not Sonnet.
- **Anti-bot on competitor sites** — unlikely for this set but possible. Mitigation: playwright fallback, commercial proxy if needed.
- **EAN vs MPN gap** — shops often have EAN in feeds, AG may not. Mitigation: GS1 or LLM-based one-time enrichment, cached in `products.ean`.

---

## 14. Glossary

- **SKU** — stock keeping unit, an internal product identifier.
- **MPN** — manufacturer part number, the brand's own identifier for a product.
- **EAN** — European article number (barcode), globally unique per product variant.
- **Heureka feed** — XML product feed Slovak/Czech e-shops publish for price comparison aggregators.
- **EDE** — Einkaufsbüro Deutscher Eisenhändler, German hardware wholesaler cooperative AG Náradie sources from.
- **Playbook** — a named pricing action (raise/hold/drop/investigate) triggered by rule thresholds.
