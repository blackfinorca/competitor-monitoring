# Competitor Brand Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure `ahprofi_sk`, `fermatshop_sk`, and `rebiop_sk` save `brand` consistently into `competitor_listings`, and that re-scrapes backfill missing brand values on existing rows.

**Architecture:** Fix brand extraction at the scraper boundary where the site exposes a trustworthy manufacturer field, and add one shared DB-side fallback for listings that already have a real EAN but still lack brand. Do not add schema changes. Reuse the existing URL upsert path in `save_competitor_listings()` to backfill old rows during reruns.

**Tech Stack:** Python 3.11, `httpx`, `html.parser`, SQLAlchemy ORM/Core upserts, pytest.

---

## File Map

- Modify: `src/agnaradie_pricing/scrapers/ahprofi.py`
  Add explicit brand extraction to `_parse_product_page()` from the product HTML already used for title/EAN/price parsing.

- Modify: `src/agnaradie_pricing/scrapers/rebiop.py`
  Add safe brand extraction only from explicit product-page fields when present. Keep the parser conservative; do not infer brand from generic site navigation or `<meta keywords>`.

- Modify: `src/agnaradie_pricing/scrapers/persistence.py`
  Add a shared pre-upsert enrichment step that fills missing `brand` from existing DB knowledge when a listing has a real numeric EAN.

- Modify: `tests/scrapers/test_ahprofi.py`
  Replace stale parser coverage with tests for the current `_parse_product_page()` implementation, including brand extraction.

- Modify: `tests/scrapers/test_rebiop.py`
  Add detail-page parser coverage for explicit brand extraction when the field exists, and coverage for the shared EAN-based brand backfill path at the scraper-save boundary.

- Modify: `tests/scrapers/test_persistence.py`
  Add focused tests for “missing brand + known EAN” enrichment from `products` / existing competitor listings before insert or conflict-update.

- Modify: `tests/scrapers/test_ferant.py`
  Add/keep regression coverage proving Fermatshop already extracts brand from the product page.

- Optional docs-only follow-up: `README.md`
  Note that rerunning `daily_scrape.py` now backfills missing brand identifiers for these competitors. Only touch if user wants operational docs updated.

---

### Task 1: Lock the behavior with failing tests first

**Files:**
- Modify: `tests/scrapers/test_ahprofi.py`
- Modify: `tests/scrapers/test_rebiop.py`
- Modify: `tests/scrapers/test_persistence.py`
- Modify: `tests/scrapers/test_ferant.py`

- [ ] **Step 1: Replace stale AHPROFI parser coverage**

Write tests against the current parser entry point in `src/agnaradie_pricing/scrapers/ahprofi.py`:
- assert `_parse_product_page()` returns `brand == "Knipex"` for the existing HTML fixture
- keep title/EAN/price assertions on the same fixture
- remove imports/tests that still target the old `_parse_first_product()` path

- [ ] **Step 2: Add Rebiop brand parser tests**

Add a detail-page fixture string that includes an explicit brand field, for example:

```html
<dl><dt>Značka:</dt><dd>BAUPRO</dd></dl>
```

Assert that `_parse_detail_page()` returns `brand == "BAUPRO"` when this field exists.

- [ ] **Step 3: Add DB-side brand enrichment tests**

Add persistence tests covering both cases:
- listing has `ean="4003773012345"` and `brand=None`, while `products` already has the same EAN with `brand="KNIPEX"` → saved competitor row gets `brand="KNIPEX"`
- listing has `brand=None`, same EAN is already present on another competitor listing with `brand="BAUPRO"` → saved row gets `brand="BAUPRO"`

- [ ] **Step 4: Keep Fermatshop protected**

Keep a simple regression test proving `_parse_product_detail()` still returns the manufacturer from `.manu_name`.

- [ ] **Step 5: Run the focused test subset and confirm failures reflect the missing behavior**

Run:

```bash
./.venv/bin/pytest tests/scrapers/test_ahprofi.py tests/scrapers/test_rebiop.py tests/scrapers/test_persistence.py tests/scrapers/test_ferant.py -q
```

Expected:
- AHPROFI fails because `brand` is still `None`
- Rebiop/persistence tests fail until implementation lands

- [ ] **Step 6: Commit the test-only red state**

```bash
git add tests/scrapers/test_ahprofi.py tests/scrapers/test_rebiop.py tests/scrapers/test_persistence.py tests/scrapers/test_ferant.py
git commit -m "test: cover competitor brand backfill paths"
```

---

### Task 2: Fix AHPROFI brand extraction at the parser

**Files:**
- Modify: `src/agnaradie_pricing/scrapers/ahprofi.py`
- Test: `tests/scrapers/test_ahprofi.py`

- [ ] **Step 1: Add a brand extractor for AHPROFI product HTML**

Extend `_parse_product_page()` to parse brand from the product page’s explicit manufacturer field, using the existing fixture structure:

```html
<span class="col col-5 label">Výrobca&nbsp;</span>
<span class="col col-7 right"><a href="https://www.ahprofi.sk/knipex">Knipex</a></span>
```

Prefer one of:
- a targeted regex tied to the `Výrobca` label, or
- a tiny HTML parser helper scoped to that label/value pair

Do not use a broad `brand` substring match; keep the parser anchored to the product-detail block.

- [ ] **Step 2: Populate `brand` in the returned `CompetitorListing`**

Update the `CompetitorListing(...)` construction in `_parse_product_page()` so `brand` uses the extracted value instead of `None`.

- [ ] **Step 3: Run the AHPROFI tests**

Run:

```bash
./.venv/bin/pytest tests/scrapers/test_ahprofi.py -q
```

Expected: PASS.

- [ ] **Step 4: Commit the parser fix**

```bash
git add src/agnaradie_pricing/scrapers/ahprofi.py tests/scrapers/test_ahprofi.py
git commit -m "feat: scrape brand from ahprofi product pages"
```

---

### Task 3: Add safe brand handling for Rebiop

**Files:**
- Modify: `src/agnaradie_pricing/scrapers/rebiop.py`
- Test: `tests/scrapers/test_rebiop.py`

- [ ] **Step 1: Add explicit-field brand parsing in `_parse_detail_page()`**

Teach `_DetailParser` / `_parse_detail_page()` to read `brand` only from trustworthy detail fields such as:
- `Značka`
- `Značka:`
- `Výrobca`
- `Výrobca:`
- `Vyrobca`
- `Vyrobca:`

Do not infer brand from:
- `<meta keywords>`
- top navigation categories
- generic site copy

- [ ] **Step 2: Thread the parsed brand into `CompetitorListing`**

Update `_parse_detail_page()` to return:

```python
brand=parsed_brand_or_none
```

and leave the search-result card parser conservative (`brand=None`) unless the search HTML exposes a clear product-level manufacturer field.

- [ ] **Step 3: Keep the search fallback path detail-first**

Do not change the current `search_by_query()` order:
- parse direct detail hit first
- parse first search card second
- re-fetch the detail URL and prefer that enriched result

This path is already correct for backfilling old rows once `_parse_detail_page()` can emit brand.

- [ ] **Step 4: Run the Rebiop tests**

Run:

```bash
./.venv/bin/pytest tests/scrapers/test_rebiop.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit the Rebiop parser fix**

```bash
git add src/agnaradie_pricing/scrapers/rebiop.py tests/scrapers/test_rebiop.py
git commit -m "feat: scrape brand from rebiop detail pages"
```

---

### Task 4: Add one shared EAN-to-brand backfill path in persistence

**Files:**
- Modify: `src/agnaradie_pricing/scrapers/persistence.py`
- Test: `tests/scrapers/test_persistence.py`
- Reference: `src/agnaradie_pricing/db/models.py`

- [ ] **Step 1: Add a small pre-save enrichment helper**

Inside `save_competitor_listings()` (or a small private helper in the same file), enrich each incoming listing before building upsert rows:

Priority:
1. keep `listing.brand` if already present
2. if `listing.ean` is a real numeric EAN and brand is missing, look up `Product.brand` by `products.ean`
3. if still missing, look up any existing `competitor_listings.brand` by the same EAN
4. if still missing, leave it `None`

Do not use this path for placeholder EANs like `NOEAN-...`.

- [ ] **Step 2: Keep conflict-update semantics unchanged**

Do not weaken the current URL conflict behavior in `save_competitor_listings()`:
- price/title/stock refresh every scrape
- `brand`, `ean`, `mpn`, `competitor_sku` backfill only when the new scrape provides data

The new enrichment must happen before `_to_dict()` so the inserted/upserted row already carries the filled brand.

- [ ] **Step 3: Run persistence tests**

Run:

```bash
./.venv/bin/pytest tests/scrapers/test_persistence.py -q
```

Expected: PASS.

- [ ] **Step 4: Commit the shared backfill helper**

```bash
git add src/agnaradie_pricing/scrapers/persistence.py tests/scrapers/test_persistence.py
git commit -m "feat: backfill missing competitor brand by ean"
```

---

### Task 5: Verify Fermatshop and plan the data backfill

**Files:**
- Reference: `src/agnaradie_pricing/scrapers/ferant.py`
- Test: `tests/scrapers/test_ferant.py`

- [ ] **Step 1: Confirm no new parser work is needed**

Run:

```bash
./.venv/bin/pytest tests/scrapers/test_ferant.py -q
```

Expected: PASS with the existing manufacturer extraction from `.manu_name`.

- [ ] **Step 2: Treat Fermatshop as a rerun/backfill problem**

Because `fermatshop_sk` already emits `brand=parsed.brand`, the likely cause of low DB coverage is historical rows created before the current parser or before the current full-catalogue crawler was used. No new EAN-based backfill applies here because the site uses placeholder `NOEAN-*` values.

- [ ] **Step 3: Document the operational rerun commands**

Use:

```bash
./.venv/bin/python jobs/daily_scrape.py --only fermatshop_sk
./.venv/bin/python jobs/daily_scrape.py --only ahprofi_sk
./.venv/bin/python jobs/daily_scrape.py --only rebiop_sk
```

These reruns should backfill existing rows via the `(competitor_id, url)` upsert path.

---

### Task 6: End-to-end verification and DB validation

**Files:**
- Reference: `jobs/daily_scrape.py`
- Reference: `src/agnaradie_pricing/db/models.py`

- [ ] **Step 1: Run the full focused scraper test suite**

Run:

```bash
./.venv/bin/pytest tests/scrapers/test_ahprofi.py tests/scrapers/test_rebiop.py tests/scrapers/test_ferant.py tests/scrapers/test_persistence.py tests/jobs/test_daily_scrape.py -q
```

Expected: PASS.

- [ ] **Step 2: Run compile checks on touched modules**

Run:

```bash
./.venv/bin/python -m py_compile \
  src/agnaradie_pricing/scrapers/ahprofi.py \
  src/agnaradie_pricing/scrapers/rebiop.py \
  src/agnaradie_pricing/scrapers/persistence.py
```

Expected: no output.

- [ ] **Step 3: Rerun the three competitors**

Run:

```bash
./.venv/bin/python jobs/daily_scrape.py --only ahprofi_sk rebiop_sk fermatshop_sk
```

or separate them if you want clearer logs.

- [ ] **Step 4: Validate brand coverage in DB**

Run:

```bash
./.venv/bin/python - <<'PY'
from sqlalchemy import create_engine, text
from agnaradie_pricing.settings import Settings

settings = Settings()
engine = create_engine(settings.database_url)
sql = text("""
SELECT competitor_id,
       COUNT(*) AS total,
       SUM(CASE WHEN ean IS NOT NULL AND ean NOT LIKE 'NOEAN-%' THEN 1 ELSE 0 END) AS with_real_ean,
       SUM(CASE WHEN brand IS NOT NULL AND TRIM(brand) <> '' THEN 1 ELSE 0 END) AS with_brand,
       SUM(CASE WHEN ean IS NOT NULL AND ean NOT LIKE 'NOEAN-%'
                    AND brand IS NOT NULL AND TRIM(brand) <> '' THEN 1 ELSE 0 END) AS with_real_ean_and_brand
FROM competitor_listings
WHERE competitor_id IN ('ahprofi_sk', 'rebiop_sk', 'fermatshop_sk')
GROUP BY competitor_id
ORDER BY competitor_id
""")
with engine.connect() as conn:
    for row in conn.execute(sql):
        print(dict(row._mapping))
PY
```

Expected outcome:
- `ahprofi_sk`: near-complete brand coverage on real-EAN rows
- `rebiop_sk`: materially improved brand coverage on real-EAN rows, with remaining misses only where the site exposes no trustworthy brand and no DB EAN match exists
- `fermatshop_sk`: materially improved overall brand coverage after rerun, still no real EANs because the site does not publish them

- [ ] **Step 5: Final commit**

```bash
git add src/agnaradie_pricing/scrapers/ahprofi.py \
        src/agnaradie_pricing/scrapers/rebiop.py \
        src/agnaradie_pricing/scrapers/persistence.py \
        tests/scrapers/test_ahprofi.py \
        tests/scrapers/test_rebiop.py \
        tests/scrapers/test_persistence.py \
        tests/scrapers/test_ferant.py
git commit -m "Improve competitor brand extraction and backfill"
```

---

## Notes / Constraints

- `fermatshop_sk` does not expose real EANs on product pages today. Do not try to force EAN-based brand logic onto its `NOEAN-*` placeholders.
- `rebiop_sk` sample product pages do not reliably expose explicit manufacturer fields. That is why the plan uses a DB-side EAN brand fallback rather than weak HTML heuristics.
- `tests/scrapers/test_ahprofi.py` is currently stale relative to `src/agnaradie_pricing/scrapers/ahprofi.py`; refreshing that test file is part of the work, not optional cleanup.
- No migration is needed. This is parser + persistence behavior only.
