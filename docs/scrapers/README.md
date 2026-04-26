# Competitor Scraping Knowledge Base

One file per competitor, modeled on
[`allegro_scraping_knowledge_base.md`](allegro_scraping_knowledge_base.md).
Each doc captures: site architecture, bot protection, page rendering behaviour,
data extraction, anti-detection timing, parallelism, output schema, known
pitfalls, and transferable patterns.

| Competitor | Country | Source code | Knowledge file |
|---|---|---|---|
| AH Profi | SK | [ahprofi.py](../../src/agnaradie_pricing/scrapers/ahprofi.py) | [ahprofi_sk.md](ahprofi_sk.md) |
| NaradieShop | SK | [naradieshop.py](../../src/agnaradie_pricing/scrapers/naradieshop.py) | [naradieshop_sk.md](naradieshop_sk.md) |
| Doktor Kladivo | SK | [doktorkladivo.py](../../src/agnaradie_pricing/scrapers/doktorkladivo.py) | [doktorkladivo_sk.md](doktorkladivo_sk.md) |
| Rebiop | SK | [rebiop.py](../../src/agnaradie_pricing/scrapers/rebiop.py) | [rebiop_sk.md](rebiop_sk.md) |
| Boukal | CZ | [boukal.py](../../src/agnaradie_pricing/scrapers/boukal.py) | [boukal_cz.md](boukal_cz.md) |
| BO-Import | CZ | [bo_import.py](../../src/agnaradie_pricing/scrapers/bo_import.py) | [bo_import_cz.md](bo_import_cz.md) |
| AGI | SK | [agi.py](../../src/agnaradie_pricing/scrapers/agi.py) | [agi_sk.md](agi_sk.md) |
| Fermatshop | SK | [ferant.py](../../src/agnaradie_pricing/scrapers/ferant.py) | [fermatshop_sk.md](fermatshop_sk.md) |
| Strendpro | SK | [strend.py](../../src/agnaradie_pricing/scrapers/strend.py) | [strendpro_sk.md](strendpro_sk.md) |
| ToolZone | SK | [toolzone.py](../../src/agnaradie_pricing/scrapers/toolzone.py) | [toolzone_sk.md](toolzone_sk.md) |
| Madmat | SK | [shoptet_generic.py](../../src/agnaradie_pricing/scrapers/shoptet_generic.py) (Heureka XML feed) | [madmat_sk.md](madmat_sk.md) |
| Centrum Náradia | SK | [shoptet_generic.py](../../src/agnaradie_pricing/scrapers/shoptet_generic.py) (Heureka XML feed) | [centrumnaradia_sk.md](centrumnaradia_sk.md) |

ToolZone is our own store (`own_store: true` in
[config/competitors.yaml](../../config/competitors.yaml)). Its scraper is
documented because we still pull our own catalogue through it for matching.

## Section template (every file)

1. **Site Architecture** — domains, URL patterns, entry points
2. **Bot Protection** — what guards the site (if any) and how we get past it
3. **Page Rendering Behaviour** — JS vs static, what comes from the wire
4. **Data Extraction** — selectors / regex / JSON-LD fields actually used
5. **Anti-Detection Timing** — sleep/jitter/stagger config used in production
6. **Parallelism** — workers, threads, queues
7. **Output Schema** — what the scraper emits into `competitor_listings`
8. **Known Pitfalls & Fixes** — bugs already hit; the workaround that stuck
9. **Transferable Patterns** — what generalises to other competitors

Sections that don't apply to a given site (e.g. bot protection on a quiet
PrestaShop) are kept but marked "n/a" with a one-line rationale, so the
template is scannable across competitors.
