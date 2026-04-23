"""Export competitor price comparison to CSV.

Produces one row per AG catalogue product.  ToolZone is the reference
competitor (our own shop); every other competitor gets a Price + URL
column pair, ordered alphabetically by name.

The output is UTF-8 with BOM so Excel opens it directly without the
import wizard (no manual encoding selection needed).

Usage
-----
    python jobs/export_prices.py
    python jobs/export_prices.py --output reports/prices_2026-04.csv
    python jobs/export_prices.py --only toolzone_sk boukal_cz madmat_sk
"""

import argparse
import csv
import sys
from datetime import datetime, UTC
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import text

from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.settings import Settings, load_competitors

# Fixed columns emitted before competitor price columns
_FIXED_FIELDS = [
    ("sku",       "sku"),
    ("brand",     "brand"),
    ("mpn",       "mpn"),
    ("ean",       "ean"),
    ("category",  "category"),
    ("title",     "title"),
    ("ag_price",  "price_eur"),
    ("cost",      "cost_eur"),
    ("stock",     "stock"),
]

_TOOLZONE_ID = "toolzone_sk"


# ---------------------------------------------------------------------------
# Data query
# ---------------------------------------------------------------------------

def _load_data(session, competitor_ids: list[str]) -> dict:
    """Return products list and {ag_product_id: {competitor_id: {price, url}}}."""
    products_raw = session.execute(text("""
        SELECT id, sku, brand, mpn, ean, category, title, price_eur, cost_eur, stock
        FROM products
        ORDER BY brand, mpn
    """)).fetchall()
    products = [dict(r._mapping) for r in products_raw]

    placeholders = ",".join(f"'{c}'" for c in competitor_ids)
    rows = session.execute(text(f"""
        SELECT
            pm.ag_product_id,
            pm.competitor_id,
            cl.price_eur,
            cl.url,
            cl.scraped_at
        FROM product_matches pm
        JOIN competitor_listings cl
            ON cl.competitor_id = pm.competitor_id
           AND cl.competitor_sku = pm.competitor_sku
        WHERE pm.competitor_id IN ({placeholders})
        ORDER BY pm.ag_product_id, pm.competitor_id, cl.scraped_at DESC
    """)).fetchall()

    prices: dict[int, dict[str, dict]] = {}
    seen: set[tuple] = set()
    for r in rows:
        key = (r.ag_product_id, r.competitor_id)
        if key in seen:
            continue  # keep only the latest row per (product, competitor) pair
        seen.add(key)
        prices.setdefault(r.ag_product_id, {})[r.competitor_id] = {
            "price": float(r.price_eur) if r.price_eur is not None else None,
            "url": r.url or "",
        }

    return {"products": products, "prices": prices}


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(
    products: list[dict],
    prices: dict[int, dict[str, dict]],
    competitors: list[dict],
    output_path: Path,
) -> None:
    # ToolZone first, then all others alphabetically by name
    comp_order = [c for c in competitors if c["id"] == _TOOLZONE_ID]
    comp_order += sorted(
        [c for c in competitors if c["id"] != _TOOLZONE_ID],
        key=lambda c: c["name"],
    )

    # Build header
    header = [col for col, _ in _FIXED_FIELDS]
    for comp in comp_order:
        slug = comp["name"].lower().replace(" ", "_")
        header.append(f"{slug}_price")
        header.append(f"{slug}_url")

    # utf-8-sig = UTF-8 with BOM → Excel auto-detects encoding
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for product in products:
            pid = product["id"]
            comp_prices = prices.get(pid, {})

            row = []
            for _, field in _FIXED_FIELDS:
                val = product.get(field)
                if val is None:
                    row.append("")
                elif isinstance(val, float):
                    row.append(round(val, 2))
                else:
                    row.append(str(val).strip())

            for comp in comp_order:
                entry = comp_prices.get(comp["id"])
                if entry and entry["price"] is not None:
                    row.append(round(entry["price"], 2))
                    row.append(entry["url"])
                else:
                    row.append("")
                    row.append("")

            writer.writerow(row)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None) -> Path:
    parser = argparse.ArgumentParser(description="Export price comparison to CSV.")
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Output .csv path (default: reports/prices_YYYY-MM-DD.csv)",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        metavar="COMPETITOR_ID",
        help="Include only these competitor IDs (default: all with matches)",
    )
    args = parser.parse_args(argv)

    output: Path = args.output or Path("reports") / f"prices_{datetime.now(UTC).strftime('%Y-%m-%d')}.csv"
    output.parent.mkdir(parents=True, exist_ok=True)

    settings = Settings()
    factory = make_session_factory(settings)
    all_competitors = load_competitors()
    comp_by_id = {c["id"]: c for c in all_competitors}

    with factory() as session:
        active_ids = {
            r[0] for r in session.execute(
                text("SELECT DISTINCT competitor_id FROM product_matches")
            ).fetchall()
        }

        selected_ids = [cid for cid in (args.only or sorted(active_ids)) if cid in active_ids]
        competitors_to_export = [comp_by_id[cid] for cid in selected_ids if cid in comp_by_id]

        if not competitors_to_export:
            print("No competitor data found. Run daily_scrape.py and match_products.py first.")
            return output

        print(f"Exporting {len(competitors_to_export)} competitors: {[c['id'] for c in competitors_to_export]}")
        data = _load_data(session, selected_ids)

    products_with_data = [p for p in data["products"] if data["prices"].get(p["id"])]
    print(f"Products with competitor data: {len(products_with_data)}")

    write_csv(products_with_data, data["prices"], competitors_to_export, output)
    print(f"Saved: {output.resolve()}")
    return output


if __name__ == "__main__":
    main()
