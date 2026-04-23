"""Streamlit dashboard for AG Naradie / ToolZone Pricing.

Pages
-----
  1. Product Search     — Search bar → live fetch / cache → ToolZone card + competitors
  2. Recommendations    — Product vs competitor price overview (tile view)
  3. Coverage Health    — Scrape freshness and match rates

Run with:
    python -m streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# Load .env before any Settings/LLM client is constructed so that env vars
# are available to os.environ.get() throughout the dashboard.
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=False)
except ModuleNotFoundError:
    pass

from datetime import date, timedelta
from decimal import Decimal

import pandas as pd
import streamlit as st
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from agnaradie_pricing.db.models import (
    CompetitorListing,
    PricingSnapshot,
    Product,
    ProductMatch,
    Recommendation,
)
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.orchestrator import SearchResult, search_product
from agnaradie_pricing.scrapers.ahprofi import AhProfiScraper
from agnaradie_pricing.scrapers.boukal import BoukalScraper
from agnaradie_pricing.scrapers.doktorkladivo import DoktorKladivoScraper
from agnaradie_pricing.scrapers.ferant import FermatshopScraper
from agnaradie_pricing.scrapers.naradieshop import NaradieShopScraper
from agnaradie_pricing.scrapers.rebiop import RebiopScraper
from agnaradie_pricing.scrapers.strend import StrendproScraper
from agnaradie_pricing.scrapers.toolzone import ToolZoneScraper
from agnaradie_pricing.settings import Settings, load_competitors, own_store_ids

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="ToolZone Pricing",
    page_icon=":wrench:",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Cached singletons
# ---------------------------------------------------------------------------

def _get_settings() -> Settings:
    # Not cached — Settings is lightweight and caching it causes stale-attribute
    # errors after hot-reloads because st.cache_resource persists across re-runs.
    return Settings()

@st.cache_resource
def _get_factory():
    return make_session_factory(_get_settings())

def _session() -> Session:
    return _get_factory()()

@st.cache_resource
def _competitor_names() -> dict[str, str]:
    try:
        names = {c["id"]: c["name"] for c in load_competitors()}
        # Keep old DB rows readable after competitor-id rename.
        if "fermatshop_sk" in names:
            names.setdefault("ferant_sk", names["fermatshop_sk"])
        if "strendpro_sk" in names:
            names.setdefault("strend_sk", names["strendpro_sk"])
        return names
    except Exception:
        return {}

def _display_name(cid: str) -> str:
    return _competitor_names().get(cid, cid)

@st.cache_resource
def _get_toolzone_scraper():
    cfg = next(
        (c for c in load_competitors() if c["id"] == "toolzone_sk"),
        {"id": "toolzone_sk", "url": "https://www.toolzone.sk", "weight": 1.0, "rate_limit_rps": 1},
    )
    return ToolZoneScraper(cfg)

@st.cache_resource
def _get_competitor_scrapers() -> dict:
    configs = {c["id"]: c for c in load_competitors() if not c.get("own_store")}
    registry = {
        "ahprofi_sk": AhProfiScraper,
        "naradieshop_sk": NaradieShopScraper,
        "doktorkladivo_sk": DoktorKladivoScraper,
        "rebiop_sk": RebiopScraper,
        "boukal_cz": BoukalScraper,
        "fermatshop_sk": FermatshopScraper,
        "strendpro_sk": StrendproScraper,
    }
    scrapers = {}
    for cid, cls in registry.items():
        if cid in configs:
            scrapers[cid] = cls(configs[cid])
    return scrapers

def _get_llm_client():
    # Not cached — re-read env on every call so .env changes take effect
    # without restarting the server. OpenAIClient is lightweight to construct.
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        try:
            s = _get_settings()
            api_key = getattr(s, "openai_api_key", None)
        except Exception:
            pass
    if not api_key:
        return None
    model = os.environ.get("OPENAI_MODEL", "o4-mini")
    try:
        s = _get_settings()
        model = getattr(s, "openai_model", model) or model
    except Exception:
        pass
    from agnaradie_pricing.matching.llm_matcher import OpenAIClient
    return OpenAIClient(api_key=api_key, model=model)

# ---------------------------------------------------------------------------
# Navigation — top tab bar
# ---------------------------------------------------------------------------

try:
    _llm_client_check = _get_llm_client()
    _llm_status = str(_llm_client_check) if _llm_client_check else "disabled (no OPENAI_API_KEY)"
except Exception:
    _llm_status = "disabled (no OPENAI_API_KEY)"
st.caption(f"ToolZone Pricing  ·  Today: {date.today().isoformat()}  ·  LLM: {_llm_status}")

tab1, tab2, tab3, tab4 = st.tabs([
    "🔍 Product Search", "💰 Price compare", "🏭 By Manufacturer", "🩺 Coverage Health",
])


# ===========================================================================
# Page 1 — Product Search
# ===========================================================================

def _render_search_tab() -> None:
    st.header("Product Search")
    st.caption(
        "Enter an EAN, MPN, or product name. "
        "Cached results (< 24 h) load instantly; everything else is fetched live."
    )

    # --- Search form ---------------------------------------------------------
    with st.form("search_form", clear_on_submit=False):
        col_input, col_btn, col_refresh = st.columns([5, 1, 1])
        with col_input:
            query = st.text_input(
                "Query",
                placeholder="e.g. 87-01-250  or  knipex cobra 250  or  4003773011965",
                label_visibility="collapsed",
                value=st.session_state.get("last_query", ""),
            )
        with col_btn:
            submitted = st.form_submit_button("Search", type="primary", use_container_width=True)
        with col_refresh:
            refresh = st.form_submit_button(
                "Refresh",
                use_container_width=True,
                help="Re-fetch live data even if the cache is fresh",
            )

    # --- Run search ----------------------------------------------------------
    run_query = query.strip()

    should_search = (submitted or refresh) and run_query
    force_refresh = bool(refresh)

    if should_search:
        st.session_state["last_query"] = run_query

        progress_messages: list[str] = []

        with st.status("Searching…", expanded=True) as status_box:
            def _on_progress(msg: str) -> None:
                st.write(msg)
                progress_messages.append(msg)

            try:
                with _session() as session:
                    result: SearchResult = search_product(
                        run_query,
                        session,
                        competitor_scrapers=_get_competitor_scrapers(),
                        toolzone_scraper=_get_toolzone_scraper(),
                        llm_client=_get_llm_client(),
                        force_refresh=force_refresh,
                        on_progress=_on_progress,
                    )
                st.session_state["search_result"] = result
                label = "From cache" if result.from_cache else "Done"
                status_box.update(label=label, state="complete", expanded=False)
            except Exception as exc:
                status_box.update(label=f"Error: {exc}", state="error")
                st.exception(exc)
                return

    # --- Display results -----------------------------------------------------
    if "search_result" not in st.session_state:
        st.info("Enter a search term above to find a product.")
        return

    result: SearchResult = st.session_state["search_result"]

    if result.product is None:
        st.warning(f"No product found for **{result.query}**. Try a different EAN, MPN, or name.")
        return

    # Show errors (non-fatal)
    if result.errors:
        with st.expander(f"⚠ {len(result.errors)} search warning(s)", expanded=False):
            for cid, msg in result.errors.items():
                st.caption(f"**{_display_name(cid)}**: {msg}")

    # Cache badge
    if result.from_cache:
        st.caption("Showing cached results  —  press **Refresh** to re-fetch live data.")

    product = result.product
    tz_row = result.tz_listing

    # Build match lookup: competitor_id → (match_type, confidence)
    match_lookup: dict[str, tuple[str, float]] = {}
    for pm in result.matches:
        match_lookup[pm.competitor_id] = (pm.match_type, float(pm.confidence))

    # ToolZone price
    tz_price: float | None = None
    if tz_row and tz_row.price_eur:
        tz_price = float(tz_row.price_eur)
    elif product.price_eur:
        tz_price = float(product.price_eur)

    # -----------------------------------------------------------------------
    # Layout: left card | right competitor panel
    # -----------------------------------------------------------------------
    left, right = st.columns([1, 2], gap="large")

    # =====================================================================
    # LEFT — ToolZone product card
    # =====================================================================
    with left:
        st.subheader("ToolZone (reference)")

        if tz_row and tz_row.url:
            st.markdown(f"#### [{product.title}]({tz_row.url})")
        else:
            st.markdown(f"#### {product.title}")

        st.divider()

        price_col, stock_col = st.columns(2)
        with price_col:
            st.metric("Price", f"€ {tz_price:.2f}" if tz_price else "—")
        with stock_col:
            if tz_row and tz_row.in_stock is not None:
                st.metric("Stock", "✅ In stock" if tz_row.in_stock else "❌ Out of stock")
            else:
                st.metric("Stock", "—")

        st.divider()

        def _field(label: str, value) -> None:
            if value:
                st.markdown(f"**{label}** `{value}`")

        _field("Brand",    product.brand)
        _field("MPN",      product.mpn or (tz_row.mpn if tz_row else None))
        _field("EAN",      product.ean or (tz_row.ean if tz_row else None))
        _field("SKU",      product.sku)
        _field("Category", product.category)

        if tz_row and tz_row.scraped_at:
            ts = pd.Timestamp(tz_row.scraped_at).tz_localize(None)
            delta = pd.Timestamp.utcnow().tz_localize(None) - ts
            hours = int(delta.total_seconds() / 3600)
            if hours < 26:
                freshness = f"🟢 {hours}h ago"
            elif hours < 50:
                freshness = f"🟡 {hours}h ago"
            else:
                freshness = f"🔴 {hours // 24}d ago"
            st.caption(f"Scraped {freshness}")

        # Market position from latest snapshot
        with _session() as session:
            snapshot = session.execute(
                select(PricingSnapshot)
                .where(PricingSnapshot.ag_product_id == product.id)
                .order_by(PricingSnapshot.snapshot_date.desc())
                .limit(1)
            ).scalar_one_or_none()

        if snapshot:
            st.divider()
            st.caption("**Market position (latest snapshot)**")
            rank_col, cnt_col = st.columns(2)
            rank_col.metric(
                "Rank",
                f"#{snapshot.ag_rank} / {(snapshot.competitor_count or 0) + 1}"
                if snapshot.ag_rank else "—",
            )
            cnt_col.metric("Competitors", snapshot.competitor_count or 0)
            if snapshot.median_price and tz_price:
                diff_vs_median = (tz_price - float(snapshot.median_price)) / float(snapshot.median_price) * 100
                st.metric(
                    "vs Market Median",
                    f"€ {float(snapshot.median_price):.2f}",
                    delta=f"{diff_vs_median:+.1f}%",
                    delta_color="inverse" if diff_vs_median > 0 else "normal",
                )

    # =====================================================================
    # RIGHT — Competitor panel
    # =====================================================================
    with right:
        comp_listings = result.competitor_hits

        if not comp_listings:
            st.subheader("Competitor Prices")
            st.info(
                "No competitor matches found. "
                "Try pressing **Refresh** to run a fresh live search."
            )
        else:
            n = len(comp_listings)
            st.subheader(f"Competitor Prices — {n} match{'es' if n != 1 else ''}")

            display_rows = []
            for cl in sorted(comp_listings, key=lambda r: float(r.price_eur)):
                price = float(cl.price_eur)
                diff_pct = (price - tz_price) / tz_price * 100 if tz_price else None

                if diff_pct is None:
                    diff_str = "—"
                elif diff_pct > 0.5:
                    diff_str = f"▲ +{diff_pct:.1f}%"
                elif diff_pct < -0.5:
                    diff_str = f"▼ {diff_pct:.1f}%"
                else:
                    diff_str = f"≈ {diff_pct:+.1f}%"

                mt, conf = match_lookup.get(cl.competitor_id, ("—", 0.0))
                badge_map = {
                    "exact_ean":          "EAN ✓",
                    "exact_mpn":          "MPN ✓",
                    "mpn_no_brand":       "MPN ~",
                    "regex_ean_title":    "EAN ~",
                    "regex_mpn_title":    "MPN ~",
                    "regex_mpn_no_brand": "MPN ~",
                    "llm_fuzzy":          "LLM ~",
                }
                match_label = badge_map.get(mt, mt)

                ts = pd.Timestamp(cl.scraped_at).tz_localize(None)
                h = int((pd.Timestamp.utcnow().tz_localize(None) - ts).total_seconds() / 3600)
                freshness = f"{h}h ago" if h < 48 else f"{h // 24}d ago"

                display_rows.append({
                    "Store":       _display_name(cl.competitor_id),
                    "Price":       f"€ {price:.2f}",
                    "vs ToolZone": diff_str,
                    "Match":       match_label,
                    "Confidence":  f"{conf:.0%}" if conf else "—",
                    "Scraped":     freshness,
                    "URL":         cl.url or "",
                })

            disp_df = pd.DataFrame(display_rows)
            st.dataframe(
                disp_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "URL": st.column_config.LinkColumn("Link", display_text="Open ↗"),
                },
            )

        # Price history chart
        with _session() as session:
            history_rows = session.execute(
                select(
                    PricingSnapshot.snapshot_date,
                    PricingSnapshot.ag_price,
                    PricingSnapshot.min_price,
                    PricingSnapshot.median_price,
                    PricingSnapshot.max_price,
                )
                .where(
                    PricingSnapshot.ag_product_id == product.id,
                    PricingSnapshot.snapshot_date >= date.today() - timedelta(days=30),
                )
                .order_by(PricingSnapshot.snapshot_date)
            ).fetchall()

        if history_rows:
            st.divider()
            st.subheader("30-day price trend")
            hist_df = pd.DataFrame(
                history_rows,
                columns=["date", "ag_price", "min_price", "median_price", "max_price"],
            )
            hist_df["date"] = pd.to_datetime(hist_df["date"])
            for col in ["ag_price", "min_price", "median_price", "max_price"]:
                hist_df[col] = hist_df[col].astype(float)
            chart_df = hist_df.set_index("date")[["ag_price", "min_price", "median_price", "max_price"]]
            chart_df.columns = ["ToolZone", "Market Min", "Market Median", "Market Max"]
            st.line_chart(chart_df)


# ===========================================================================
# Page 2 — Price compare (tile view)
# ===========================================================================

PLAYBOOK_BADGE = {
    "raise":       "🟢 RAISE",
    "drop":        "🔴 DROP",
    "hold":        "🔵 HOLD",
    "investigate": "🟠 INVESTIGATE",
}


@st.cache_data(ttl=300, show_spinner="Loading price data…")
def _load_price_compare_data() -> dict:
    """Fetch all Price Compare data in one session. Cached for 5 minutes.

    Returns plain dicts/lists (no ORM objects) so Streamlit can serialise them.
    """
    def _f(v) -> float | None:
        return float(v) if v is not None else None

    with _session() as session:
        # 1. Matched product IDs (high-confidence matches only)
        matched_ids: list[int] = list(session.scalars(
            select(ProductMatch.ag_product_id)
            .where(
                ProductMatch.ag_product_id.isnot(None),
                ProductMatch.confidence >= Decimal("0.85"),
            )
            .distinct()
        ).all())

        if not matched_ids:
            return {"matched_ids": [], "products": {}, "snapshots": {}, "recommendations": {}, "comp_map": {}}

        # 2. Products
        products = {
            p.id: {
                "sku": p.sku,
                "title": p.title,
                "brand": p.brand,
                "price_eur": _f(p.price_eur),
            }
            for p in session.scalars(
                select(Product).where(Product.id.in_(matched_ids))
            ).all()
        }

        # 3. Latest pricing snapshot per product
        latest_snap_sub = (
            select(
                PricingSnapshot.ag_product_id,
                func.max(PricingSnapshot.snapshot_date).label("max_date"),
            )
            .where(PricingSnapshot.ag_product_id.in_(matched_ids))
            .group_by(PricingSnapshot.ag_product_id)
            .subquery()
        )
        snapshots = {
            row.ag_product_id: {
                "min_price":        _f(row.min_price),
                "max_price":        _f(row.max_price),
                "median_price":     _f(row.median_price),
                "ag_rank":          row.ag_rank,
                "competitor_count": row.competitor_count,
            }
            for row in session.execute(
                select(
                    PricingSnapshot.ag_product_id,
                    PricingSnapshot.min_price,
                    PricingSnapshot.max_price,
                    PricingSnapshot.median_price,
                    PricingSnapshot.ag_rank,
                    PricingSnapshot.competitor_count,
                ).join(
                    latest_snap_sub,
                    (PricingSnapshot.ag_product_id == latest_snap_sub.c.ag_product_id)
                    & (PricingSnapshot.snapshot_date == latest_snap_sub.c.max_date),
                )
            ).fetchall()
        }

        # 4. Latest recommendation per product (any status, most recent date)
        latest_rec_sub = (
            select(
                Recommendation.ag_product_id,
                func.max(Recommendation.snapshot_date).label("max_date"),
            )
            .where(Recommendation.ag_product_id.in_(matched_ids))
            .group_by(Recommendation.ag_product_id)
            .subquery()
        )
        recommendations = {
            row.ag_product_id: {
                "playbook":        row.playbook,
                "suggested_price": _f(row.suggested_price),
                "rationale":       row.rationale,
            }
            for row in session.execute(
                select(
                    Recommendation.ag_product_id,
                    Recommendation.playbook,
                    Recommendation.suggested_price,
                    Recommendation.rationale,
                ).join(
                    latest_rec_sub,
                    (Recommendation.ag_product_id == latest_rec_sub.c.ag_product_id)
                    & (Recommendation.snapshot_date == latest_rec_sub.c.max_date),
                )
            ).fetchall()
        }

        # 5. Latest competitor listing per matched pair.
        #    Correlated subquery on (competitor_id, competitor_sku, scraped_at) —
        #    the new index idx_cl_cid_csku_scraped makes each MAX lookup O(log n)
        #    instead of a full table scan.
        ids_csv = ",".join(str(i) for i in matched_ids)  # ints from DB — safe
        comp_rows = session.execute(
            text(f"""
                SELECT pm.ag_product_id,
                       pm.competitor_id,
                       cl.price_eur,
                       cl.url,
                       cl.in_stock,
                       cl.scraped_at
                FROM   product_matches pm
                JOIN   competitor_listings cl
                       ON  cl.competitor_id  = pm.competitor_id
                       AND cl.competitor_sku = pm.competitor_sku
                WHERE  pm.ag_product_id IN ({ids_csv})
                  AND  pm.confidence    >= 0.85
                  AND  pm.competitor_sku IS NOT NULL
                  AND  cl.scraped_at = (
                           SELECT MAX(cl2.scraped_at)
                           FROM   competitor_listings cl2
                           WHERE  cl2.competitor_id  = pm.competitor_id
                             AND  cl2.competitor_sku = pm.competitor_sku
                       )
            """)
        ).fetchall()

    comp_map: dict[int, list] = {}
    for row in comp_rows:
        comp_map.setdefault(row.ag_product_id, []).append({
            "competitor_id": row.competitor_id,
            "price_eur":     float(row.price_eur),
            "url":           row.url or "",
            "in_stock":      row.in_stock,
            "scraped_at":    row.scraped_at,
        })

    return {
        "matched_ids":     matched_ids,
        "products":        products,
        "snapshots":       snapshots,
        "recommendations": recommendations,
        "comp_map":        comp_map,
    }


def _render_price_compare_tab() -> None:
    st.header("Price Compare")

    data = _load_price_compare_data()
    matched_ids    = data["matched_ids"]
    products       = data["products"]
    snapshots      = data["snapshots"]
    recommendations = data["recommendations"]
    comp_map       = data["comp_map"]

    if not matched_ids:
        st.info("No matched products yet. Run a product search to start matching products to competitors.")
        return

    # KPI row
    total_matches = sum(len(v) for v in comp_map.values())
    k1, k2, _, per_page_col, refresh_col = st.columns([1, 1, 2, 1, 1])
    k1.metric("Matched products", len(matched_ids))
    k2.metric("Total competitor prices", total_matches)
    per_page = per_page_col.selectbox("Per page", [10, 20, 50], index=0, label_visibility="visible")
    if refresh_col.button("↺ Refresh", use_container_width=True, help="Reload data from the database"):
        _load_price_compare_data.clear()
        st.rerun()
    st.divider()

    # Sort tiles: most competitor matches first
    sorted_ids = sorted(matched_ids, key=lambda pid: len(comp_map.get(pid, [])), reverse=True)

    # ---- Pagination ---------------------------------------------------------
    total_pages = max(1, -(-len(sorted_ids) // per_page))  # ceiling division
    if "pc_page" not in st.session_state:
        st.session_state["pc_page"] = 1
    if st.session_state.get("pc_per_page") != per_page:
        st.session_state["pc_page"] = 1
        st.session_state["pc_per_page"] = per_page

    page_num = st.session_state["pc_page"]
    start = (page_num - 1) * per_page
    page_ids = sorted_ids[start : start + per_page]

    # ---- Render tiles -------------------------------------------------------
    for pid in page_ids:
        product = products.get(pid)
        if not product:
            continue

        competitors = comp_map.get(pid, [])
        snap = snapshots.get(pid)
        rec  = recommendations.get(pid)
        tz_price = product["price_eur"]

        with st.container(border=True):
            left, right = st.columns([1, 3], gap="large")

            # --- Left: ToolZone reference ------------------------------------
            with left:
                st.markdown(f"**{product['sku'] or '—'}**")
                if product["brand"]:
                    st.caption(product["brand"])
                st.markdown(str(product["title"] or "")[:80])
                st.metric(
                    "ToolZone price",
                    f"€ {tz_price:.2f}" if tz_price else "—",
                )
                n_comp = len(competitors)
                st.caption(f"{n_comp} competitor match{'es' if n_comp != 1 else ''}")
                if snap and snap["ag_rank"] and snap["competitor_count"]:
                    st.caption(f"Rank #{snap['ag_rank']} of {snap['competitor_count'] + 1}")

            # --- Right: competitor prices ------------------------------------
            with right:
                if not competitors:
                    st.info("No competitor listings available.")
                else:
                    comp_display = []
                    for c in sorted(competitors, key=lambda r: r["price_eur"]):
                        price = c["price_eur"]
                        diff_pct = (price - tz_price) / tz_price * 100 if tz_price else None

                        if diff_pct is None:
                            diff_str = "—"
                        elif diff_pct > 0.5:
                            diff_str = f"▲ +{diff_pct:.1f}%"
                        elif diff_pct < -0.5:
                            diff_str = f"▼ {diff_pct:.1f}%"
                        else:
                            diff_str = f"≈ {diff_pct:+.1f}%"

                        ts = pd.Timestamp(c["scraped_at"]).tz_localize(None)
                        h = int((pd.Timestamp.utcnow().tz_localize(None) - ts).total_seconds() / 3600)
                        freshness = f"{h}h ago" if h < 48 else f"{h // 24}d ago"
                        stock = "✅" if c["in_stock"] else ("❌" if c["in_stock"] is False else "—")

                        comp_display.append({
                            "Store":       _display_name(c["competitor_id"]),
                            "Price":       f"€ {price:.2f}",
                            "vs ToolZone": diff_str,
                            "In Stock":    stock,
                            "Scraped":     freshness,
                            "URL":         c["url"],
                        })

                    st.dataframe(
                        pd.DataFrame(comp_display),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "URL": st.column_config.LinkColumn("Link", display_text="Open ↗"),
                        },
                    )

            # --- Bottom: stats + recommendation badge ------------------------
            st.divider()
            stat_cols = st.columns(5)
            stat_cols[0].metric("Min price",  f"€ {snap['min_price']:.2f}"  if snap and snap["min_price"]  else "—")
            stat_cols[1].metric("Max price",  f"€ {snap['max_price']:.2f}"  if snap and snap["max_price"]  else "—")
            stat_cols[2].metric("Median",     f"€ {snap['median_price']:.2f}" if snap and snap["median_price"] else "—")
            stat_cols[3].metric("Suggested",  f"€ {rec['suggested_price']:.2f}" if rec and rec["suggested_price"] else "—")
            with stat_cols[4]:
                if rec:
                    badge = PLAYBOOK_BADGE.get(rec["playbook"], rec["playbook"].upper())
                    st.markdown(f"**{badge}**")
                    if rec["rationale"]:
                        st.caption(rec["rationale"])

    # ---- Pagination controls ------------------------------------------------
    st.divider()
    nav_left, nav_mid, nav_right = st.columns([1, 2, 1])
    with nav_left:
        if st.button("← Previous", disabled=(page_num <= 1), use_container_width=True):
            st.session_state["pc_page"] = page_num - 1
            st.rerun()
    with nav_mid:
        st.markdown(
            f"<div style='text-align:center; padding-top:6px;'>Page {page_num} of {total_pages}"
            f"  —  showing {start + 1}–{min(start + per_page, len(sorted_ids))} of {len(sorted_ids)}</div>",
            unsafe_allow_html=True,
        )
    with nav_right:
        if st.button("Next →", disabled=(page_num >= total_pages), use_container_width=True):
            st.session_state["pc_page"] = page_num + 1
            st.rerun()


# ===========================================================================
# Page 3 — Coverage Health
# ===========================================================================

def _render_coverage_tab() -> None:
    st.header("Coverage Health")

    with _session() as session:
        week_ago = date.today() - timedelta(days=7)

        listing_counts = session.execute(
            select(
                CompetitorListing.competitor_id,
                func.count(CompetitorListing.id).label("listings"),
                func.max(CompetitorListing.scraped_at).label("last_scraped"),
            )
            .where(CompetitorListing.scraped_at >= pd.Timestamp(week_ago))
            .group_by(CompetitorListing.competitor_id)
        ).fetchall()

        # Count distinct matched competitor listings from listing_matches
        # (the table written by match_products.py / daily_match.py new pipeline)
        match_counts = session.execute(
            text("""
                SELECT cl.competitor_id,
                       COUNT(DISTINCT lm.competitor_listing_id) AS matches
                FROM listing_matches lm
                JOIN competitor_listings cl ON cl.id = lm.competitor_listing_id
                WHERE lm.confidence >= 0.72
                GROUP BY cl.competitor_id
            """)
        ).fetchall()

    own_stores = own_store_ids()

    # Build base from ALL configured competitors so every one appears even if
    # it has no data in the DB yet.
    all_configs = load_competitors()
    base_df = pd.DataFrame([
        {"competitor_id": c["id"], "name": c.get("name", c["id"])}
        for c in all_configs
    ])

    counts_df = pd.DataFrame(listing_counts, columns=["competitor_id", "listings", "last_scraped"])
    match_df = pd.DataFrame(match_counts, columns=["competitor_id", "matches"])

    all_df = (
        base_df
        .merge(counts_df, on="competitor_id", how="left")
        .merge(match_df, on="competitor_id", how="left")
    )
    all_df["listings"] = all_df["listings"].fillna(0).astype(int)
    all_df["matches"] = all_df["matches"].fillna(0).astype(int)
    all_df["match_rate"] = (
        (all_df["matches"] / all_df["listings"].replace(0, float("nan")) * 100)
        .round(1)
        .fillna(0.0)
    )
    all_df["own_store"] = all_df["competitor_id"].isin(own_stores)
    all_df = all_df.sort_values(["own_store", "listings"], ascending=[True, False])

    competitor_df = all_df[~all_df["own_store"]]
    own_df = all_df[all_df["own_store"]]

    active_competitors = int((competitor_df["listings"] > 0).sum())
    total_listings = int(competitor_df["listings"].sum())
    total_matched = int(competitor_df["matches"].sum())
    overall_rate = round(total_matched / total_listings * 100, 1) if total_listings else 0.0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Competitors (configured)", len(competitor_df))
    k2.metric(f"Active last 7d / Total listings", f"{active_competitors} / {total_listings}")
    k3.metric("Total matched", total_matched)
    k4.metric("Overall match rate", f"{overall_rate:.1f}%")

    st.divider()

    def _freshness_badge(ts) -> str:
        if ts is None or pd.isna(ts):
            return "⚪ Never"
        delta = pd.Timestamp.utcnow().tz_localize(None) - pd.Timestamp(ts).tz_localize(None)
        hours = delta.total_seconds() / 3600
        if hours < 26:
            return f"🟢 {delta.components.hours}h ago"
        if hours < 50:
            return f"🟡 {int(hours)}h ago"
        return f"🔴 {int(hours // 24)}d ago"

    def _render_health_table(df: pd.DataFrame) -> None:
        display = df.copy()
        display["competitor_id"] = display["competitor_id"].apply(_display_name)
        display["freshness"] = display["last_scraped"].apply(_freshness_badge)
        display["match_rate"] = display["match_rate"].apply(lambda x: f"{x:.1f}%" if x else "—")
        display["listings"] = display["listings"].apply(lambda x: x if x > 0 else "—")
        display["matches"] = display["matches"].apply(lambda x: x if x > 0 else "—")
        st.dataframe(
            display[["competitor_id", "listings", "matches", "match_rate", "freshness"]].rename(
                columns={
                    "competitor_id": "Store",
                    "listings": "Listings (7d)",
                    "matches": "Matched",
                    "match_rate": "Match Rate",
                    "freshness": "Last Scraped",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Competitors")
    _render_health_table(competitor_df)

    if not own_df.empty:
        st.subheader("Own stores")
        st.caption("Scraped as baseline — excluded from competitor benchmarks.")
        _render_health_table(own_df)

    chart_data = competitor_df[competitor_df["listings"].apply(lambda x: isinstance(x, (int, float)) and x > 0)].copy()
    if not chart_data.empty:
        st.subheader("Listings per competitor (last 7 days)")
        chart_data["competitor_id"] = chart_data["competitor_id"].apply(_display_name)
        st.bar_chart(chart_data.set_index("competitor_id")[["listings"]])

    # ---------------------------------------------------------------------------
    # Matching pipeline reference
    # ---------------------------------------------------------------------------
    st.divider()
    with st.expander("How matching works", expanded=False):
        st.markdown("""
Each competitor listing is matched to an AG catalogue product by running layers in
order — the first layer that fires wins. Higher confidence = stronger evidence.

| Layer | Match type | What fires it | Confidence |
|-------|-----------|---------------|-----------|
| 1 | `exact_ean` | EAN barcode identical on both sides | **1.00** |
| 2 | `exact_mpn` | Brand + MPN both match (normalised) | **1.00** |
| 3 | `mpn_no_brand` | MPN matches; listing has no brand field | **0.90** |
| 4 | `regex_ean_title` | EAN-13 extracted from listing title matches | **0.93** |
| 5 | `regex_mpn_title` | MPN extracted from title + brand agrees | **0.90** |
| 6 | `regex_mpn_no_brand` | MPN extracted from title; brand absent or mismatched | **0.72–0.78** |
| 7 *(opt-in)* | `llm_fuzzy` | gpt-4o-mini title/spec similarity after pre-filter | **0.75–0.84** |

**LLM pre-filter** (layer 7): before calling the API, candidates are narrowed to
≤ 5 products that share the same brand (or have no brand) **and** have ≥ 2 meaningful
title words in common. This avoids wasting API quota on clearly unrelated products.

**Normalisation**: MPN comparison strips spaces, dashes, and lowercases both sides
(e.g. `87-01-250` = `8701250`). Brand comparison collapses accents and common
abbreviations (e.g. `knipex` = `KNIPEX`).

**Confidence thresholds used elsewhere**:
- Price Compare and recommendations: `≥ 0.85` (layers 1–5 high-confidence only)
- Coverage Health match rate: `≥ 0.72` (all deterministic layers) — shows what % of a competitor's scraped listings were matched to a ToolZone product
- LLM layer default accept: `≥ 0.75` (overridable with `--min-confidence`)
""")

    # Coverage notes
    with st.expander("Competitor notes", expanded=False):
        st.markdown("""
| Competitor | Scrape method | Notes |
|---|---|---|
| Madmat | Heureka XML feed | Full catalogue via `/heureka.xml` |
| Centrum Naradia | Heureka XML feed | Full catalogue via `/heureka.xml` |
| ToolZone | Sitemap crawl | Own store — used as AG reference price |
| Boukal (CZ) | HTTP brand-page pagination | Opens every product page; EAN + Katalog from spec table |
| AH Profi | Search-by-MPN | Custom HTML parser |
| NaradieShop | Search-by-MPN | ThirtyBees HTML parser |
| Doktor Kladivo | Search-by-MPN | JSON-LD ItemList |
| Rebiop | Full catalogue crawl | BFS over categories + detail pages; EAN + internal code |
| BO-Import (CZ) | Manufacturer-page crawl | Authorized KNIPEX distributor CZ; JSON-LD; CZK→EUR |
| AGI (SK) | Manufacturer-page crawl | rshop platform; JSON-LD; EAN via gtin; EUR prices |
| Fermatshop | Sitemap crawl | Full catalogue via `/sitemap.xml` |
| Strendpro | Full catalogue crawl | Category + pagination crawl on `strendpro.sk` |
""")



# ===========================================================================
# Page 3 — Manufacturer View
# ===========================================================================

@st.cache_data(ttl=300, show_spinner="Loading manufacturer data…")
def _load_manufacturer_data(manufacturer: str) -> dict:
    """Load all competitor listings for a given manufacturer brand.

    Uses listing_matches as the source of truth so EAN, MPN, regex and LLM
    matches all appear in the UI.

    Returns:
        toolzone: list of {id, ean, title, price_eur, in_stock, url, scraped_at}
        competitors: {tz_id: {competitor_id: {price_eur, in_stock, url, scraped_at, match_type}}}
        competitor_ids: sorted list of active competitor IDs found in data
    """
    with _session() as session:
        # All ToolZone products for this brand (latest scrape per URL)
        tz_rows = session.execute(
            text("""
                SELECT id, ean, title, price_eur, in_stock, url, scraped_at
                FROM competitor_listings
                WHERE competitor_id = 'toolzone_sk'
                  AND brand LIKE :brand
                ORDER BY title
            """),
            {"brand": f"%{manufacturer}%"},
        ).fetchall()

        if not tz_rows:
            return {"toolzone": [], "competitors": {}, "competitor_ids": []}

        # All competitor matches via listing_matches (covers EAN, MPN, regex, LLM)
        tz_ids = [r.id for r in tz_rows]
        placeholders = ",".join(str(i) for i in tz_ids)
        comp_rows = session.execute(
            text(f"""
                SELECT
                    lm.toolzone_listing_id  AS tz_id,
                    lm.match_type,
                    lm.confidence,
                    cl.competitor_id,
                    cl.price_eur,
                    cl.in_stock,
                    cl.url,
                    cl.scraped_at
                FROM listing_matches lm
                JOIN competitor_listings cl ON cl.id = lm.competitor_listing_id
                WHERE lm.toolzone_listing_id IN ({placeholders})
                  AND cl.scraped_at = (
                      SELECT MAX(cl2.scraped_at)
                      FROM competitor_listings cl2
                      WHERE cl2.id = lm.competitor_listing_id
                  )
            """)
        ).fetchall()

    competitors: dict[int, dict[str, dict]] = {}  # tz_id → {cid → data}
    comp_ids: set[str] = set()
    for row in comp_rows:
        if row.tz_id not in competitors:
            competitors[row.tz_id] = {}
        # Keep lowest price if same competitor appears via multiple match paths
        existing = competitors[row.tz_id].get(row.competitor_id)
        price = float(row.price_eur) if row.price_eur else None
        if existing is None or (price and price < existing["price_eur"]):
            competitors[row.tz_id][row.competitor_id] = {
                "price_eur": price,
                "in_stock": row.in_stock,
                "url": row.url or "",
                "scraped_at": row.scraped_at,
                "match_type": row.match_type,
            }
        comp_ids.add(row.competitor_id)

    return {
        "toolzone": [
            {
                "id": r.id,
                "ean": r.ean,
                "title": r.title,
                "price_eur": float(r.price_eur) if r.price_eur else None,
                "in_stock": r.in_stock,
                "url": r.url or "",
                "scraped_at": r.scraped_at,
            }
            for r in tz_rows
        ],
        "competitors": competitors,
        "competitor_ids": sorted(comp_ids),
    }


@st.cache_data(ttl=600, show_spinner=False)
def _load_manufacturer_list() -> list[str]:
    """Return distinct brand values from ToolZone listings, sorted."""
    with _session() as session:
        rows = session.execute(
            text("""
                SELECT DISTINCT brand FROM competitor_listings
                WHERE competitor_id = 'toolzone_sk'
                  AND brand IS NOT NULL AND brand != ''
                ORDER BY brand
            """)
        ).fetchall()
    return [r.brand for r in rows]


def _render_manufacturer_tab() -> None:
    st.header("By Manufacturer")

    manufacturers = _load_manufacturer_list()

    if not manufacturers:
        st.info(
            "No manufacturer data yet. "
            "Run: `python jobs/manufacturer_scrape.py --manufacturer knipex`"
        )
        return

    # ---- Controls row -------------------------------------------------------
    col_mfr, col_stock, col_per_page, col_refresh, col_scrape = st.columns([3, 1, 1, 1, 2])

    with col_mfr:
        selected = st.selectbox(
            "Manufacturer", manufacturers,
            index=manufacturers.index("KNIPEX") if "KNIPEX" in manufacturers else 0,
            label_visibility="collapsed",
        )

    with col_stock:
        in_stock_only = st.toggle("In stock only", value=False)

    with col_per_page:
        per_page = st.selectbox("Per page", [10, 20, 50], index=0, label_visibility="visible", key="mfr_per_page_select")

    with col_refresh:
        if st.button("↺ Refresh", use_container_width=True, help="Reload data from the database", key="mfr_refresh"):
            _load_manufacturer_data.clear()
            _load_manufacturer_list.clear()
            st.rerun()

    with col_scrape:
        if st.button(
            f"▶ Scrape {selected}",
            use_container_width=True,
            help="Run manufacturer_scrape.py for this manufacturer",
            type="primary",
        ):
            jobs_dir = Path(__file__).parent.parent / "jobs"
            proc = subprocess.Popen(
                [sys.executable, str(jobs_dir / "manufacturer_scrape.py"),
                 "--manufacturer", selected.lower().replace(" ", "-")],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            st.session_state["mfr_scrape_proc"] = proc
            st.session_state["mfr_scrape_manufacturer"] = selected
            st.toast(f"Scrape started for {selected}", icon="🚀")

    # ---- Scrape progress expander -------------------------------------------
    if "mfr_scrape_proc" in st.session_state:
        proc = st.session_state["mfr_scrape_proc"]
        with st.expander(
            f"Scrape output — {st.session_state.get('mfr_scrape_manufacturer', '')}",
            expanded=True,
        ):
            if proc.poll() is None:
                st.info("Scrape in progress… refresh the page once it completes.")
            else:
                out, _ = proc.communicate()
                st.code(out or "(no output)")
                if st.button("Clear output", key="mfr_clear_output"):
                    del st.session_state["mfr_scrape_proc"]
                    del st.session_state["mfr_scrape_manufacturer"]
                    st.rerun()

    # ---- Load data ----------------------------------------------------------
    data = _load_manufacturer_data(selected)
    toolzone_products = data["toolzone"]
    competitors_data = data["competitors"]
    competitor_ids = data["competitor_ids"]

    if not toolzone_products:
        st.warning(
            f"No ToolZone listings found for **{selected}**. "
            f"Run: `python jobs/manufacturer_scrape.py --manufacturer {selected.lower()}`"
        )
        return

    # ---- Apply in-stock filter and sort by number of competitor matches ------
    visible_products = [
        p for p in toolzone_products
        if not in_stock_only or p["in_stock"]
    ]
    visible_products.sort(
        key=lambda p: len(competitors_data.get(p["id"], {})),
        reverse=True,
    )

    if not visible_products:
        st.info("No in-stock products found for this manufacturer.")
        return

    # ---- KPI row ------------------------------------------------------------
    matched_count = sum(1 for p in visible_products if competitors_data.get(p["id"]))
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Products", len(visible_products))
    k2.metric("With competitor match", matched_count)
    k3.metric("Competitors", len(competitor_ids))
    if visible_products:
        prices = [p["price_eur"] for p in visible_products if p["price_eur"]]
        if prices:
            k4.metric("Avg ToolZone price", f"€ {sum(prices)/len(prices):.2f}")

    st.divider()

    # ---- Pagination ---------------------------------------------------------
    total_pages = max(1, -(-len(visible_products) // per_page))
    mfr_page_key = f"mfr_page_{selected}"
    if mfr_page_key not in st.session_state:
        st.session_state[mfr_page_key] = 1
    if st.session_state.get(f"mfr_per_page_{selected}") != per_page:
        st.session_state[mfr_page_key] = 1
        st.session_state[f"mfr_per_page_{selected}"] = per_page

    page_num = st.session_state[mfr_page_key]
    start = (page_num - 1) * per_page
    page_products = visible_products[start: start + per_page]

    # ---- Tiles --------------------------------------------------------------
    for product in page_products:
        ean = product["ean"] or ""
        tz_price = product["price_eur"]
        comp_data = competitors_data.get(product["id"], {})

        with st.container(border=True):
            left, right = st.columns([1, 3], gap="large")

            # --- Left: ToolZone reference card (compact) ---------------------
            with left:
                # Title — truncated so it doesn't push the column too wide
                title = product["title"] or "—"
                st.markdown(f"**{title[:80]}{'…' if len(title) > 80 else ''}**")

                # Price + stock on one compact line
                price_str = f"**€ {tz_price:.2f}**" if tz_price else "**—**"
                if product["in_stock"] is True:
                    stock_str = "✅ in stock"
                elif product["in_stock"] is False:
                    stock_str = "❌ out of stock"
                else:
                    stock_str = ""
                st.markdown(f"{price_str}{'  ·  ' + stock_str if stock_str else ''}")

                # EAN + freshness as captions
                meta_parts = []
                if ean:
                    meta_parts.append(f"EAN {ean}")
                if product["scraped_at"]:
                    ts = pd.Timestamp(product["scraped_at"]).tz_localize(None)
                    h = int((pd.Timestamp.utcnow().tz_localize(None) - ts).total_seconds() / 3600)
                    freshness = f"🟢 {h}h ago" if h < 26 else (f"🟡 {h}h ago" if h < 50 else f"🔴 {h // 24}d ago")
                    meta_parts.append(freshness)
                if meta_parts:
                    st.caption("  ·  ".join(meta_parts))

                if product["url"]:
                    st.caption(f"[Open on ToolZone ↗]({product['url']})")

            # --- Right: competitor prices ------------------------------------
            with right:
                if not comp_data:
                    st.caption("No competitor matches found.")
                else:
                    n = len(comp_data)
                    st.caption(f"{n} competitor match{'es' if n != 1 else ''}")

                    comp_rows = []
                    for cid, c in sorted(comp_data.items(), key=lambda x: x[1]["price_eur"]):
                        c_price = c["price_eur"]
                        diff_pct = (c_price - tz_price) / tz_price * 100 if tz_price and c_price else None

                        if diff_pct is None:
                            diff_str = "—"
                        elif diff_pct > 0.5:
                            diff_str = f"▲ +{diff_pct:.1f}%"
                        elif diff_pct < -0.5:
                            diff_str = f"▼ {diff_pct:.1f}%"
                        else:
                            diff_str = f"≈ {diff_pct:+.1f}%"

                        stock = "✅" if c["in_stock"] else ("❌" if c["in_stock"] is False else "—")

                        ts = pd.Timestamp(c["scraped_at"]).tz_localize(None)
                        h = int((pd.Timestamp.utcnow().tz_localize(None) - ts).total_seconds() / 3600)
                        freshness = f"{h}h ago" if h < 48 else f"{h // 24}d ago"

                        match_badge = {
                            "exact_ean":   "EAN ✓",
                            "exact_mpn":   "MPN ✓",
                            "mpn_no_brand": "MPN ~",
                            "regex_ean_title": "Regex ~",
                            "regex_mpn_title": "Regex ~",
                            "regex_mpn_no_brand": "Regex ~",
                            "llm_fuzzy":   "LLM ~",
                        }.get(c.get("match_type", ""), c.get("match_type", ""))

                        comp_rows.append({
                            "Store":       _display_name(cid),
                            "Price":       f"€ {c_price:.2f}",
                            "vs ToolZone": diff_str,
                            "Match":       match_badge,
                            "In Stock":    stock,
                            "Scraped":     freshness,
                            "URL":         c["url"],
                        })

                    st.dataframe(
                        pd.DataFrame(comp_rows),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "URL": st.column_config.LinkColumn("Link", display_text="Open ↗"),
                        },
                    )

    # ---- Pagination controls ------------------------------------------------
    st.divider()
    nav_left, nav_mid, nav_right = st.columns([1, 2, 1])
    with nav_left:
        if st.button("← Previous", disabled=(page_num <= 1), use_container_width=True, key="mfr_prev"):
            st.session_state[mfr_page_key] = page_num - 1
            st.rerun()
    with nav_mid:
        st.markdown(
            f"<div style='text-align:center; padding-top:6px;'>Page {page_num} of {total_pages}"
            f"  —  showing {start + 1}–{min(start + per_page, len(visible_products))} "
            f"of {len(visible_products)}</div>",
            unsafe_allow_html=True,
        )
    with nav_right:
        if st.button("Next →", disabled=(page_num >= total_pages), use_container_width=True, key="mfr_next"):
            st.session_state[mfr_page_key] = page_num + 1
            st.rerun()


# ===========================================================================
# Wire up tabs
# ===========================================================================

with tab1:
    _render_search_tab()

with tab2:
    _render_price_compare_tab()

with tab3:
    _render_manufacturer_tab()

with tab4:
    _render_coverage_tab()
