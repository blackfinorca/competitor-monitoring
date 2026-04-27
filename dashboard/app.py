"""Streamlit dashboard for AG Naradie / ToolZone Pricing.

Pages
-----
  1. Product Overview   — Landing dashboard with listing volume, freshness and coverage
  2. Product Search     — Search bar → live fetch / cache → ToolZone card + competitors
  3. Recommendations    — Product vs competitor price overview (tile view)

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

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import bindparam, func, select, text
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

from dashboard.seller_dashboard_data import load_seller_dashboard_data
from dashboard.seller_dashboard_view import render_seller_dashboard

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="ToolZone Pricing",
    page_icon=":wrench:",
    layout="wide",
)


def _normalize_dashboard_theme(mode: str | None) -> str:
    return "light" if mode == "light" else "dark"


def _dashboard_theme_tokens(mode: str | None) -> dict[str, object]:
    normalized = _normalize_dashboard_theme(mode)
    if normalized == "light":
        return {
            "mode": "light",
            "label": "Light",
            "background": "#f7f9fc",
            "surface": "#ffffff",
            "surface_alt": "#eef5f8",
            "text": "#172033",
            "muted": "#5d6b7c",
            "border": "#cbd5e1",
            "grid": "#d9e2ec",
            "accent": "#0f9f8f",
            "accent_2": "#d97706",
            "danger": "#dc2626",
            "plotly_template": "plotly_white",
            "chart_palette": ["#0f9f8f", "#2563eb", "#d97706", "#7c3aed", "#dc2626", "#64748b"],
        }
    return {
        "mode": "dark",
        "label": "Dark",
        "background": "#111318",
        "surface": "#191d24",
        "surface_alt": "#222733",
        "text": "#f6f7fb",
        "muted": "#a9b4c2",
        "border": "#2f3645",
        "grid": "#344052",
        "accent": "#2dd4bf",
        "accent_2": "#f59e0b",
        "danger": "#f87171",
        "plotly_template": "plotly_dark",
        "chart_palette": ["#2dd4bf", "#60a5fa", "#f59e0b", "#a78bfa", "#f87171", "#94a3b8"],
    }


def _dashboard_theme_css(mode: str | None) -> str:
    t = _dashboard_theme_tokens(mode)
    return f"""
<style>
:root {{
    --tz-bg: {t["background"]};
    --tz-surface: {t["surface"]};
    --tz-surface-alt: {t["surface_alt"]};
    --tz-text: {t["text"]};
    --tz-muted: {t["muted"]};
    --tz-border: {t["border"]};
    --tz-accent: {t["accent"]};
    --tz-accent-2: {t["accent_2"]};
}}
.stApp {{
    background: var(--tz-bg);
    color: var(--tz-text);
}}
.stApp p, .stApp li, .stApp span, .stApp label, .stApp div {{
    color: inherit;
}}
[data-testid="stSidebar"] > div:first-child {{
    background: var(--tz-surface);
    border-right: 1px solid var(--tz-border);
}}
[data-testid="stHeader"] {{
    background: var(--tz-bg);
}}
[data-testid="stMetric"], [data-testid="stDataFrame"], div[data-testid="stExpander"] {{
    background: var(--tz-surface);
    border: 1px solid var(--tz-border);
    border-radius: 8px;
}}
[data-testid="stMetric"] {{
    padding: 0.75rem 0.85rem;
}}
[data-testid="stTabs"] button[role="tab"] {{
    color: var(--tz-muted);
}}
[data-testid="stTabs"] button[aria-selected="true"] {{
    color: var(--tz-text) !important;
    border-bottom-color: var(--tz-accent) !important;
    box-shadow: inset 0 -2px 0 var(--tz-accent);
}}
.stButton > button,
button[kind="secondary"],
button[kind="secondaryFormSubmit"],
button[data-testid="stBaseButton-secondary"],
button[data-testid="stBaseButton-secondaryFormSubmit"] {{
    border-color: var(--tz-border);
    background: var(--tz-surface-alt) !important;
    color: var(--tz-text) !important;
}}
.stButton > button[kind="primary"],
button[kind="primary"],
button[kind="primaryFormSubmit"],
button[data-testid="stBaseButton-primary"],
button[data-testid="stBaseButton-primaryFormSubmit"] {{
    border-color: var(--tz-accent);
    background: var(--tz-accent) !important;
    color: #ffffff !important;
}}
button:disabled,
button[disabled] {{
    opacity: 0.58;
    cursor: not-allowed;
    color: var(--tz-muted) !important;
    background: var(--tz-surface-alt) !important;
}}
.stTextInput input,
.stTextArea textarea,
.stNumberInput input {{
    background: var(--tz-surface-alt) !important;
    color: var(--tz-text) !important;
    border-color: var(--tz-border) !important;
    caret-color: var(--tz-accent);
}}
.stTextInput input:focus,
.stTextArea textarea:focus,
.stNumberInput input:focus {{
    border-color: var(--tz-accent) !important;
    box-shadow: 0 0 0 1px var(--tz-accent) !important;
    outline: none !important;
}}
.stTextInput input::placeholder,
.stTextArea textarea::placeholder {{
    color: var(--tz-muted) !important;
    opacity: 1;
}}
div[data-baseweb="select"] > div,
div[data-baseweb="popover"] div[role="listbox"] {{
    background: var(--tz-surface-alt) !important;
    border-color: var(--tz-border) !important;
}}
div[data-baseweb="select"]:focus-within > div {{
    border-color: var(--tz-accent) !important;
    box-shadow: 0 0 0 1px var(--tz-accent) !important;
}}
div[data-baseweb="select"] span,
div[data-baseweb="select"] input,
div[data-baseweb="popover"] div[role="option"],
div[data-baseweb="popover"] span {{
    color: var(--tz-text) !important;
}}
div[data-baseweb="checkbox"] span {{
    color: var(--tz-text) !important;
}}
input[type="checkbox"],
input[type="radio"] {{
    accent-color: var(--tz-accent);
}}
a, a:visited {{
    color: var(--tz-accent);
}}
hr {{
    border-color: var(--tz-border);
}}
</style>
"""


def _dashboard_top_bar_columns() -> list[float]:
    return [0.88, 0.12]


def _select_dashboard_theme() -> tuple[str, dict[str, object]]:
    current = _normalize_dashboard_theme(st.session_state.get("dashboard_theme", "dark"))
    light_mode = st.toggle(
        "Light",
        value=current == "light",
        key="dashboard_light_mode",
        help="Switch dashboard theme.",
    )
    selected = "light" if light_mode else "dark"
    st.session_state["dashboard_theme"] = selected
    tokens = _dashboard_theme_tokens(selected)
    st.markdown(_dashboard_theme_css(selected), unsafe_allow_html=True)
    return selected, tokens


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
    model = os.environ.get("OPENAI_MODEL", "gpt-5-nano")
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
_status_col, _theme_col = st.columns(_dashboard_top_bar_columns(), gap="small")
with _status_col:
    st.caption(f"ToolZone Pricing  ·  Cycle: 1 month  ·  As of: {date.today().isoformat()}  ·  LLM: {_llm_status}")
with _theme_col:
    _dashboard_theme_mode, _dashboard_theme = _select_dashboard_theme()

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Product Overview", "🔍 Product Search", "💰 Price compare",
    "🏭 By Manufacturer", "⚔️ Compare Competitors", "🧐 Matching review",
])


# ===========================================================================
# Page 2 — Product Search
# ===========================================================================

def _product_search_match_lookup(
    matches,
    extra_match_info: dict[tuple[str, str | None], tuple[str, float]] | None = None,
) -> dict[tuple[str, str | None], tuple[str, float]]:
    lookup: dict[tuple[str, str | None], tuple[str, float]] = {}
    for pm in matches:
        lookup[(pm.competitor_id, pm.competitor_sku)] = (pm.match_type, float(pm.confidence))
    if extra_match_info:
        lookup.update(extra_match_info)
    return lookup


def _render_search_tab() -> None:
    st.header("Product Search")
    st.caption(
        "Enter an EAN, MPN, or product name. "
        "Cached results (< 30 days) load instantly; everything else is fetched live."
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

    if (submitted or refresh) and not run_query:
        st.session_state.pop("search_result", None)
        st.session_state["last_query"] = ""

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
    match_lookup = _product_search_match_lookup(
        result.matches,
        getattr(result, "match_info", None),
    )

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
            days = max(hours // 24, 0)
            if days < 31:
                freshness = f"🟢 {days}d ago" if days else f"🟢 {hours}h ago"
            elif days < 45:
                freshness = f"🟡 {days}d ago"
            else:
                freshness = f"🔴 {days}d ago"
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

                mt, conf = match_lookup.get(
                    (cl.competitor_id, cl.competitor_sku),
                    match_lookup.get((cl.competitor_id, None), ("—", 0.0)),
                )
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
# Page 3 — Price compare (tile view)
# ===========================================================================

PLAYBOOK_BADGE = {
    "raise":       "🟢 RAISE",
    "drop":        "🔴 DROP",
    "hold":        "🔵 HOLD",
    "investigate": "🟠 INVESTIGATE",
}


@st.cache_data(ttl=300, show_spinner="Loading clusters…")
def _load_price_compare_data() -> dict:
    """Fetch all clusters and their approved members for the Price Compare view.

    Returns plain dicts/lists (no ORM objects) so Streamlit can serialise them.

    Each cluster maps to a tile with:
      - title / brand / ean / method
      - members: latest-scrape competitor_listings rows whose membership is approved
    """
    with _session() as session:
        rows = session.execute(
            text(
                """
                SELECT pc.id            AS cluster_id,
                       pc.ean           AS ean,
                       pc.cluster_method AS method,
                       pc.representative_brand AS rep_brand,
                       pc.representative_title AS rep_title,
                       cm.match_method  AS match_method,
                       cm.llm_confidence AS llm_confidence,
                       cl.id            AS listing_id,
                       cl.competitor_id AS competitor_id,
                       cl.brand         AS brand,
                       cl.title         AS title,
                       cl.price_eur     AS price_eur,
                       cl.url           AS url,
                       cl.in_stock      AS in_stock,
                       cl.scraped_at    AS scraped_at
                FROM   product_clusters pc
                JOIN   cluster_members cm ON cm.cluster_id = pc.id
                JOIN   competitor_listings cl ON cl.id = cm.listing_id
                WHERE  cm.status = 'approved'
                """
            )
        ).fetchall()

    clusters: dict[int, dict] = {}
    for r in rows:
        cl = clusters.setdefault(
            r.cluster_id,
            {
                "ean":      r.ean,
                "method":   r.method,
                "brand":    r.rep_brand,
                "title":    r.rep_title,
                "members":  [],
            },
        )
        is_tz = r.competitor_id == "toolzone_sk"
        cl["members"].append({
            "listing_id":     r.listing_id,
            "competitor_id":  r.competitor_id,
            "brand":          r.brand,
            "title":          r.title,
            "price_eur":      float(r.price_eur) if r.price_eur is not None else None,
            "url":            r.url or "",
            "in_stock":       r.in_stock,
            "scraped_at":     r.scraped_at,
            "is_toolzone":    is_tz,
            "match_method":   r.match_method,
            "llm_confidence": float(r.llm_confidence) if r.llm_confidence is not None else None,
        })
        if is_tz:
            cl["title"] = r.title or cl["title"]
            cl["brand"] = r.brand or cl["brand"]

    return {"clusters": clusters}


def _render_price_compare_tab() -> None:
    st.header("Price Compare")
    st.caption("EAN-led product clusters. ToolZone listing pinned to top of each tile; tiles ordered by match count.")

    data = _load_price_compare_data()
    clusters: dict[int, dict] = data["clusters"]

    if not clusters:
        st.info("No clusters yet. Run `python jobs/run_new_matching.py` to build them.")
        return

    total_members = sum(len(c["members"]) for c in clusters.values())
    multi = sum(1 for c in clusters.values() if len(c["members"]) > 1)

    k1, k2, k3, per_page_col, refresh_col = st.columns([1, 1, 1, 1, 1])
    k1.metric("Clusters", len(clusters))
    k2.metric("Multi-store clusters", multi)
    k3.metric("Member listings", total_members)
    per_page = per_page_col.selectbox("Per page", [10, 20, 50], index=0)
    if refresh_col.button("↺ Refresh", use_container_width=True, help="Reload data from the database"):
        _load_price_compare_data.clear()
        st.rerun()
    st.divider()

    # Sort tiles by member count desc; multi-store before solo; stable by cluster_id.
    sorted_ids = sorted(
        clusters.keys(),
        key=lambda cid: (-len(clusters[cid]["members"]), cid),
    )

    total_pages = max(1, -(-len(sorted_ids) // per_page))
    if "pc_page" not in st.session_state:
        st.session_state["pc_page"] = 1
    if st.session_state.get("pc_per_page") != per_page:
        st.session_state["pc_page"] = 1
        st.session_state["pc_per_page"] = per_page

    page_num = st.session_state["pc_page"]
    start = (page_num - 1) * per_page
    page_ids = sorted_ids[start : start + per_page]

    for cid in page_ids:
        cluster = clusters[cid]
        members = cluster["members"]
        # ToolZone first, then others by price asc.
        members_sorted = sorted(
            members,
            key=lambda m: (
                0 if m["is_toolzone"] else 1,
                m["price_eur"] if m["price_eur"] is not None else float("inf"),
            ),
        )
        tz_member = next((m for m in members_sorted if m["is_toolzone"]), None)
        tz_price = tz_member["price_eur"] if tz_member else None

        with st.container(border=True):
            left, right = st.columns([1, 3], gap="large")

            with left:
                title = (cluster["title"] or "—")[:80]
                brand = cluster["brand"] or ""
                st.markdown(f"**{title}**")
                if brand:
                    st.caption(brand)
                if cluster["ean"]:
                    st.caption(f"EAN: `{cluster['ean']}`")
                else:
                    st.caption(f"Method: {cluster['method']}")
                if tz_member:
                    st.metric("ToolZone price", f"€ {tz_price:.2f}" if tz_price else "—")
                else:
                    st.caption("⚠️ No ToolZone listing in this cluster")
                n = len(members_sorted)
                st.caption(f"{n} listing{'s' if n != 1 else ''}")

            with right:
                comp_display = []
                for c in members_sorted:
                    price = c["price_eur"]
                    if tz_price and price is not None and not c["is_toolzone"]:
                        diff_pct = (price - tz_price) / tz_price * 100
                        if diff_pct > 0.5:
                            diff_str = f"▲ +{diff_pct:.1f}%"
                        elif diff_pct < -0.5:
                            diff_str = f"▼ {diff_pct:.1f}%"
                        else:
                            diff_str = f"≈ {diff_pct:+.1f}%"
                    else:
                        diff_str = "—"

                    ts = pd.Timestamp(c["scraped_at"]).tz_localize(None) if c["scraped_at"] else None
                    if ts is not None:
                        h = int((pd.Timestamp.utcnow().tz_localize(None) - ts).total_seconds() / 3600)
                        freshness = f"{h}h ago" if h < 48 else f"{h // 24}d ago"
                    else:
                        freshness = "—"
                    stock = "✅" if c["in_stock"] else ("❌" if c["in_stock"] is False else "—")
                    method = "EAN" if c["match_method"] == "ean" else (
                        f"LLM {c['llm_confidence']:.2f}" if c["llm_confidence"] else c["match_method"]
                    )

                    comp_display.append({
                        "Store":      ("⭐ " if c["is_toolzone"] else "") + _display_name(c["competitor_id"]),
                        "Price":      f"€ {price:.2f}" if price is not None else "—",
                        "vs TZ":      diff_str,
                        "Stock":      stock,
                        "Match":      method,
                        "Scraped":    freshness,
                        "URL":        c["url"],
                    })

                st.dataframe(
                    pd.DataFrame(comp_display),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "URL": st.column_config.LinkColumn("Link", display_text="Open ↗"),
                    },
                )

    st.divider()
    nav_left, nav_mid, nav_right = st.columns([1, 2, 1])
    with nav_left:
        if st.button("← Previous", disabled=(page_num <= 1), use_container_width=True, key="pc_prev"):
            st.session_state["pc_page"] = page_num - 1
            st.rerun()
    with nav_mid:
        st.markdown(
            f"<div style='text-align:center; padding-top:6px;'>Page {page_num} of {total_pages}"
            f"  —  showing {start + 1}–{min(start + per_page, len(sorted_ids))} of {len(sorted_ids)}</div>",
            unsafe_allow_html=True,
        )
    with nav_right:
        if st.button("Next →", disabled=(page_num >= total_pages), use_container_width=True, key="pc_next"):
            st.session_state["pc_page"] = page_num + 1
            st.rerun()


# ===========================================================================
# Page 1 — Product Overview
# ===========================================================================

def _build_product_overview_frame(
    *,
    all_configs: list[dict],
    all_time_df: pd.DataFrame,
    fresh_df: pd.DataFrame,
    match_df: pd.DataFrame,
    own_store_ids_value: set[str] | frozenset[str],
    product_count_df: pd.DataFrame | None = None,
    lower_price_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if product_count_df is None:
        product_count_df = pd.DataFrame(columns=["competitor_id", "product_count"])
    if lower_price_df is None:
        lower_price_df = pd.DataFrame(columns=[
            "competitor_id",
            "lower_price_wins",
            "price_comparisons",
            "lower_price_rate",
        ])

    base_df = pd.DataFrame([
        {
            "competitor_id": c["id"],
            "name": c.get("name", c["id"]),
            "own_store": bool(c.get("own_store", False)) or c["id"] in own_store_ids_value,
        }
        for c in all_configs
    ])

    all_df = (
        base_df
        .merge(all_time_df, on="competitor_id", how="left")
        .merge(fresh_df, on="competitor_id", how="left")
        .merge(match_df, on="competitor_id", how="left")
        .merge(product_count_df, on="competitor_id", how="left")
        .merge(lower_price_df, on="competitor_id", how="left")
    )
    all_df["listings_total"] = all_df["listings_total"].fillna(0).astype(int)
    all_df["listings_30d"] = all_df["listings_30d"].fillna(0).astype(int)
    all_df["matches"] = all_df["matches"].fillna(0).astype(int)
    all_df["product_count"] = all_df["product_count"].fillna(0).astype(int)
    all_df["lower_price_wins"] = all_df["lower_price_wins"].fillna(0).astype(int)
    all_df["price_comparisons"] = all_df["price_comparisons"].fillna(0).astype(int)
    all_df["lower_price_rate"] = all_df["lower_price_rate"].fillna(0.0).astype(float)
    all_df["match_rate"] = (
        (all_df["matches"] / all_df["listings_total"].replace(0, float("nan")) * 100)
        .round(1)
        .fillna(0.0)
    )
    all_df["fresh_share"] = (
        (all_df["listings_30d"] / all_df["listings_total"].replace(0, float("nan")) * 100)
        .round(1)
        .fillna(0.0)
    )
    return all_df.sort_values(["own_store", "listings_total"], ascending=[True, False]).reset_index(drop=True)


def _build_lower_price_rate_frame(
    price_df: pd.DataFrame,
    competitor_ids: set[str] | frozenset[str],
) -> pd.DataFrame:
    if price_df.empty:
        return pd.DataFrame(columns=[
            "competitor_id",
            "lower_price_wins",
            "price_comparisons",
            "lower_price_rate",
        ])

    scoped = price_df[price_df["competitor_id"].isin(competitor_ids)].copy()
    if scoped.empty:
        return pd.DataFrame(columns=[
            "competitor_id",
            "lower_price_wins",
            "price_comparisons",
            "lower_price_rate",
        ])

    scoped["price_eur"] = pd.to_numeric(scoped["price_eur"], errors="coerce")
    scoped = scoped.dropna(subset=["price_eur"])
    stats: dict[str, dict[str, int]] = {
        cid: {"lower_price_wins": 0, "price_comparisons": 0}
        for cid in competitor_ids
    }

    for _, group in scoped.groupby("cluster_id"):
        prices = group.groupby("competitor_id")["price_eur"].min().to_dict()
        for cid, price in prices.items():
            for other_id, other_price in prices.items():
                if cid == other_id:
                    continue
                stats[cid]["price_comparisons"] += 1
                if price < other_price:
                    stats[cid]["lower_price_wins"] += 1

    rows = []
    for cid, values in stats.items():
        comparisons = values["price_comparisons"]
        wins = values["lower_price_wins"]
        rows.append({
            "competitor_id": cid,
            "lower_price_wins": wins,
            "price_comparisons": comparisons,
            "lower_price_rate": round(wins / comparisons * 100, 1) if comparisons else 0.0,
        })
    return pd.DataFrame(rows)


def _build_product_overlap_frames(
    membership_df: pd.DataFrame,
    competitor_ids: set[str] | frozenset[str],
    *,
    top_n: int = 8,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    empty_counts = pd.DataFrame(columns=["competitor_id", "product_count"])
    empty_points = pd.DataFrame(columns=[
        "pair_label",
        "competitor_id",
        "x",
        "y",
        "products",
        "overlap_products",
        "overlap_rate",
    ])
    if membership_df.empty:
        return empty_counts, empty_points

    scoped = membership_df[membership_df["competitor_id"].isin(competitor_ids)].copy()
    if scoped.empty:
        return empty_counts, empty_points

    clusters_by_comp = {
        cid: set(group["cluster_id"])
        for cid, group in scoped.groupby("competitor_id")
    }
    product_counts = pd.DataFrame([
        {"competitor_id": cid, "product_count": len(clusters)}
        for cid, clusters in clusters_by_comp.items()
    ])

    pair_rows = []
    comp_ids = sorted(clusters_by_comp)
    for index, left_id in enumerate(comp_ids):
        for right_id in comp_ids[index + 1:]:
            left_clusters = clusters_by_comp[left_id]
            right_clusters = clusters_by_comp[right_id]
            overlap = len(left_clusters & right_clusters)
            if overlap == 0:
                continue
            smaller_count = max(min(len(left_clusters), len(right_clusters)), 1)
            overlap_rate = overlap / smaller_count
            pair_rows.append({
                "left_id": left_id,
                "right_id": right_id,
                "left_products": len(left_clusters),
                "right_products": len(right_clusters),
                "overlap_products": overlap,
                "overlap_rate": overlap_rate,
            })

    pair_rows.sort(key=lambda row: (row["overlap_products"], row["overlap_rate"]), reverse=True)
    points = []
    for row_index, pair in enumerate(pair_rows[:top_n]):
        distance = 1.8 - min(pair["overlap_rate"], 1.0) * 1.1
        pair_label = f"{pair['left_id']} / {pair['right_id']} · {pair['overlap_products']} matched"
        points.extend([
            {
                "pair_label": pair_label,
                "competitor_id": pair["left_id"],
                "x": -distance / 2,
                "y": row_index,
                "products": pair["left_products"],
                "overlap_products": pair["overlap_products"],
                "overlap_rate": round(pair["overlap_rate"] * 100, 1),
            },
            {
                "pair_label": pair_label,
                "competitor_id": pair["right_id"],
                "x": distance / 2,
                "y": row_index,
                "products": pair["right_products"],
                "overlap_products": pair["overlap_products"],
                "overlap_rate": round(pair["overlap_rate"] * 100, 1),
            },
        ])

    return product_counts, pd.DataFrame(points) if points else empty_points


def _build_product_overlap_layout(
    membership_df: pd.DataFrame,
    competitor_ids: set[str] | frozenset[str],
    *,
    iterations: int = 250,
    seed: int = 0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    import math
    import random

    node_columns = [
        "competitor_id",
        "x",
        "y",
        "products",
        "radius",
        "partners",
    ]
    edge_columns = [
        "left_id",
        "right_id",
        "x1",
        "y1",
        "x2",
        "y2",
        "mid_x",
        "mid_y",
        "overlap_products",
        "overlap_rate",
        "label",
    ]
    empty_nodes = pd.DataFrame(columns=node_columns)
    empty_edges = pd.DataFrame(columns=edge_columns)
    if membership_df.empty:
        return empty_nodes, empty_edges

    scoped = membership_df[membership_df["competitor_id"].isin(competitor_ids)]
    if scoped.empty:
        return empty_nodes, empty_edges

    clusters_by_comp: dict[str, set[int]] = {
        cid: set(group["cluster_id"]) for cid, group in scoped.groupby("competitor_id")
    }
    comp_ids = sorted(clusters_by_comp)
    products = {cid: len(clusters_by_comp[cid]) for cid in comp_ids}
    max_p = max(products.values()) or 1
    radii = {
        cid: 0.6 + 1.6 * math.sqrt(products[cid] / max_p) for cid in comp_ids
    }

    rng = random.Random(seed)
    n = len(comp_ids)
    positions: dict[str, list[float]] = {}
    for i, cid in enumerate(comp_ids):
        angle = 2 * math.pi * i / max(n, 1)
        positions[cid] = [
            math.cos(angle) * 4.0 + rng.uniform(-0.05, 0.05),
            math.sin(angle) * 4.0 + rng.uniform(-0.05, 0.05),
        ]

    overlaps: dict[tuple[str, str], int] = {}
    for i, a in enumerate(comp_ids):
        for b in comp_ids[i + 1 :]:
            ov = len(clusters_by_comp[a] & clusters_by_comp[b])
            if ov:
                overlaps[(a, b)] = ov

    for _ in range(iterations):
        for (a, b), ov in overlaps.items():
            ra, rb = radii[a], radii[b]
            smaller = min(products[a], products[b]) or 1
            frac = min(ov / smaller, 1.0)
            target = (ra + rb) * (1.0 - 0.55 * frac)
            pa, pb = positions[a], positions[b]
            dx, dy = pb[0] - pa[0], pb[1] - pa[1]
            dist = math.sqrt(dx * dx + dy * dy) or 1e-6
            shift = (dist - target) * 0.05
            ux, uy = dx / dist, dy / dist
            positions[a][0] += ux * shift
            positions[a][1] += uy * shift
            positions[b][0] -= ux * shift
            positions[b][1] -= uy * shift
        for i, a in enumerate(comp_ids):
            for b in comp_ids[i + 1 :]:
                if (a, b) in overlaps:
                    continue
                ra, rb = radii[a], radii[b]
                pa, pb = positions[a], positions[b]
                dx, dy = pb[0] - pa[0], pb[1] - pa[1]
                dist = math.sqrt(dx * dx + dy * dy) or 1e-6
                target = (ra + rb) * 1.15
                if dist < target:
                    push = (target - dist) * 0.05
                    ux, uy = dx / dist, dy / dist
                    positions[a][0] -= ux * push
                    positions[a][1] -= uy * push
                    positions[b][0] += ux * push
                    positions[b][1] += uy * push

    partners_per_comp: dict[str, list[tuple[str, int, float]]] = {cid: [] for cid in comp_ids}
    edges = []
    for (a, b), ov in overlaps.items():
        smaller = min(products[a], products[b]) or 1
        rate = ov / smaller
        partners_per_comp[a].append((b, ov, rate))
        partners_per_comp[b].append((a, ov, rate))
        edges.append({
            "left_id": a,
            "right_id": b,
            "x1": positions[a][0],
            "y1": positions[a][1],
            "x2": positions[b][0],
            "y2": positions[b][1],
            "mid_x": (positions[a][0] + positions[b][0]) / 2,
            "mid_y": (positions[a][1] + positions[b][1]) / 2,
            "overlap_products": ov,
            "overlap_rate": round(rate * 100, 1),
            "label": f"{round(rate * 100)}%",
        })

    rows = []
    for cid in comp_ids:
        sorted_partners = sorted(
            partners_per_comp[cid], key=lambda p: (-p[2], -p[1])
        )
        rows.append({
            "competitor_id": cid,
            "x": positions[cid][0],
            "y": positions[cid][1],
            "products": products[cid],
            "radius": radii[cid],
            "partners": [
                {
                    "competitor_id": partner,
                    "overlap_products": count,
                    "overlap_rate": round(rate * 100, 1),
                }
                for partner, count, rate in sorted_partners
            ],
        })
    nodes_df = pd.DataFrame(rows, columns=node_columns)
    edges_df = (
        pd.DataFrame(edges, columns=edge_columns)
        if edges else empty_edges
    )
    return nodes_df, edges_df


@st.cache_data(ttl=300, show_spinner="Loading seller dashboard…")
def _load_seller_dashboard_data_cached() -> dict:
    return load_seller_dashboard_data()


@st.cache_data(ttl=300, show_spinner="Loading product overview…")
def _load_product_overview_data() -> dict:
    with _session() as session:
        fresh_cutoff = datetime.now(UTC) - timedelta(days=30)

        all_time_counts = session.execute(
            select(
                CompetitorListing.competitor_id,
                func.count(CompetitorListing.id).label("listings_total"),
                func.max(CompetitorListing.scraped_at).label("last_scraped"),
            )
            .group_by(CompetitorListing.competitor_id)
        ).fetchall()

        fresh_counts = session.execute(
            select(
                CompetitorListing.competitor_id,
                func.count(CompetitorListing.id).label("listings_30d"),
            )
            .where(CompetitorListing.scraped_at >= fresh_cutoff)
            .group_by(CompetitorListing.competitor_id)
        ).fetchall()

        match_counts = session.execute(
            text("""
                SELECT cl.competitor_id,
                       COUNT(DISTINCT cl.id) AS matches
                FROM competitor_listings cl
                WHERE cl.id IN (
                    SELECT lm.competitor_listing_id
                    FROM listing_matches lm
                    WHERE lm.confidence >= 0.72
                    UNION
                    SELECT cm.listing_id
                    FROM cluster_members cm
                    WHERE cm.status = 'approved'
                )
                GROUP BY cl.competitor_id
            """)
        ).fetchall()

        membership_rows = session.execute(
            text("""
                SELECT cm.cluster_id   AS cluster_id,
                       cl.competitor_id AS competitor_id,
                       cl.price_eur    AS price_eur
                FROM   cluster_members cm
                JOIN   competitor_listings cl ON cl.id = cm.listing_id
                WHERE  cm.status = 'approved'
            """)
        ).fetchall()

    all_time_df = pd.DataFrame(all_time_counts, columns=["competitor_id", "listings_total", "last_scraped"])
    fresh_df = pd.DataFrame(fresh_counts, columns=["competitor_id", "listings_30d"])
    match_df = pd.DataFrame(match_counts, columns=["competitor_id", "matches"])
    membership_df = pd.DataFrame(membership_rows, columns=["cluster_id", "competitor_id", "price_eur"])

    own_ids = own_store_ids()
    competitor_ids = {
        c["id"]
        for c in load_competitors()
        if c["id"] not in own_ids and not c.get("own_store", False)
    }
    lower_price_df = _build_lower_price_rate_frame(membership_df, competitor_ids)
    product_count_df, _ = _build_product_overlap_frames(membership_df, competitor_ids)
    overlap_nodes_df, overlap_edges_df = _build_product_overlap_layout(
        membership_df, competitor_ids
    )

    all_df = _build_product_overview_frame(
        all_configs=load_competitors(),
        all_time_df=all_time_df,
        fresh_df=fresh_df,
        match_df=match_df,
        own_store_ids_value=own_ids,
        product_count_df=product_count_df,
        lower_price_df=lower_price_df,
    )
    competitor_df = all_df[~all_df["own_store"]].copy()
    own_df = all_df[all_df["own_store"]].copy()

    total_listings = int(competitor_df["listings_total"].sum())
    fresh_listings = int(competitor_df["listings_30d"].sum())
    total_matched = int(competitor_df["matches"].sum())
    active_competitors = int((competitor_df["listings_total"] > 0).sum())

    return {
        "all": all_df,
        "competitors": competitor_df,
        "own": own_df,
        "overlap_nodes": overlap_nodes_df,
        "overlap_edges": overlap_edges_df,
        "summary": {
            "configured_competitors": int(len(competitor_df)),
            "active_competitors": active_competitors,
            "total_listings": total_listings,
            "fresh_listings": fresh_listings,
            "total_matched": total_matched,
            "overall_rate": round(total_matched / total_listings * 100, 1) if total_listings else 0.0,
            "fresh_share": round(fresh_listings / total_listings * 100, 1) if total_listings else 0.0,
        },
    }


def _format_count(value: int | float) -> str:
    return f"{int(value):,}"


def _freshness_badge(ts) -> str:
    if ts is None or pd.isna(ts):
        return "Never"
    delta = pd.Timestamp.utcnow().tz_localize(None) - pd.Timestamp(ts).tz_localize(None)
    hours = delta.total_seconds() / 3600
    if hours < 26:
        return f"{max(int(hours), 0)}h ago"
    if hours < 50:
        return f"{int(hours)}h ago"
    return f"{int(hours // 24)}d ago"


def _render_product_overview_table(df: pd.DataFrame) -> None:
    display = df.copy()
    display["Store"] = display["competitor_id"].apply(_display_name)
    display["Listings"] = display["listings_total"].apply(lambda x: _format_count(x) if x else "—")
    display["Fresh 30d"] = display["listings_30d"].apply(lambda x: _format_count(x) if x else "—")
    display["Matched"] = display["matches"].apply(lambda x: _format_count(x) if x else "—")
    display["Coverage"] = display["match_rate"].apply(lambda x: f"{x:.1f}%" if x else "—")
    display["Fresh share"] = display["fresh_share"].apply(lambda x: f"{x:.1f}%" if x else "—")
    display["Last scraped"] = display["last_scraped"].apply(_freshness_badge)
    st.dataframe(
        display[["Store", "Listings", "Fresh 30d", "Matched", "Coverage", "Fresh share", "Last scraped"]],
        use_container_width=True,
        hide_index=True,
    )


def _render_product_overview_tab() -> None:
    st.header("Product Overview")
    st.caption("Project landing dashboard: competitor listing volume, matching coverage, and scrape freshness.")

    data = _load_product_overview_data()
    competitor_df: pd.DataFrame = data["competitors"]
    own_df: pd.DataFrame = data["own"]
    summary = data["summary"]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Active competitors", f"{summary['active_competitors']} / {summary['configured_competitors']}")
    k2.metric("All-time listings", _format_count(summary["total_listings"]))
    k3.metric("Fresh listings 30d", _format_count(summary["fresh_listings"]), delta=f"{summary['fresh_share']:.1f}% fresh")
    k4.metric("Matched listings", _format_count(summary["total_matched"]), delta=f"{summary['overall_rate']:.1f}% coverage")

    st.divider()

    seller_data = _load_seller_dashboard_data_cached()
    if seller_data["offers_total"] == 0:
        st.info("No clustered offers yet — approve cluster matches to populate the seller dashboard.")
    else:
        render_seller_dashboard(seller_data, theme=_dashboard_theme)

    st.divider()

    chart_df = competitor_df[competitor_df["listings_total"] > 0].copy()
    if chart_df.empty:
        st.info("No competitor listing data yet.")
    else:
        chart_df["Store"] = chart_df["competitor_id"].apply(_display_name)
        volume_df = (
            chart_df.sort_values("listings_total", ascending=False)
            .head(15)
            .rename(columns={"listings_total": "All-time listings"})
        )

        left, right = st.columns([1.15, 1], gap="large")
        with left:
            st.subheader("Listings by competitor")
            st.caption("All-time scraped listings per competitor.")
            st.bar_chart(volume_df.set_index("Store")[["All-time listings"]])

        with right:
            st.subheader("Competitor portfolio")
            st.caption("Right is better coverage; higher is more often the cheapest; larger bubbles are bigger scraped catalogues.")
            portfolio_df = chart_df.rename(columns={
                "match_rate": "Coverage %",
                "lower_price_rate": "Lower price %",
                "listings_total": "All-time listings",
            })
            portfolio_df["Bubble size"] = portfolio_df["All-time listings"].clip(lower=1)
            portfolio_df["Volume tier"] = pd.cut(
                portfolio_df["All-time listings"],
                bins=[0, 500, 5000, 25000, float("inf")],
                labels=["Small", "Medium", "Large", "Strategic"],
            ).astype(str)
            st.scatter_chart(
                portfolio_df,
                x="Coverage %",
                y="Lower price %",
                size="Bubble size",
                color="Volume tier",
                use_container_width=True,
            )

    overlap_nodes_df: pd.DataFrame = data.get("overlap_nodes", pd.DataFrame())
    overlap_edges_df: pd.DataFrame = data.get("overlap_edges", pd.DataFrame())
    st.subheader("Product overlap")
    st.caption(
        "One bubble per competitor — size is the number of clustered products; "
        "edges show the % of the smaller catalogue shared with the partner store."
    )
    if overlap_nodes_df.empty:
        st.info("No product clusters yet — approve matches to populate this view.")
    else:
        nodes = overlap_nodes_df.copy()
        nodes["Store"] = nodes["competitor_id"].apply(_display_name)

        def _format_partner_summary(partners) -> str:
            if not partners:
                return "—"
            return " • ".join(
                f"{_display_name(p['competitor_id'])} {p['overlap_rate']:.0f}% ({p['overlap_products']})"
                for p in partners
            )

        nodes["Partners"] = nodes["partners"].apply(_format_partner_summary)

        max_radius = float(nodes["radius"].max())
        extent = max(
            float(nodes["x"].abs().max()),
            float(nodes["y"].abs().max()),
            1.0,
        ) + max_radius + 0.6
        chart_px = 560
        ppu = chart_px / (2 * extent)
        nodes["bubble_size"] = (nodes["radius"] * ppu).pow(2) * 3.14159265
        size_max = float(nodes["bubble_size"].max())

        x_scale = alt.Scale(domain=[-extent, extent])
        y_scale = alt.Scale(domain=[-extent, extent])

        layers: list[alt.Chart] = []
        if not overlap_edges_df.empty:
            edges = overlap_edges_df.copy()
            edges["Pair"] = (
                edges["left_id"].apply(_display_name)
                + " ↔ "
                + edges["right_id"].apply(_display_name)
            )
            edge_lines = (
                alt.Chart(edges)
                .mark_rule(stroke=str(_dashboard_theme["muted"]), strokeDash=[3, 3], opacity=0.6)
                .encode(
                    x=alt.X("x1:Q", scale=x_scale, axis=None),
                    y=alt.Y("y1:Q", scale=y_scale, axis=None),
                    x2="x2:Q",
                    y2="y2:Q",
                    tooltip=[
                        alt.Tooltip("Pair:N", title="Pair"),
                        alt.Tooltip("overlap_products:Q", title="Shared products"),
                        alt.Tooltip("overlap_rate:Q", title="% of smaller catalogue", format=".1f"),
                    ],
                )
            )
            edge_labels = (
                alt.Chart(edges)
                .mark_text(fontSize=10, color=str(_dashboard_theme["muted"]), fontWeight="bold")
                .encode(
                    x=alt.X("mid_x:Q", scale=x_scale, axis=None),
                    y=alt.Y("mid_y:Q", scale=y_scale, axis=None),
                    text="label:N",
                )
            )
            layers.extend([edge_lines, edge_labels])

        bubbles = (
            alt.Chart(nodes)
            .mark_circle(opacity=0.62, stroke=str(_dashboard_theme["surface"]), strokeWidth=1)
            .encode(
                x=alt.X("x:Q", scale=x_scale, axis=None),
                y=alt.Y("y:Q", scale=y_scale, axis=None),
                size=alt.Size(
                    "bubble_size:Q",
                    legend=None,
                    scale=alt.Scale(domain=[0, size_max], range=[0, size_max]),
                ),
                color=alt.Color(
                    "Store:N",
                    scale=alt.Scale(range=list(_dashboard_theme["chart_palette"])),
                    legend=alt.Legend(title="Competitor", orient="right", symbolOpacity=0.85),
                ),
                tooltip=[
                    alt.Tooltip("Store:N", title="Competitor"),
                    alt.Tooltip("products:Q", title="Products"),
                    alt.Tooltip("Partners:N", title="Overlaps"),
                ],
            )
        )
        labels = (
            alt.Chart(nodes)
            .mark_text(fontSize=11, fontWeight="bold", color=str(_dashboard_theme["text"]))
            .encode(
                x=alt.X("x:Q", scale=x_scale, axis=None),
                y=alt.Y("y:Q", scale=y_scale, axis=None),
                text="Store:N",
            )
        )
        layers.extend([bubbles, labels])
        chart = (
            alt.layer(*layers)
            .properties(width=chart_px, height=chart_px)
            .configure_view(stroke=None)
            .configure(background=str(_dashboard_theme["background"]))
        )
        st.altair_chart(chart, use_container_width=False)

    # ---------------------------------------------------------------------------
    # Matching pipeline reference
    # ---------------------------------------------------------------------------
    st.divider()
    with st.expander("How matching works", expanded=False):
        st.markdown("""
Each competitor listing is matched to a ToolZone product by running layers in
order — the first layer that fires wins. Higher confidence = stronger evidence.

| Layer | Match type | What fires it | Confidence |
|-------|-----------|---------------|-----------|
| 1 | `exact_ean` | EAN barcode identical on both sides | **1.00** |
| 2 | `exact_mpn` | Brand + MPN both match (normalised) | **1.00** |
| 3 | `mpn_no_brand` | MPN matches; listing has no brand field | **0.90** |
| 4 | `regex_ean` | EAN extracted from listing title matches | **0.95** |
| 5 *(opt-in)* | `llm_fuzzy` | gpt-5-nano title/spec similarity after vector retrieval | **≥ 0.85** |

Coverage uses all-time competitor listings as the denominator and matched listings
from `listing_matches` plus approved `cluster_members` as the numerator.
""")

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

    st.divider()
    st.subheader("Competitors")
    st.caption("Detailed competitor table using all-time listings with a fresh-30-day signal.")
    _render_product_overview_table(competitor_df)

    if not own_df.empty:
        st.subheader("Own stores")
        st.caption("Scraped as baseline — excluded from competitor benchmarks.")
        _render_product_overview_table(own_df)

# ===========================================================================
# Page 4 — Manufacturer View
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
# Page 5 — Compare Competitors
# ===========================================================================

@st.cache_data(ttl=300, show_spinner="Loading comparison data…")
def _load_comparison_data(ref_id: str, opp_ids: tuple[str, ...], min_confidence: float) -> dict:
    """Return price comparison data for ref vs each opponent in opp_ids.

    SQL strategy per pair — three cases based on whether ToolZone is involved:
      • ref = toolzone_sk → ref_price from toolzone side of listing_matches
      • opp = toolzone_sk → opp_price from toolzone side of listing_matches
      • neither           → double join bridged by toolzone_listing_id
    Returns per_opp data, a merged wide-format product list, and brand aggregates.
    """
    def _fetch_pair(session, ref: str, opp: str, min_conf: float) -> list:
        if ref == "toolzone_sk":
            return session.execute(text("""
                SELECT cl_tz.title, cl_tz.brand,
                       cl_tz.price_eur  AS ref_price, cl_tz.url  AS ref_url,
                       cl_opp.price_eur AS opp_price, cl_opp.url AS opp_url
                FROM   listing_matches lm
                JOIN   competitor_listings cl_tz  ON cl_tz.id  = lm.toolzone_listing_id
                JOIN   competitor_listings cl_opp ON cl_opp.id = lm.competitor_listing_id
                                                 AND cl_opp.competitor_id = :opp
                WHERE  lm.confidence >= :mc
                ORDER  BY cl_tz.brand, cl_tz.title
            """), {"opp": opp, "mc": min_conf}).fetchall()
        if opp == "toolzone_sk":
            return session.execute(text("""
                SELECT cl_tz.title, cl_tz.brand,
                       cl_ref.price_eur AS ref_price, cl_ref.url AS ref_url,
                       cl_tz.price_eur  AS opp_price, cl_tz.url  AS opp_url
                FROM   listing_matches lm
                JOIN   competitor_listings cl_tz  ON cl_tz.id  = lm.toolzone_listing_id
                JOIN   competitor_listings cl_ref ON cl_ref.id = lm.competitor_listing_id
                                                 AND cl_ref.competitor_id = :ref
                WHERE  lm.confidence >= :mc
                ORDER  BY cl_tz.brand, cl_tz.title
            """), {"ref": ref, "mc": min_conf}).fetchall()
        return session.execute(text("""
            SELECT cl_tz.title, cl_tz.brand,
                   cl_ref.price_eur AS ref_price, cl_ref.url AS ref_url,
                   cl_opp.price_eur AS opp_price, cl_opp.url AS opp_url
            FROM   listing_matches lm_ref
            JOIN   competitor_listings cl_ref ON cl_ref.id = lm_ref.competitor_listing_id
                                             AND cl_ref.competitor_id = :ref
            JOIN   listing_matches lm_opp    ON lm_opp.toolzone_listing_id = lm_ref.toolzone_listing_id
            JOIN   competitor_listings cl_opp ON cl_opp.id = lm_opp.competitor_listing_id
                                             AND cl_opp.competitor_id = :opp
            JOIN   competitor_listings cl_tz  ON cl_tz.id = lm_ref.toolzone_listing_id
            WHERE  lm_ref.confidence >= :mc AND lm_opp.confidence >= :mc
            ORDER  BY cl_tz.brand, cl_tz.title
        """), {"ref": ref, "opp": opp, "mc": min_conf}).fetchall()

    def _summarise(rows: list[dict]) -> dict:
        valid    = [r for r in rows if r["delta_pct"] is not None]
        wins     = [r for r in valid if r["delta_pct"] >  0.5]
        losses   = [r for r in valid if r["delta_pct"] < -0.5]
        brand_acc: dict[str, dict] = {}
        for r in valid:
            if not r["brand"]:
                continue
            b = brand_acc.setdefault(r["brand"], {"count": 0, "wins": 0, "delta_sum": 0.0})
            b["count"] += 1
            if r["delta_pct"] > 0.5:
                b["wins"] += 1
            b["delta_sum"] += r["delta_pct"]
        by_brand = sorted(
            [{"brand": br, "count": s["count"],
              "ref_wins": s["wins"],
              "ref_wins_pct": round(s["wins"] / s["count"] * 100, 1),
              "avg_delta_pct": round(s["delta_sum"] / s["count"], 1)}
             for br, s in brand_acc.items()],
            key=lambda x: x["count"], reverse=True,
        )
        return {
            "total":                len(valid),
            "ref_cheaper_count":    len(wins),
            "ref_cheaper_pct":      round(len(wins) / len(valid) * 100, 1) if valid else 0.0,
            "avg_advantage_pct":    round(sum(r["delta_pct"] for r in wins)   / len(wins),   1) if wins   else 0.0,
            "avg_disadvantage_pct": round(sum(r["delta_pct"] for r in losses) / len(losses), 1) if losses else 0.0,
            "by_brand":             by_brand,
        }

    per_opp: dict[str, dict] = {}
    with _session() as session:
        for opp_id in opp_ids:
            raw = _fetch_pair(session, ref_id, opp_id, min_confidence)
            rows: list[dict] = []
            for r in raw:
                rp = float(r.ref_price) if r.ref_price is not None else None
                op = float(r.opp_price) if r.opp_price is not None else None
                dp = (op - rp) / rp * 100 if rp and op and rp != 0 else None
                rows.append({
                    "title": r.title or "", "brand": r.brand or "",
                    "ref_price": rp, "ref_url": r.ref_url or "",
                    "opp_price": op, "opp_url": r.opp_url or "",
                    "delta_pct": dp,
                })
            per_opp[opp_id] = {"rows": rows, **_summarise(rows)}

    # Merge into wide-format keyed by (title, brand)
    merged_map: dict[tuple, dict] = {}
    for opp_id, od in per_opp.items():
        for r in od["rows"]:
            key = (r["title"], r["brand"])
            if key not in merged_map:
                merged_map[key] = {
                    "title": r["title"], "brand": r["brand"],
                    "ref_price": r["ref_price"], "ref_url": r["ref_url"],
                    "opponents": {},
                }
            merged_map[key]["opponents"][opp_id] = {
                "price": r["opp_price"], "delta_pct": r["delta_pct"], "url": r["opp_url"],
            }

    for m in merged_map.values():
        m["wins"] = sum(
            1 for od in m["opponents"].values()
            if od["delta_pct"] is not None and od["delta_pct"] > 0.5
        )
    merged = sorted(merged_map.values(), key=lambda m: (-m["wins"], m["brand"], m["title"]))

    # Cross-opponent brand aggregates
    brand_agg: dict[str, dict] = {}
    for m in merged:
        if not m["brand"]:
            continue
        b = brand_agg.setdefault(m["brand"], {"count": 0, "delta_sum": 0.0, "delta_n": 0})
        b["count"] += 1
        for od in m["opponents"].values():
            if od["delta_pct"] is not None:
                b["delta_sum"] += od["delta_pct"]
                b["delta_n"]   += 1
    brand_summary = sorted(
        [{"brand": br, "count": s["count"],
          "avg_delta_pct": round(s["delta_sum"] / s["delta_n"], 1) if s["delta_n"] else 0.0}
         for br, s in brand_agg.items()],
        key=lambda x: x["count"], reverse=True,
    )
    return {"per_opp": per_opp, "merged": merged, "brand_summary": brand_summary}


def _generate_insights(data: dict, ref_name: str, opp_ids: list[str]) -> str:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return "No OPENAI_API_KEY configured."

    from agnaradie_pricing.matching.llm_matcher import OpenAIClient
    from agnaradie_pricing.pricing.compare_competitors_insights import build_compare_competitors_insights_prompt

    client = OpenAIClient(api_key=api_key, model="gpt-4.1-mini", max_tokens=1800)
    opponents = [(opp_id, _display_name(opp_id)) for opp_id in opp_ids]
    prompt = build_compare_competitors_insights_prompt(
        data,
        ref_name=ref_name,
        opponents=opponents,
    )
    return client.complete(prompt)


def _available_compare_brands(rows: list[dict]) -> list[str]:
    return sorted({(row.get("brand") or "").strip() for row in rows if (row.get("brand") or "").strip()})


def _compare_brand_match_counts(rows: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        brand = (row.get("brand") or "").strip()
        if not brand:
            continue
        counts[brand] = counts.get(brand, 0) + 1
    return counts


def _toggle_compare_brand_selection(selected_brands: list[str], brand: str) -> list[str]:
    current = list(selected_brands)
    if brand in current:
        return [item for item in current if item != brand]
    return sorted([*current, brand])


def _filter_compare_rows(
    rows: list[dict],
    *,
    filter_opt: str,
    selected_brands: list[str] | None,
    n_opp: int,
) -> list[dict]:
    if "Stronger" in filter_opt:
        filtered = [row for row in rows if row["wins"] == n_opp]
    elif "Weaker" in filter_opt:
        filtered = [row for row in rows if row["wins"] == 0]
    else:
        filtered = rows

    if selected_brands is not None:
        allowed_brands = set(selected_brands)
        filtered = [row for row in filtered if row.get("brand") in allowed_brands]

    return filtered


def _render_compare_tab() -> None:
    st.header("Compare Competitors")
    st.caption("Pick a reference store and up to 4 competitors to compare against.")

    names   = _competitor_names()
    all_ids = sorted(names.keys(), key=lambda cid: names.get(cid, cid))
    if not all_ids:
        st.info("No competitor data yet.")
        return

    ref_default = "toolzone_sk" if "toolzone_sk" in all_ids else all_ids[0]

    col_ref, col_conf, col_refresh = st.columns([2, 1, 1])
    with col_ref:
        ref_id = st.selectbox(
            "Reference store",
            all_ids,
            index=all_ids.index(ref_default),
            format_func=_display_name,
            key="cc_ref",
        )
    with col_conf:
        min_conf = st.select_slider(
            "Min confidence",
            options=[0.72, 0.80, 0.85, 0.90, 1.0],
            value=0.85,
            key="cc_conf",
        )
    with col_refresh:
        st.write("")
        if st.button("↺ Refresh", use_container_width=True, key="cc_refresh"):
            _load_comparison_data.clear()
            for k in list(st.session_state.keys()):
                if k.startswith("cc_insights_"):
                    del st.session_state[k]
            st.rerun()

    opp_options = [cid for cid in all_ids if cid != ref_id]
    opp_ids_list: list[str] = st.multiselect(
        "Compare against (up to 4)",
        opp_options,
        default=opp_options[:2] if len(opp_options) >= 2 else opp_options[:1],
        format_func=_display_name,
        max_selections=4,
        key="cc_opp",
    )

    if not opp_ids_list:
        st.info("Select at least one competitor to compare against.")
        return

    opp_ids = tuple(opp_ids_list)
    data         = _load_comparison_data(ref_id, opp_ids, min_conf)
    merged       = data["merged"]
    per_opp      = data["per_opp"]
    brand_summary = data["brand_summary"]
    ref_name     = _display_name(ref_id)

    if not merged:
        st.info(
            f"No matched products found at confidence ≥ {min_conf:.0%}. "
            "Try lowering the confidence threshold or running more scraping/matching jobs."
        )
        return

    # KPI row: total products + one metric per opponent
    kpi_cols = st.columns(1 + len(opp_ids_list))
    kpi_cols[0].metric("Products with data", len(merged))
    for i, opp_id in enumerate(opp_ids_list):
        s = per_opp[opp_id]
        kpi_cols[i + 1].metric(
            f"Cheaper vs {_display_name(opp_id)}",
            f"{s['ref_cheaper_pct']:.0f}%",
            help=f"{s['ref_cheaper_count']} of {s['total']} matched products",
        )

    st.divider()

    # Wide product table with filter
    filter_opt = st.radio(
        "Show",
        ["All products", f"✅ Stronger (beats all {len(opp_ids_list)})", "❌ Weaker (loses to all)"],
        horizontal=True,
        key="cc_filter",
    )

    n_opp = len(opp_ids_list)
    brand_options = _available_compare_brands(merged)
    brand_counts = _compare_brand_match_counts(merged)
    brand_state_key = f"cc_brands_{ref_id}_{'_'.join(sorted(opp_ids_list))}_{min_conf}"
    if brand_state_key not in st.session_state:
        st.session_state[brand_state_key] = list(brand_options)
    else:
        st.session_state[brand_state_key] = [
            brand for brand in st.session_state[brand_state_key]
            if brand in brand_options
        ]

    selected_brands = list(st.session_state[brand_state_key])

    st.caption("Brands")
    action_col1, action_col2 = st.columns([1, 1])
    with action_col1:
        if st.button("Select all", use_container_width=True, key=f"{brand_state_key}_all"):
            st.session_state[brand_state_key] = list(brand_options)
            st.rerun()
    with action_col2:
        if st.button("Deselect all", use_container_width=True, key=f"{brand_state_key}_none"):
            st.session_state[brand_state_key] = []
            st.rerun()

    brand_button_cols = st.columns(min(6, len(brand_options)) or 1)
    for idx, brand in enumerate(brand_options):
        is_selected = brand in selected_brands
        label = f"{brand} ({brand_counts.get(brand, 0)})"
        with brand_button_cols[idx % len(brand_button_cols)]:
            if st.button(
                label,
                key=f"{brand_state_key}_{idx}",
                type="primary" if is_selected else "secondary",
                use_container_width=False,
            ):
                st.session_state[brand_state_key] = _toggle_compare_brand_selection(selected_brands, brand)
                st.rerun()

    display_rows = _filter_compare_rows(
        merged,
        filter_opt=filter_opt,
        selected_brands=None if set(selected_brands) == set(brand_options) else selected_brands,
        n_opp=n_opp,
    )

    if not display_rows:
        st.info("No products match the current show/brand filters.")
        return

    table_data = []
    col_cfg: dict = {}
    for m in display_rows:
        row: dict = {
            "Brand":              m["brand"],
            "Product":            m["title"][:55] + ("…" if len(m["title"]) > 55 else ""),
            f"{ref_name} (€)":   f"{m['ref_price']:.2f}" if m["ref_price"] else "—",
        }
        for opp_id in opp_ids_list:
            opp_disp = _display_name(opp_id)
            od = m["opponents"].get(opp_id)
            price_col = f"{opp_disp} (€)"
            delta_col = f"vs {opp_disp}"
            if od and od["price"]:
                row[price_col] = f"{od['price']:.2f}"
                dp = od["delta_pct"]
                if dp is not None:
                    row[delta_col] = f"+{dp:.1f}%" if dp > 0.5 else (f"{dp:.1f}%" if dp < -0.5 else "≈")
                else:
                    row[delta_col] = "—"
                col_cfg[price_col] = st.column_config.TextColumn(price_col)
            else:
                row[price_col] = "—"
                row[delta_col] = "—"
        row["Wins"] = f"{m['wins']}/{n_opp}"
        table_data.append(row)

    st.dataframe(
        pd.DataFrame(table_data),
        use_container_width=True,
        hide_index=True,
        column_config=col_cfg,
    )

    # Brand breakdown chart
    if brand_summary:
        st.divider()
        st.subheader("Brand breakdown — avg price delta across all competitors")
        brand_df = (
            pd.DataFrame(brand_summary[:15])
            .set_index("brand")[["avg_delta_pct"]]
            .rename(columns={"avg_delta_pct": "Avg Δ% (positive = ref cheaper)"})
        )
        st.bar_chart(brand_df)

    # AI Insights panel
    st.divider()
    insights_key  = f"cc_insights_{ref_id}_{'_'.join(sorted(opp_ids_list))}"
    llm_available = bool(os.environ.get("OPENAI_API_KEY"))

    hdr_col, btn_col = st.columns([4, 1])
    hdr_col.subheader("🤖 AI Insights")
    with btn_col:
        st.write("")
        if st.button(
            "Generate",
            disabled=not llm_available,
            use_container_width=True,
            help=(
                "Generate narrative insights with GPT-4.1-mini"
                if llm_available
                else "No OPENAI_API_KEY configured"
            ),
            key="cc_gen_insights",
        ):
            with st.spinner("Analysing pricing data…"):
                try:
                    st.session_state[insights_key] = _generate_insights(data, ref_name, opp_ids_list)
                except Exception as exc:
                    st.session_state[insights_key] = f"Error generating insights: {exc}"

    if insights_key in st.session_state:
        st.markdown(st.session_state[insights_key])
    elif not llm_available:
        st.caption("Configure `OPENAI_API_KEY` in `.env` to enable AI insights.")
    else:
        st.caption("Click **Generate** to produce narrative insights from the comparison data.")


# ===========================================================================
# Page 6 — Matching Review
# ===========================================================================

_MATCH_REVIEW_PER_PAGE = 50
_MATCH_REVIEW_SELECTION_KEY = "mr_selected_member_ids"
_MATCH_REVIEW_AUTO_APPROVE_SIMILARITY = 0.95


def _load_matching_review(status_filter: str) -> list[dict]:
    """Return fuzzy ClusterMember rows joined with the orphan listing and a peer member.

    status_filter ∈ {'pending', 'approved', 'all'}
    """
    where = "cm.match_method = 'vector_llm'"
    if status_filter == "pending":
        where += " AND cm.status = 'pending'"
    elif status_filter == "approved":
        where += " AND cm.status = 'approved'"
    elif status_filter == "rejected":
        where += " AND cm.status = 'rejected'"

    with _session() as session:
        rows = session.execute(
            text(
                f"""
                SELECT cm.id              AS member_id,
                       cm.cluster_id      AS cluster_id,
                       cm.status          AS status,
                       cm.similarity      AS similarity,
                       cm.llm_confidence  AS llm_confidence,
                       cm.created_at      AS created_at,
                       cm.reviewed_at     AS reviewed_at,
                       cm.reviewer        AS reviewer,
                       cl.id              AS listing_id,
                       cl.competitor_id   AS competitor_id,
                       cl.brand           AS brand,
                       cl.title           AS title,
                       cl.price_eur       AS price_eur,
                       cl.url             AS url,
                       pc.ean             AS cluster_ean,
                       pc.representative_title AS cluster_title,
                       pc.representative_brand AS cluster_brand
                FROM   cluster_members cm
                JOIN   competitor_listings cl ON cl.id = cm.listing_id
                JOIN   product_clusters pc    ON pc.id = cm.cluster_id
                WHERE  {where}
                ORDER  BY cm.created_at DESC, cm.id DESC
                """
            )
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def _load_cluster_peers(cluster_ids: list[int]) -> dict[int, list[dict]]:
    """Return non-fuzzy approved peer members for the given clusters (for context)."""
    if not cluster_ids:
        return {}
    ids_csv = ",".join(str(int(c)) for c in cluster_ids)
    with _session() as session:
        rows = session.execute(
            text(
                f"""
                SELECT cm.cluster_id AS cluster_id,
                       cl.competitor_id AS competitor_id,
                       cl.title AS title,
                       cl.price_eur AS price_eur
                FROM   cluster_members cm
                JOIN   competitor_listings cl ON cl.id = cm.listing_id
                WHERE  cm.cluster_id IN ({ids_csv})
                  AND  cm.status = 'approved'
                """
            )
        ).fetchall()
    out: dict[int, list[dict]] = {}
    for r in rows:
        out.setdefault(r.cluster_id, []).append({
            "competitor_id": r.competitor_id,
            "title":         r.title,
            "price_eur":     float(r.price_eur) if r.price_eur is not None else None,
        })
    return out


def _set_member_status(member_id: int, new_status: str) -> None:
    with _session() as session:
        session.execute(
            text(
                """
                UPDATE cluster_members
                SET status = :s,
                    reviewed_at = CURRENT_TIMESTAMP,
                    reviewer = :r
                WHERE id = :id
                """
            ),
            {"s": new_status, "r": "dashboard", "id": member_id},
        )
        session.commit()
    _load_price_compare_data.clear()


def _approve_pending_members(member_ids: list[int]) -> int:
    unique_ids = sorted({int(member_id) for member_id in member_ids})
    if not unique_ids:
        return 0

    stmt = text(
        """
        UPDATE cluster_members
        SET status = 'approved',
            reviewed_at = CURRENT_TIMESTAMP,
            reviewer = :reviewer
        WHERE status = 'pending'
          AND id IN :member_ids
        """
    ).bindparams(bindparam("member_ids", expanding=True))

    with _session() as session:
        result = session.execute(
            stmt,
            {"member_ids": unique_ids, "reviewer": "dashboard"},
        )
        session.commit()

    updated = result.rowcount or 0
    if updated:
        _load_price_compare_data.clear()
    return updated


def _auto_approve_high_similarity_matches() -> int:
    with _session() as session:
        result = session.execute(
            text(
                """
                UPDATE cluster_members
                SET status = 'approved',
                    reviewed_at = CURRENT_TIMESTAMP,
                    reviewer = :reviewer
                WHERE status = 'pending'
                  AND match_method = 'vector_llm'
                  AND similarity >= :threshold
                """
            ),
            {
                "reviewer": "auto_similarity_0.95",
                "threshold": _MATCH_REVIEW_AUTO_APPROVE_SIMILARITY,
            },
        )
        session.commit()

    updated = result.rowcount or 0
    if updated:
        _load_price_compare_data.clear()
    return updated


def _selected_matching_review_ids() -> set[int]:
    return {
        int(member_id)
        for member_id in st.session_state.get(_MATCH_REVIEW_SELECTION_KEY, [])
    }


def _store_selected_matching_review_ids(member_ids: set[int]) -> None:
    st.session_state[_MATCH_REVIEW_SELECTION_KEY] = sorted(member_ids)


def _discard_selected_matching_review_ids(member_ids: set[int]) -> None:
    selected_ids = _selected_matching_review_ids()
    selected_ids.difference_update(member_ids)
    _store_selected_matching_review_ids(selected_ids)
    for member_id in member_ids:
        st.session_state.pop(f"mr_select_{member_id}", None)


def _render_matching_review_tab() -> None:
    st.header("Matching Review")
    auto_approved = _auto_approve_high_similarity_matches()
    st.caption(
        "LLM-suggested fuzzy matches below 0.95 similarity are queued for review. "
        "Matches at 0.95 similarity or higher are auto-approved."
    )
    if auto_approved:
        st.success(f"Auto-approved {auto_approved} high-similarity pending match{'es' if auto_approved != 1 else ''}.")

    # KPIs
    with _session() as session:
        kpi_rows = session.execute(
            text(
                """
                SELECT status, COUNT(*) AS n
                FROM   cluster_members
                WHERE  match_method = 'vector_llm'
                GROUP  BY status
                """
            )
        ).fetchall()
    counts = {r.status: r.n for r in kpi_rows}

    k1, k2, k3, _spacer = st.columns([1, 1, 1, 3])
    k1.metric("Pending",  counts.get("pending", 0))
    k2.metric("Approved", counts.get("approved", 0))
    k3.metric("Rejected", counts.get("rejected", 0))

    filter_col, search_col = st.columns([1, 3])
    status_filter = filter_col.selectbox(
        "Status",
        ["pending", "approved", "rejected", "all"],
        index=0,
        key="mr_status",
    )
    search = search_col.text_input("Search title", key="mr_search").strip().lower()

    rows = _load_matching_review(status_filter)
    if search:
        rows = [r for r in rows if search in (r.get("title") or "").lower()]

    if not rows:
        st.info("No matches in this view.")
        return

    # Pagination
    per_page = _MATCH_REVIEW_PER_PAGE
    total_pages = max(1, -(-len(rows) // per_page))
    if "mr_page" not in st.session_state:
        st.session_state["mr_page"] = 1
    page_num = min(st.session_state["mr_page"], total_pages)
    start = (page_num - 1) * per_page
    page_rows = rows[start : start + per_page]

    # Pre-fetch peers for the visible page
    peers = _load_cluster_peers([int(r["cluster_id"]) for r in page_rows])
    pending_page_ids = [
        int(r["member_id"])
        for r in page_rows
        if r["status"] == "pending"
    ]
    selected_pending_ids = _selected_matching_review_ids()
    bulk_bar = st.empty()

    for r in page_rows:
        with st.container(border=True):
            top_left, top_right = st.columns([4, 1])
            with top_left:
                title = r["title"] or "—"
                store = _display_name(r["competitor_id"])
                brand = r["brand"] or ""
                price = float(r["price_eur"]) if r["price_eur"] is not None else None
                st.markdown(f"**[{store}]** {title}")
                meta = [brand]
                if price is not None:
                    meta.append(f"€ {price:.2f}")
                if r["url"]:
                    meta.append(f"[link]({r['url']})")
                st.caption(" · ".join(m for m in meta if m))

                cluster_label = (
                    f"EAN cluster `{r['cluster_ean']}`" if r["cluster_ean"]
                    else f"Fuzzy cluster #{r['cluster_id']}"
                )
                cluster_title = r["cluster_title"] or "—"
                st.caption(f"→ joins {cluster_label}: {cluster_title}")

                peer_list = [p for p in peers.get(r["cluster_id"], []) if p["competitor_id"] != r["competitor_id"]]
                if peer_list:
                    peer_text = "; ".join(
                        f"[{_display_name(p['competitor_id'])}] {(p['title'] or '')[:40]}"
                        + (f" — € {p['price_eur']:.2f}" if p["price_eur"] is not None else "")
                        for p in peer_list[:5]
                    )
                    st.caption(f"Peers: {peer_text}")

                sim = float(r["similarity"]) if r["similarity"] is not None else None
                conf = float(r["llm_confidence"]) if r["llm_confidence"] is not None else None
                st.caption(
                    f"Similarity: {sim:.3f}  ·  LLM conf: {conf:.2f}  ·  Status: **{r['status']}**"
                    if sim is not None and conf is not None
                    else f"Status: **{r['status']}**"
                )

            with top_right:
                mid = int(r["member_id"])
                if r["status"] == "pending":
                    checkbox_key = f"mr_select_{mid}"
                    if checkbox_key not in st.session_state:
                        st.session_state[checkbox_key] = mid in selected_pending_ids
                    if st.checkbox("Select", key=checkbox_key):
                        selected_pending_ids.add(mid)
                    else:
                        selected_pending_ids.discard(mid)
                    if st.button("✓ Approve", key=f"app_{mid}", use_container_width=True, type="primary"):
                        _set_member_status(mid, "approved")
                        _discard_selected_matching_review_ids({mid})
                        st.rerun()
                    if st.button("✗ Reject", key=f"rej_{mid}", use_container_width=True):
                        _set_member_status(mid, "rejected")
                        _discard_selected_matching_review_ids({mid})
                        st.rerun()
                elif r["status"] == "approved":
                    if st.button("↶ Revoke", key=f"rev_{mid}", use_container_width=True):
                        _set_member_status(mid, "rejected")
                        st.rerun()
                else:  # rejected
                    if st.button("↺ Re-approve", key=f"reapp_{mid}", use_container_width=True):
                        _set_member_status(mid, "approved")
                        st.rerun()

    _store_selected_matching_review_ids(selected_pending_ids)
    if pending_page_ids:
        selected_count = len(selected_pending_ids)
        selected_list = sorted(selected_pending_ids)
        with bulk_bar.container():
            bulk_left, bulk_mid, bulk_right = st.columns([1, 1, 4])
            with bulk_left:
                st.caption(f"{selected_count} selected")
            with bulk_mid:
                if st.button(
                    "✓ Approve selected",
                    key="mr_approve_selected",
                    disabled=not selected_list,
                    use_container_width=True,
                    type="primary",
                ):
                    updated = _approve_pending_members(selected_list)
                    _discard_selected_matching_review_ids(set(selected_list))
                    st.success(f"Approved {updated} selected match{'es' if updated != 1 else ''}.")
                    st.rerun()
            with bulk_right:
                st.empty()

    # Pagination controls
    st.divider()
    nav_left, nav_mid, nav_right = st.columns([1, 2, 1])
    with nav_left:
        if st.button("← Previous", disabled=(page_num <= 1), use_container_width=True, key="mr_prev"):
            st.session_state["mr_page"] = page_num - 1
            st.rerun()
    with nav_mid:
        st.markdown(
            f"<div style='text-align:center; padding-top:6px;'>Page {page_num} of {total_pages}"
            f"  —  showing {start + 1}–{min(start + per_page, len(rows))} of {len(rows)}</div>",
            unsafe_allow_html=True,
        )
    with nav_right:
        if st.button("Next →", disabled=(page_num >= total_pages), use_container_width=True, key="mr_next"):
            st.session_state["mr_page"] = page_num + 1
            st.rerun()


# ===========================================================================
# Wire up tabs
# ===========================================================================

with tab1:
    _render_product_overview_tab()

with tab2:
    _render_search_tab()

with tab3:
    _render_price_compare_tab()

with tab4:
    _render_manufacturer_tab()

with tab5:
    _render_compare_tab()

with tab6:
    _render_matching_review_tab()
