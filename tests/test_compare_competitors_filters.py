from __future__ import annotations

from pathlib import Path
import ast
import copy

import pandas as pd


def _load_compare_helpers():
    app_path = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
    source = app_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(app_path))

    wanted = {
        "_available_compare_brands",
        "_build_product_overview_frame",
        "_build_product_overlap_frames",
        "_build_product_overlap_layout",
        "_build_lower_price_rate_frame",
        "_compare_brand_match_counts",
        "_dashboard_theme_css",
        "_dashboard_theme_tokens",
        "_dashboard_top_bar_columns",
        "_normalize_dashboard_theme",
        "_filter_compare_rows",
        "_product_search_match_lookup",
        "_toggle_compare_brand_selection",
    }
    selected_nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    module = ast.Module(body=selected_nodes, type_ignores=[])
    namespace: dict[str, object] = {"pd": pd}
    exec(compile(module, str(app_path), "exec"), namespace)
    return namespace


def test_available_compare_brands_are_sorted_and_unique() -> None:
    helpers = _load_compare_helpers()
    rows = [
        {"brand": "Wiha", "wins": 2},
        {"brand": "Baupro", "wins": 0},
        {"brand": "Wiha", "wins": 1},
        {"brand": "", "wins": 1},
        {"brand": None, "wins": 1},
    ]

    brands = helpers["_available_compare_brands"](rows)

    assert brands == ["Baupro", "Wiha"]


def test_compare_brand_match_counts_counts_rows_per_brand() -> None:
    helpers = _load_compare_helpers()
    rows = [
        {"brand": "Wiha", "wins": 2},
        {"brand": "Baupro", "wins": 0},
        {"brand": "Wiha", "wins": 1},
        {"brand": "", "wins": 1},
        {"brand": None, "wins": 1},
    ]

    counts = helpers["_compare_brand_match_counts"](rows)

    assert counts == {"Baupro": 1, "Wiha": 2}


def test_filter_compare_rows_applies_outcome_and_brand_filters() -> None:
    helpers = _load_compare_helpers()
    merged = [
        {"brand": "Wiha", "wins": 2, "title": "A"},
        {"brand": "Baupro", "wins": 0, "title": "B"},
        {"brand": "Wiha", "wins": 0, "title": "C"},
        {"brand": "Knipex", "wins": 1, "title": "D"},
    ]
    original = copy.deepcopy(merged)

    rows = helpers["_filter_compare_rows"](
        merged,
        filter_opt="❌ Weaker (loses to all)",
        selected_brands=["Wiha", "Knipex"],
        n_opp=2,
    )

    assert rows == [{"brand": "Wiha", "wins": 0, "title": "C"}]
    assert merged == original


def test_filter_compare_rows_with_no_selected_brands_returns_no_rows() -> None:
    helpers = _load_compare_helpers()
    merged = [
        {"brand": "Wiha", "wins": 2, "title": "A"},
        {"brand": "Baupro", "wins": 0, "title": "B"},
    ]

    rows = helpers["_filter_compare_rows"](
        merged,
        filter_opt="All products",
        selected_brands=[],
        n_opp=2,
    )

    assert rows == []


def test_toggle_compare_brand_selection_adds_and_removes_brand() -> None:
    helpers = _load_compare_helpers()

    selected = helpers["_toggle_compare_brand_selection"](["Baupro", "Wiha"], "Wiha")
    assert selected == ["Baupro"]

    selected = helpers["_toggle_compare_brand_selection"](selected, "Knipex")
    assert selected == ["Baupro", "Knipex"]


def test_dashboard_theme_defaults_to_dark_and_exposes_chart_tokens() -> None:
    helpers = _load_compare_helpers()

    assert helpers["_normalize_dashboard_theme"](None) == "dark"
    assert helpers["_normalize_dashboard_theme"]("unexpected") == "dark"
    assert helpers["_normalize_dashboard_theme"]("light") == "light"

    dark = helpers["_dashboard_theme_tokens"]("dark")
    light = helpers["_dashboard_theme_tokens"]("light")

    assert dark["mode"] == "dark"
    assert dark["plotly_template"] == "plotly_dark"
    assert light["mode"] == "light"
    assert light["plotly_template"] == "plotly_white"
    assert dark["surface"] != light["surface"]
    assert dark["text"] != light["text"]


def test_dashboard_theme_css_uses_selected_palette() -> None:
    helpers = _load_compare_helpers()

    css = helpers["_dashboard_theme_css"]("light")

    assert "<style>" in css
    assert helpers["_dashboard_theme_tokens"]("light")["background"] in css
    assert helpers["_dashboard_theme_tokens"]("light")["accent"] in css
    assert 'data-baseweb="select"' in css
    assert ".stTextInput input" in css
    assert 'kind="primaryFormSubmit"' in css
    assert 'kind="secondaryFormSubmit"' in css
    assert "focus-within" in css
    assert "accent-color: var(--tz-accent)" in css
    assert "box-shadow: inset 0 -2px 0 var(--tz-accent)" in css


def test_dashboard_top_bar_reserves_small_space_for_theme_toggle() -> None:
    helpers = _load_compare_helpers()

    columns = helpers["_dashboard_top_bar_columns"]()

    assert len(columns) == 2
    assert sum(columns) == 1.0
    assert columns[1] <= 0.16


def test_product_search_match_lookup_keeps_skus_separate() -> None:
    helpers = _load_compare_helpers()

    class Match:
        def __init__(self, competitor_id: str, competitor_sku: str, match_type: str, confidence: float) -> None:
            self.competitor_id = competitor_id
            self.competitor_sku = competitor_sku
            self.match_type = match_type
            self.confidence = confidence

    lookup = helpers["_product_search_match_lookup"]([
        Match("example_sk", "A", "exact_ean", 1.0),
        Match("example_sk", "B", "llm_fuzzy", 0.86),
    ])

    assert lookup[("example_sk", "A")] == ("exact_ean", 1.0)
    assert lookup[("example_sk", "B")] == ("llm_fuzzy", 0.86)


def test_build_product_overview_frame_uses_all_time_and_fresh_30d_counts() -> None:
    helpers = _load_compare_helpers()

    frame = helpers["_build_product_overview_frame"](
        all_configs=[
            {"id": "toolzone_sk", "name": "ToolZone", "own_store": True},
            {"id": "a_sk", "name": "Alpha"},
            {"id": "b_sk", "name": "Beta"},
        ],
        all_time_df=pd.DataFrame([
            {"competitor_id": "a_sk", "listings_total": 100, "last_scraped": "2026-04-25"},
            {"competitor_id": "b_sk", "listings_total": 50, "last_scraped": "2026-04-20"},
            {"competitor_id": "toolzone_sk", "listings_total": 200, "last_scraped": "2026-04-26"},
        ]),
        fresh_df=pd.DataFrame([
            {"competitor_id": "a_sk", "listings_30d": 80},
            {"competitor_id": "toolzone_sk", "listings_30d": 190},
        ]),
        match_df=pd.DataFrame([
            {"competitor_id": "a_sk", "matches": 40},
            {"competitor_id": "b_sk", "matches": 10},
        ]),
        own_store_ids_value={"toolzone_sk"},
    )

    competitors = frame[~frame["own_store"]]
    alpha = competitors[competitors["competitor_id"] == "a_sk"].iloc[0]
    beta = competitors[competitors["competitor_id"] == "b_sk"].iloc[0]

    assert list(competitors["competitor_id"]) == ["a_sk", "b_sk"]
    assert alpha["listings_total"] == 100
    assert alpha["listings_30d"] == 80
    assert alpha["matches"] == 40
    assert alpha["match_rate"] == 40.0
    assert alpha["fresh_share"] == 80.0
    assert beta["listings_30d"] == 0
    assert beta["fresh_share"] == 0.0


def test_build_lower_price_rate_frame_counts_pairwise_wins() -> None:
    helpers = _load_compare_helpers()

    frame = helpers["_build_lower_price_rate_frame"](
        pd.DataFrame([
            {"cluster_id": 1, "competitor_id": "a_sk", "price_eur": 10.0},
            {"cluster_id": 1, "competitor_id": "b_sk", "price_eur": 12.0},
            {"cluster_id": 1, "competitor_id": "c_sk", "price_eur": 9.0},
            {"cluster_id": 2, "competitor_id": "a_sk", "price_eur": 20.0},
            {"cluster_id": 2, "competitor_id": "b_sk", "price_eur": 18.0},
        ]),
        {"a_sk", "b_sk", "c_sk"},
    )

    rows = {row["competitor_id"]: row for row in frame.to_dict("records")}

    assert rows["a_sk"]["price_comparisons"] == 3
    assert rows["a_sk"]["lower_price_wins"] == 1
    assert rows["a_sk"]["lower_price_rate"] == 33.3
    assert rows["b_sk"]["lower_price_rate"] == 33.3
    assert rows["c_sk"]["lower_price_rate"] == 100.0


def test_build_product_overlap_frames_sizes_products_and_pair_overlap() -> None:
    helpers = _load_compare_helpers()

    product_counts, overlap_points = helpers["_build_product_overlap_frames"](
        pd.DataFrame([
            {"cluster_id": 1, "competitor_id": "a_sk"},
            {"cluster_id": 2, "competitor_id": "a_sk"},
            {"cluster_id": 3, "competitor_id": "a_sk"},
            {"cluster_id": 2, "competitor_id": "b_sk"},
            {"cluster_id": 3, "competitor_id": "b_sk"},
            {"cluster_id": 4, "competitor_id": "b_sk"},
            {"cluster_id": 3, "competitor_id": "c_sk"},
        ]),
        {"a_sk", "b_sk", "c_sk"},
        top_n=2,
    )

    counts = {row["competitor_id"]: row["product_count"] for row in product_counts.to_dict("records")}
    assert counts == {"a_sk": 3, "b_sk": 3, "c_sk": 1}

    pair_labels = list(overlap_points["pair_label"].unique())
    assert pair_labels[0] == "a_sk / b_sk · 2 matched"
    assert len(overlap_points[overlap_points["pair_label"] == pair_labels[0]]) == 2
    assert set(overlap_points[overlap_points["pair_label"] == pair_labels[0]]["products"]) == {3}


def test_build_product_overlap_layout_emits_nodes_and_edges() -> None:
    helpers = _load_compare_helpers()

    nodes, edges = helpers["_build_product_overlap_layout"](
        pd.DataFrame([
            {"cluster_id": 1, "competitor_id": "a_sk"},
            {"cluster_id": 2, "competitor_id": "a_sk"},
            {"cluster_id": 3, "competitor_id": "a_sk"},
            {"cluster_id": 2, "competitor_id": "b_sk"},
            {"cluster_id": 3, "competitor_id": "b_sk"},
            {"cluster_id": 4, "competitor_id": "b_sk"},
            {"cluster_id": 5, "competitor_id": "c_sk"},
        ]),
        {"a_sk", "b_sk", "c_sk"},
        iterations=50,
    )

    rows = {row["competitor_id"]: row for row in nodes.to_dict("records")}
    assert set(rows) == {"a_sk", "b_sk", "c_sk"}
    assert rows["a_sk"]["products"] == 3
    assert rows["b_sk"]["products"] == 3
    assert rows["c_sk"]["products"] == 1
    assert rows["a_sk"]["radius"] > rows["c_sk"]["radius"]

    a_partners = rows["a_sk"]["partners"]
    assert len(a_partners) == 1
    assert a_partners[0]["competitor_id"] == "b_sk"
    assert a_partners[0]["overlap_products"] == 2
    assert a_partners[0]["overlap_rate"] > 60.0
    assert rows["c_sk"]["partners"] == []

    edge_rows = edges.to_dict("records")
    assert len(edge_rows) == 1
    assert {edge_rows[0]["left_id"], edge_rows[0]["right_id"]} == {"a_sk", "b_sk"}
    assert edge_rows[0]["overlap_products"] == 2
    assert edge_rows[0]["label"].endswith("%")


def test_build_product_overlap_layout_returns_empty_when_no_data() -> None:
    helpers = _load_compare_helpers()

    nodes, edges = helpers["_build_product_overlap_layout"](
        pd.DataFrame(columns=["cluster_id", "competitor_id"]),
        {"a_sk", "b_sk"},
    )

    assert nodes.empty
    assert edges.empty
    assert list(nodes.columns) == [
        "competitor_id",
        "x",
        "y",
        "products",
        "radius",
        "partners",
    ]
    assert list(edges.columns) == [
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
