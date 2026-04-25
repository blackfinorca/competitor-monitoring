from __future__ import annotations

from pathlib import Path
import ast
import copy


def _load_compare_helpers():
    app_path = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
    source = app_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(app_path))

    wanted = {
        "_available_compare_brands",
        "_compare_brand_match_counts",
        "_filter_compare_rows",
        "_toggle_compare_brand_selection",
    }
    selected_nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    module = ast.Module(body=selected_nodes, type_ignores=[])
    namespace: dict[str, object] = {}
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
