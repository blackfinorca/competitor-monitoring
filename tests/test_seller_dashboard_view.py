"""Unit tests for the pure helpers in dashboard.seller_dashboard_view."""

from __future__ import annotations

import dashboard.seller_dashboard_view as v


# --------------------------------------------------------------------------- #
# bucket_code                                                                 #
# --------------------------------------------------------------------------- #


def test_bucket_code_boundaries():
    assert v.bucket_code(-11) == "A"
    assert v.bucket_code(-10) == "A"
    assert v.bucket_code(-5) == "B"
    assert v.bucket_code(-2) == "B"
    assert v.bucket_code(0) == "C"
    assert v.bucket_code(2) == "C"
    assert v.bucket_code(5) == "D"
    assert v.bucket_code(10) == "D"
    assert v.bucket_code(11) == "E"
    assert v.bucket_code(None) == "M"


# --------------------------------------------------------------------------- #
# compute_per_sku                                                             #
# --------------------------------------------------------------------------- #


def _data_with_three_skus() -> dict:
    # ref lists three SKUs:
    #   E1: ref @ 100, rival @ 80   => ref pricier (gap +25%)  bucket E
    #   E2: ref @ 50,  rival @ 100  => ref cheaper (gap -50%)  bucket A
    #   E3: ref @ 20  alone         => monopoly                bucket M
    return {
        "top_sellers": ["ref", "rival"],
        "all_sellers": ["ref", "rival"],
        "titles": {"E1": "t1", "E2": "t2", "E3": "t3"},
        "offers": [
            {"e": "E1", "s": "ref", "p": 90.0, "d": 10.0, "t": 100.0},
            {"e": "E1", "s": "rival", "p": 70.0, "d": 10.0, "t": 80.0},
            {"e": "E2", "s": "ref", "p": 45.0, "d": 5.0, "t": 50.0},
            {"e": "E2", "s": "rival", "p": 95.0, "d": 5.0, "t": 100.0},
            {"e": "E3", "s": "ref", "p": 18.0, "d": 2.0, "t": 20.0},
        ],
    }


def test_compute_per_sku_signs_and_buckets():
    rows = v.compute_per_sku(_data_with_three_skus(), "ref")
    by_ean = {r["ean"]: r for r in rows}
    assert set(by_ean) == {"E1", "E2", "E3"}

    e1 = by_ean["E1"]
    assert e1["bestSeller"] == "rival"
    assert e1["bestTotal"] == 80.0
    assert e1["gapPct"] == 25.0
    assert e1["bucket"] == "E"
    assert e1["compCount"] == 1
    assert e1["compSellers"] == ["rival"]

    e2 = by_ean["E2"]
    assert e2["gapPct"] == -50.0
    assert e2["bucket"] == "A"

    e3 = by_ean["E3"]
    assert e3["gapPct"] is None
    assert e3["bestSeller"] is None
    assert e3["compCount"] == 0
    assert e3["bucket"] == "M"


def test_compute_per_sku_uses_competitors_not_own_store_as_best_rival():
    data = {
        "own_store_ids": ["toolzone_sk"],
        "top_sellers": ["agi_sk", "toolzone_sk", "rival"],
        "all_sellers": ["agi_sk", "toolzone_sk", "rival"],
        "titles": {"E1": "abrasive disc"},
        "offers": [
            {"e": "E1", "s": "agi_sk", "p": 100.0, "d": None, "t": 100.0},
            {"e": "E1", "s": "toolzone_sk", "p": 1.0, "d": None, "t": 1.0},
            {"e": "E1", "s": "rival", "p": 80.0, "d": None, "t": 80.0},
        ],
    }

    row = v.compute_per_sku(data, "agi_sk")[0]

    assert row["bestSeller"] == "rival"
    assert row["bestTotal"] == 80.0
    assert row["gapPct"] == 25.0
    assert row["compCount"] == 1
    assert row["compSellers"] == ["rival"]


def test_compute_per_sku_limits_best_rival_to_selected_competitors():
    data = {
        "top_sellers": ["agi_sk", "cheap_unselected", "selected_rival"],
        "all_sellers": ["agi_sk", "cheap_unselected", "selected_rival"],
        "titles": {"E1": "abrasive disc"},
        "offers": [
            {"e": "E1", "s": "agi_sk", "p": 100.0, "d": None, "t": 100.0},
            {"e": "E1", "s": "cheap_unselected", "p": 1.0, "d": None, "t": 1.0},
            {"e": "E1", "s": "selected_rival", "p": 80.0, "d": None, "t": 80.0},
        ],
    }

    row = v.compute_per_sku(data, "agi_sk", comparison_sellers={"selected_rival"})[0]

    assert row["bestSeller"] == "selected_rival"
    assert row["bestTotal"] == 80.0
    assert row["gapPct"] == 25.0
    assert row["compCount"] == 1
    assert row["compSellers"] == ["selected_rival"]


def test_compute_per_sku_flags_large_price_outliers_for_review():
    data = {
        "top_sellers": ["ref", "rival"],
        "all_sellers": ["ref", "rival"],
        "titles": {"E1": "huge gap", "E2": "normal gap"},
        "offers": [
            {"e": "E1", "s": "ref", "p": 500.0, "d": None, "t": 500.0},
            {"e": "E1", "s": "rival", "p": 100.0, "d": None, "t": 100.0},
            {"e": "E2", "s": "ref", "p": 120.0, "d": None, "t": 120.0},
            {"e": "E2", "s": "rival", "p": 100.0, "d": None, "t": 100.0},
        ],
    }

    rows = {r["ean"]: r for r in v.compute_per_sku(data, "ref")}

    assert rows["E1"]["gapPct"] == 400.0
    assert rows["E1"]["priceOutlier"] is True
    assert rows["E2"]["priceOutlier"] is False


def test_price_scatter_rows_require_all_formatted_values():
    rows = [
        {
            "ean": "E1",
            "refTotal": None,
            "bestTotal": 10.0,
            "gapPct": None,
        },
        {
            "ean": "E2",
            "refTotal": 12.0,
            "bestTotal": None,
            "gapPct": None,
        },
        {
            "ean": "E3",
            "refTotal": 12.0,
            "bestTotal": 10.0,
            "gapPct": 20.0,
        },
        {
            "ean": "E4",
            "refTotal": 500.0,
            "bestTotal": 100.0,
            "gapPct": 400.0,
            "priceOutlier": True,
        },
    ]

    assert v.price_scatter_rows(rows) == [rows[2]]


def test_selected_scatter_point_extracts_clicked_point_customdata():
    event = {
        "selection": {
            "points": [
                {
                    "point_index": 2,
                    "customdata": [
                        "4003773022022",
                        "Knipex Cobra",
                        "rival",
                        12.5,
                        10.0,
                        25.0,
                    ],
                }
            ]
        }
    }

    selected = v.selected_scatter_point(event)

    assert selected == {
        "point_index": 2,
        "ean": "4003773022022",
        "title": "Knipex Cobra",
        "bestSeller": "rival",
        "refTotal": 12.5,
        "bestTotal": 10.0,
        "gapPct": 25.0,
    }


def test_selected_scatter_point_returns_none_without_selection():
    assert v.selected_scatter_point({"selection": {"points": []}}) is None
    assert v.selected_scatter_point(None) is None


def test_selected_marker_style_is_valid_for_plotly_scatter():
    import plotly.graph_objects as go

    go.Scatter(selected=v.scatter_selected_style())


def test_plotly_layout_for_theme_defaults_and_switches_templates():
    dark = v.plotly_layout_for_theme(None)
    light = v.plotly_layout_for_theme({
        "plotly_template": "plotly_white",
        "surface": "#ffffff",
        "text": "#111827",
        "grid": "#e5e7eb",
    })

    assert dark["template"] == "plotly_dark"
    assert light["template"] == "plotly_white"
    assert light["paper_bgcolor"] == "#ffffff"
    assert light["font"]["color"] == "#111827"
    assert light["xaxis"]["gridcolor"] == "#e5e7eb"


# --------------------------------------------------------------------------- #
# head_to_head_rows                                                           #
# --------------------------------------------------------------------------- #


def test_head_to_head_rows_overlap_and_winrate():
    data = {
        "top_sellers": ["ref", "rivalA", "rivalB", "rivalC"],
        "offers": [
            # rivalA shares 2 SKUs with ref: ref wins one, rivalA wins one
            {"e": "X1", "s": "ref", "p": None, "d": None, "t": 100.0},
            {"e": "X2", "s": "ref", "p": None, "d": None, "t": 100.0},
            {"e": "X1", "s": "rivalA", "p": None, "d": None, "t": 120.0},  # ref cheaper
            {"e": "X2", "s": "rivalA", "p": None, "d": None, "t": 80.0},   # comp cheaper
            # rivalB shares 1 SKU, same price
            {"e": "X1", "s": "rivalB", "p": None, "d": None, "t": 100.0},
            # rivalC has no overlap
            {"e": "Y1", "s": "rivalC", "p": None, "d": None, "t": 50.0},
        ],
    }
    rows = v.head_to_head_rows(data, "ref")
    by_seller = {r["seller"]: r for r in rows}

    assert "rivalC" not in by_seller  # no overlap
    assert by_seller["rivalA"]["overlap"] == 2
    assert by_seller["rivalA"]["refCheaper"] == 1
    assert by_seller["rivalA"]["compCheaper"] == 1
    assert by_seller["rivalA"]["same"] == 0
    assert by_seller["rivalA"]["winRate"] == 50.0

    assert by_seller["rivalB"]["overlap"] == 1
    assert by_seller["rivalB"]["same"] == 1
    assert by_seller["rivalB"]["winRate"] == 0.0
    assert by_seller["rivalB"]["medianGap"] == 0.0


# --------------------------------------------------------------------------- #
# overlap_rows                                                                #
# --------------------------------------------------------------------------- #


def test_overlap_rows_filters_and_percentages():
    # Build a data dict where:
    #   - ref carries 40 SKUs
    #   - bigA carries 50 SKUs, 20 overlap with ref
    #   - tiny carries  5 SKUs, all overlap   (must be filtered)
    ref_eans = [f"R{i}" for i in range(40)]
    bigA_eans = [f"R{i}" for i in range(20)] + [f"A{i}" for i in range(30)]
    tiny_eans = [f"R{i}" for i in range(5)]

    offers: list[dict] = []
    for e in ref_eans:
        offers.append({"e": e, "s": "ref", "p": 1.0, "d": 0.0, "t": 1.0})
    for e in bigA_eans:
        offers.append({"e": e, "s": "bigA", "p": 1.0, "d": 0.0, "t": 1.0})
    for e in tiny_eans:
        offers.append({"e": e, "s": "tiny", "p": 1.0, "d": 0.0, "t": 1.0})

    data = {"top_sellers": ["ref"], "offers": offers}
    rows = v.overlap_rows(data, "ref")

    sellers = {r["seller"] for r in rows}
    assert "tiny" not in sellers  # filtered (< 30 SKUs)
    assert "bigA" in sellers
    assert "ref" not in sellers

    bigA = next(r for r in rows if r["seller"] == "bigA")
    assert bigA["sellerSkus"] == 50
    assert bigA["overlap"] == 20
    assert bigA["pctOfSeller"] == 40.0
    assert bigA["pctOfRef"] == 50.0
