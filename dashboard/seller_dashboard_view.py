"""Streamlit rendering for the per-seller "Product Overview" dashboard.

The four pure helpers at the top of this module are importable without any
Streamlit / Plotly side effects so they can be unit-tested directly.
"""

from __future__ import annotations

from collections import defaultdict
from statistics import median
from typing import Any


# --------------------------------------------------------------------------- #
# Pure helpers                                                                #
# --------------------------------------------------------------------------- #


def bucket_code(gap_pct: float | None) -> str:
    if gap_pct is None:
        return "M"
    if gap_pct <= -10:
        return "A"
    if gap_pct <= -2:
        return "B"
    if gap_pct <= 2:
        return "C"
    if gap_pct <= 10:
        return "D"
    return "E"


def _index_offers(offers: list[dict]) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    by_ean: dict[str, list[dict]] = defaultdict(list)
    by_seller: dict[str, list[dict]] = defaultdict(list)
    for o in offers:
        by_ean[o["e"]].append(o)
        by_seller[o["s"]].append(o)
    return by_ean, by_seller


def compute_per_sku(data: dict, ref: str) -> list[dict]:
    by_ean, by_seller = _index_offers(data.get("offers", []))
    titles = data.get("titles", {})
    rows: list[dict] = []
    for o in by_seller.get(ref, []):
        all_on_ean = by_ean.get(o["e"], [])
        best_other = None
        others_count = 0
        others: list[str] = []
        for x in all_on_ean:
            if x["s"] == ref:
                continue
            others_count += 1
            others.append(x["s"])
            if best_other is None or x["t"] < best_other["t"]:
                best_other = x
        gap = (
            (o["t"] - best_other["t"]) / best_other["t"] * 100.0
            if best_other and best_other["t"]
            else None
        )
        rows.append(
            {
                "ean": o["e"],
                "title": titles.get(o["e"], ""),
                "refTotal": o["t"],
                "refPrice": o.get("p"),
                "refDelivery": o.get("d"),
                "bestSeller": best_other["s"] if best_other else None,
                "bestTotal": best_other["t"] if best_other else None,
                "compCount": others_count,
                "compSellers": others,
                "gapPct": gap,
                "bucket": bucket_code(gap),
            }
        )
    return rows


def price_scatter_rows(per_sku: list[dict]) -> list[dict]:
    return [
        r
        for r in per_sku
        if r.get("refTotal") is not None
        and r.get("bestTotal") is not None
        and r.get("gapPct") is not None
    ]


def head_to_head_rows(data: dict, ref: str) -> list[dict]:
    _, by_seller = _index_offers(data.get("offers", []))
    ref_map = {o["e"]: o["t"] for o in by_seller.get(ref, [])}
    out: list[dict] = []
    for s in data.get("top_sellers", []):
        if s == ref:
            continue
        overlap = ref_cheaper = comp_cheaper = same = 0
        gaps: list[float] = []
        for o in by_seller.get(s, []):
            ref_t = ref_map.get(o["e"])
            if ref_t is None:
                continue
            overlap += 1
            if o["t"] < ref_t:
                comp_cheaper += 1
            elif o["t"] > ref_t:
                ref_cheaper += 1
            else:
                same += 1
            if o["t"]:
                gaps.append((ref_t - o["t"]) / o["t"] * 100.0)
        if overlap == 0:
            continue
        out.append(
            {
                "seller": s,
                "overlap": overlap,
                "refCheaper": ref_cheaper,
                "compCheaper": comp_cheaper,
                "same": same,
                "winRate": ref_cheaper / overlap * 100.0,
                "medianGap": median(gaps) if gaps else None,
            }
        )
    out.sort(key=lambda r: r["winRate"], reverse=True)
    return out


def overlap_rows(data: dict, ref: str) -> list[dict]:
    _, by_seller = _index_offers(data.get("offers", []))
    ref_eans = {o["e"] for o in by_seller.get(ref, [])}
    ref_size = len(ref_eans)
    rows: list[dict] = []
    for s, offers in by_seller.items():
        if s == ref:
            continue
        s_eans = {o["e"] for o in offers}
        if len(s_eans) < 30:
            continue
        ov = len(s_eans & ref_eans)
        rows.append(
            {
                "seller": s,
                "sellerSkus": len(s_eans),
                "overlap": ov,
                "pctOfSeller": ov / len(s_eans) * 100.0 if s_eans else 0.0,
                "pctOfRef": ov / ref_size * 100.0 if ref_size else 0.0,
            }
        )
    rows.sort(key=lambda r: r["overlap"], reverse=True)
    return rows


# --------------------------------------------------------------------------- #
# Streamlit renderer                                                          #
# --------------------------------------------------------------------------- #


_BUCKET_LABELS = {
    "A": "A: Deep cheaper",
    "B": "B: Cheaper",
    "C": "C: Parity",
    "D": "D: Pricier",
    "E": "E: Deep pricier",
    "M": "No competitor",
}
_BUCKET_KEYS = ["A", "B", "C", "D", "E", "M"]
_BUCKET_COLORS = {
    "A": "#16a34a",
    "B": "#86efac",
    "C": "#94a3b8",
    "D": "#fbbf24",
    "E": "#dc2626",
    "M": "#cbd5e1",
}


def _fmt_eur(v: float | None) -> str:
    return "-" if v is None else f"{v:.2f} €"


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "-"
    return ("+" if v >= 0 else "") + f"{v:.1f}%"


def _tick(name: str, highlighted: set[str]) -> str:
    return f"<b>{name}</b>" if name in highlighted else name


def plotly_layout_for_theme(theme: dict | None) -> dict:
    theme = theme or {}
    return {
        "template": theme.get("plotly_template", "plotly_dark"),
        "paper_bgcolor": theme.get("surface", "#191d24"),
        "plot_bgcolor": theme.get("surface", "#191d24"),
        "font": {"color": theme.get("text", "#f6f7fb")},
        "xaxis": {"gridcolor": theme.get("grid", "#344052")},
        "yaxis": {"gridcolor": theme.get("grid", "#344052")},
    }


def render_seller_dashboard(data: dict, theme: dict | None = None) -> None:
    import streamlit as st
    import plotly.graph_objects as go

    chart_layout = plotly_layout_for_theme(theme)
    chart_base_layout = {
        key: value for key, value in chart_layout.items()
        if key not in {"xaxis", "yaxis"}
    }
    chart_xaxis = dict(chart_layout["xaxis"])
    chart_yaxis = dict(chart_layout["yaxis"])
    parity_color = str((theme or {}).get("muted", "#94a3b8"))
    primary_color = str((theme or {}).get("accent", "#2563eb"))
    neutral_color = str((theme or {}).get("border", "#cbd5e1"))

    top_sellers: list[str] = list(data.get("top_sellers") or [])
    all_sellers: list[str] = list(data.get("all_sellers") or [])

    if not top_sellers:
        st.info("No top sellers in this snapshot.")
        return

    # -- 1. Reference selector --------------------------------------------- #
    ref_key = "seller_dashboard_ref"
    hl_key = "seller_dashboard_highlight"
    if st.session_state.get(ref_key) not in top_sellers:
        st.session_state[ref_key] = top_sellers[0]
    if hl_key not in st.session_state:
        st.session_state[hl_key] = []

    ref = st.radio(
        "Reference seller",
        top_sellers,
        index=top_sellers.index(st.session_state[ref_key]),
        horizontal=True,
        key=ref_key,
    )
    highlighted_list = st.multiselect(
        "Highlight competitors",
        all_sellers,
        default=st.session_state[hl_key],
        key=hl_key,
    )
    highlighted = set(highlighted_list)

    # -- 2. Header line ---------------------------------------------------- #
    st.caption(
        f"Snapshot {data.get('snapshot_date', '?')} - "
        f"{data.get('eans_total', 0):,} EANs - "
        f"{data.get('sellers_total', 0):,} sellers - "
        f"{data.get('offers_total', 0):,} live offers"
    )

    per_sku = compute_per_sku(data, ref)
    by_ean, by_seller = _index_offers(data.get("offers", []))
    titles: dict[str, str] = data.get("titles", {})

    # -- 3. KPI row -------------------------------------------------------- #
    comp = [r for r in per_sku if r["gapPct"] is not None]
    win_rate = (
        sum(1 for r in comp if r["gapPct"] <= 0) / len(comp) * 100.0 if comp else 0.0
    )
    deep_pricey = sum(1 for r in per_sku if r["gapPct"] is not None and r["gapPct"] >= 10)
    deep_cheap = sum(1 for r in per_sku if r["gapPct"] is not None and r["gapPct"] <= -10)
    mono = sum(1 for r in per_sku if r["compCount"] == 0)
    med = median([r["gapPct"] for r in comp]) if comp else None

    cols = st.columns(7)
    cols[0].metric(f"{ref} SKUs", f"{len(per_sku):,}")
    cols[1].metric("Win rate", f"{win_rate:.1f}%")
    cols[2].metric("Comparable", f"{len(comp):,}")
    cols[3].metric("Deep pricier", f"{deep_pricey:,}")
    cols[4].metric("Deep cheaper", f"{deep_cheap:,}")
    cols[5].metric("Monopoly", f"{mono:,}")
    cols[6].metric("Median gap", _fmt_pct(med))

    # -- 4. Pricing position bucket bar ------------------------------------ #
    st.subheader(f"1. Pricing position - {ref}")
    counts = {k: 0 for k in _BUCKET_KEYS}
    for r in per_sku:
        counts[r["bucket"]] += 1
    bucket_vals = [counts[k] for k in _BUCKET_KEYS]
    bucket_fig = go.Figure(
        data=[
            go.Bar(
                x=[_BUCKET_LABELS[k] for k in _BUCKET_KEYS],
                y=bucket_vals,
                marker_color=[_BUCKET_COLORS[k] for k in _BUCKET_KEYS],
                text=[f"{v:,}" for v in bucket_vals],
                textposition="outside",
            )
        ]
    )
    bucket_fig.update_layout(
        **chart_base_layout,
        height=380,
        margin=dict(l=50, r=20, t=10, b=80),
        xaxis={**chart_xaxis, "tickangle": -15},
        yaxis={**chart_yaxis, "title": "SKU count"},
    )
    st.plotly_chart(bucket_fig, use_container_width=True)

    # -- 5. Reference vs cheapest competitor scatter ----------------------- #
    st.subheader(f"2. {ref} vs cheapest competitor")
    scatter_rows = price_scatter_rows(per_sku)
    if scatter_rows:
        colors = [_BUCKET_COLORS[r["bucket"]] for r in scatter_rows]
        hover = [
            (
                f"EAN: {r['ean']}<br>"
                f"{ref}: {r['refTotal']:.2f} EUR<br>"
                f"Best rival: {_tick(r['bestSeller'] or '-', highlighted)} @ {r['bestTotal']:.2f} EUR<br>"
                f"Gap: {r['gapPct']:.1f}%<br>"
                f"Rivals: {r['compCount']}"
            )
            for r in scatter_rows
        ]
        max_v = max(
            [1.0]
            + [max(r["refTotal"], r["bestTotal"]) for r in scatter_rows]
        )
        scatter_fig = go.Figure()
        scatter_fig.add_trace(
            go.Scatter(
                x=[r["bestTotal"] for r in scatter_rows],
                y=[r["refTotal"] for r in scatter_rows],
                mode="markers",
                marker=dict(color=colors, size=6, opacity=0.65, line=dict(width=0)),
                text=hover,
                hoverinfo="text",
                name="SKUs",
            )
        )
        scatter_fig.add_trace(
            go.Scatter(
                x=[0.1, max_v],
                y=[0.1, max_v],
                mode="lines",
                line=dict(dash="dash", color=parity_color, width=1),
                name="Parity",
                hoverinfo="skip",
            )
        )
        scatter_fig.update_layout(
            **chart_base_layout,
            height=540,
            margin=dict(l=60, r=20, t=30, b=50),
            xaxis={**chart_xaxis, "title": "Cheapest competitor total (EUR)", "type": "log"},
            yaxis={**chart_yaxis, "title": f"{ref} total (EUR)", "type": "log"},
            showlegend=False,
        )
        st.plotly_chart(scatter_fig, use_container_width=True)
    else:
        st.info("No comparable SKUs to plot.")

    # -- 6. Head-to-head --------------------------------------------------- #
    st.subheader(f"3. Head-to-head - {ref} vs other top sellers")
    h2h = head_to_head_rows(data, ref)
    if h2h:
        sorted_h = sorted(h2h, key=lambda r: r["winRate"])
        y_labels = [_tick(d["seller"], highlighted) for d in sorted_h]
        h2h_fig = go.Figure()
        h2h_fig.add_trace(
            go.Bar(
                orientation="h",
                name=f"{ref} cheaper",
                y=y_labels,
                x=[d["refCheaper"] for d in sorted_h],
                marker_color="#16a34a",
                hovertemplate="%{y}<br>" + ref + " cheaper on %{x} SKUs<extra></extra>",
            )
        )
        h2h_fig.add_trace(
            go.Bar(
                orientation="h",
                name="Same",
                y=y_labels,
                x=[d["same"] for d in sorted_h],
                marker_color="#cbd5e1",
                hovertemplate="%{y}<br>Same price on %{x} SKUs<extra></extra>",
            )
        )
        h2h_fig.add_trace(
            go.Bar(
                orientation="h",
                name="Competitor cheaper",
                y=y_labels,
                x=[d["compCheaper"] for d in sorted_h],
                marker_color="#dc2626",
                hovertemplate="%{y}<br>Competitor cheaper on %{x} SKUs<extra></extra>",
            )
        )
        h2h_fig.update_layout(
            **chart_base_layout,
            height=380,
            barmode="stack",
            margin=dict(l=150, r=20, t=30, b=40),
            xaxis={**chart_xaxis, "title": "SKUs in overlap"},
            yaxis=chart_yaxis,
            legend=dict(orientation="h", y=1.15),
        )
        st.plotly_chart(h2h_fig, use_container_width=True)

        h2h_table = [
            {
                "Competitor": d["seller"],
                "Highlighted": d["seller"] in highlighted,
                "Overlap": d["overlap"],
                f"{ref} cheaper": d["refCheaper"],
                "Comp cheaper": d["compCheaper"],
                "Same": d["same"],
                f"{ref} win %": round(d["winRate"], 1),
                "Median gap %": None if d["medianGap"] is None else round(d["medianGap"], 1),
            }
            for d in h2h
        ]
        st.dataframe(h2h_table, use_container_width=True, hide_index=True)
    else:
        st.info("No head-to-head overlap with other top sellers.")

    # -- 7. Competitor density -------------------------------------------- #
    st.subheader(f"4. Competitor density - {ref}'s catalog")
    groups: dict[int, dict[str, Any]] = {}
    for r in per_sku:
        g = groups.setdefault(r["compCount"], {"sku": 0, "freq": defaultdict(int)})
        g["sku"] += 1
        for s in r["compSellers"]:
            g["freq"][s] += 1
    xs = sorted(groups.keys())
    if xs:
        ys = [groups[x]["sku"] for x in xs]
        hover_lines: list[str] = []
        for x in xs:
            g = groups[x]
            if not g["freq"]:
                hover_lines.append(
                    f"<b>{x} competitors</b><br>SKUs: {g['sku']}<br><i>No rivals (monopoly)</i>"
                )
                continue
            top = sorted(g["freq"].items(), key=lambda t: t[1], reverse=True)[:8]
            lines = [
                f"- {_tick(s, highlighted)} ({n} SKUs, {n / g['sku'] * 100:.0f}%)"
                for s, n in top
            ]
            plural = "" if x == 1 else "s"
            hover_lines.append(
                f"<b>{x} competitor{plural}</b><br>"
                f"SKUs at this density: {g['sku']}<br><br>"
                f"<b>Top sellers competing here:</b><br>" + "<br>".join(lines)
            )
        density_fig = go.Figure(
            data=[
                go.Bar(
                    x=xs,
                    y=ys,
                    marker_color=primary_color,
                    text=ys,
                    textposition="outside",
                    customdata=hover_lines,
                    hovertemplate="%{customdata}<extra></extra>",
                )
            ]
        )
        density_fig.update_layout(
            **chart_base_layout,
            height=380,
            margin=dict(l=60, r=20, t=30, b=60),
            xaxis={**chart_xaxis, "title": f"Number of competing sellers (excl. {ref})", "dtick": 1},
            yaxis={**chart_yaxis, "title": "SKU count", "type": "log"},
            hoverlabel=dict(align="left"),
        )
        st.plotly_chart(density_fig, use_container_width=True)

    # -- 8. Catalog overlap ------------------------------------------------ #
    st.subheader(f"5. Catalog overlap with {ref}")
    overlap = overlap_rows(data, ref)[:15]
    if overlap:
        sorted_o = sorted(overlap, key=lambda r: r["overlap"])
        y_labels = [_tick(d["seller"], highlighted) for d in sorted_o]
        ref_size = len({o["e"] for o in by_seller.get(ref, [])})
        ov_fig = go.Figure()
        ov_fig.add_trace(
            go.Bar(
                orientation="h",
                name=f"Overlap with {ref}",
                y=y_labels,
                x=[d["overlap"] for d in sorted_o],
                marker_color=primary_color,
                text=[f"{d['overlap']:,}" for d in sorted_o],
                textposition="inside",
                insidetextanchor="middle",
                textfont=dict(color="white", size=11),
                customdata=[
                    [d["sellerSkus"], f"{d['pctOfSeller']:.1f}", f"{d['pctOfRef']:.1f}"]
                    for d in sorted_o
                ],
                hovertemplate=(
                    "%{y}<br>Overlap: %{x} SKUs<br>"
                    "%{customdata[1]}% of their %{customdata[0]} SKUs<br>"
                    "%{customdata[2]}% of " + ref + f"'s {ref_size} SKUs<extra></extra>"
                ),
            )
        )
        ov_fig.add_trace(
            go.Bar(
                orientation="h",
                name="Their non-overlapping SKUs",
                y=y_labels,
                x=[d["sellerSkus"] - d["overlap"] for d in sorted_o],
                marker_color=neutral_color,
                text=[f"{d['sellerSkus'] - d['overlap']:,}" for d in sorted_o],
                textposition="inside",
                insidetextanchor="middle",
                textfont=dict(color="#475569", size=11),
                hovertemplate="%{y}<br>Not in " + ref + ": %{x} SKUs<extra></extra>",
            )
        )
        ov_fig.update_layout(
            **chart_base_layout,
            height=460,
            barmode="stack",
            margin=dict(l=150, r=30, t=30, b=40),
            xaxis={**chart_xaxis, "title": "Competitor's catalog (SKU count)"},
            yaxis=chart_yaxis,
            legend=dict(orientation="h", y=1.12),
        )
        st.plotly_chart(ov_fig, use_container_width=True)

    # -- 9. Pricing opportunities ----------------------------------------- #
    st.subheader(f"6. Pricing opportunities for {ref}")
    comp_rows = [r for r in per_sku if r["gapPct"] is not None]
    worst = sorted(comp_rows, key=lambda r: r["gapPct"], reverse=True)[:25]
    best = sorted(comp_rows, key=lambda r: r["gapPct"])[:25]
    monopoly = [r for r in per_sku if r["compCount"] == 0][:30]

    def _opp_table(rows: list[dict]) -> list[dict]:
        return [
            {
                "EAN": r["ean"],
                "Title": r["title"],
                "Ref EUR": None if r["refTotal"] is None else round(r["refTotal"], 2),
                "Cheapest seller": r["bestSeller"] or "-",
                "Highlighted": r["bestSeller"] in highlighted if r["bestSeller"] else False,
                "Their EUR": None if r["bestTotal"] is None else round(r["bestTotal"], 2),
                "Gap %": None if r["gapPct"] is None else round(r["gapPct"], 1),
                "Rivals": r["compCount"],
            }
            for r in rows
        ]

    st.markdown(f"**Top 25 SKUs where {ref} is most overpriced**")
    st.dataframe(_opp_table(worst), use_container_width=True, hide_index=True)
    st.markdown(f"**Top 25 SKUs where {ref} is far cheapest**")
    st.dataframe(_opp_table(best), use_container_width=True, hide_index=True)
    st.markdown(f"**Sample of monopoly SKUs (only {ref})**")
    st.dataframe(
        [
            {
                "EAN": r["ean"],
                "Title": r["title"],
                "Ref EUR": None if r["refTotal"] is None else round(r["refTotal"], 2),
            }
            for r in monopoly
        ],
        use_container_width=True,
        hide_index=True,
    )

    # -- 10. SKU explorer ------------------------------------------------- #
    st.subheader("7. SKU explorer")

    # Build all-SKU view: ref rows + non-listed EANs (bucket "N").
    ref_map = {r["ean"]: r for r in per_sku}
    all_view: list[dict] = []
    for ean in by_ean.keys():
        if ean in ref_map:
            all_view.append(ref_map[ean])
            continue
        offers = by_ean.get(ean, [])
        cheapest = None
        for o in offers:
            if cheapest is None or o["t"] < cheapest["t"]:
                cheapest = o
        all_view.append(
            {
                "ean": ean,
                "title": titles.get(ean, ""),
                "refTotal": None,
                "refPrice": None,
                "refDelivery": None,
                "bestSeller": cheapest["s"] if cheapest else None,
                "bestTotal": cheapest["t"] if cheapest else None,
                "compCount": len(offers),
                "compSellers": [o["s"] for o in offers],
                "gapPct": None,
                "bucket": "N",
            }
        )

    q = st.text_input("Search by EAN or title", key="seller_dashboard_search").strip().lower()
    bucket_choice = st.selectbox(
        "Position bucket",
        ["All", "A", "B", "C", "D", "E", "M", "N"],
        key="seller_dashboard_bucket",
    )
    rows = all_view
    if q:
        rows = [r for r in rows if q in r["ean"].lower() or q in (r["title"] or "").lower()]
    if bucket_choice != "All":
        rows = [r for r in rows if r["bucket"] == bucket_choice]
    st.caption(f"{len(rows):,} SKUs")
    rows = rows[:300]
    st.dataframe(
        [
            {
                "EAN": r["ean"],
                "Title": r["title"],
                "Ref EUR": None if r["refTotal"] is None else round(r["refTotal"], 2),
                "Best comp EUR": None if r["bestTotal"] is None else round(r["bestTotal"], 2),
                "Cheapest seller": r["bestSeller"] or "-",
                "Highlighted": r["bestSeller"] in highlighted if r["bestSeller"] else False,
                "Gap %": None if r["gapPct"] is None else round(r["gapPct"], 1),
                "Rivals": r["compCount"],
                "Bucket": r["bucket"],
            }
            for r in rows
        ],
        use_container_width=True,
        hide_index=True,
    )

    if rows:
        ean_options = [r["ean"] for r in rows]
        chosen = st.selectbox(
            "Inspect SKU",
            ean_options,
            key="seller_dashboard_detail_ean",
        )
        offers = sorted(by_ean.get(chosen, []), key=lambda o: o["t"])
        if offers:
            st.markdown(f"**{chosen} - {titles.get(chosen, '')}**")
            colors = [
                "#dc2626"
                if o["s"] == ref
                else ("#f59e0b" if o["s"] in highlighted else "#2563eb")
                for o in offers
            ]
            detail_fig = go.Figure(
                data=[
                    go.Bar(
                        x=[_tick(o["s"], highlighted) for o in offers],
                        y=[o["t"] for o in offers],
                        marker_color=colors,
                        text=[f"{o['t']:.2f} EUR" for o in offers],
                        textposition="outside",
                        customdata=[[o.get("p"), o.get("d")] for o in offers],
                        hovertemplate=(
                            "%{x}<br>Price: %{customdata[0]} EUR<br>"
                            "Delivery: %{customdata[1]} EUR<br>"
                            "Total: %{y} EUR<extra></extra>"
                        ),
                    )
                ]
            )
            detail_fig.update_layout(
                **chart_base_layout,
                height=320,
                margin=dict(l=50, r=10, t=30, b=100),
                xaxis={**chart_xaxis, "tickangle": -45},
                yaxis={**chart_yaxis, "title": "Total (EUR)"},
            )
            st.plotly_chart(detail_fig, use_container_width=True)
            st.dataframe(
                [
                    {
                        "Seller": o["s"],
                        "Reference": o["s"] == ref,
                        "Highlighted": o["s"] in highlighted,
                        "Price EUR": None if o.get("p") is None else round(o["p"], 2),
                        "Delivery EUR": None if o.get("d") is None else round(o["d"], 2),
                        "Total EUR": round(o["t"], 2),
                    }
                    for o in offers
                ],
                use_container_width=True,
                hide_index=True,
            )
