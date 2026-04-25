from __future__ import annotations

from pathlib import Path


_PROMPT_TEMPLATE_PATH = Path(__file__).resolve().parents[3] / "config" / "compare_competitors_ai_insights_prompt.txt"


def load_compare_competitors_insights_prompt_template(template_path: Path | None = None) -> str:
    path = template_path or _PROMPT_TEMPLATE_PATH
    return path.read_text(encoding="utf-8")


def build_compare_competitors_dataset(
    data: dict,
    *,
    ref_name: str,
    opponents: list[tuple[str, str]],
) -> str:
    header = ["BRAND", "PRODUCT", f"{ref_name.upper()} PRICE (€)"]
    for _opp_id, opp_name in opponents:
        header.extend([f"{opp_name.upper()} PRICE (€)", f"{opp_name.upper()} GAP %"])
    header.append("WIN RATIO")

    lines = ["\t".join(header)]
    n_opp = len(opponents)
    for row in data.get("merged", []):
        fields = [
            (row.get("brand") or "").strip() or "—",
            row.get("title") or "",
            _format_price(row.get("ref_price")),
        ]
        opponents_data = row.get("opponents") or {}
        for opp_id, _opp_name in opponents:
            opponent = opponents_data.get(opp_id) or {}
            fields.append(_format_price(opponent.get("price")))
            fields.append(_format_gap(opponent.get("delta_pct")))
        wins = int(row.get("wins") or 0)
        fields.append(f"{wins}/{n_opp}")
        lines.append("\t".join(fields))

    return "\n".join(lines)


def build_compare_competitors_insights_prompt(
    data: dict,
    *,
    ref_name: str,
    opponents: list[tuple[str, str]],
    template: str | None = None,
) -> str:
    prompt_template = template or load_compare_competitors_insights_prompt_template()
    dataset = build_compare_competitors_dataset(data, ref_name=ref_name, opponents=opponents)
    return prompt_template.format(reference_store=ref_name, dataset=dataset)


def _format_price(value: float | int | None) -> str:
    if value is None:
        return "—"
    return f"{float(value):.2f}"


def _format_gap(value: float | int | None) -> str:
    if value is None:
        return "—"
    return f"{float(value):.1f}%"
