from agnaradie_pricing.pricing.compare_competitors_insights import (
    build_compare_competitors_dataset,
    build_compare_competitors_insights_prompt,
    load_compare_competitors_insights_prompt_template,
)


def test_load_compare_competitors_insights_prompt_template_contains_required_blocks() -> None:
    template = load_compare_competitors_insights_prompt_template()

    assert "BLOCK 1 — COMPETITIVE POSITION" in template
    assert "BLOCK 4 — ACTIONS" in template
    assert "{dataset}" in template


def test_build_compare_competitors_dataset_formats_full_merged_rows() -> None:
    data = {
        "merged": [
            {
                "brand": "Wiha",
                "title": "Specialne klieste 180 mm",
                "ref_price": 10.0,
                "wins": 1,
                "opponents": {
                    "boukal_cz": {"price": 12.0, "delta_pct": 20.0, "url": "https://boukal.example/a"},
                    "agi_sk": {"price": None, "delta_pct": None, "url": ""},
                },
            },
            {
                "brand": "Baupro",
                "title": "Zmetak B20",
                "ref_price": 18.5,
                "wins": 0,
                "opponents": {
                    "boukal_cz": {"price": 17.0, "delta_pct": -8.1, "url": "https://boukal.example/b"},
                    "agi_sk": {"price": 16.5, "delta_pct": -10.8, "url": "https://agi.example/b"},
                },
            },
        ]
    }

    dataset = build_compare_competitors_dataset(
        data,
        ref_name="ToolZone",
        opponents=[("boukal_cz", "Boukal"), ("agi_sk", "AGI")],
    )

    assert "BRAND\tPRODUCT\tTOOLZONE PRICE (€)\tBOUKAL PRICE (€)\tBOUKAL GAP %\tAGI PRICE (€)\tAGI GAP %\tWIN RATIO" in dataset
    assert "Wiha\tSpecialne klieste 180 mm\t10.00\t12.00\t20.0%\t—\t—\t1/2" in dataset
    assert "Baupro\tZmetak B20\t18.50\t17.00\t-8.1%\t16.50\t-10.8%\t0/2" in dataset


def test_build_compare_competitors_insights_prompt_includes_dataset_and_reference_store() -> None:
    data = {
        "merged": [
            {
                "brand": "Wiha",
                "title": "Specialne klieste 180 mm",
                "ref_price": 10.0,
                "wins": 1,
                "opponents": {
                    "boukal_cz": {"price": 12.0, "delta_pct": 20.0, "url": "https://boukal.example/a"},
                },
            },
        ]
    }

    prompt = build_compare_competitors_insights_prompt(
        data,
        ref_name="ToolZone",
        opponents=[("boukal_cz", "Boukal")],
    )

    assert "You are a strategic pricing analyst." in prompt
    assert "Reference store: ToolZone" in prompt
    assert "BOUKAL PRICE (€)" in prompt
    assert "Wiha\tSpecialne klieste 180 mm\t10.00\t12.00\t20.0%\t1/1" in prompt
