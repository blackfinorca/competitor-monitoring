from importlib.util import module_from_spec, spec_from_file_location
import asyncio
import csv
import io
from pathlib import Path


def _load_scrape_allegro_module():
    module_path = Path(__file__).resolve().parents[1] / "item-analysis" / "scrape_allegro.py"
    spec = spec_from_file_location("scrape_allegro", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_all_offers_label_matches_vsetky_ponuky() -> None:
    module = _load_scrape_allegro_module()

    assert module._looks_like_all_offers_label("Všetky ponuky (5)")


def test_all_offers_label_rejects_vsetky_parametre() -> None:
    module = _load_scrape_allegro_module()

    assert not module._looks_like_all_offers_label("Všetky parametre")


def test_persist_offers_batch_flushes_csv_and_rebuilds_excel() -> None:
    module = _load_scrape_allegro_module()
    sink = io.StringIO()
    writer = csv.DictWriter(sink, fieldnames=module._FIELDNAMES)
    writer.writeheader()

    class FlushTracker:
        def __init__(self) -> None:
            self.calls = 0

        def flush(self) -> None:
            self.calls += 1

    flush_tracker = FlushTracker()
    rebuilt_paths: list[str] = []
    offers = [{
        "ean": "123",
        "title": "Test product",
        "seller": "Seller",
        "seller_url": "https://example.com",
        "price_eur": "10.00",
        "delivery_eur": "2.00",
        "box_price_eur": "12.00",
        "scraped_at": "2026-04-21T00:00:00+00:00",
    }]

    module._persist_offers_batch(
        offers,
        writer,
        flush_tracker,
        "item-analysis/allegro_offers.csv",
        rebuilt_paths.append,
    )

    assert "123" in sink.getvalue()
    assert flush_tracker.calls == 1
    assert rebuilt_paths == ["item-analysis/allegro_offers.csv"]


def test_skip_found_keeps_only_missing_or_not_found_eans() -> None:
    module = _load_scrape_allegro_module()

    remaining = module._filter_eans_for_existing_output(
        ["ean-1", "ean-2", "ean-3"],
        [
            {"ean": "ean-1", "seller": "Seller A"},
            {"ean": "ean-1", "seller": "Seller B"},
            {"ean": "ean-2", "seller": ""},
        ],
        resume=False,
        skip_found=True,
    )

    assert remaining == ["ean-2", "ean-3"]


def test_skip_found_appends_to_existing_output() -> None:
    module = _load_scrape_allegro_module()

    assert module._should_append_output(resume=False, skip_found=True, output_exists=True)


def test_progress_snapshot_formats_elapsed_and_eta() -> None:
    module = _load_scrape_allegro_module()

    line = module._format_progress_snapshot(
        counters={"scraped_eans": 12, "not_found": 3, "done": 15},
        total=40,
        elapsed_seconds=300.0,
    )

    assert "scraped=12" in line
    assert "not_found=3" in line
    assert "missing=25" in line
    assert "elapsed=05:00" in line
    assert "eta=08:20" in line


def test_turn_controller_alternates_workers() -> None:
    module = _load_scrape_allegro_module()
    controller = module._TurnController(worker_ids=[0, 1])

    async def scenario() -> list[int]:
        await controller.wait_for_turn(0)
        controller.handoff(0)
        await controller.wait_for_turn(1)
        controller.handoff(1)
        await controller.wait_for_turn(0)
        return [controller.current_turn, controller.active_workers]

    current_turn, active_workers = asyncio.run(scenario())

    assert current_turn == 0
    assert active_workers == {0, 1}


def test_turn_controller_skips_inactive_worker() -> None:
    module = _load_scrape_allegro_module()
    controller = module._TurnController(worker_ids=[0, 1])
    controller.mark_inactive(1)
    controller.handoff(0)

    assert controller.current_turn == 0
