from importlib.util import module_from_spec, spec_from_file_location
import asyncio
import csv
import io
from pathlib import Path
from unittest.mock import patch


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


def test_persist_offers_batch_flushes_csv_without_rebuilding_excel() -> None:
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

    module._persist_offers_batch(offers, writer, flush_tracker)

    assert "123" in sink.getvalue()
    assert flush_tracker.calls == 1
    assert rebuilt_paths == []


def test_finalize_output_flushes_csv_and_rebuilds_excel() -> None:
    module = _load_scrape_allegro_module()

    class FlushTracker:
        def __init__(self) -> None:
            self.calls = 0

        def flush(self) -> None:
            self.calls += 1

    flush_tracker = FlushTracker()
    rebuilt_paths: list[str] = []

    module._finalize_output(
        flush_tracker,
        "item-analysis/allegro_offers.csv",
        rebuild_excel=rebuilt_paths.append,
    )

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


def test_worker_startup_stagger_seconds() -> None:
    module = _load_scrape_allegro_module()

    assert module._worker_startup_stagger_seconds(0) == 0.0
    assert module._worker_startup_stagger_seconds(1) == 1.0
    assert module._worker_startup_stagger_seconds(2) == 2.0


def test_worker_applies_initial_stagger_before_first_scrape() -> None:
    module = _load_scrape_allegro_module()
    queue: asyncio.Queue[str] = asyncio.Queue()
    queue.put_nowait("123")

    sleep_calls: list[float] = []
    scraped_eans: list[str] = []
    persisted_batches: list[list[dict]] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    async def fake_scrape(page, ean: str) -> list[dict]:
        scraped_eans.append(ean)
        return [{
            "ean": ean,
            "title": "Test product",
            "seller": "Seller",
            "seller_url": "https://example.com",
            "price_eur": "10.00",
            "delivery_eur": "2.00",
            "box_price_eur": "12.00",
            "scraped_at": "2026-04-21T00:00:00+00:00",
        }]

    def fake_persist(offers, writer, out_file, output_path, rebuild_excel=None) -> None:
        persisted_batches.append(offers)

    original_sleep = module.asyncio.sleep
    original_scrape = module.scrape_ean
    original_persist = module._persist_offers_batch
    original_uniform = module.random.uniform
    try:
        module.asyncio.sleep = fake_sleep
        module.scrape_ean = fake_scrape
        module._persist_offers_batch = fake_persist
        module.random.uniform = lambda _lo, _hi: 2.0

        async def scenario() -> None:
            await module._worker(
                worker_id=1,
                page=object(),
                queue=queue,
                writer=None,
                out_file=None,
                write_lock=asyncio.Lock(),
                counters={"total": 0, "not_found": 0, "done": 0, "scraped_eans": 0},
                total=1,
                output_path="item-analysis/allegro_offers.csv",
                started_at=0.0,
            )

        asyncio.run(scenario())
    finally:
        module.asyncio.sleep = original_sleep
        module.scrape_ean = original_scrape
        module._persist_offers_batch = original_persist
        module.random.uniform = original_uniform

    assert sleep_calls[0] == 1.0
    assert scraped_eans == ["123"]
    assert len(persisted_batches) == 1


def test_launch_chrome_does_not_require_enable_automation() -> None:
    module = _load_scrape_allegro_module()
    popen_calls: list[list[str]] = []

    def fake_popen(args):
        popen_calls.append(args)
        return object()

    original_platform = module.sys.platform
    original_popen = module.subprocess.Popen
    try:
        module.sys.platform = "linux"
        module.subprocess.Popen = fake_popen
        with patch("shutil.which", side_effect=["/usr/bin/google-chrome", None]):
            module._launch_chrome(9222)
    finally:
        module.sys.platform = original_platform
        module.subprocess.Popen = original_popen

    assert popen_calls
    assert "--enable-automation" not in popen_calls[0]
    assert "--remote-debugging-port=9222" in popen_calls[0]
