from datetime import UTC, datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import threading

from agnaradie_pricing.scrapers.base import CompetitorListing


def _load_daily_scrape_module():
    module_path = Path(__file__).resolve().parents[2] / "jobs" / "daily_scrape.py"
    spec = spec_from_file_location("daily_scrape", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _listing(idx: int) -> CompetitorListing:
    return CompetitorListing(
        competitor_id="example_sk",
        competitor_sku=f"sku-{idx}",
        brand="Brand",
        mpn=f"MPN-{idx}",
        ean=None,
        title=f"Product {idx}",
        price_eur=10.0 + idx,
        currency="EUR",
        in_stock=True,
        url=f"https://example.test/{idx}",
        scraped_at=datetime(2026, 4, 23, tzinfo=UTC),
    )


class FakeSession:
    def __init__(self, calls: list[list[CompetitorListing]]) -> None:
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def commit(self) -> None:
        return None


class FakeFactory:
    def __init__(self, calls: list[list[CompetitorListing]]) -> None:
        self.calls = calls

    def __call__(self):
        return FakeSession(self.calls)


class FakeScraper:
    def __init__(self, config: dict) -> None:
        self.config = config

    def run_daily_iter(self, catalogue: list[dict]):
        yield from [_listing(1), _listing(2), _listing(3)]


class StoppingScraper:
    def __init__(self, config: dict) -> None:
        self.config = config

    def run_daily_iter(self, catalogue: list[dict]):
        yield _listing(1)
        yield _listing(2)
        yield _listing(3)


class InterruptingScraper:
    def __init__(self, config: dict) -> None:
        self.config = config

    def run_daily_iter(self, catalogue: list[dict]):
        yield _listing(1)
        yield _listing(2)
        raise KeyboardInterrupt


def test_scrape_one_flushes_in_batches_and_final_partial(monkeypatch) -> None:
    daily_scrape = _load_daily_scrape_module()
    calls: list[list[CompetitorListing]] = []
    monkeypatch.setattr(daily_scrape, "build_scraper", lambda config: FakeScraper(config))
    monkeypatch.setattr(
        daily_scrape,
        "save_competitor_listings",
        lambda session, listings: calls.append(list(listings)),
    )

    cid, saved = daily_scrape._scrape_one(
        {"id": "example_sk"},
        catalogue=[],
        factory=FakeFactory(calls),
        save_batch_size=2,
    )

    assert cid == "example_sk"
    assert saved == 3
    assert [len(batch) for batch in calls] == [2, 1]


def test_scrape_one_flushes_partial_batch_when_stop_requested(monkeypatch) -> None:
    daily_scrape = _load_daily_scrape_module()
    calls: list[list[CompetitorListing]] = []
    stop_event = threading.Event()
    seen = {"count": 0}
    monkeypatch.setattr(daily_scrape, "build_scraper", lambda config: StoppingScraper(config))

    def save_and_stop(session, listings):
        calls.append(list(listings))
        seen["count"] += len(listings)
        stop_event.set()

    monkeypatch.setattr(daily_scrape, "save_competitor_listings", save_and_stop)

    cid, saved = daily_scrape._scrape_one(
        {"id": "example_sk"},
        catalogue=[],
        factory=FakeFactory(calls),
        stop_event=stop_event,
        save_batch_size=2,
    )

    assert cid == "example_sk"
    assert saved == 2
    assert [len(batch) for batch in calls] == [2]


def test_scrape_one_flushes_buffer_when_interrupted(monkeypatch) -> None:
    daily_scrape = _load_daily_scrape_module()
    calls: list[list[CompetitorListing]] = []
    monkeypatch.setattr(daily_scrape, "build_scraper", lambda config: InterruptingScraper(config))
    monkeypatch.setattr(
        daily_scrape,
        "save_competitor_listings",
        lambda session, listings: calls.append(list(listings)),
    )

    try:
        daily_scrape._scrape_one(
            {"id": "example_sk"},
            catalogue=[],
            factory=FakeFactory(calls),
            save_batch_size=50,  # keep all listings in memory to test final flush
        )
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("Expected KeyboardInterrupt to propagate")

    assert [len(batch) for batch in calls] == [2]


def test_main_only_runs_requested_competitor(monkeypatch) -> None:
    daily_scrape = _load_daily_scrape_module()

    monkeypatch.setattr(
        daily_scrape,
        "load_competitors",
        lambda: [
            {"id": "fermatshop_sk", "name": "Fermatshop"},
            {"id": "agi_sk", "name": "AGI"},
            {"id": "strendpro_sk", "name": "Strendpro"},
        ],
    )
    monkeypatch.setattr(
        daily_scrape,
        "load_catalogue_csv",
        lambda _path: [],
    )
    monkeypatch.setattr(
        daily_scrape,
        "make_session_factory",
        lambda _settings: object(),
    )

    seen: list[str] = []

    def fake_scrape_one(comp_config, catalogue, factory, stop_event=None, save_batch_size=50):
        del catalogue, factory, stop_event, save_batch_size
        seen.append(comp_config["id"])
        return comp_config["id"], 123

    monkeypatch.setattr(daily_scrape, "_scrape_one", fake_scrape_one)

    counts = daily_scrape.main(
        only=["fermatshop_sk"],
        sequential=True,
    )

    assert seen == ["fermatshop_sk"]
    assert counts == {"fermatshop_sk": 123}
