from pathlib import Path

from agnaradie_pricing.settings import Settings, load_competitors, load_playbooks


def test_settings_reads_database_url_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://example/test")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

    settings = Settings()

    assert settings.database_url == "postgresql+psycopg://example/test"
    assert settings.anthropic_api_key == "test-key"


def test_load_competitors_reads_yaml_config() -> None:
    competitors = load_competitors(Path("config/competitors.yaml"))

    assert len(competitors) == 12
    assert competitors[0]["id"] == "doktorkladivo_sk"


def test_load_playbooks_reads_threshold_config() -> None:
    playbooks = load_playbooks(Path("config/playbooks.yaml"))

    assert playbooks["raise"]["min_gap_below_next"] == 0.08
    assert playbooks["investigate"]["min_day_on_day_move"] == 0.20

