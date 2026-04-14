from pathlib import Path

from jobs import daily_ingest


def test_daily_ingest_main_uses_catalogue_path_and_settings(monkeypatch) -> None:
    calls = {}

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def commit(self) -> None:
            calls["committed"] = True

    class FakeFactory:
        def __call__(self):
            return FakeSession()

    monkeypatch.setattr(daily_ingest, "Settings", lambda: "settings")
    monkeypatch.setattr(daily_ingest, "make_session_factory", lambda settings: FakeFactory())
    monkeypatch.setattr(
        daily_ingest,
        "ingest_catalogue_csv",
        lambda session, path: calls.update({"path": path}) or 3,
    )

    count = daily_ingest.main(Path("data/ag_catalogue.csv"))

    assert count == 3
    assert calls["path"] == Path("data/ag_catalogue.csv")
    assert calls["committed"] is True

