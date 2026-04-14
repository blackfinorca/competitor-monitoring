"""Tests for jobs.daily_alert."""

from datetime import date, datetime, UTC
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from jobs.daily_alert import (
    _build_blocks,
    _collect_alerts,
    _format_price,
    post_slack_alerts,
    main,
)


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------

class TestFormatPrice:
    def test_decimal_formats_two_places(self):
        assert _format_price(Decimal("29.81")) == "29.81 €"

    def test_none_returns_na(self):
        assert _format_price(None) == "N/A"

    def test_zero(self):
        assert _format_price(Decimal("0")) == "0.00 €"


class TestBuildBlocks:
    def _sample_alert(self, playbook="investigate") -> dict:
        return {
            "sku": "KNI-8701250",
            "title": "Knipex Cobra 250mm",
            "playbook": playbook,
            "current_price": Decimal("31.90"),
            "suggested_price": Decimal("29.81"),
            "median_price": Decimal("29.81"),
            "rationale": "Market median moved 22% day-on-day.",
        }

    def test_header_contains_date(self):
        blocks = _build_blocks([self._sample_alert()], date(2026, 4, 12))
        header_block = blocks[0]
        assert header_block["type"] == "header"
        assert "2026-04-12" in header_block["text"]["text"]

    def test_section_per_alert(self):
        alerts = [self._sample_alert("investigate"), self._sample_alert("raise")]
        blocks = _build_blocks(alerts, date(2026, 4, 12))
        # header + divider + 2 sections + divider + context
        section_blocks = [b for b in blocks if b["type"] == "section"]
        assert len(section_blocks) == 2

    def test_alert_text_contains_sku_and_prices(self):
        alerts = [self._sample_alert()]
        blocks = _build_blocks(alerts, date(2026, 4, 12))
        section = next(b for b in blocks if b["type"] == "section")
        text = section["text"]["text"]
        assert "KNI-8701250" in text
        assert "31.90" in text

    def test_footer_shows_total(self):
        blocks = _build_blocks([self._sample_alert()], date(2026, 4, 12))
        ctx = next(b for b in blocks if b["type"] == "context")
        assert "Total alerts: 1" in ctx["elements"][0]["text"]

    def test_empty_alerts_still_renders_header(self):
        blocks = _build_blocks([], date(2026, 4, 12))
        assert any(b["type"] == "header" for b in blocks)


# ---------------------------------------------------------------------------
# post_slack_alerts
# ---------------------------------------------------------------------------

class TestPostSlackAlerts:
    def _mock_client(self, status=200):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        if status != 200:
            import httpx
            mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                "error", request=MagicMock(), response=MagicMock()
            )
        client = MagicMock()
        client.post.return_value = mock_response
        return client

    def test_posts_json_to_webhook(self):
        client = self._mock_client()
        result = post_slack_alerts(
            "https://hooks.slack.com/test",
            [{"sku": "X", "title": "Y", "playbook": "raise",
              "current_price": Decimal("10"), "suggested_price": Decimal("11"),
              "median_price": Decimal("10.5"), "rationale": "Test"}],
            date(2026, 4, 12),
            http_client=client,
        )
        assert result is True
        assert client.post.call_count == 1
        call_kwargs = client.post.call_args[1]
        assert "application/json" in call_kwargs.get("headers", {}).get("Content-Type", "")

    def test_returns_false_on_http_error(self):
        client = self._mock_client(status=500)
        result = post_slack_alerts(
            "https://hooks.slack.com/test",
            [],
            date(2026, 4, 12),
            http_client=client,
        )
        assert result is False


# ---------------------------------------------------------------------------
# main() integration stubs
# ---------------------------------------------------------------------------

class TestDailyAlertMain:
    def test_returns_zero_when_no_webhook(self, monkeypatch):
        monkeypatch.setattr(
            "jobs.daily_alert.Settings",
            lambda: MagicMock(alert_webhook_url=None),
        )
        result = main(alert_date=date(2026, 4, 12))
        assert result == 0

    def test_returns_zero_when_no_alerts(self, monkeypatch):
        monkeypatch.setattr(
            "jobs.daily_alert.Settings",
            lambda: MagicMock(alert_webhook_url="https://hooks.slack.com/x"),
        )

        fake_session_ctx = MagicMock()
        fake_session_ctx.__enter__ = MagicMock(return_value=fake_session_ctx)
        fake_session_ctx.__exit__ = MagicMock(return_value=False)
        fake_factory = MagicMock(return_value=fake_session_ctx)

        monkeypatch.setattr(
            "jobs.daily_alert.make_session_factory",
            lambda settings: fake_factory,
        )
        monkeypatch.setattr(
            "jobs.daily_alert._collect_alerts",
            lambda session, dt, playbooks: [],
        )

        result = main(alert_date=date(2026, 4, 12))
        assert result == 0

    def test_returns_alert_count_on_success(self, monkeypatch):
        monkeypatch.setattr(
            "jobs.daily_alert.Settings",
            lambda: MagicMock(alert_webhook_url="https://hooks.slack.com/x"),
        )

        fake_session_ctx = MagicMock()
        fake_session_ctx.__enter__ = MagicMock(return_value=fake_session_ctx)
        fake_session_ctx.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr(
            "jobs.daily_alert.make_session_factory",
            lambda settings: MagicMock(return_value=fake_session_ctx),
        )

        alerts = [
            {"sku": "A", "title": "T1", "playbook": "investigate",
             "current_price": Decimal("10"), "suggested_price": Decimal("10"),
             "median_price": Decimal("8"), "rationale": "big move"},
        ]
        monkeypatch.setattr(
            "jobs.daily_alert._collect_alerts",
            lambda session, dt, playbooks: alerts,
        )
        monkeypatch.setattr(
            "jobs.daily_alert.post_slack_alerts",
            lambda *a, **kw: True,
        )

        result = main(alert_date=date(2026, 4, 12))
        assert result == 1
