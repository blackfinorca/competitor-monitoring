"""Tests for jobs.export_manufacturer helpers."""

import pytest

from jobs.export_manufacturer import _diff_pct


class TestDiffPct:
    def test_competitor_cheaper(self):
        result = _diff_pct(10.0, 8.0)
        assert result == pytest.approx(-20.0)

    def test_competitor_more_expensive(self):
        result = _diff_pct(10.0, 12.0)
        assert result == pytest.approx(20.0)

    def test_equal_prices(self):
        assert _diff_pct(10.0, 10.0) == pytest.approx(0.0)

    def test_tz_price_none_returns_none(self):
        assert _diff_pct(None, 8.0) is None

    def test_comp_price_none_returns_none(self):
        assert _diff_pct(10.0, None) is None

    def test_both_none_returns_none(self):
        assert _diff_pct(None, None) is None

    def test_comp_price_zero_returns_minus_100(self):
        # Fix regression: comp_price=0.0 was treated as falsy and returned None
        result = _diff_pct(10.0, 0.0)
        assert result == pytest.approx(-100.0)

    def test_tz_price_zero_returns_none(self):
        # Can't compute percentage change from a zero base
        assert _diff_pct(0.0, 8.0) is None
