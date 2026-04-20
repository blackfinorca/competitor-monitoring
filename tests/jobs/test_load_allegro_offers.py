"""Tests for jobs.load_allegro_offers helpers."""

import csv
import io
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from jobs.load_allegro_offers import _parse_dt, _parse_float, main


class TestParseFloat:
    def test_valid_string(self):
        assert _parse_float("3.14") == pytest.approx(3.14)

    def test_zero_string_returns_zero_not_none(self):
        assert _parse_float("0.0") == pytest.approx(0.0)

    def test_integer_string(self):
        assert _parse_float("10") == pytest.approx(10.0)

    def test_empty_string_returns_none(self):
        assert _parse_float("") is None

    def test_none_returns_none(self):
        assert _parse_float(None) is None

    def test_non_numeric_returns_none(self):
        assert _parse_float("abc") is None

    def test_comma_decimal_returns_none(self):
        # CSV from scraper uses "." already, "," should fail gracefully
        assert _parse_float("3,14") is None


class TestParseDt:
    def test_iso_with_utc_offset(self):
        result = _parse_dt("2026-04-18T12:34:56+00:00")
        assert result is not None
        assert result.year == 2026
        assert result.month == 4
        assert result.day == 18

    def test_iso_with_z_suffix(self):
        result = _parse_dt("2026-04-18T12:34:56Z")
        assert result is not None
        assert result.hour == 12

    def test_none_returns_none(self):
        assert _parse_dt(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_dt("") is None

    def test_garbage_returns_none(self):
        assert _parse_dt("not-a-date") is None


class TestMainDryRun:
    def test_dry_run_prints_counts(self, tmp_path, capsys):
        csv_file = tmp_path / "offers.csv"
        csv_file.write_text(
            "ean,title,seller,seller_url,price_eur,delivery_eur,scraped_at\n"
            "1234567890123,Widget A,SellerA,https://example.com,9.99,2.50,2026-04-18T10:00:00+00:00\n"
            "1234567890123,Widget A,SellerB,https://example.com,8.99,1.50,2026-04-18T10:00:00+00:00\n"
        )
        rc = main(str(csv_file), dry_run=True)
        assert rc == 0
        out = capsys.readouterr().out
        assert "2 rows" in out
        assert "1 unique EANs" in out

    def test_missing_file_returns_error(self):
        rc = main("/nonexistent/offers.csv")
        assert rc == 1

    def test_empty_csv_returns_zero(self, tmp_path, capsys):
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("ean,title,seller,seller_url,price_eur,delivery_eur,scraped_at\n")
        rc = main(str(csv_file))
        assert rc == 0
        assert "No rows" in capsys.readouterr().out
