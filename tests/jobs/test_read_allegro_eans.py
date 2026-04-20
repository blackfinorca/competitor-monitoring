"""Tests for jobs.read_allegro_eans."""

import csv
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from jobs.read_allegro_eans import main


def _make_fake_wb(rows: list[tuple], headers=("products_ean", "title", "price_sk")):
    """Return a minimal openpyxl-shaped fake workbook."""
    header_cells = [MagicMock(value=h) for h in headers]

    ws = MagicMock()
    ws.iter_rows.side_effect = lambda min_row, max_row, **kw: (
        iter([header_cells]) if min_row == 1 and max_row == 1 else iter([])
    )

    def iter_rows_values(min_row, values_only):
        if min_row == 2 and values_only:
            return iter(rows)
        return iter([])

    ws.iter_rows = MagicMock(side_effect=None)
    ws.iter_rows.return_value = iter([header_cells])

    # Patch both call signatures
    def smart_iter_rows(min_row=1, max_row=None, values_only=False):
        if not values_only:
            return iter([header_cells])
        return iter(rows)

    ws.iter_rows = smart_iter_rows

    wb = MagicMock()
    wb.active = ws
    wb.close = MagicMock()
    return wb


class TestReadAllegroEans:
    def _run(self, tmp_path, wb):
        out_csv = tmp_path / "eans.csv"
        with patch("jobs.read_allegro_eans.openpyxl.load_workbook", return_value=wb):
            rc = main("dummy.xlsx", str(out_csv))
        return rc, out_csv

    def test_deduplicates_eans(self, tmp_path):
        wb = _make_fake_wb([
            ("1234567890123", "Widget A", "9.99"),
            ("1234567890123", "Widget A duplicate", "8.99"),
            ("9876543210987", "Widget B", "5.00"),
        ])
        rc, out_csv = self._run(tmp_path, wb)
        assert rc == 0
        rows = list(csv.DictReader(out_csv.open()))
        eans = [r["ean"] for r in rows]
        assert len(eans) == 2
        assert "1234567890123" in eans
        assert "9876543210987" in eans

    def test_strips_float_suffix_from_numeric_ean(self, tmp_path):
        # Excel often stores EANs as float: 1234567890123.0
        wb = _make_fake_wb([(1234567890123.0, "Widget A", "9.99")])
        rc, out_csv = self._run(tmp_path, wb)
        assert rc == 0
        rows = list(csv.DictReader(out_csv.open()))
        assert rows[0]["ean"] == "1234567890123"

    def test_skips_rows_with_none_ean(self, tmp_path):
        wb = _make_fake_wb([
            (None, "No EAN product", "5.00"),
            ("1234567890123", "Valid", "9.99"),
        ])
        rc, out_csv = self._run(tmp_path, wb)
        assert rc == 0
        rows = list(csv.DictReader(out_csv.open()))
        assert len(rows) == 1
        assert rows[0]["ean"] == "1234567890123"

    def test_missing_column_returns_error(self, tmp_path, capsys):
        wb = _make_fake_wb([], headers=("wrong_col", "title", "price_sk"))
        rc, _ = self._run(tmp_path, wb)
        assert rc == 1
        assert "missing column" in capsys.readouterr().err
