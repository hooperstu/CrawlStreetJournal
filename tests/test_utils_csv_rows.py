"""Tests for utils.count_csv_rows (RFC 4180 logical rows)."""

import csv
import os
import tempfile
import unittest

import utils


class TestCountCsvRows(unittest.TestCase):
    def test_skips_header(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8", newline="",
        ) as f:
            w = csv.writer(f)
            w.writerow(["a", "b"])
            w.writerow(["1", "2"])
            w.writerow(["3", "4"])
            path = f.name
        try:
            self.assertEqual(utils.count_csv_rows(path), 2)
        finally:
            os.unlink(path)

    def test_quoted_multiline_field_counts_as_one_row(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8", newline="",
        ) as f:
            w = csv.writer(f)
            w.writerow(["requested_url", "heading_outline"])
            w.writerow(["https://x.test/a", "line1\nline2\nline3"])
            w.writerow(["https://x.test/b", "ok"])
            path = f.name
        try:
            self.assertEqual(utils.count_csv_rows(path), 2)
        finally:
            os.unlink(path)
