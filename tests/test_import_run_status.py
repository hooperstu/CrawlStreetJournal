"""Run status when _state.json is missing but crawl outputs exist (imported projects)."""

import csv
import os
import tempfile

import config
import storage


def test_get_run_status_missing_state_with_no_pages_is_new():
    with tempfile.TemporaryDirectory() as tmp:
        assert storage.get_run_status(tmp) == "new"


def test_get_run_status_missing_state_with_pages_is_interrupted():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, config.PAGES_CSV)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(storage.PAGES_FIELDS)[:3])
            w.writeheader()
            w.writerow(
                {
                    storage.PAGES_FIELDS[0]: "https://example.com/a",
                    storage.PAGES_FIELDS[1]: "https://example.com/a",
                    storage.PAGES_FIELDS[2]: "example.com",
                },
            )
        assert storage.get_run_status(tmp) == "interrupted"


def test_get_run_status_explicit_new_with_pages_is_interrupted():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, config.PAGES_CSV)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(storage.PAGES_FIELDS)[:3])
            w.writeheader()
            w.writerow(
                {
                    storage.PAGES_FIELDS[0]: "https://example.com/b",
                    storage.PAGES_FIELDS[1]: "https://example.com/b",
                    storage.PAGES_FIELDS[2]: "example.com",
                },
            )
        storage.save_crawl_state(
            tmp,
            status="new",
            pages_crawled=0,
            assets_from_pages=0,
            queue=[],
        )
        assert storage.get_run_status(tmp) == "interrupted"
