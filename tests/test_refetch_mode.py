"""Tests for refetch gap URL collection and CrawlConfig fields."""

import csv
import os

import storage


def test_refetch_gap_requested_urls_includes_row_with_any_blank(tmp_path):
    run_dir = tmp_path / "run_a"
    run_dir.mkdir()
    pages = run_dir / "pages.csv"
    with open(pages, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["requested_url", "author", "publisher"])
        w.writeheader()
        w.writerow(
            {
                "requested_url": "https://a.example/page1",
                "author": "A",
                "publisher": "",
            },
        )
        w.writerow(
            {
                "requested_url": "https://a.example/page2",
                "author": "B",
                "publisher": "P",
            },
        )

    urls = storage.refetch_gap_requested_urls(
        str(run_dir),
        gap_columns=("author", "publisher"),
    )
    assert urls == ["https://a.example/page1"]


def test_refetch_gap_requested_urls_dedupes(tmp_path):
    run_dir = tmp_path / "run_b"
    run_dir.mkdir()
    pages = run_dir / "pages.csv"
    with open(pages, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["requested_url", "author"])
        w.writeheader()
        w.writerow({"requested_url": "https://x.test/a", "author": ""})
        w.writerow({"requested_url": "https://x.test/a", "author": "z"})

    urls = storage.refetch_gap_requested_urls(str(run_dir), ("author",))
    assert urls == ["https://x.test/a"]


def test_crawl_config_has_refetch_fields():
    from config import CrawlConfig

    c = CrawlConfig.from_module()
    assert hasattr(c, "REFETCH_MODE")
    assert c.REFETCH_MODE is False
    d = c.to_dict()
    assert "REFETCH_MODE" in d
    roundtrip = CrawlConfig.from_dict(d, base=CrawlConfig.from_module())
    assert roundtrip.REFETCH_MODE is False
