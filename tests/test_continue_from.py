"""Tests for continuing a new run from a prior run without duplicate page fetches."""

import csv
import os
import tempfile

import config
import scraper
import storage


def test_visited_keys_from_prior_run_normalises_urls():
    with tempfile.TemporaryDirectory() as tmp:
        pages = os.path.join(tmp, config.PAGES_CSV)
        with open(pages, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["requested_url"])
            w.writeheader()
            w.writerow({"requested_url": "https://example.com/a?x=1"})
        keys = scraper.visited_keys_from_prior_run(tmp)
        assert keys
        assert scraper.normalise_url("https://example.com/a?x=1") in keys


def test_init_run_merges_visited_seed_skips_fetch():
    with tempfile.TemporaryDirectory() as base:
        run_dir = os.path.join(base, "run_test")
        os.makedirs(run_dir)
        cfg = scraper.CrawlConfig.from_module()
        cfg.OUTPUT_DIR = base
        cfg.SEED_URLS = ["https://example.com/seed"]
        ctx = storage.StorageContext(base, cfg)
        seed_norm = scraper.normalise_url("https://example.com/seed")
        run_dir_out, pq, queued, visited, sitemap_meta, *_ = scraper._init_run(
            cfg,
            ctx,
            seed_urls=["https://example.com/seed"],
            run_name=None,
            run_folder="run_test",
            resume=False,
            max_pages=100,
            delay_cfg=0,
            max_depth=5,
            on_phase=None,
            visited_seed={seed_norm},
        )
        assert run_dir_out == run_dir
        assert seed_norm in visited
        # Queue may still list the seed; _process_one_url skips duplicate fetches.
        url, ref, depth = pq.pop()
        page_written, _, _ = scraper._process_one_url(
            url,
            ref,
            depth,
            visited,
            queued,
            pq,
            sitemap_meta,
            cfg,
            ctx,
            cfg.MAX_DEPTH,
            delay_cfg=0,
        )
        assert page_written is False
