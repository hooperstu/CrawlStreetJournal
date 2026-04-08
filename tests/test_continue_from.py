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


def test_visited_keys_from_all_prior_runs_unions_and_excludes():
    with tempfile.TemporaryDirectory() as base:
        r1 = os.path.join(base, "run_a")
        r2 = os.path.join(base, "run_b")
        cur = os.path.join(base, "run_current")
        os.makedirs(r1)
        os.makedirs(r2)
        os.makedirs(cur)
        for folder, url in (
            (r1, "https://example.com/one"),
            (r2, "https://example.com/two"),
            (cur, "https://example.com/three"),
        ):
            pages = os.path.join(folder, config.PAGES_CSV)
            with open(pages, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["requested_url"])
                w.writeheader()
                w.writerow({"requested_url": url})
        keys = scraper.visited_keys_from_all_prior_runs(base, exclude_run="run_current")
        assert scraper.normalise_url("https://example.com/one") in keys
        assert scraper.normalise_url("https://example.com/two") in keys
        assert scraper.normalise_url("https://example.com/three") not in keys


def test_create_run_all_prior_copies_config_from_existing_run_not_empty_new():
    with tempfile.TemporaryDirectory() as base:
        cfg = scraper.CrawlConfig.from_module()
        cfg.OUTPUT_DIR = base
        ctx = storage.StorageContext(base, cfg)
        first = ctx.create_run(run_name="first")
        first_dir = os.path.join(base, first)
        patched = storage.load_run_config(first_dir)
        assert patched is not None
        patched["SEED_URLS"] = ["https://example.com/from-first"]
        storage.save_run_config(first_dir, patched)
        second = ctx.create_run(
            run_name="second",
            continue_from_folder=storage.CONTINUE_FROM_ALL_PRIOR_RUNS,
        )
        second_dir = os.path.join(base, second)
        second_cfg = storage.load_run_config(second_dir)
        assert second_cfg.get("SEED_URLS") == ["https://example.com/from-first"]
        with open(os.path.join(second_dir, ".continue_from"), encoding="utf-8") as cf:
            assert cf.read().strip() == storage.CONTINUE_FROM_ALL_PRIOR_RUNS


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
