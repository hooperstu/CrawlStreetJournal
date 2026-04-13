"""Tests for HTTP 503 handling: deprioritise host and re-queue at back."""

import os
from unittest.mock import patch

import config
import scraper
import storage


def _clear_domain_rate_state():
    scraper._domain_503_cooldown_until.clear()
    scraper._domain_last_fetch.clear()


def test_priority_queue_deprioritise_hostname_then_other_host_pops_first():
    pq = scraper._PriorityQueue()
    pq.push("https://a.com/x", "", 0, is_seed=True)
    pq.push("https://b.com/y", "", 0, is_seed=True)
    first, _, _ = pq.pop()
    assert "a.com" in first
    n = pq.deprioritise_hostname("a.com")
    assert n == 0
    pq.push_at_back(first, "", 0, is_seed=True)
    second, _, _ = pq.pop()
    assert "b.com" in second
    third, _, _ = pq.pop()
    assert "a.com" in third


def test_deprioritise_hostname_bumps_queued_urls_for_that_host():
    pq = scraper._PriorityQueue()
    pq.push("https://a.com/1", "", 0, is_seed=True)
    pq.push("https://a.com/2", "", 1, is_seed=False)
    pq.push("https://b.com/z", "", 0, is_seed=True)
    moved = pq.deprioritise_hostname("a.com")
    assert moved == 2
    u, _, _ = pq.pop()
    assert "b.com" in u


def _setup_run(tmp_path, run_folder: str):
    os.makedirs(os.path.join(tmp_path, run_folder), exist_ok=True)
    cfg = scraper.CrawlConfig.from_module()
    cfg.OUTPUT_DIR = tmp_path
    cfg.RESPECT_ROBOTS_TXT = False
    cfg.SEED_URLS = ["https://example.com/page"]
    ctx = storage.StorageContext(tmp_path, cfg)
    ctx.initialise_outputs(run_folder=run_folder, run_name="t503")
    run_dir_out, pq, queued, visited, sitemap_meta, *_ = scraper._init_run(
        cfg,
        ctx,
        seed_urls=["https://example.com/page"],
        run_name=None,
        run_folder=run_folder,
        resume=False,
        max_pages=100,
        delay_cfg=0,
        max_depth=5,
        on_phase=None,
    )
    return cfg, ctx, run_dir_out, pq, queued, visited, sitemap_meta


def test_min_seconds_between_requests_for_global_rpm_cap_respects_config():
    cfg = scraper.CrawlConfig.from_module()
    cfg.MAX_GLOBAL_REQUESTS_PER_MINUTE = 60
    assert scraper._min_seconds_between_requests_for_global_rpm_cap(cfg) == 1.0


def test_503_host_cooldown_schedules_until_monotonic_gap(monkeypatch):
    _clear_domain_rate_state()
    cfg = scraper.CrawlConfig.from_module()
    cfg.MAX_GLOBAL_REQUESTS_PER_MINUTE = 60
    t0 = 10_000.0
    monkeypatch.setattr(scraper.time, "monotonic", lambda: t0)
    scraper._schedule_503_host_cooldown("Example.COM", cfg)
    assert scraper._domain_503_cooldown_until["example.com"] == t0 + 1.0


def test_wait_for_domain_applies_503_cooldown(monkeypatch):
    _clear_domain_rate_state()
    cfg = scraper.CrawlConfig.from_module()
    cfg.RESPECT_ROBOTS_TXT = False
    scraper._domain_503_cooldown_until["example.com"] = 100.0
    sleeps = []

    def fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(scraper.time, "monotonic", lambda: 0.0)
    monkeypatch.setattr(scraper.time, "sleep", fake_sleep)
    scraper._wait_for_domain("example.com", 0.0, url="", cfg=cfg)
    assert sleeps == [100.0]


@patch.object(scraper, "fetch_page")
def test_process_one_url_503_defers_without_crawl_error_row(mock_fetch, tmp_path):
    _clear_domain_rate_state()
    mock_fetch.return_value = (
        None,
        503,
        "https://example.com/page",
        "",
        {},
        "HTTP 503",
        0,
        "",
        1,
    )
    cfg, ctx, _run, pq, queued, visited, sitemap_meta = _setup_run(tmp_path, "run_503a")
    url, ref, depth = pq.pop()
    deferrals = scraper._ThreadSafeDict()
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
        http_503_deferrals=deferrals,
    )
    assert page_written is False
    assert scraper.normalise_url(url) not in visited
    err_path = os.path.join(ctx.get_active_run_dir(), config.ERRORS_CSV)
    with open(err_path, encoding="utf-8") as f:
        lines = f.readlines()
    assert len(lines) == 1
    assert len(pq) == 1
    assert "example.com" in scraper._domain_503_cooldown_until


@patch.object(scraper, "fetch_page")
def test_process_one_url_503_after_max_deferrals_writes_error(mock_fetch, tmp_path):
    _clear_domain_rate_state()
    mock_fetch.return_value = (
        None,
        503,
        "https://example.com/page",
        "",
        {},
        "HTTP 503",
        0,
        "",
        1,
    )
    cfg, ctx, _run, pq, queued, visited, sitemap_meta = _setup_run(tmp_path, "run_503b")
    url, ref, depth = pq.pop()
    key = scraper.normalise_url(url)
    deferrals = scraper._ThreadSafeDict()
    deferrals[key] = scraper._MAX_503_DEFERRALS_PER_URL
    scraper._process_one_url(
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
        http_503_deferrals=deferrals,
    )
    err_path = os.path.join(ctx.get_active_run_dir(), config.ERRORS_CSV)
    with open(err_path, encoding="utf-8") as f:
        lines = f.readlines()
    assert len(lines) >= 2
