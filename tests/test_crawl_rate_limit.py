"""Tests for per-request rate limiting vs robots.txt Crawl-delay."""

import scraper
from config import CrawlConfig


def test_wait_for_domain_does_not_query_crawl_delay_when_robots_disabled(monkeypatch):
    """Ignoring robots should not apply Crawl-delay (avoids extra robots fetches + 10s floors)."""
    called = []

    def fake_get_crawl_delay(url, cfg=None):
        called.append(url)
        return 99.0

    monkeypatch.setattr(scraper, "_get_crawl_delay", fake_get_crawl_delay)
    cfg = CrawlConfig.from_module()
    cfg.RESPECT_ROBOTS_TXT = False
    scraper._wait_for_domain(
        "example.com",
        0.0,
        url="https://example.com/page",
        cfg=cfg,
    )
    assert called == []


def test_wait_for_domain_consults_crawl_delay_when_robots_enabled(monkeypatch):
    """When obeying robots, Crawl-delay may be merged into the wait (here: None → no extra wait)."""
    called = []

    def fake_get_crawl_delay(url, cfg=None):
        called.append(url)
        return None

    monkeypatch.setattr(scraper, "_get_crawl_delay", fake_get_crawl_delay)
    cfg = CrawlConfig.from_module()
    cfg.RESPECT_ROBOTS_TXT = True
    scraper._wait_for_domain(
        "example.com",
        0.0,
        url="https://example.com/page",
        cfg=cfg,
    )
    assert len(called) == 1
    assert called[0] == "https://example.com/page"
