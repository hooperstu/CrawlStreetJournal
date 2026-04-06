"""Tests for SITEMAP_DISCOVERY_MODE and project-level discovery cache."""

import scraper
import storage
from config import CrawlConfig


def test_discovery_fingerprint_changes_with_seeds():
    a = CrawlConfig.from_dict(
        {"SEED_URLS": ["https://a.com"], "SITEMAP_URLS": [], "LOAD_SITEMAPS_FROM_ROBOTS": True},
        base=CrawlConfig.from_module(),
    )
    b = CrawlConfig.from_dict(
        {"SEED_URLS": ["https://b.com"], "SITEMAP_URLS": [], "LOAD_SITEMAPS_FROM_ROBOTS": True},
        base=CrawlConfig.from_module(),
    )
    assert scraper._discovery_fingerprint(a) != scraper._discovery_fingerprint(b)


def test_discovered_sitemaps_cache_roundtrip(monkeypatch, tmp_path):
    import config as project_config

    monkeypatch.setattr(project_config, "PROJECTS_DIR", str(tmp_path / "projects"))
    slug = "test-proj"
    (tmp_path / "projects" / slug).mkdir(parents=True)

    items = [("https://example.com/a", "seed"), ("https://example.com/b", "sitemap:x")]
    meta = {
        scraper.normalise_url("https://example.com/a"): {
            "sitemap_lastmod": "",
            "source_sitemap": "https://example.com/sitemap.xml",
        }
    }
    fp = scraper._discovery_fingerprint(
        CrawlConfig.from_dict(
            {"SEED_URLS": ["https://example.com/"]},
            base=CrawlConfig.from_module(),
        )
    )
    storage.save_discovered_sitemaps_cache(slug, fp, items, meta)
    loaded = storage.load_discovered_sitemaps_cache(slug)
    assert loaded is not None
    assert loaded["fingerprint"] == fp
    assert len(loaded["items"]) == 2
