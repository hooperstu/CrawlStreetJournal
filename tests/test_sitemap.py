"""Tests for sitemap lastmod extraction."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import sitemap


URLSET_WITH_LASTMOD = """\
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://www.example.com/page-a</loc>
    <lastmod>2024-11-15</lastmod>
  </url>
  <url>
    <loc>https://www.example.com/page-b</loc>
  </url>
  <url>
    <loc>https://www.example.com/page-c</loc>
    <lastmod>2025-01-20T10:30:00+00:00</lastmod>
  </url>
</urlset>
"""

SITEMAP_INDEX = """\
<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>https://www.example.com/sitemap-posts.xml</loc>
  </sitemap>
</sitemapindex>
"""


def test_parse_urlset_extracts_lastmod():
    children, pages = sitemap.parse_sitemap_xml(
        URLSET_WITH_LASTMOD, "https://www.example.com/sitemap.xml"
    )
    assert len(children) == 0
    assert len(pages) == 3
    assert pages["https://www.example.com/page-a"] == "2024-11-15"
    assert pages["https://www.example.com/page-b"] == ""
    assert pages["https://www.example.com/page-c"] == "2025-01-20T10:30:00+00:00"


def test_parse_sitemap_index():
    children, pages = sitemap.parse_sitemap_xml(
        SITEMAP_INDEX, "https://www.example.com/sitemap.xml"
    )
    assert "https://www.example.com/sitemap-posts.xml" in children
    assert len(pages) == 0


def test_parse_empty_xml():
    children, pages = sitemap.parse_sitemap_xml(
        "not xml at all", "https://example.com/sitemap.xml"
    )
    assert len(children) == 0
    assert len(pages) == 0
