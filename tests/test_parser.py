"""Tests for Phase 2 parser extraction functions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bs4 import BeautifulSoup
import parser as parser_module


SAMPLE_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <title>Test Page</title>
  <meta name="description" content="A test page.">
  <meta property="article:published_time" content="2024-06-01T09:00:00Z">
  <meta property="article:modified_time" content="2025-02-14T12:00:00Z">
  <script type="application/ld+json">
  {
    "@type": "WebPage",
    "datePublished": "2024-06-01",
    "dateModified": "2025-02-14"
  }
  </script>
  <script src="https://www.googletagmanager.com/gtag/js?id=G-XXXXX"></script>
</head>
<body>
  <nav>
    <a href="/">Home</a>
    <a href="/about">About</a>
    <a href="/contact">Contact</a>
  </nav>
  <h1>Main Heading</h1>
  <h2>Section One</h2>
  <p>Some content here. Last updated: 12 March 2025</p>
  <h3>Subsection A</h3>
  <p>More content</p>
  <h2>Section Two</h2>
  <img src="/img/photo.jpg" alt="A photo">
  <img src="/img/icon.png">
  <a href="/privacy-policy">Privacy Policy</a>
  <a href="/some-page">Internal link</a>
  <a href="https://other.nhs.uk/ext">External link</a>
  <p>Review date: 15 January 2026</p>
  <footer>
    <a href="/privacy-policy">Privacy</a>
  </footer>
</body>
</html>
"""


def _soup():
    return BeautifulSoup(SAMPLE_HTML, "lxml")


def test_heading_outline():
    outline = parser_module._extract_heading_outline(_soup())
    assert "H2:Section One" in outline
    assert "H3:Subsection A" in outline
    assert "H2:Section Two" in outline
    assert "H1:" not in outline


def test_structured_dates():
    pub, mod = parser_module._extract_structured_dates(_soup())
    assert "2024-06-01" in pub
    assert "2025-02-14" in mod


def test_visible_dates():
    dates = parser_module._extract_visible_dates(SAMPLE_HTML)
    assert "12 March 2025" in dates
    assert "15 January 2026" in dates


def test_count_links():
    internal, external, total = parser_module._count_links(
        _soup(), "https://www.example.nhs.uk/test"
    )
    assert internal >= 4
    assert external >= 1
    assert total == internal + external


def test_count_images():
    total, missing = parser_module._count_images(_soup())
    assert total == 2
    assert missing == 1


def test_privacy_policy_url():
    url = parser_module._find_privacy_policy_url(
        _soup(), "https://www.example.nhs.uk/test"
    )
    assert "/privacy-policy" in url


def test_detect_analytics():
    signals = parser_module._detect_analytics(SAMPLE_HTML)
    assert "googletagmanager.com" in signals


def test_detect_analytics_absent():
    signals = parser_module._detect_analytics("<html><body>No analytics</body></html>")
    assert signals == ""


def test_training_keywords():
    flag = parser_module._detect_training_keywords(
        "https://example.nhs.uk/training/cpd-courses",
        "CPD Training Portal",
        "Training Courses",
    )
    assert "training" in flag
    assert "course" in flag or "courses" in flag


def test_training_keywords_absent():
    flag = parser_module._detect_training_keywords(
        "https://example.nhs.uk/about",
        "About Us",
        "Who we are",
    )
    assert flag == ""


def test_count_nav_links():
    count = parser_module._count_nav_links(_soup())
    assert count == 3


def test_extract_nav_links():
    rows = parser_module.extract_nav_links(
        _soup(), "https://www.example.nhs.uk/test", "2025-01-01 00:00:00"
    )
    assert len(rows) == 3
    hrefs = {r["nav_href"] for r in rows}
    assert any("/about" in h for h in hrefs)
    assert all(r["page_url"] == "https://www.example.nhs.uk/test" for r in rows)


def test_build_page_row_includes_new_fields():
    row, tags = parser_module.build_page_inventory_row(
        SAMPLE_HTML,
        requested_url="https://www.example.nhs.uk/test",
        final_url="https://www.example.nhs.uk/test",
        http_status=200,
        content_type="text/html",
        referrer_url="seed",
        depth=0,
        discovered_at="2025-01-01 00:00:00",
        response_meta={"last_modified": "Tue, 14 Feb 2025 12:00:00 GMT", "etag": '"abc123"'},
        sitemap_meta={"sitemap_lastmod": "2025-02-14", "source_sitemap": "https://example.nhs.uk/sitemap.xml"},
    )
    assert row["http_last_modified"] == "Tue, 14 Feb 2025 12:00:00 GMT"
    assert row["etag"] == '"abc123"'
    assert row["sitemap_lastmod"] == "2025-02-14"
    assert row["referrer_sitemap_url"] == "https://example.nhs.uk/sitemap.xml"
    assert "H2:" in row["heading_outline"]
    assert row["date_published"]
    assert row["date_modified"]
    assert int(row["img_count"]) == 2
    assert int(row["img_missing_alt_count"]) == 1
    assert "googletagmanager.com" in row["analytics_signals"]
    assert "/privacy-policy" in row["privacy_policy_url"]
    assert int(row["nav_link_count"]) == 3
