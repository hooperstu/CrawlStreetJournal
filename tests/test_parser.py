"""Parser extraction test suite — Phase 2 and Phase 4.

Covers every public and private extraction helper in the parser module:
  - Heading outline construction (H2–H6, excluding H1)
  - Structured and visible date extraction (OG, JSON-LD, body text)
  - Link and image counting (internal vs external, missing alt)
  - Privacy-policy URL detection
  - Analytics / tag-manager fingerprinting
  - Training-keyword flagging (URL, title, H1 heuristics)
  - Navigation link extraction and counting
  - build_page_inventory_row integration (Phase 2 + Phase 4 columns)
  - Phase 4 metadata: author, publisher, CMS generator, robots
    directives, hreflang, feeds, pagination, breadcrumb schema,
    microdata, RDFa, JSON-LD @id, schema-specific fields (Product,
    Event, JobPosting), content-kind guessing, and extraction
    coverage percentage.

Fixture HTML fragments (SAMPLE_HTML, PHASE4_HTML, PRODUCT_HTML, etc.)
are defined at module level so individual tests remain concise.
"""

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
  <a href="https://other.example.com/ext">External link</a>
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
    """H1 is deliberately excluded from the outline; only H2–H6 appear."""
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
        _soup(), "https://www.example.com/test"
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
        _soup(), "https://www.example.com/test"
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
        "https://example.com/training/cpd-courses",
        "CPD Training Portal",
        "Training Courses",
    )
    assert "training" in flag
    assert "course" in flag or "courses" in flag


def test_training_keywords_absent():
    flag = parser_module._detect_training_keywords(
        "https://example.com/about",
        "About Us",
        "Who we are",
    )
    assert flag == ""


def test_count_nav_links():
    count = parser_module._count_nav_links(_soup())
    assert count == 3


def test_extract_nav_links():
    rows = parser_module.extract_nav_links(
        _soup(), "https://www.example.com/test", "2025-01-01 00:00:00"
    )
    assert len(rows) == 3
    hrefs = {r["nav_href"] for r in rows}
    assert any("/about" in h for h in hrefs)
    assert all(r["page_url"] == "https://www.example.com/test" for r in rows)


def test_build_page_row_includes_new_fields():
    """Integration: build_page_inventory_row populates all Phase 2 columns
    including response_meta and sitemap_meta pass-through."""
    row, tags = parser_module.build_page_inventory_row(
        SAMPLE_HTML,
        requested_url="https://www.example.com/test",
        final_url="https://www.example.com/test",
        http_status=200,
        content_type="text/html",
        referrer_url="seed",
        depth=0,
        discovered_at="2025-01-01 00:00:00",
        response_meta={"last_modified": "Tue, 14 Feb 2025 12:00:00 GMT", "etag": '"abc123"'},
        sitemap_meta={"sitemap_lastmod": "2025-02-14", "source_sitemap": "https://example.com/sitemap.xml"},
    )
    assert row["http_last_modified"] == "Tue, 14 Feb 2025 12:00:00 GMT"
    assert row["etag"] == '"abc123"'
    assert row["sitemap_lastmod"] == "2025-02-14"
    assert row["referrer_sitemap_url"] == "https://example.com/sitemap.xml"
    assert "H2:" in row["heading_outline"]
    assert row["date_published"]
    assert row["date_modified"]
    assert int(row["img_count"]) == 2
    assert int(row["img_missing_alt_count"]) == 1
    assert "googletagmanager.com" in row["analytics_signals"]
    assert "/privacy-policy" in row["privacy_policy_url"]
    assert int(row["nav_link_count"]) == 3
    # Phase 4 fields present
    assert "author" in row
    assert "publisher" in row
    assert "cms_generator" in row
    assert "robots_directives" in row
    assert "microdata_types" in row
    assert "rdfa_types" in row
    assert "extraction_coverage_pct" in row


# ── Phase 4 tests ─────────────────────────────────────────────────────────

PHASE4_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta name="author" content="John Smith">
  <meta name="generator" content="Ghost 5.0">
  <meta name="robots" content="noindex, nofollow">
  <meta property="og:site_name" content="My Publisher">
  <link rel="alternate" hreflang="de" href="https://example.com/de/page">
  <link rel="alternate" hreflang="fr" href="https://example.com/fr/page">
  <link rel="alternate" type="application/rss+xml" href="/feed.xml">
  <link rel="next" href="https://example.com/page/2">
  <link rel="prev" href="https://example.com/page/0">
  <script type="application/ld+json">
  {
    "@type": "Article",
    "@id": "https://example.com/page#article",
    "author": {"@type": "Person", "name": "John Smith"},
    "publisher": {"@type": "Organization", "name": "Big Publisher"},
    "datePublished": "2024-06-01"
  }
  </script>
  <script type="application/ld+json">
  {
    "@type": "BreadcrumbList",
    "itemListElement": [
      {"@type": "ListItem", "position": 1, "name": "Home"},
      {"@type": "ListItem", "position": 2, "name": "Blog"},
      {"@type": "ListItem", "position": 3, "name": "Article"}
    ]
  }
  </script>
  <title>Phase 4 Test</title>
</head>
<body>
  <main>
    <article>
      <div itemscope itemtype="https://schema.org/Article">
        <span itemprop="name">Test Article</span>
      </div>
      <div typeof="BlogPosting" vocab="https://schema.org/">
        <span property="name">Test RDFa Blog</span>
      </div>
    </article>
  </main>
</body>
</html>
"""


PRODUCT_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <title>Widget Pro</title>
  <script type="application/ld+json">
  {
    "@type": "Product",
    "name": "Widget Pro",
    "offers": {
      "@type": "Offer",
      "price": "29.99",
      "priceCurrency": "GBP",
      "availability": "https://schema.org/InStock"
    },
    "aggregateRating": {
      "@type": "AggregateRating",
      "ratingValue": "4.5",
      "reviewCount": "120"
    }
  }
  </script>
</head>
<body><main><p>Product page.</p></main></body>
</html>
"""


EVENT_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <title>Community Meetup</title>
  <script type="application/ld+json">
  {
    "@type": "Event",
    "name": "Community Meetup",
    "startDate": "2025-09-15T18:00:00Z",
    "location": {"@type": "Place", "name": "Town Hall"}
  }
  </script>
</head>
<body><main><p>Event page.</p></main></body>
</html>
"""


JOB_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <title>Senior Engineer</title>
  <script type="application/ld+json">
  {
    "@type": "JobPosting",
    "title": "Senior Engineer",
    "jobLocation": {
      "@type": "Place",
      "address": {"@type": "PostalAddress", "addressLocality": "London"}
    }
  }
  </script>
</head>
<body><main><p>Job page.</p></main></body>
</html>
"""


def _phase4_soup():
    return BeautifulSoup(PHASE4_HTML, "lxml")


def test_extract_author():
    assert parser_module._extract_author(_phase4_soup()) == "John Smith"


def test_extract_author_from_meta():
    """Fallback path: author is read from <meta name="author"> when no
    JSON-LD author is present."""
    html = '<html><head><meta name="author" content="Alice"></head><body></body></html>'
    soup = BeautifulSoup(html, "lxml")
    assert parser_module._extract_author(soup) == "Alice"


def test_extract_publisher():
    assert parser_module._extract_publisher(_phase4_soup()) == "Big Publisher"


def test_extract_publisher_fallback_og():
    """Fallback path: publisher is derived from og:site_name when JSON-LD
    publisher is absent."""
    html = '<html><head><meta property="og:site_name" content="Fallback Pub"></head><body></body></html>'
    soup = BeautifulSoup(html, "lxml")
    assert parser_module._extract_publisher(soup) == "Fallback Pub"


def test_extract_json_ld_id():
    assert parser_module._extract_json_ld_id(_phase4_soup()) == "https://example.com/page#article"


def test_detect_cms_generator():
    assert parser_module._detect_cms_generator(_phase4_soup()) == "Ghost 5.0"


def test_detect_cms_generator_from_html_signals():
    """Fallback path: CMS is inferred from known CDN script-src patterns
    when no <meta name="generator"> tag is present."""
    html = '<html><head></head><body><script src="https://cdn.shopify.com/s/files/foo.js"></script></body></html>'
    soup = BeautifulSoup(html, "lxml")
    assert parser_module._detect_cms_generator(soup) == "Shopify"


def test_extract_robots_directives():
    directives = parser_module._extract_robots_directives(_phase4_soup())
    assert "meta:noindex, nofollow" in directives


def test_extract_robots_directives_with_header():
    """Robots directives also surface from the X-Robots-Tag HTTP header,
    prefixed with 'header:' to distinguish from meta-tag directives."""
    html = "<html><head></head><body></body></html>"
    soup = BeautifulSoup(html, "lxml")
    result = parser_module._extract_robots_directives(
        soup, response_meta={"x_robots_tag": "noindex"}
    )
    assert "header:noindex" in result


def test_extract_hreflang_links():
    links = parser_module._extract_hreflang_links(
        _phase4_soup(), "https://example.com/page"
    )
    assert "de=" in links
    assert "fr=" in links


def test_extract_feed_urls():
    feeds = parser_module._extract_feed_urls(
        _phase4_soup(), "https://example.com/page"
    )
    assert "feed.xml" in feeds


def test_extract_pagination():
    next_url, prev_url = parser_module._extract_pagination(
        _phase4_soup(), "https://example.com/page"
    )
    assert "page/2" in next_url
    assert "page/0" in prev_url


def test_extract_breadcrumb_schema():
    bc = parser_module._extract_breadcrumb_schema(_phase4_soup())
    assert "Home" in bc
    assert "Blog" in bc
    assert "Article" in bc


def test_extract_microdata():
    md = parser_module._extract_microdata(_phase4_soup())
    assert "Article" in md


def test_extract_rdfa_types():
    rdfa = parser_module._extract_rdfa_types(_phase4_soup())
    assert "BlogPosting" in rdfa


def test_schema_specific_product():
    soup = BeautifulSoup(PRODUCT_HTML, "lxml")
    result = parser_module._extract_schema_specific(soup)
    assert result["schema_price"] == "29.99"
    assert result["schema_currency"] == "GBP"
    assert result["schema_availability"] == "InStock"
    assert result["schema_rating"] == "4.5"
    assert result["schema_review_count"] == "120"


def test_schema_specific_event():
    soup = BeautifulSoup(EVENT_HTML, "lxml")
    result = parser_module._extract_schema_specific(soup)
    assert "2025-09-15" in result["schema_event_date"]
    assert "Town Hall" in result["schema_event_location"]


def test_schema_specific_job():
    soup = BeautifulSoup(JOB_HTML, "lxml")
    result = parser_module._extract_schema_specific(soup)
    assert result["schema_job_title"] == "Senior Engineer"
    assert "London" in result["schema_job_location"]


def test_url_content_hint_product():
    hint = parser_module.url_content_hint("https://shop.example.com/products/widget")
    assert "product_path" in hint


def test_url_content_hint_recipe():
    hint = parser_module.url_content_hint("https://food.example.com/recipes/cake")
    assert "recipe_path" in hint


def test_url_content_hint_faq():
    hint = parser_module.url_content_hint("https://help.example.com/faq")
    assert "faq_path" in hint


def test_guess_content_kind_product():
    kind = parser_module.guess_content_kind(
        "product_path", ["Product"], "", "/products/widget"
    )
    assert kind == "product"


def test_guess_content_kind_recipe():
    kind = parser_module.guess_content_kind(
        "recipe_path", ["Recipe"], "", "/recipes/cake"
    )
    assert kind == "recipe"


def test_guess_content_kind_job():
    """URL hint is empty — classification relies solely on schema type."""
    kind = parser_module.guess_content_kind(
        "", ["JobPosting"], "", "/careers/senior-dev"
    )
    assert kind == "job_posting"


def test_guess_content_kind_event():
    kind = parser_module.guess_content_kind(
        "events_path", ["Event"], "", "/events/meetup"
    )
    assert kind == "event"


def test_extraction_coverage():
    """Coverage percentage must be >0 (fields are populated) and <=100."""
    row, _ = parser_module.build_page_inventory_row(
        PHASE4_HTML,
        requested_url="https://example.com/page",
        final_url="https://example.com/page",
        http_status=200,
        content_type="text/html",
        referrer_url="seed",
        depth=0,
        discovered_at="2025-01-01 00:00:00",
    )
    pct = float(row["extraction_coverage_pct"])
    assert 0 < pct <= 100


def test_build_page_row_phase4_fields():
    """Integration: build_page_inventory_row propagates all Phase 4 columns
    and merges both meta-tag and HTTP-header robots directives."""
    row, _ = parser_module.build_page_inventory_row(
        PHASE4_HTML,
        requested_url="https://example.com/page",
        final_url="https://example.com/page",
        http_status=200,
        content_type="text/html",
        referrer_url="seed",
        depth=0,
        discovered_at="2025-01-01 00:00:00",
        response_meta={"x_robots_tag": "noarchive"},
    )
    assert row["author"] == "John Smith"
    assert row["publisher"] == "Big Publisher"
    assert row["json_ld_id"] == "https://example.com/page#article"
    assert row["cms_generator"] == "Ghost 5.0"
    assert "noindex" in row["robots_directives"]
    assert "noarchive" in row["robots_directives"]
    assert "de=" in row["hreflang_links"]
    assert "feed.xml" in row["feed_urls"]
    assert "page/2" in row["pagination_next"]
    assert "page/0" in row["pagination_prev"]
    assert "Home" in row["breadcrumb_schema"]
    assert "Article" in row["microdata_types"]
    assert "BlogPosting" in row["rdfa_types"]
