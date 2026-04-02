"""Signals-audit test suite.

The signals_audit module is a standalone research tool that inventories
every discoverable metadata signal on a page.  This suite verifies
audit_page() and summarise_audit() against a single richly-annotated
HTML fixture that contains:

  - Meta tags: generator, author, robots, description, OG, Twitter
  - Link tags: canonical, alternate (hreflang + RSS), icon
  - JSON-LD (Article with @id, author, publisher, datePublished)
  - Microdata (schema.org/Article via itemscope/itemprop)
  - RDFa (schema.org/Article via typeof/property)
  - HTML signals: lang attribute, body classes, landmark elements
  - Data attributes on body
  - <time> elements with datetime values
  - Response headers passed in externally

Each test targets one category so regressions are easy to localise.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import signals_audit


SAMPLE_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="generator" content="WordPress 6.4">
  <meta name="author" content="Jane Doe">
  <meta name="robots" content="index, follow">
  <meta name="description" content="A sample page.">
  <meta property="og:title" content="Sample Page">
  <meta property="og:type" content="article">
  <meta property="og:site_name" content="My Site">
  <meta property="article:tag" content="python">
  <meta name="twitter:card" content="summary">
  <meta name="twitter:site" content="@example">
  <link rel="canonical" href="https://example.com/page">
  <link rel="alternate" hreflang="fr" href="https://example.com/fr/page">
  <link rel="alternate" type="application/rss+xml" href="/feed.xml">
  <link rel="icon" href="/favicon.ico">
  <title>Sample Page</title>
  <script type="application/ld+json">
  {
    "@type": "Article",
    "@id": "https://example.com/page#article",
    "author": {"@type": "Person", "name": "Jane Doe"},
    "publisher": {"@type": "Organization", "name": "My Pub"},
    "datePublished": "2024-06-01"
  }
  </script>
</head>
<body class="post-template single-post" data-page-id="42">
  <header>
    <nav aria-label="breadcrumb">
      <a href="/">Home</a> > <a href="/blog">Blog</a> > Sample
    </nav>
  </header>
  <main>
    <article>
      <time datetime="2024-06-01T09:00:00Z">1 June 2024</time>
      <div itemscope itemtype="https://schema.org/Article">
        <span itemprop="name">Sample Page</span>
        <span itemprop="author">Jane Doe</span>
      </div>
      <div typeof="Article" vocab="https://schema.org/">
        <span property="name">Sample RDFa Page</span>
      </div>
      <p>Content goes here with enough text for testing.</p>
    </article>
  </main>
</body>
</html>
"""


def test_audit_page_returns_all_categories():
    """The top-level report must contain exactly the expected category keys
    — no more, no fewer — so downstream consumers can rely on the shape."""
    report = signals_audit.audit_page(SAMPLE_HTML, url="https://example.com/page")
    expected_keys = {
        "url", "meta_tags", "link_tags", "json_ld", "microdata",
        "rdfa", "open_graph", "twitter_cards", "html_signals",
        "response_headers", "data_attributes", "time_elements",
    }
    assert set(report.keys()) == expected_keys


def test_audit_meta_tags():
    report = signals_audit.audit_page(SAMPLE_HTML)
    meta_names = [m.get("name", "") for m in report["meta_tags"] if m.get("name")]
    assert "generator" in meta_names
    assert "author" in meta_names
    assert "robots" in meta_names
    assert "description" in meta_names


def test_audit_link_tags():
    report = signals_audit.audit_page(SAMPLE_HTML, url="https://example.com/page")
    rels = [link.get("rel", "") for link in report["link_tags"]]
    assert "canonical" in rels
    assert "alternate" in rels
    assert "icon" in rels


def test_audit_json_ld():
    report = signals_audit.audit_page(SAMPLE_HTML)
    assert len(report["json_ld"]) >= 1
    types = [j.get("@type", "") for j in report["json_ld"]]
    assert "Article" in types
    ids = [j.get("@id", "") for j in report["json_ld"]]
    assert "https://example.com/page#article" in ids


def test_audit_microdata():
    report = signals_audit.audit_page(SAMPLE_HTML)
    assert len(report["microdata"]) >= 1
    types = [m.get("itemtype", "") for m in report["microdata"]]
    assert any("Article" in t for t in types)


def test_audit_rdfa():
    report = signals_audit.audit_page(SAMPLE_HTML)
    assert len(report["rdfa"]) >= 1
    types = [r.get("typeof", "") for r in report["rdfa"]]
    assert "Article" in types


def test_audit_open_graph():
    report = signals_audit.audit_page(SAMPLE_HTML)
    assert "og:title" in report["open_graph"]
    assert report["open_graph"]["og:title"] == "Sample Page"


def test_audit_twitter_cards():
    report = signals_audit.audit_page(SAMPLE_HTML)
    assert "twitter:card" in report["twitter_cards"]
    assert report["twitter_cards"]["twitter:card"] == "summary"


def test_audit_html_signals():
    report = signals_audit.audit_page(SAMPLE_HTML)
    signals = report["html_signals"]
    assert signals["html_lang"] == "en"
    assert "post-template" in signals["body_classes"]
    assert signals["has_main"] is True
    assert signals["has_nav"] is True
    assert signals["has_article"] is True


def test_audit_data_attributes():
    report = signals_audit.audit_page(SAMPLE_HTML)
    assert "data-page-id" in report["data_attributes"]


def test_audit_time_elements():
    report = signals_audit.audit_page(SAMPLE_HTML)
    assert len(report["time_elements"]) >= 1
    assert report["time_elements"][0]["datetime"] == "2024-06-01T09:00:00Z"


def test_audit_response_headers():
    headers = {"Server": "nginx", "X-Powered-By": "Express"}
    report = signals_audit.audit_page(SAMPLE_HTML, response_headers=headers)
    assert report["response_headers"]["Server"] == "nginx"
    assert report["response_headers"]["X-Powered-By"] == "Express"


def test_summarise_audit():
    """summarise_audit() condenses the full audit dict into a flat
    overview with boolean has_* flags and comma-separated property lists."""
    report = signals_audit.audit_page(
        SAMPLE_HTML,
        url="https://example.com/page",
        response_headers={"Server": "nginx"},
    )
    summary = signals_audit.summarise_audit(report)
    assert summary["url"] == "https://example.com/page"
    assert summary["generator"] == "WordPress 6.4"
    assert summary["server"] == "nginx"
    assert summary["has_json_ld"] is True
    assert summary["has_microdata"] is True
    assert summary["has_rdfa"] is True
    assert "og:title" in summary["og_properties"]
