"""Visualisation data-layer test suite.

Validates the aggregation functions and global filter that power the
Flask GUI dashboards (viz_data module).  Coverage includes:

  - filter_pages: no-op, single-facet, and combined filtering by CMS,
    content kind, schema format, and minimum extraction coverage.
  - aggregate_domains: per-domain roll-ups including Phase 4 fields
    (CMS, authors, publishers, JSON-LD / hreflang / feed adoption
    percentages, average extraction coverage).
  - aggregate_technology: CMS distribution, structured-data adoption,
    schema-type frequency, SEO-readiness, and coverage histogram.
  - aggregate_authorship: author/publisher tables and co-occurrence
    network (nodes + links).
  - aggregate_schema_insights: vertical-specific summaries (Product,
    Event, JobPosting) with a minimum-count threshold.
  - get_filter_options: dynamic option lists derived from crawl data.

Fixture helpers (_write_csv, _make_run_dir) build temporary crawl-run
directories with CSV files matching the real output schema so that
tests exercise the file-reading paths without touching actual projects.
"""

import csv
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import viz_data


# ── Helpers to create temporary run directories with CSV data ─────────

def _write_csv(path, fieldnames, rows):
    """Write a single CSV file at *path*, creating parent directories."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _make_run_dir(tmp_path, pages=None, edges=None, tags=None, errors=None):
    """Scaffold a temporary crawl-run directory with optional CSV files.

    Accepts lists of row dicts; fieldnames are inferred from the first
    row of each list.  Returns the absolute path to the run directory.
    """
    run_dir = os.path.join(str(tmp_path), "test_run")
    os.makedirs(run_dir, exist_ok=True)
    if pages:
        fieldnames = list(pages[0].keys())
        _write_csv(os.path.join(run_dir, "pages.csv"), fieldnames, pages)
    if edges:
        fieldnames = list(edges[0].keys())
        _write_csv(os.path.join(run_dir, "edges.csv"), fieldnames, edges)
    if tags:
        fieldnames = list(tags[0].keys())
        _write_csv(os.path.join(run_dir, "tags.csv"), fieldnames, tags)
    if errors:
        fieldnames = list(errors[0].keys())
        _write_csv(os.path.join(run_dir, "crawl_errors.csv"), fieldnames, errors)
    return run_dir


SAMPLE_PAGES = [
    {
        "requested_url": "https://example.com/",
        "final_url": "https://example.com/",
        "domain": "example.com",
        "http_status": "200",
        "content_type": "text/html",
        "title": "Example Home",
        "content_kind_guess": "homepage",
        "word_count": "500",
        "img_count": "3",
        "img_missing_alt_count": "1",
        "readability_fk_grade": "8.5",
        "json_ld_types": "WebPage",
        "microdata_types": "",
        "rdfa_types": "",
        "cms_generator": "WordPress 6.4",
        "author": "Alice Smith",
        "publisher": "Example Corp",
        "robots_directives": "meta:index, follow",
        "hreflang_links": "fr=https://example.com/fr/",
        "feed_urls": "https://example.com/feed.xml",
        "pagination_next": "",
        "pagination_prev": "",
        "breadcrumb_schema": "Home",
        "canonical_url": "https://example.com/",
        "extraction_coverage_pct": "55.0",
        "extraction_coverage_core_pct": "60.0",
        "date_published": "2025-01-15",
        "date_modified": "2025-03-01",
        "schema_price": "",
        "schema_currency": "",
        "schema_availability": "",
        "schema_rating": "",
        "schema_review_count": "",
        "schema_event_date": "",
        "schema_event_location": "",
        "schema_job_title": "",
        "schema_job_location": "",
        "schema_recipe_time": "",
        "analytics_signals": "googletagmanager.com",
        "privacy_policy_url": "/privacy",
        "depth": "0",
        "sitemap_lastmod": "",
        "link_count_internal": "15",
        "link_count_external": "3",
        "training_related_flag": "",
        "wcag_lang_valid": "1",
        "wcag_heading_order_valid": "1",
        "wcag_title_present": "1",
        "wcag_form_labels_pct": "1.0",
        "wcag_landmarks_present": "1",
        "wcag_vague_link_pct": "0.05",
        "nav_link_count": "5",
    },
    {
        "requested_url": "https://example.com/blog",
        "final_url": "https://example.com/blog",
        "domain": "example.com",
        "http_status": "200",
        "content_type": "text/html",
        "title": "Blog",
        "content_kind_guess": "blog",
        "word_count": "300",
        "img_count": "1",
        "img_missing_alt_count": "0",
        "readability_fk_grade": "10.2",
        "json_ld_types": "BlogPosting",
        "microdata_types": "Article",
        "rdfa_types": "",
        "cms_generator": "WordPress 6.4",
        "author": "Bob Jones",
        "publisher": "Example Corp",
        "robots_directives": "",
        "hreflang_links": "",
        "feed_urls": "",
        "pagination_next": "https://example.com/blog/page/2",
        "pagination_prev": "",
        "breadcrumb_schema": "Home > Blog",
        "canonical_url": "https://example.com/blog",
        "extraction_coverage_pct": "42.0",
        "extraction_coverage_core_pct": "48.0",
        "date_published": "2025-02-10",
        "date_modified": "",
        "schema_price": "",
        "schema_currency": "",
        "schema_availability": "",
        "schema_rating": "",
        "schema_review_count": "",
        "schema_event_date": "",
        "schema_event_location": "",
        "schema_job_title": "",
        "schema_job_location": "",
        "schema_recipe_time": "",
        "analytics_signals": "googletagmanager.com|dataLayer",
        "privacy_policy_url": "",
        "depth": "1",
        "sitemap_lastmod": "2025-02-10",
        "link_count_internal": "8",
        "link_count_external": "2",
        "training_related_flag": "",
        "wcag_lang_valid": "1",
        "wcag_heading_order_valid": "0",
        "wcag_title_present": "1",
        "wcag_form_labels_pct": "1.0",
        "wcag_landmarks_present": "0",
        "wcag_vague_link_pct": "0.1",
        "nav_link_count": "5",
    },
    {
        "requested_url": "https://shop.example.com/products/widget",
        "final_url": "https://shop.example.com/products/widget",
        "domain": "shop.example.com",
        "http_status": "200",
        "content_type": "text/html",
        "title": "Widget Pro",
        "content_kind_guess": "product",
        "word_count": "200",
        "img_count": "5",
        "img_missing_alt_count": "2",
        "readability_fk_grade": "6.0",
        "json_ld_types": "Product",
        "microdata_types": "",
        "rdfa_types": "",
        "cms_generator": "Shopify",
        "author": "",
        "publisher": "Widget Store",
        "robots_directives": "",
        "hreflang_links": "",
        "feed_urls": "",
        "pagination_next": "",
        "pagination_prev": "",
        "breadcrumb_schema": "",
        "canonical_url": "https://shop.example.com/products/widget",
        "extraction_coverage_pct": "60.0",
        "extraction_coverage_core_pct": "65.0",
        "date_published": "",
        "date_modified": "",
        "schema_price": "29.99",
        "schema_currency": "GBP",
        "schema_availability": "InStock",
        "schema_rating": "4.5",
        "schema_review_count": "120",
        "schema_event_date": "",
        "schema_event_location": "",
        "schema_job_title": "",
        "schema_job_location": "",
        "schema_recipe_time": "",
        "analytics_signals": "",
        "privacy_policy_url": "",
        "depth": "0",
        "sitemap_lastmod": "",
        "link_count_internal": "10",
        "link_count_external": "0",
        "training_related_flag": "",
        "wcag_lang_valid": "1",
        "wcag_heading_order_valid": "1",
        "wcag_title_present": "1",
        "wcag_form_labels_pct": "1.0",
        "wcag_landmarks_present": "1",
        "wcag_vague_link_pct": "0.0",
        "nav_link_count": "3",
    },
]


# ── Filter tests ──────────────────────────────────────────────────────

def test_filter_pages_no_filter():
    result = viz_data.filter_pages(SAMPLE_PAGES, None)
    assert len(result) == 3


def test_filter_pages_empty_filter():
    result = viz_data.filter_pages(SAMPLE_PAGES, {})
    assert len(result) == 3


def test_filter_pages_by_cms():
    result = viz_data.filter_pages(SAMPLE_PAGES, {"cms": ["WordPress"]})
    assert len(result) == 2
    assert all("WordPress" in r["cms_generator"] for r in result)


def test_filter_pages_by_content_kind():
    result = viz_data.filter_pages(SAMPLE_PAGES, {"content_kinds": ["product"]})
    assert len(result) == 1
    assert result[0]["content_kind_guess"] == "product"


def test_filter_pages_by_schema_format():
    result = viz_data.filter_pages(SAMPLE_PAGES, {"schema_formats": ["microdata"]})
    assert len(result) == 1
    assert result[0]["microdata_types"] == "Article"


def test_filter_pages_by_min_coverage():
    result = viz_data.filter_pages(SAMPLE_PAGES, {"min_coverage": 50})
    assert len(result) == 2
    assert all(float(r["extraction_coverage_pct"]) >= 50 for r in result)


def test_filter_pages_combined():
    """Multiple filter facets are AND-ed together, not OR-ed."""
    result = viz_data.filter_pages(SAMPLE_PAGES, {
        "cms": ["WordPress"],
        "min_coverage": 50,
    })
    assert len(result) == 1
    assert result[0]["domain"] == "example.com"


# ── Aggregation tests ────────────────────────────────────────────────

def test_aggregate_domains_basic(tmp_path):
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_domains([run_dir])
    assert len(result) == 2
    domains = {r["domain"] for r in result}
    assert "example.com" in domains
    assert "shop.example.com" in domains


def test_aggregate_domains_phase4_fields(tmp_path):
    """Phase 4 roll-up columns (CMS, top authors/publishers, adoption
    percentages) are present and correctly aggregated per domain."""
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_domains([run_dir])
    ex = next(r for r in result if r["domain"] == "example.com")
    assert ex["cms_generator"] == "WordPress 6.4"
    assert "Alice Smith" in ex["top_authors"]
    assert "Example Corp" in ex["top_publishers"]
    assert ex["has_json_ld_pct"] == 100.0
    assert ex["has_hreflang_pct"] == 50.0
    assert ex["has_feed_pct"] == 50.0
    assert ex["avg_extraction_coverage"] > 0
    assert ex["avg_extraction_coverage_core"] > 0


def test_aggregate_domains_with_filter(tmp_path):
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_domains([run_dir], filters={"cms": ["Shopify"]})
    assert len(result) == 1
    assert result[0]["domain"] == "shop.example.com"


def test_aggregate_technology(tmp_path):
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_technology([run_dir])

    assert len(result["cms_distribution"]) >= 2
    cms_names = [c["cms"] for c in result["cms_distribution"]]
    assert "WordPress 6.4" in cms_names
    assert "Shopify" in cms_names

    adoption = result["structured_data_adoption"]
    assert adoption["total_pages"] == 3
    assert adoption["json_ld"] == 3

    schema_types = [t["type"] for t in result["schema_type_frequency"]]
    assert "Product" in schema_types

    assert len(result["seo_readiness"]) == 2
    assert len(result["coverage_histogram"]) == 10


def test_aggregate_authorship(tmp_path):
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_authorship([run_dir])

    assert len(result["authors"]) == 2
    author_names = [a["author"] for a in result["authors"]]
    assert "Alice Smith" in author_names
    assert "Bob Jones" in author_names

    assert len(result["publishers"]) == 2

    net = result["author_network"]
    assert len(net["nodes"]) > 0
    assert len(net["links"]) > 0


def test_aggregate_schema_insights(tmp_path):
    """Vertical summaries are None when fewer than the minimum threshold
    of items exist; once enough Product rows are added, the summary
    materialises with the correct count."""
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_schema_insights([run_dir])

    assert result["products"] is None
    assert result["events"] is None
    assert result["jobs"] is None

    extra_products = SAMPLE_PAGES + [
        dict(SAMPLE_PAGES[2], schema_price="19.99", title="Widget Lite"),
        dict(SAMPLE_PAGES[2], schema_price="49.99", title="Widget Max"),
    ]
    run_dir2 = _make_run_dir(tmp_path.parent / (tmp_path.name + "_2"), pages=extra_products)
    result2 = viz_data.aggregate_schema_insights([run_dir2])
    assert result2["products"] is not None
    assert result2["products"]["count"] == 3


def test_get_filter_options(tmp_path):
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    opts = viz_data.get_filter_options([run_dir])
    assert "example.com" in opts["domains"]
    assert "shop.example.com" in opts["domains"]
    assert "WordPress 6.4" in opts["cms_values"]
    assert "Shopify" in opts["cms_values"]
    assert "product" in opts["content_kinds"]
    assert "Product" in opts["schema_types"]
    assert opts["total_pages"] == 3


# ── Page Depth Analysis tests ─────────────────────────────────────────

def test_aggregate_page_depth_basic(tmp_path):
    """Histogram and quality data are produced for each depth level."""
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_page_depth([run_dir])

    assert len(result["depth_histogram"]) >= 2
    depth_0 = next(d for d in result["depth_histogram"] if d["depth"] == 0)
    depth_1 = next(d for d in result["depth_histogram"] if d["depth"] == 1)
    assert depth_0["count"] == 2
    assert depth_1["count"] == 1

    assert len(result["depth_quality"]) >= 2
    q0 = next(q for q in result["depth_quality"] if q["depth"] == 0)
    assert q0["avg_words"] > 0
    assert q0["page_count"] == 2
    assert "avg_coverage_core" in q0
    assert q0["avg_coverage_core"] >= q0["avg_coverage"]


def test_aggregate_page_depth_domain_breakdown(tmp_path):
    """Domain depth breakdown includes the top domains."""
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_page_depth([run_dir])

    dom_names = [d["domain"] for d in result["domain_depth"]]
    assert "example.com" in dom_names


def test_aggregate_page_depth_empty(tmp_path):
    """Empty run dirs return empty lists."""
    run_dir = _make_run_dir(tmp_path, pages=[])
    result = viz_data.aggregate_page_depth([run_dir])
    assert result["depth_histogram"] == []
    assert result["depth_quality"] == []
    assert result["domain_depth"] == []


def test_aggregate_page_depth_with_filter(tmp_path):
    """Filters restrict the depth analysis to matching rows."""
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_page_depth(
        [run_dir], filters={"cms": ["WordPress"]},
    )
    total = sum(d["count"] for d in result["depth_histogram"])
    assert total == 2


# ── Content Health Matrix tests ───────────────────────────────────────

def test_aggregate_content_health_basic(tmp_path):
    """Matrix has expected dimensions and percentage values."""
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_content_health([run_dir])

    assert len(result["domains"]) == 2
    assert len(result["signals"]) > 5
    assert len(result["matrix"]) == 2
    assert len(result["matrix"][0]) == len(result["signals"])
    assert len(result["page_counts"]) == 2

    for row in result["matrix"]:
        for val in row:
            assert 0 <= val <= 100


def test_aggregate_content_health_signals(tmp_path):
    """Key signals (Title, Meta Desc, JSON-LD, etc.) are in the list."""
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_content_health([run_dir])

    assert "Title" in result["signals"]
    assert "JSON-LD" in result["signals"]
    assert "Canonical" in result["signals"]
    assert "Lang Attr" in result["signals"]


def test_aggregate_content_health_empty(tmp_path):
    """Empty run dirs return empty matrix."""
    run_dir = _make_run_dir(tmp_path, pages=[])
    result = viz_data.aggregate_content_health([run_dir])
    assert result["domains"] == []
    assert result["matrix"] == []


def test_aggregate_content_health_with_filter(tmp_path):
    """Filters restrict the health matrix to matching domains."""
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    result = viz_data.aggregate_content_health(
        [run_dir], filters={"domains": ["shop.example.com"]},
    )
    assert len(result["domains"]) == 1
    assert result["domains"][0] == "shop.example.com"


# ── Content & On-Site Performance Audit tests ────────────────────────────

def test_aggregate_content_performance_audit(tmp_path):
    """Thin pages, hash duplicates, internal edges, keyword gaps."""
    p1 = dict(SAMPLE_PAGES[0])
    p1["word_count"] = "80"
    p1["content_hash"] = "abc123"
    p1["tags_all"] = "widgets|pricing"
    p1["h1_joined"] = "Welcome"

    p2 = dict(SAMPLE_PAGES[1])
    p2["content_hash"] = "abc123"
    p2["tags_all"] = "widgets"
    p2["h1_joined"] = "Our widgets catalogue"

    p3 = dict(SAMPLE_PAGES[2])
    p3["canonical_url"] = "https://shop.example.com/products/widget"
    p3["final_url"] = "https://shop.example.com/products/widget-copy"
    p3["requested_url"] = "https://shop.example.com/products/widget-copy"
    p3["tags_all"] = "xyzunknownterm"
    p3["title"] = "Different title"
    p3["h1_joined"] = "No match here"

    p4 = dict(SAMPLE_PAGES[2])
    p4["canonical_url"] = "https://shop.example.com/products/widget"
    p4["final_url"] = "https://shop.example.com/products/widget-alt"
    p4["requested_url"] = "https://shop.example.com/products/widget-alt"
    p4["title"] = "Alt product"

    edges = [
        {
            "from_url": "https://example.com/",
            "to_url": "https://example.com/blog",
            "link_text": "Blog",
            "discovered_at": "",
        },
        {
            "from_url": "https://example.com/blog",
            "to_url": "https://example.com/",
            "link_text": "Home",
            "discovered_at": "",
        },
    ]

    run_dir = _make_run_dir(tmp_path, pages=[p1, p2, p3, p4], edges=edges)
    result = viz_data.aggregate_content_performance_audit([run_dir])

    assert result["summary"]["thin_count"] >= 1
    assert result["summary"]["duplicate_hash_cluster_count"] >= 1
    assert result["summary"]["canonical_duplicate_group_count"] >= 1
    assert result["summary"]["internal_edge_count"] >= 1
    assert result["summary"]["pages_with_tags_all"] >= 1

    gaps = result["keyword_mapping"]["gap_sample"]
    assert any("xyzunknownterm" in (g.get("tags_all") or "") for g in gaps)


def test_aggregate_content_performance_audit_empty(tmp_path):
    """Empty pages returns empty-shaped payload."""
    run_dir = _make_run_dir(tmp_path, pages=[])
    result = viz_data.aggregate_content_performance_audit([run_dir])
    assert result["summary"]["page_count"] == 0
    assert result["thin_content"]["sample"] == []


def test_aggregate_content_performance_audit_full_lists_flag(tmp_path):
    """full_lists=True returns full_lists key and uncapped thin sample."""
    run_dir = _make_run_dir(tmp_path, pages=SAMPLE_PAGES)
    r = viz_data.aggregate_content_performance_audit(
        [run_dir], full_lists=True,
    )
    assert r.get("full_lists") is True
    assert len(r["thin_content"]["sample"]) == len([
        p for p in SAMPLE_PAGES
        if int(p.get("word_count", "0") or 0) <= viz_data._THIN_WORD_THRESHOLD
    ])


def test_aggregate_technical_performance_per_domain(tmp_path):
    """Per-domain roll-ups include fetch time and asset categories."""
    p1 = dict(SAMPLE_PAGES[0])
    p1["fetch_time_ms"] = "500"
    p1["has_viewport_meta"] = "1"
    p2 = dict(SAMPLE_PAGES[1])
    p2["fetch_time_ms"] = "4000"
    p2["has_viewport_meta"] = "0"

    img_row = {
        "referrer_page_url": p1["final_url"],
        "asset_url": "https://example.com/hero.jpg",
        "link_text": "",
        "category": "image",
        "head_content_type": "image/jpeg",
        "head_content_length": "600000",
        "discovered_at": "",
    }
    script_row = {
        "referrer_page_url": p1["final_url"],
        "asset_url": "https://cdn.other.com/big.js",
        "link_text": "",
        "category": "script",
        "head_content_type": "application/javascript",
        "head_content_length": "",
        "discovered_at": "",
    }
    run_dir = _make_run_dir(tmp_path, pages=[p1, p2], edges=None)
    for name, row in (("assets_image.csv", img_row), ("assets_script.csv", script_row)):
        ap = os.path.join(run_dir, name)
        with open(ap, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(row.keys()), quoting=csv.QUOTE_ALL)
            w.writeheader()
            w.writerow(row)

    result = viz_data.aggregate_technical_performance([run_dir])
    assert "domains" in result
    ex = next((d for d in result["domains"] if d["domain"] == "example.com"), None)
    assert ex is not None
    assert ex["slow_page_count"] >= 1
    assert ex["assets_by_category"].get("image", 0) >= 1
    assert len(ex["external_scripts_top"]) >= 1


def test_aggregate_key_metrics_snapshot(tmp_path):
    """Discovery mix and structural proxies per domain."""
    a = dict(SAMPLE_PAGES[0])
    a["referrer_url"] = "seed"
    b = dict(SAMPLE_PAGES[1])
    b["referrer_url"] = "https://www.facebook.com/share"
    c = dict(SAMPLE_PAGES[2])
    c["referrer_url"] = "sitemap:https://example.com/sitemap.xml"
    run_dir = _make_run_dir(tmp_path, pages=[a, b, c])
    result = viz_data.aggregate_key_metrics_snapshot([run_dir])
    assert "disclaimer" in result
    ex = next((d for d in result["domains"] if d["domain"] == "example.com"), None)
    assert ex is not None
    assert ex["discovery_mix_counts"].get("direct_seed", 0) >= 1
    assert ex["discovery_mix_counts"].get("social_referrer", 0) >= 1
    shop = next((d for d in result["domains"] if d["domain"] == "shop.example.com"), None)
    assert shop is not None
    assert shop["discovery_mix_counts"].get("sitemap", 0) >= 1

    full = viz_data.aggregate_key_metrics_snapshot(
        [run_dir], filters=None, full_lists=True,
    )
    assert full.get("full_lists") is True
    assert len(full.get("page_breakdown") or []) == 3
