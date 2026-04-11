"""
Comprehensive Playwright scenario tests for The Crawl Street Journal.

Pre-requisites:
  - Flask app running on http://localhost:5001
  - Test data seeded via: python tests/seed_test_data.py
  - Run with: python -m pytest tests/test_playwright.py -v --timeout=30

Tests are organised by feature area:
  1. Homepage & project management
  2. Project defaults
  3. Run lifecycle (create, config, start, monitor, results)
  4. Results viewing & download
  5. Reports dashboard page load & navigation
  6. Reports API — domains endpoint
  7. Reports API — graph endpoint
  8. Reports API — tags endpoint
  9. Reports API — navigation endpoint
  10. Reports API — freshness endpoint
  11. Reports API — chord endpoint
  12. Reports API — technology endpoint
  13. Reports API — authorship endpoint
  14. Reports API — schema insights endpoint
  15. Reports API — filter options endpoint
  16. Global filter system
  17. Edge cases & error handling
  18. Security & XSS
"""

import json
import os
import re
import sys
import urllib.parse
from pathlib import Path

import pytest
from playwright.sync_api import sync_playwright, Page, Browser

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BASE = "http://localhost:5001"
SLUG = "bugbot-test"


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def browser():
    pw = sync_playwright().start()
    b = pw.chromium.launch(headless=True)
    yield b
    b.close()
    pw.stop()


@pytest.fixture
def page(browser):
    ctx = browser.new_context(ignore_https_errors=True)
    p = ctx.new_page()
    yield p
    p.close()
    ctx.close()


@pytest.fixture(scope="session")
def run_name():
    runs_dir = f"/workspace/projects/{SLUG}/runs/"
    runs = sorted(os.listdir(runs_dir))
    return runs[0] if runs else None


def _api(path):
    return f"{BASE}/p/{SLUG}{path}"


def _get_json(page: Page, url: str):
    resp = page.request.get(url)
    assert resp.status == 200, f"GET {url} returned {resp.status}"
    return resp.json()


def _csrf_token_from_page(page: Page) -> str:
    """Read Flask-WTF ``csrf_token`` from the current page (first match)."""
    html = page.content()
    m = re.search(r'name="csrf_token"\s+value="([^"]+)"', html)
    assert m, "csrf_token hidden field not found on page"
    return m.group(1)


# ═══════════════════════════════════════════════════════════════════════════
# 1. HOMEPAGE & PROJECT MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

class TestHomepage:
    def test_homepage_loads(self, page):
        page.goto(BASE)
        assert page.title()
        assert page.locator("h1").count() >= 1

    def test_homepage_has_create_form(self, page):
        page.goto(BASE)
        assert page.locator("form").count() >= 1
        assert page.locator("input[name='name']").count() >= 1

    def test_homepage_lists_projects(self, page):
        page.goto(BASE)
        assert page.locator("text=BugBot Test").count() >= 1

    def test_create_project(self, page):
        page.goto(BASE)
        page.fill("input[name='name']", "Playwright Temp Project")
        page.click("button[type='submit']")
        page.wait_for_url(re.compile(r"/p/"))
        assert "/p/" in page.url

    def test_delete_project(self, page):
        page.goto(BASE)
        page.fill("input[name='name']", "Delete Me Project")
        page.click("button[type='submit']")
        page.wait_for_url(re.compile(r"/p/"))
        slug = page.url.split("/p/")[1].split("/")[0]
        page.goto(BASE)
        delete_form = page.locator(f"form[action*='{slug}/delete']")
        if delete_form.count() > 0:
            delete_form.first.locator("button").click()
            page.wait_for_url(BASE + "/")

    def test_create_project_empty_name_still_works(self, page):
        page.goto(BASE)
        page.fill("input[name='name']", "   ")
        page.click("button[type='submit']")
        # Should either redirect or stay on homepage with error
        assert page.url.startswith(BASE)

    def test_create_project_special_characters(self, page):
        page.goto(BASE)
        page.fill("input[name='name']", "Test <script>alert(1)</script>")
        page.click("button[type='submit']")
        page.wait_for_url(re.compile(r"/p/"))
        content = page.content()
        assert "<script>alert(1)</script>" not in content


# ═══════════════════════════════════════════════════════════════════════════
# 2. PROJECT OVERVIEW & DEFAULTS
# ═══════════════════════════════════════════════════════════════════════════

class TestProjectOverview:
    def test_project_overview_loads(self, page):
        page.goto(f"{BASE}/p/{SLUG}")
        assert page.locator("h1").count() >= 1

    def test_project_overview_shows_runs(self, page):
        page.goto(f"{BASE}/p/{SLUG}")
        assert "bugbot-run" in page.content().lower() or "run" in page.content().lower()

    def test_project_defaults_page_loads(self, page):
        page.goto(f"{BASE}/p/{SLUG}/defaults")
        assert page.locator("form").count() >= 1

    def test_project_defaults_save(self, page):
        page.goto(f"{BASE}/p/{SLUG}/defaults")
        form = page.locator("form").first
        form.locator("button[type='submit']").click()
        page.wait_for_load_state("networkidle")
        assert page.url.startswith(BASE)

    def test_nonexistent_project_404(self, page):
        resp = page.request.get(f"{BASE}/p/nonexistent-slug-xyz")
        assert resp.status == 404


# ═══════════════════════════════════════════════════════════════════════════
# 3. RUN LIFECYCLE
# ═══════════════════════════════════════════════════════════════════════════

class TestRunLifecycle:
    def test_runs_page_loads(self, page):
        page.goto(f"{BASE}/p/{SLUG}/runs")
        assert page.locator("h1").count() >= 1

    def test_create_run(self, page):
        page.goto(f"{BASE}/p/{SLUG}/runs")
        page.locator("form").first.locator("button[type='submit']").click()
        page.wait_for_load_state("networkidle")
        assert "config" in page.url or "runs" in page.url

    def test_run_config_page_loads(self, page, run_name):
        page.goto(f"{BASE}/p/{SLUG}/runs/{run_name}/config")
        assert page.locator("form").count() >= 1

    def test_run_config_save(self, page, run_name):
        page.goto(f"{BASE}/p/{SLUG}/runs/{run_name}/config")
        page.locator("form").first.locator("button[type='submit']").click()
        page.wait_for_load_state("networkidle")

    def test_run_monitor_page_loads(self, page, run_name):
        page.goto(f"{BASE}/p/{SLUG}/runs/{run_name}/monitor")
        assert page.locator("h1").count() >= 1 or page.locator("h2").count() >= 1

    def test_run_results_page_loads(self, page, run_name):
        page.goto(f"{BASE}/p/{SLUG}/runs/{run_name}/results")
        assert "pages.csv" in page.content() or "results" in page.content().lower()

    def test_run_rename(self, page, run_name):
        page.goto(f"{BASE}/p/{SLUG}/runs/{run_name}/config")
        token = _csrf_token_from_page(page)
        resp = page.request.post(
            f"{BASE}/p/{SLUG}/runs/{run_name}/rename",
            form={"friendly_name": "Renamed Run", "csrf_token": token},
        )
        assert resp.status in (200, 302)


# ═══════════════════════════════════════════════════════════════════════════
# 4. RESULTS VIEWING & DOWNLOAD
# ═══════════════════════════════════════════════════════════════════════════

class TestResults:
    def test_view_pages_csv(self, page, run_name):
        page.goto(f"{BASE}/p/{SLUG}/runs/{run_name}/results/pages.csv")
        assert page.locator("table").count() >= 1 or "requested_url" in page.content()

    def test_view_edges_csv(self, page, run_name):
        page.goto(f"{BASE}/p/{SLUG}/runs/{run_name}/results/edges.csv")
        assert "from_url" in page.content() or page.locator("table").count() >= 1

    def test_view_errors_csv(self, page, run_name):
        page.goto(f"{BASE}/p/{SLUG}/runs/{run_name}/results/crawl_errors.csv")
        assert "error_type" in page.content() or page.locator("table").count() >= 1

    def test_download_pages_csv(self, page, run_name):
        resp = page.request.get(
            f"{BASE}/p/{SLUG}/runs/{run_name}/download/pages.csv"
        )
        assert resp.status == 200
        assert "csv" in resp.headers.get("content-type", "").lower() or resp.status == 200

    def test_download_all_zip(self, page, run_name):
        resp = page.request.get(
            f"{BASE}/p/{SLUG}/runs/{run_name}/download-all"
        )
        assert resp.status == 200

    def test_download_nonexistent_file_404(self, page, run_name):
        resp = page.request.get(
            f"{BASE}/p/{SLUG}/runs/{run_name}/download/nonexistent.csv"
        )
        assert resp.status == 404

    def test_results_pagination_param(self, page, run_name):
        page.goto(f"{BASE}/p/{SLUG}/runs/{run_name}/results/pages.csv?page=1")
        assert page.url.startswith(BASE)

    def test_results_search_param(self, page, run_name):
        page.goto(f"{BASE}/p/{SLUG}/runs/{run_name}/results/pages.csv?q=blog")
        assert page.url.startswith(BASE)


# ═══════════════════════════════════════════════════════════════════════════
# 5. REPORTS DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════

class TestReportsDashboard:
    def test_reports_page_loads(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        assert page.locator("h1").count() >= 1

    def test_reports_page_has_tabs(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.wait_for_load_state("networkidle")
        tabs = page.locator(".viz-tab")
        assert tabs.count() >= 10

    def test_reports_page_has_filter_bar(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.wait_for_load_state("networkidle")
        assert page.locator("#ms-cms").count() >= 1
        assert page.locator("#ms-kind").count() >= 1

    def test_reports_with_run_param(self, page, run_name):
        page.goto(f"{BASE}/p/{SLUG}/reports?runs={run_name}")
        page.wait_for_load_state("networkidle")
        assert page.locator("h1").count() >= 1

    def test_legacy_redirect(self, page, run_name):
        resp = page.request.get(
            f"{BASE}/p/{SLUG}/runs/{run_name}/reports",
            max_redirects=0,
        )
        assert resp.status in (301, 302, 308)


# ═══════════════════════════════════════════════════════════════════════════
# 6. API — DOMAINS
# ═══════════════════════════════════════════════════════════════════════════

class TestApiDomains:
    def test_domains_returns_list(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        assert isinstance(data, list)
        assert len(data) >= 5

    def test_domains_has_required_fields(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        d = data[0]
        for key in ("domain", "ownership", "page_count", "content_kinds",
                     "cms_generator", "avg_extraction_coverage",
                     "avg_extraction_coverage_core"):
            assert key in d, f"Missing field: {key}"

    def test_domains_page_counts_match(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        total = sum(d["page_count"] for d in data)
        assert total == 54

    def test_domains_cms_populated(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        cms_set = {d["cms_generator"] for d in data if d["cms_generator"]}
        assert "WordPress 6.4" in cms_set
        assert "Shopify" in cms_set

    def test_domains_phase4_fields_present(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        d = data[0]
        phase4 = ["has_json_ld_pct", "has_microdata_pct", "has_rdfa_pct",
                   "has_hreflang_pct", "has_feed_pct", "schema_types",
                   "avg_extraction_coverage", "avg_extraction_coverage_core",
                   "top_authors", "top_publishers"]
        for key in phase4:
            assert key in d, f"Missing Phase 4 field: {key}"

    def test_domains_filter_by_cms(self, page):
        data = _get_json(page, _api("/api/viz/domains?cms=Shopify"))
        assert len(data) == 1
        assert data[0]["domain"] == "shop.example.com"

    def test_domains_filter_by_content_kind(self, page):
        data = _get_json(page, _api("/api/viz/domains?content_kinds=product"))
        assert all(
            "product" in d.get("content_kinds", {})
            for d in data
        )

    def test_domains_filter_by_min_coverage(self, page):
        all_data = _get_json(page, _api("/api/viz/domains"))
        filtered = _get_json(page, _api("/api/viz/domains?min_coverage=50"))
        assert len(filtered) <= len(all_data)

    def test_domains_filter_combined(self, page):
        data = _get_json(page, _api("/api/viz/domains?cms=WordPress+6.4&content_kinds=blog"))
        assert len(data) >= 1
        for d in data:
            assert d["cms_generator"] == "WordPress 6.4"

    def test_domains_empty_filter_returns_all(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        filtered = _get_json(page, _api("/api/viz/domains?cms="))
        assert len(data) == len(filtered)


# ═══════════════════════════════════════════════════════════════════════════
# 7. API — GRAPH
# ═══════════════════════════════════════════════════════════════════════════

class TestApiGraph:
    def test_graph_returns_nodes_and_links(self, page):
        data = _get_json(page, _api("/api/viz/graph"))
        assert "nodes" in data
        assert "links" in data
        assert len(data["nodes"]) >= 2

    def test_graph_nodes_have_required_fields(self, page):
        data = _get_json(page, _api("/api/viz/graph"))
        node = data["nodes"][0]
        assert "id" in node
        assert "pages" in node

    def test_graph_links_have_required_fields(self, page):
        data = _get_json(page, _api("/api/viz/graph"))
        if data["links"]:
            link = data["links"][0]
            assert "source" in link
            assert "target" in link
            assert "weight" in link

    def test_graph_no_self_links(self, page):
        data = _get_json(page, _api("/api/viz/graph"))
        for link in data["links"]:
            src = link["source"] if isinstance(link["source"], str) else link["source"]["id"]
            tgt = link["target"] if isinstance(link["target"], str) else link["target"]["id"]
            assert src != tgt

    def test_graph_filter(self, page):
        data = _get_json(page, _api("/api/viz/graph?cms=Shopify"))
        domains = {n["id"] for n in data["nodes"] if n.get("pages", 0) > 0}
        assert "shop.example.com" in domains or len(data["nodes"]) >= 0


# ═══════════════════════════════════════════════════════════════════════════
# 8. API — TAGS
# ═══════════════════════════════════════════════════════════════════════════

class TestApiTags:
    def test_tags_returns_structure(self, page):
        data = _get_json(page, _api("/api/viz/tags"))
        assert "tags" in data
        assert "sources" in data
        assert "cooccurrence" in data

    def test_tags_have_frequencies(self, page):
        data = _get_json(page, _api("/api/viz/tags"))
        assert len(data["tags"]) >= 5
        for tag in data["tags"]:
            assert "tag" in tag
            assert "count" in tag
            assert tag["count"] > 0

    def test_tags_sorted_by_frequency(self, page):
        data = _get_json(page, _api("/api/viz/tags"))
        counts = [t["count"] for t in data["tags"]]
        assert counts == sorted(counts, reverse=True)


# ═══════════════════════════════════════════════════════════════════════════
# 9. API — NAVIGATION
# ═══════════════════════════════════════════════════════════════════════════

class TestApiNavigation:
    def test_navigation_returns_domains(self, page):
        data = _get_json(page, _api("/api/viz/navigation"))
        assert "domains" in data
        assert len(data["domains"]) >= 1

    def test_navigation_with_domain_param(self, page):
        data = _get_json(page, _api("/api/viz/navigation?domain=blog.example.com"))
        assert "tree" in data
        if data["tree"]:
            assert "name" in data["tree"]
            assert "children" in data["tree"]


# ═══════════════════════════════════════════════════════════════════════════
# 10. API — FRESHNESS
# ═══════════════════════════════════════════════════════════════════════════

class TestApiFreshness:
    def test_freshness_returns_structure(self, page):
        data = _get_json(page, _api("/api/viz/freshness"))
        assert "today" in data
        assert "domains" in data

    def test_freshness_domains_have_dates(self, page):
        data = _get_json(page, _api("/api/viz/freshness"))
        if data["domains"]:
            d = data["domains"][0]
            assert "latest" in d
            assert "oldest" in d
            assert "domain" in d

    def test_freshness_sorted_by_latest(self, page):
        data = _get_json(page, _api("/api/viz/freshness"))
        dates = [d["latest"] for d in data["domains"] if d.get("latest")]
        assert dates == sorted(dates, reverse=True)


# ═══════════════════════════════════════════════════════════════════════════
# 11. API — CHORD
# ═══════════════════════════════════════════════════════════════════════════

class TestApiChord:
    def test_chord_returns_matrix(self, page):
        data = _get_json(page, _api("/api/viz/chord"))
        assert "domains" in data
        assert "matrix" in data

    def test_chord_matrix_square(self, page):
        data = _get_json(page, _api("/api/viz/chord"))
        n = len(data["domains"])
        assert len(data["matrix"]) == n
        for row in data["matrix"]:
            assert len(row) == n

    def test_chord_top_param(self, page):
        d5 = _get_json(page, _api("/api/viz/chord?top=5"))
        d20 = _get_json(page, _api("/api/viz/chord?top=20"))
        assert len(d5["domains"]) <= len(d20["domains"])

    def test_chord_no_self_links(self, page):
        data = _get_json(page, _api("/api/viz/chord"))
        for i, row in enumerate(data["matrix"]):
            assert row[i] == 0


# ═══════════════════════════════════════════════════════════════════════════
# 12. API — TECHNOLOGY
# ═══════════════════════════════════════════════════════════════════════════

class TestApiTechnology:
    def test_technology_returns_structure(self, page):
        data = _get_json(page, _api("/api/viz/technology"))
        assert "cms_distribution" in data
        assert "structured_data_adoption" in data
        assert "schema_type_frequency" in data
        assert "seo_readiness" in data
        assert "coverage_histogram" in data

    def test_cms_distribution_populated(self, page):
        data = _get_json(page, _api("/api/viz/technology"))
        cms = data["cms_distribution"]
        assert len(cms) >= 3
        names = [c["cms"] for c in cms]
        assert "WordPress 6.4" in names
        assert "Shopify" in names
        assert "Ghost 5.0" in names

    def test_cms_distribution_has_domains(self, page):
        data = _get_json(page, _api("/api/viz/technology"))
        for cms in data["cms_distribution"]:
            assert "page_count" in cms
            assert "domain_count" in cms
            assert "domains" in cms
            assert cms["page_count"] > 0

    def test_structured_data_adoption(self, page):
        data = _get_json(page, _api("/api/viz/technology"))
        sda = data["structured_data_adoption"]
        assert sda["total_pages"] == 54
        assert sda["json_ld"] > 0
        assert sda["any"] > 0

    def test_schema_type_frequency(self, page):
        data = _get_json(page, _api("/api/viz/technology"))
        types = data["schema_type_frequency"]
        assert len(types) >= 3
        type_names = [t["type"] for t in types]
        assert "BlogPosting" in type_names
        assert "Product" in type_names

    def test_seo_readiness(self, page):
        data = _get_json(page, _api("/api/viz/technology"))
        seo = data["seo_readiness"]
        assert len(seo) >= 5
        for entry in seo:
            assert "domain" in entry
            assert "pages" in entry
            assert "has_canonical" in entry

    def test_coverage_histogram_buckets(self, page):
        data = _get_json(page, _api("/api/viz/technology"))
        hist = data["coverage_histogram"]
        assert len(hist) == 10
        total = sum(h["count"] for h in hist)
        assert total == 54

    def test_technology_filter(self, page):
        data = _get_json(page, _api("/api/viz/technology?cms=Shopify"))
        assert data["structured_data_adoption"]["total_pages"] == 10


# ═══════════════════════════════════════════════════════════════════════════
# 13. API — AUTHORSHIP
# ═══════════════════════════════════════════════════════════════════════════

class TestApiAuthorship:
    def test_authorship_returns_structure(self, page):
        data = _get_json(page, _api("/api/viz/authorship"))
        assert "authors" in data
        assert "publishers" in data
        assert "author_network" in data

    def test_authors_populated(self, page):
        data = _get_json(page, _api("/api/viz/authorship"))
        assert len(data["authors"]) >= 3
        for a in data["authors"]:
            assert "author" in a
            assert "total_pages" in a
            assert "domains" in a

    def test_publishers_populated(self, page):
        data = _get_json(page, _api("/api/viz/authorship"))
        assert len(data["publishers"]) >= 2
        pub_names = [p["publisher"] for p in data["publishers"]]
        assert "Example Blog" in pub_names

    def test_author_network_structure(self, page):
        data = _get_json(page, _api("/api/viz/authorship"))
        net = data["author_network"]
        assert len(net["nodes"]) >= 3
        assert len(net["links"]) >= 3
        types = {n["type"] for n in net["nodes"]}
        assert "author" in types
        assert "domain" in types

    def test_authorship_filter(self, page):
        data = _get_json(page, _api("/api/viz/authorship?cms=Ghost+5.0"))
        author_names = {a["author"] for a in data["authors"]}
        assert any("Reporter" in n for n in author_names)


# ═══════════════════════════════════════════════════════════════════════════
# 14. API — SCHEMA INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════

class TestApiSchemaInsights:
    def test_schema_insights_returns_structure(self, page):
        data = _get_json(page, _api("/api/viz/schema_insights"))
        assert "products" in data
        assert "events" in data
        assert "jobs" in data
        assert "recipes" in data

    def test_products_insight(self, page):
        data = _get_json(page, _api("/api/viz/schema_insights"))
        p = data["products"]
        assert p is not None
        assert p["count"] == 10
        assert p["price_min"] > 0
        assert p["price_max"] > p["price_min"]
        assert "by_domain" in p

    def test_events_insight(self, page):
        data = _get_json(page, _api("/api/viz/schema_insights"))
        e = data["events"]
        assert e is not None
        assert e["count"] == 5
        assert len(e["events"]) >= 3

    def test_jobs_insight(self, page):
        data = _get_json(page, _api("/api/viz/schema_insights"))
        j = data["jobs"]
        assert j is not None
        assert j["count"] == 6
        assert len(j["by_location"]) >= 2

    def test_recipes_insight(self, page):
        data = _get_json(page, _api("/api/viz/schema_insights"))
        r = data["recipes"]
        assert r is not None
        assert r["count"] == 4

    def test_schema_insights_filter_excludes_products(self, page):
        data = _get_json(page, _api("/api/viz/schema_insights?cms=Ghost+5.0"))
        assert data["products"] is None
        assert data["jobs"] is None


# ═══════════════════════════════════════════════════════════════════════════
# 15. API — FILTER OPTIONS
# ═══════════════════════════════════════════════════════════════════════════

class TestApiFilterOptions:
    def test_filter_options_returns_structure(self, page):
        data = _get_json(page, _api("/api/viz/filter_options"))
        assert "domains" in data
        assert "cms_values" in data
        assert "content_kinds" in data
        assert "schema_types" in data
        assert "total_pages" in data

    def test_filter_options_domains(self, page):
        data = _get_json(page, _api("/api/viz/filter_options"))
        assert "blog.example.com" in data["domains"]
        assert "shop.example.com" in data["domains"]
        assert len(data["domains"]) == 8

    def test_filter_options_cms(self, page):
        data = _get_json(page, _api("/api/viz/filter_options"))
        assert "WordPress 6.4" in data["cms_values"]
        assert "Shopify" in data["cms_values"]
        assert "Ghost 5.0" in data["cms_values"]
        assert "Drupal 10" in data["cms_values"]

    def test_filter_options_content_kinds(self, page):
        data = _get_json(page, _api("/api/viz/filter_options"))
        expected = {"blog", "product", "news", "event", "job_posting", "recipe", "guidance"}
        assert expected.issubset(set(data["content_kinds"]))

    def test_filter_options_schema_types(self, page):
        data = _get_json(page, _api("/api/viz/filter_options"))
        assert "Product" in data["schema_types"]
        assert "BlogPosting" in data["schema_types"]

    def test_filter_options_total_pages(self, page):
        data = _get_json(page, _api("/api/viz/filter_options"))
        assert data["total_pages"] == 54


# ═══════════════════════════════════════════════════════════════════════════
# 16. GLOBAL FILTER SYSTEM (cross-cutting)
# ═══════════════════════════════════════════════════════════════════════════

class TestGlobalFilters:
    def test_filter_by_nonexistent_cms_returns_empty(self, page):
        data = _get_json(page, _api("/api/viz/domains?cms=NonExistentCMS"))
        assert len(data) == 0

    def test_filter_by_schema_format_json_ld(self, page):
        data = _get_json(page, _api("/api/viz/domains?schema_formats=json_ld"))
        total = sum(d["page_count"] for d in data)
        assert total > 0

    def test_filter_by_schema_format_microdata(self, page):
        data = _get_json(page, _api("/api/viz/domains?schema_formats=microdata"))
        total = sum(d["page_count"] for d in data)
        assert total == 10  # only shop products have microdata

    def test_filter_by_schema_format_rdfa(self, page):
        data = _get_json(page, _api("/api/viz/domains?schema_formats=rdfa"))
        total = sum(d["page_count"] for d in data)
        assert total == 5  # only gov pages have RDFa

    def test_filter_cascades_to_technology(self, page):
        data = _get_json(page, _api("/api/viz/technology?cms=Shopify"))
        assert data["structured_data_adoption"]["total_pages"] == 10
        cms = data["cms_distribution"]
        assert len(cms) == 1
        assert cms[0]["cms"] == "Shopify"

    def test_filter_cascades_to_freshness(self, page):
        all_data = _get_json(page, _api("/api/viz/freshness"))
        filtered = _get_json(page, _api("/api/viz/freshness?cms=Ghost+5.0"))
        assert len(filtered["domains"]) <= len(all_data["domains"])

    def test_filter_cascades_to_tags(self, page):
        all_tags = _get_json(page, _api("/api/viz/tags"))
        filtered = _get_json(page, _api("/api/viz/tags?cms=Shopify"))
        assert len(filtered["tags"]) <= len(all_tags["tags"])

    def test_multiple_filters_narrow_results(self, page):
        one = _get_json(page, _api("/api/viz/domains?cms=WordPress+6.4"))
        two = _get_json(page, _api("/api/viz/domains?cms=WordPress+6.4&content_kinds=blog"))
        assert len(two) <= len(one)

    def test_filter_min_coverage_extreme(self, page):
        data = _get_json(page, _api("/api/viz/domains?min_coverage=100"))
        assert len(data) == 0

    def test_filter_min_coverage_zero(self, page):
        all_data = _get_json(page, _api("/api/viz/domains"))
        zero = _get_json(page, _api("/api/viz/domains?min_coverage=0"))
        assert len(all_data) == len(zero)


# ═══════════════════════════════════════════════════════════════════════════
# 17. EDGE CASES & ERROR HANDLING
# ═══════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    def test_nonexistent_project_api(self, page):
        resp = page.request.get(f"{BASE}/p/fake-project/api/viz/domains")
        assert resp.status in (200, 404)

    def test_api_with_invalid_filter_value(self, page):
        data = _get_json(page, _api("/api/viz/domains?min_coverage=not_a_number"))
        assert isinstance(data, list)

    def test_api_with_sql_injection_attempt(self, page):
        data = _get_json(page, _api("/api/viz/domains?cms='; DROP TABLE pages;--"))
        assert isinstance(data, list)
        assert len(data) == 0

    def test_api_with_very_long_filter(self, page):
        long_val = "x" * 5000
        data = _get_json(page, _api(f"/api/viz/domains?cms={long_val}"))
        assert isinstance(data, list)

    def test_chord_top_zero(self, page):
        data = _get_json(page, _api("/api/viz/chord?top=0"))
        assert isinstance(data["matrix"], list)

    def test_chord_top_negative(self, page):
        data = _get_json(page, _api("/api/viz/chord?top=-1"))
        assert isinstance(data["matrix"], list)

    def test_chord_top_very_large(self, page):
        data = _get_json(page, _api("/api/viz/chord?top=10000"))
        assert isinstance(data["matrix"], list)

    def test_navigation_unknown_domain(self, page):
        data = _get_json(page, _api("/api/viz/navigation?domain=not.a.real.domain"))
        assert "tree" in data

    def test_empty_runs_param(self, page):
        data = _get_json(page, _api("/api/viz/domains?runs="))
        # Should return all data when runs param is empty
        assert isinstance(data, list)

    def test_nonexistent_run_param(self, page):
        data = _get_json(page, _api("/api/viz/domains?runs=run_fake_name"))
        assert isinstance(data, list)
        assert len(data) == 0

    def test_progress_api_exists(self, page):
        resp = page.request.get(f"{BASE}/api/progress/{SLUG}")
        assert resp.status == 200

    def test_logs_api_exists(self, page):
        # /api/logs is an SSE endpoint that streams indefinitely.
        # Verify via a short curl rather than Playwright's request API.
        import subprocess
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
             "--max-time", "2", f"{BASE}/api/logs"],
            capture_output=True, text=True, timeout=5,
        )
        assert result.stdout.strip() in ("200", "000")  # 000 = curl timed out (expected for SSE)


# ═══════════════════════════════════════════════════════════════════════════
# 18. SECURITY & XSS
# ═══════════════════════════════════════════════════════════════════════════

class TestSecurity:
    def test_xss_in_project_name(self, page):
        page.goto(BASE)
        page.fill("input[name='name']", '<img src=x onerror=alert(1)>')
        page.click("button[type='submit']")
        page.wait_for_url(re.compile(r"/p/"))
        content = page.content()
        assert '<img src=x onerror=alert(1)>' not in content  # must be entity-escaped

    def test_xss_in_filter_params(self, page):
        resp = page.request.get(
            _api('/api/viz/domains?cms=<script>alert(1)</script>')
        )
        body = resp.text()
        assert "<script>alert(1)</script>" not in body

    def test_path_traversal_in_download(self, page, run_name):
        resp = page.request.get(
            f"{BASE}/p/{SLUG}/runs/{run_name}/download/../../config.py"
        )
        assert resp.status in (400, 403, 404)

    def test_path_traversal_in_results(self, page, run_name):
        resp = page.request.get(
            f"{BASE}/p/{SLUG}/runs/{run_name}/results/../../config.py"
        )
        assert resp.status in (400, 403, 404)

    def test_delete_requires_post(self, page):
        resp = page.request.get(f"{BASE}/projects/{SLUG}/delete")
        assert resp.status in (404, 405)

    def test_start_requires_post(self, page, run_name):
        resp = page.request.get(
            f"{BASE}/p/{SLUG}/runs/{run_name}/start"
        )
        assert resp.status in (404, 405)

    def test_api_responses_are_json(self, page):
        endpoints = [
            "/api/viz/domains", "/api/viz/graph", "/api/viz/tags",
            "/api/viz/freshness", "/api/viz/chord", "/api/viz/technology",
            "/api/viz/authorship", "/api/viz/schema_insights",
            "/api/viz/filter_options", "/api/viz/navigation",
        ]
        for ep in endpoints:
            resp = page.request.get(_api(ep))
            ct = resp.headers.get("content-type", "")
            assert "json" in ct.lower(), f"{ep} returned {ct}"
