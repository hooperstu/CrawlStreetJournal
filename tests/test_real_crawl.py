"""
Playwright tests against the real NHS estate crawl data.

These tests verify that real-world scraped data renders correctly
through every layer of the application — from CSV parsing through
aggregation to API responses to D3.js visualisation rendering.
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BASE = "http://localhost:5001"
SLUG = "nhs-estate-crawl"


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


def _api(path):
    return f"{BASE}/p/{SLUG}{path}"


def _get_json(page, url):
    resp = page.request.get(url)
    assert resp.status == 200, f"GET {url} returned {resp.status}"
    return resp.json()


# ═══════════════════════════════════════════════════════════════════════════
# DATA INTEGRITY — real scraped metadata
# ═══════════════════════════════════════════════════════════════════════════

class TestRealDataIntegrity:
    def test_pages_csv_not_empty(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        total = sum(d["page_count"] for d in data)
        assert total >= 100, f"Expected 100+ pages, got {total}"

    def test_multiple_domains_crawled(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        assert len(data) >= 3, f"Expected 3+ domains, got {len(data)}"

    def test_nhs_domains_present(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        doms = {d["domain"] for d in data}
        nhs_doms = [d for d in doms if "nhs" in d.lower()]
        assert len(nhs_doms) >= 2, f"Expected NHS domains, got {doms}"

    def test_cms_detected_on_real_sites(self, page):
        data = _get_json(page, _api("/api/viz/filter_options"))
        assert len(data["cms_values"]) >= 1, "Expected at least one CMS detected"

    def test_content_kinds_diverse(self, page):
        data = _get_json(page, _api("/api/viz/filter_options"))
        assert len(data["content_kinds"]) >= 3, f"Expected 3+ content kinds, got {data['content_kinds']}"

    def test_extraction_coverage_non_zero(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        coverages = [d.get("avg_extraction_coverage", 0) for d in data]
        assert any(c > 0 for c in coverages), "Expected non-zero extraction coverage"

    def test_no_nan_in_domain_stats(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        for d in data:
            for key, val in d.items():
                if isinstance(val, float):
                    assert val == val, f"NaN found in {d['domain']}.{key}"
                    assert val != float("inf"), f"Inf found in {d['domain']}.{key}"

    def test_no_empty_domains(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        for d in data:
            assert d["domain"], "Empty domain name found"
            assert d["page_count"] > 0

    def test_readability_scores_reasonable(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        for d in data:
            r = d.get("avg_readability", 0)
            if r > 0:
                # FK grade scale: 0 (easiest) to ~40+ (dense academic text)
                assert -5 < r < 50, f"Unreasonable readability {r} for {d['domain']}"

    def test_wcag_percentages_in_range(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        pct_fields = ["wcag_lang_pct", "wcag_heading_order_pct", "wcag_title_pct",
                       "wcag_landmarks_pct"]
        for d in data:
            for f in pct_fields:
                val = d.get(f, 0)
                assert 0 <= val <= 100, f"{f}={val} out of range for {d['domain']}"

    def test_phase4_fields_populated(self, page):
        data = _get_json(page, _api("/api/viz/domains"))
        has_json_ld = any(d.get("has_json_ld_pct", 0) > 0 for d in data)
        assert has_json_ld, "Expected at least one domain with JSON-LD"


# ═══════════════════════════════════════════════════════════════════════════
# TECHNOLOGY ENDPOINT — real CMS/structured data
# ═══════════════════════════════════════════════════════════════════════════

class TestRealTechnology:
    def test_cms_distribution_realistic(self, page):
        data = _get_json(page, _api("/api/viz/technology"))
        cms = data["cms_distribution"]
        assert len(cms) >= 1
        for c in cms:
            assert c["page_count"] > 0
            assert c["domain_count"] > 0
            assert len(c["domains"]) > 0

    def test_structured_data_totals_consistent(self, page):
        data = _get_json(page, _api("/api/viz/technology"))
        sda = data["structured_data_adoption"]
        assert sda["total_pages"] > 0
        assert sda["any"] + sda["none"] == sda["total_pages"]
        assert sda["json_ld"] <= sda["total_pages"]
        assert sda["microdata"] <= sda["total_pages"]
        assert sda["rdfa"] <= sda["total_pages"]

    def test_seo_readiness_all_domains_covered(self, page):
        data = _get_json(page, _api("/api/viz/technology"))
        domains_api = _get_json(page, _api("/api/viz/domains"))
        seo_domains = {s["domain"] for s in data["seo_readiness"]}
        api_domains = {d["domain"] for d in domains_api}
        assert seo_domains == api_domains

    def test_coverage_histogram_sums_to_total(self, page):
        data = _get_json(page, _api("/api/viz/technology"))
        hist_total = sum(h["count"] for h in data["coverage_histogram"])
        assert hist_total == data["structured_data_adoption"]["total_pages"]


# ═══════════════════════════════════════════════════════════════════════════
# GRAPH DATA — real link relationships
# ═══════════════════════════════════════════════════════════════════════════

class TestRealGraph:
    def test_graph_has_cross_domain_links(self, page):
        data = _get_json(page, _api("/api/viz/graph"))
        assert len(data["links"]) > 0, "Expected cross-domain links from real crawl"

    def test_graph_node_pages_match_domains(self, page):
        graph = _get_json(page, _api("/api/viz/graph"))
        domains = _get_json(page, _api("/api/viz/domains"))
        dom_pages = {d["domain"]: d["page_count"] for d in domains}
        for node in graph["nodes"]:
            if node["id"] in dom_pages:
                # Allow small tolerance: crawl may still be running between calls
                diff = abs(node["pages"] - dom_pages[node["id"]])
                assert diff <= 5, f"{node['id']}: graph={node['pages']} domains={dom_pages[node['id']]}"

    def test_chord_matrix_consistent_with_graph(self, page):
        graph = _get_json(page, _api("/api/viz/graph"))
        chord = _get_json(page, _api("/api/viz/chord"))
        graph_links = sum(l["weight"] for l in graph["links"])
        chord_links = sum(sum(row) for row in chord["matrix"])
        # Chord may have fewer due to top-N filtering
        assert chord_links <= graph_links


# ═══════════════════════════════════════════════════════════════════════════
# FRESHNESS — real date data
# ═══════════════════════════════════════════════════════════════════════════

class TestRealFreshness:
    def test_freshness_dates_parseable(self, page):
        data = _get_json(page, _api("/api/viz/freshness"))
        for d in data["domains"]:
            if d.get("latest"):
                assert re.match(r"\d{4}-\d{2}-\d{2}", d["latest"]), f"Unparseable date: {d['latest']}"
            if d.get("oldest"):
                assert re.match(r"\d{4}-\d{2}-\d{2}", d["oldest"])

    def test_freshness_oldest_before_latest(self, page):
        data = _get_json(page, _api("/api/viz/freshness"))
        for d in data["domains"]:
            if d.get("oldest") and d.get("latest"):
                assert d["oldest"] <= d["latest"], f"oldest > latest for {d['domain']}"


# ═══════════════════════════════════════════════════════════════════════════
# REPORTS PAGE — real rendering with real data
# ═══════════════════════════════════════════════════════════════════════════

class TestRealReportsDashboard:
    def test_reports_page_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.wait_for_load_state("networkidle")
        assert page.locator("h1").count() >= 1

    def test_network_tab_renders_svg(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.wait_for_load_state("networkidle")
        page.wait_for_selector("#viz-network svg", timeout=15000)
        svg = page.locator("#viz-network svg")
        assert svg.count() >= 1
        circles = page.locator("#viz-network circle")
        assert circles.count() >= 3, "Expected network nodes to render"

    def test_treemap_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='treemap']").click()
        page.wait_for_selector("#viz-treemap svg", timeout=10000)
        rects = page.locator("#viz-treemap rect")
        assert rects.count() >= 3

    def test_status_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='status']").click()
        page.wait_for_selector("#viz-status svg", timeout=10000)

    def test_freshness_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='freshness']").click()
        page.wait_for_selector("#viz-freshness svg", timeout=10000)

    def test_chord_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='chord']").click()
        page.wait_for_selector("#viz-chord svg", timeout=10000)

    def test_radar_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='radar']").click()
        page.wait_for_selector("#viz-radar svg", timeout=10000)

    def test_wordcloud_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='wordcloud']").click()
        page.wait_for_selector("#viz-wordcloud svg", timeout=10000)

    def test_contenttypes_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='contenttypes']").click()
        page.wait_for_selector("#viz-contenttypes svg", timeout=10000)

    def test_cmslandscape_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='cmslandscape']").click()
        page.wait_for_timeout(3000)
        has_svg = page.locator("#viz-cmslandscape svg").count() >= 1
        has_msg = page.locator("#viz-cmslandscape p").count() >= 1
        assert has_svg or has_msg

    def test_structureddata_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='structureddata']").click()
        page.wait_for_timeout(3000)
        has_svg = page.locator("#viz-structureddata svg").count() >= 1
        has_msg = page.locator("#viz-structureddata p").count() >= 1
        assert has_svg or has_msg

    def test_seoreadiness_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='seoreadiness']").click()
        page.wait_for_timeout(3000)
        has_svg = page.locator("#viz-seoreadiness svg").count() >= 1
        has_msg = page.locator("#viz-seoreadiness p").count() >= 1
        assert has_svg or has_msg

    def test_coverage_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='coverage']").click()
        page.wait_for_timeout(3000)
        has_svg = page.locator("#viz-coverage svg").count() >= 1
        has_msg = page.locator("#viz-coverage p").count() >= 1
        assert has_svg or has_msg

    def test_authornetwork_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='authornetwork']").click()
        page.wait_for_timeout(3000)
        has_svg = page.locator("#viz-authornetwork svg").count() >= 1
        has_msg = page.locator("#viz-authornetwork p").count() >= 1
        assert has_svg or has_msg

    def test_publishers_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='publishers']").click()
        page.wait_for_timeout(3000)
        has_svg = page.locator("#viz-publishers svg").count() >= 1
        has_msg = page.locator("#viz-publishers p").count() >= 1
        assert has_svg or has_msg

    def test_schemainsights_tab_renders(self, page):
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.locator("button[data-panel='schemainsights']").click()
        page.wait_for_timeout(3000)
        container = page.locator("#viz-schemainsights")
        assert container.inner_text().strip(), "Schema insights should render content"

    def test_no_js_errors_on_dashboard(self, page):
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(3000)
        assert len(errors) == 0, f"JS errors on dashboard: {errors}"

    def test_no_js_errors_clicking_all_tabs(self, page):
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto(f"{BASE}/p/{SLUG}/reports")
        page.wait_for_load_state("networkidle")
        tabs = page.locator(".viz-tab")
        count = tabs.count()
        for i in range(count):
            tabs.nth(i).click()
            page.wait_for_timeout(1500)
        critical = [e for e in errors if "TypeError" in e or "ReferenceError" in e]
        assert len(critical) == 0, f"Critical JS errors: {critical}"


# ═══════════════════════════════════════════════════════════════════════════
# RESULTS PAGE — real CSV viewing
# ═══════════════════════════════════════════════════════════════════════════

class TestRealResults:
    @staticmethod
    def _get_run():
        runs_dir = f"/workspace/projects/{SLUG}/runs/"
        runs = [d for d in sorted(os.listdir(runs_dir)) if d.startswith("run_")]
        return runs[0] if runs else None

    def test_results_page_lists_csvs(self, page):
        run = self._get_run()
        assert run, "No runs found"
        page.goto(f"{BASE}/p/{SLUG}/runs/{run}/results")
        assert "pages.csv" in page.content()

    def test_pages_csv_table_renders(self, page):
        run = self._get_run()
        assert run, "No runs found"
        page.goto(f"{BASE}/p/{SLUG}/runs/{run}/results/pages.csv")
        page.wait_for_load_state("networkidle")
        assert page.locator("table").count() >= 1 or "requested_url" in page.content()

    def test_download_pages_csv(self, page):
        run = self._get_run()
        assert run, "No runs found"
        resp = page.request.get(f"{BASE}/p/{SLUG}/runs/{run}/download/pages.csv")
        # 409 is correct if crawl is still running (prevents partial download)
        assert resp.status in (200, 409), f"Expected 200 or 409, got {resp.status}"
        if resp.status == 200:
            body = resp.text()
            assert "requested_url" in body
            assert "nhs" in body.lower()
