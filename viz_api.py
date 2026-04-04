"""
Reports — Flask Blueprint.

JSON API endpoints that serve aggregated crawl data to D3.js
visualisations, plus the HTML page route for the dashboard itself.

The dashboard lives at project level (``/p/<slug>/reports``) and
aggregates data from one or more crawl runs.  A ``?runs=`` query
parameter selects specific runs; omitting it includes all runs.

Legacy per-run URLs (``/p/<slug>/runs/<run_name>/reports``) redirect
to the project-level dashboard with the corresponding ``?runs=`` preset.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from flask import Blueprint, jsonify, redirect, render_template, request, url_for

import config
import storage as storage_module
import viz_data

eco_bp = Blueprint("reports", __name__)


def _resolve_run_dirs(slug: str) -> List[str]:
    """Return run directories for the active project, filtered by ``?runs=``.

    When ``runs`` is absent or empty every ``run_*`` directory under the
    project is included.  Otherwise the parameter is a comma-separated
    list of run folder names (e.g. ``run_2025-04-01_12-00-00``).
    """
    base = storage_module.get_project_runs_dir(slug)

    runs_param = request.args.get("runs", "").strip()
    if runs_param:
        names = [n.strip() for n in runs_param.split(",") if n.strip()]
        dirs = []
        for n in names:
            full = os.path.join(base, n)
            if os.path.isdir(full):
                dirs.append(full)
        return dirs

    if not os.path.isdir(base):
        return []
    return [
        os.path.join(base, n)
        for n in sorted(os.listdir(base))
        if n.startswith("run_") and os.path.isdir(os.path.join(base, n))
    ]


def _parse_filters() -> Optional[Dict[str, Any]]:
    """Build a filter dict from the current request's query string.

    Comma-separated list parameters (``domains``, ``cms``,
    ``content_kinds``, ``schema_formats``, ``schema_types``) are split
    into Python lists.  Scalar parameters (``date_from``, ``date_to``,
    ``min_coverage``) are passed through as-is.

    Returns:
        A filter dict compatible with ``viz_data.filter_pages``, or
        None when no filter parameters are present — which lets
        ``filter_pages`` skip work entirely.
    """
    filters: Dict[str, Any] = {}
    for key, param, split in (
        ("domains", "domains", True),
        ("cms", "cms", True),
        ("content_kinds", "content_kinds", True),
        ("schema_formats", "schema_formats", True),
        ("schema_types", "schema_types", True),
    ):
        val = request.args.get(param, "").strip()
        if val:
            filters[key] = [v.strip() for v in val.split(",") if v.strip()]

    for key in ("date_from", "date_to"):
        val = request.args.get(key, "").strip()
        if val:
            filters[key] = val

    mc = request.args.get("min_coverage", "").strip()
    if mc:
        try:
            filters["min_coverage"] = float(mc)
        except ValueError:
            pass

    return filters if filters else None


# ── Project-level page route ─────────────────────────────────────────────

@eco_bp.route("/p/<slug>/reports")
def reports_dashboard(slug: str):
    """Render the reports dashboard at project level."""
    project = storage_module.load_project(slug)
    if not project:
        return "Project not found", 404
    project["slug"] = slug
    runs_base = storage_module.get_project_runs_dir(slug)
    os.makedirs(runs_base, exist_ok=True)
    ctx = storage_module.StorageContext(runs_base, config.CrawlConfig.from_module())
    runs = ctx.list_run_dirs()
    preselected = request.args.get("runs", "")

    overview = {
        "run_count": len(runs),
        "total_pages": sum(r.get("page_count", 0) for r in runs),
        "total_assets": 0,
        "total_errors": 0,
    }
    for r in runs:
        run_dir_path = os.path.join(runs_base, r["name"])
        state = storage_module.load_crawl_state(run_dir_path)
        if state:
            overview["total_assets"] += state.get("assets_from_pages", 0)

    return render_template(
        "reports.html",
        project=project,
        runs=runs,
        preselected_runs=preselected,
        overview=overview,
    )


# ── Legacy per-run redirect ─────────────────────────────────────────────

@eco_bp.route("/p/<slug>/runs/<run_name>/reports")
def reports_dashboard_legacy(slug: str, run_name: str):
    """Redirect old per-run URLs to the project-level dashboard."""
    return redirect(
        url_for("reports.reports_dashboard", slug=slug, runs=run_name)
    )


# ── Runs list endpoint ──────────────────────────────────────────────────

@eco_bp.route("/p/<slug>/api/viz/runs")
def api_runs(slug: str):
    """Return metadata for every run in this project (for the run picker)."""
    project = storage_module.load_project(slug)
    if not project:
        return jsonify({"error": "Project not found"}), 404
    runs_base = storage_module.get_project_runs_dir(slug)
    os.makedirs(runs_base, exist_ok=True)
    ctx = storage_module.StorageContext(runs_base, config.CrawlConfig.from_module())
    return jsonify(ctx.list_run_dirs())


# ── JSON API endpoints (project-level) ──────────────────────────────────

@eco_bp.route("/p/<slug>/api/viz/filter_options")
def api_filter_options(slug: str):
    run_dirs = _resolve_run_dirs(slug)
    if not run_dirs:
        return jsonify({"domains": [], "cms_values": [],
                        "content_kinds": [], "schema_types": [],
                        "total_pages": 0})
    data = viz_data.get_filter_options(run_dirs)
    return jsonify(data)


@eco_bp.route("/p/<slug>/api/viz/domains")
def api_domains(slug: str):
    run_dirs = _resolve_run_dirs(slug)
    if not run_dirs:
        return jsonify([])
    data = viz_data.aggregate_domains(run_dirs, filters=_parse_filters())
    return jsonify(data)


@eco_bp.route("/p/<slug>/api/viz/graph")
def api_graph(slug: str):
    run_dirs = _resolve_run_dirs(slug)
    if not run_dirs:
        return jsonify({"nodes": [], "links": []})
    data = viz_data.aggregate_domain_graph(run_dirs, filters=_parse_filters())
    return jsonify(data)


@eco_bp.route("/p/<slug>/api/viz/tags")
def api_tags(slug: str):
    run_dirs = _resolve_run_dirs(slug)
    if not run_dirs:
        return jsonify({"tags": [], "sources": {}, "cooccurrence": []})
    data = viz_data.aggregate_tags(run_dirs, filters=_parse_filters())
    return jsonify(data)


@eco_bp.route("/p/<slug>/api/viz/navigation")
def api_navigation(slug: str):
    run_dirs = _resolve_run_dirs(slug)
    if not run_dirs:
        return jsonify({"domains": [], "tree": None})
    domain = request.args.get("domain")
    max_depth = request.args.get("depth", 2, type=int)
    max_depth = max(1, min(max_depth, 8))
    data = viz_data.aggregate_navigation(
        run_dirs, domain=domain, max_depth=max_depth, filters=_parse_filters(),
    )
    return jsonify(data)


@eco_bp.route("/p/<slug>/api/viz/freshness")
def api_freshness(slug: str):
    run_dirs = _resolve_run_dirs(slug)
    if not run_dirs:
        return jsonify({"today": "", "domains": []})
    data = viz_data.aggregate_freshness(run_dirs, filters=_parse_filters())
    return jsonify(data)


@eco_bp.route("/p/<slug>/api/viz/chord")
def api_chord(slug: str):
    run_dirs = _resolve_run_dirs(slug)
    if not run_dirs:
        return jsonify({"domains": [], "matrix": []})
    top_n = request.args.get("top", 30, type=int)
    data = viz_data.aggregate_chord(
        run_dirs, top_n=top_n, filters=_parse_filters(),
    )
    return jsonify(data)


@eco_bp.route("/p/<slug>/api/viz/technology")
def api_technology(slug: str):
    run_dirs = _resolve_run_dirs(slug)
    if not run_dirs:
        return jsonify({"cms_distribution": [],
                        "structured_data_adoption": {},
                        "schema_type_frequency": [],
                        "seo_readiness": [],
                        "coverage_histogram": []})
    data = viz_data.aggregate_technology(run_dirs, filters=_parse_filters())
    return jsonify(data)


@eco_bp.route("/p/<slug>/api/viz/authorship")
def api_authorship(slug: str):
    run_dirs = _resolve_run_dirs(slug)
    if not run_dirs:
        return jsonify({"authors": [], "publishers": [], "author_network": {"nodes": [], "links": []}})
    data = viz_data.aggregate_authorship(run_dirs, filters=_parse_filters())
    return jsonify(data)


@eco_bp.route("/p/<slug>/api/viz/schema_insights")
def api_schema_insights(slug: str):
    run_dirs = _resolve_run_dirs(slug)
    if not run_dirs:
        return jsonify({"products": None, "events": None, "jobs": None, "recipes": None})
    data = viz_data.aggregate_schema_insights(
        run_dirs, filters=_parse_filters(),
    )
    return jsonify(data)
