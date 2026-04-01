"""
Ecosystem mapping — Flask Blueprint.

JSON API endpoints that serve aggregated crawl data to D3.js
visualisations, plus the HTML page route for the dashboard itself.
"""
from __future__ import annotations

import os
from typing import Optional

from flask import Blueprint, jsonify, render_template, request

import config
import storage as storage_module
import viz_data

eco_bp = Blueprint("ecosystem", __name__)


def _resolve_run_dir(slug: str, run_name: str) -> Optional[str]:
    storage_module.activate_project(slug)
    run_dir = os.path.join(config.OUTPUT_DIR, run_name)
    if not os.path.isdir(run_dir):
        return None
    return run_dir


# ── Page route ───────────────────────────────────────────────────────────

@eco_bp.route("/p/<slug>/runs/<run_name>/ecosystem")
def ecosystem_dashboard(slug: str, run_name: str):
    project = storage_module.load_project(slug)
    if not project:
        return "Project not found", 404
    project["slug"] = slug
    storage_module.activate_project(slug)
    run_dir = os.path.join(config.OUTPUT_DIR, run_name)
    if not os.path.isdir(run_dir):
        return "Run not found", 404
    friendly = storage_module._read_run_name(run_dir) or ""
    return render_template(
        "ecosystem.html",
        project=project,
        run_name=run_name,
        friendly_name=friendly,
    )


# ── JSON API endpoints ──────────────────────────────────────────────────

@eco_bp.route("/p/<slug>/runs/<run_name>/api/viz/domains")
def api_domains(slug: str, run_name: str):
    run_dir = _resolve_run_dir(slug, run_name)
    if not run_dir:
        return jsonify({"error": "Run not found"}), 404
    data = viz_data.aggregate_domains(run_dir)
    return jsonify(data)


@eco_bp.route("/p/<slug>/runs/<run_name>/api/viz/graph")
def api_graph(slug: str, run_name: str):
    run_dir = _resolve_run_dir(slug, run_name)
    if not run_dir:
        return jsonify({"error": "Run not found"}), 404
    data = viz_data.aggregate_domain_graph(run_dir)
    return jsonify(data)


@eco_bp.route("/p/<slug>/runs/<run_name>/api/viz/tags")
def api_tags(slug: str, run_name: str):
    run_dir = _resolve_run_dir(slug, run_name)
    if not run_dir:
        return jsonify({"error": "Run not found"}), 404
    data = viz_data.aggregate_tags(run_dir)
    return jsonify(data)


@eco_bp.route("/p/<slug>/runs/<run_name>/api/viz/navigation")
def api_navigation(slug: str, run_name: str):
    run_dir = _resolve_run_dir(slug, run_name)
    if not run_dir:
        return jsonify({"error": "Run not found"}), 404
    domain = request.args.get("domain")
    data = viz_data.aggregate_navigation(run_dir, domain=domain)
    return jsonify(data)


@eco_bp.route("/p/<slug>/runs/<run_name>/api/viz/freshness")
def api_freshness(slug: str, run_name: str):
    run_dir = _resolve_run_dir(slug, run_name)
    if not run_dir:
        return jsonify({"error": "Run not found"}), 404
    data = viz_data.aggregate_freshness(run_dir)
    return jsonify(data)


@eco_bp.route("/p/<slug>/runs/<run_name>/api/viz/chord")
def api_chord(slug: str, run_name: str):
    run_dir = _resolve_run_dir(slug, run_name)
    if not run_dir:
        return jsonify({"error": "Run not found"}), 404
    top_n = request.args.get("top", 30, type=int)
    data = viz_data.aggregate_chord(run_dir, top_n=top_n)
    return jsonify(data)
