#!/usr/bin/env python3
"""
The Crawl Street Journal — Web GUI

Flask application providing a browser interface for managing projects,
configuring crawls, running them, and reviewing results.

    python gui.py          # http://localhost:5001
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import sys
import threading
import time
import zipfile
from collections import Counter, deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import (
    Flask,
    Response,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)

import config
from config import CrawlConfig
import storage as storage_module
from storage import StorageContext

app = Flask(
    __name__,
    template_folder=os.path.join(config.BUNDLE_DIR, "templates"),
    static_folder=os.path.join(config.BUNDLE_DIR, "static"),
    static_url_path="/static",
)
app.secret_key = os.urandom(24)

from viz_api import eco_bp  # noqa: E402
app.register_blueprint(eco_bp)

# ── Crawl state (per-project slots) ───────────────────────────────────────

_EMPTY_STATUS: Dict[str, Any] = {
    "running": False,
    "stopping": False,
    "pages": 0,
    "assets": 0,
    "current_url": "",
    "start_time": "",
    "elapsed": "",
    "finished_message": "",
    "run_folder": "",
    "project_slug": "",
    "phase": "",
    "phase_detail": "",
}


class CrawlSlot:
    """Mutable state for a single in-progress crawl."""
    __slots__ = ("thread", "stop_event", "status", "cfg", "ctx", "start_mono")

    def __init__(
        self,
        thread: threading.Thread,
        stop_event: threading.Event,
        status: Dict[str, Any],
        cfg: CrawlConfig,
        ctx: StorageContext,
    ) -> None:
        self.thread = thread
        self.stop_event = stop_event
        self.status = status
        self.cfg = cfg
        self.ctx = ctx
        self.start_mono: Optional[float] = None


_active_crawls: Dict[str, CrawlSlot] = {}
_crawls_lock = threading.Lock()


def _project_status(slug: str) -> Dict[str, Any]:
    """Return a snapshot of the crawl status for *slug*."""
    with _crawls_lock:
        slot = _active_crawls.get(slug)
        if slot:
            return dict(slot.status)
    return dict(_EMPTY_STATUS, project_slug=slug)


# ── In-memory log buffer ─────────────────────────────────────────────────

_log_buffer: deque[Dict[str, str]] = deque(maxlen=2000)


class _BufferHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        entry = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
                "%H:%M:%S"
            ),
            "level": record.levelname,
            "message": self.format(record),
        }
        _log_buffer.append(entry)


_buffer_handler = _BufferHandler()
_buffer_handler.setFormatter(logging.Formatter("%(message)s"))
logging.root.addHandler(_buffer_handler)
logging.root.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))
logging.getLogger("werkzeug").setLevel(logging.WARNING)


def _apply_log_level() -> None:
    level = getattr(logging, config.LOG_LEVEL, logging.INFO)
    logging.root.setLevel(level)


# ── Crawl runner ──────────────────────────────────────────────────────────

def _run_crawl(
    slot: CrawlSlot,
    project_slug: str,
    run_folder: Optional[str] = None,
    run_name: Optional[str] = None,
    resume: bool = False,
) -> None:
    import scraper

    start = time.monotonic()
    slot.start_mono = start
    slot.status["running"] = True
    slot.status["run_folder"] = run_folder or ""
    slot.status["project_slug"] = project_slug
    slot.status["start_time"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    def on_progress(crawled: int, assets: int, current_url: str) -> None:
        slot.status["pages"] = crawled
        slot.status["assets"] = assets
        slot.status["current_url"] = current_url
        slot.status["phase"] = "crawling"
        slot.status["phase_detail"] = ""
        elapsed = time.monotonic() - start
        m, s = divmod(int(elapsed), 60)
        h, m = divmod(m, 60)
        slot.status["elapsed"] = f"{h:02d}:{m:02d}:{s:02d}"

    def on_phase(phase: str, detail: str) -> None:
        slot.status["phase"] = phase
        slot.status["phase_detail"] = detail
        elapsed = time.monotonic() - start
        m, s = divmod(int(elapsed), 60)
        h, m = divmod(m, 60)
        slot.status["elapsed"] = f"{h:02d}:{m:02d}:{s:02d}"

    try:
        pages, assets = scraper.crawl(
            on_progress=on_progress,
            on_phase=on_phase,
            should_stop=lambda: slot.stop_event.is_set(),
            run_name=run_name,
            run_folder=run_folder,
            resume=resume,
            cfg=slot.cfg,
            ctx=slot.ctx,
        )
        slot.status["pages"] = pages
        slot.status["assets"] = assets
        slot.status["finished_message"] = (
            f"Finished: {pages} pages, {assets} asset rows"
        )
    except Exception as exc:
        logging.exception("Crawl failed: %s", exc)
        slot.status["finished_message"] = f"Crawl failed: {exc}"
    finally:
        slot.status["running"] = False
        slot.status["stopping"] = False
        with _crawls_lock:
            _active_crawls.pop(project_slug, None)


def _start_crawl_thread(
    project_slug: str,
    run_folder: Optional[str] = None,
    run_name: Optional[str] = None,
    resume: bool = False,
) -> bool:
    """Start a crawl for *project_slug*. Returns True if started."""
    with _crawls_lock:
        if project_slug in _active_crawls:
            return False

    runs_dir = storage_module.get_project_runs_dir(project_slug)
    cfg = CrawlConfig.from_module()
    if run_folder:
        run_dir = os.path.join(runs_dir, run_folder)
        saved = storage_module.load_run_config(run_dir)
        if saved:
            cfg = CrawlConfig.from_dict(saved, base=cfg)
    cfg.OUTPUT_DIR = runs_dir
    ctx = StorageContext(runs_dir, cfg)

    status: Dict[str, Any] = {
        "running": True, "stopping": False, "pages": 0, "assets": 0,
        "current_url": "", "start_time": "", "elapsed": "",
        "finished_message": "",
        "run_folder": run_folder or "", "project_slug": project_slug,
    }

    stop_event = threading.Event()
    slot = CrawlSlot(
        thread=None,  # type: ignore[arg-type]
        stop_event=stop_event,
        status=status,
        cfg=cfg,
        ctx=ctx,
    )

    t = threading.Thread(
        target=_run_crawl,
        args=(slot, project_slug),
        kwargs=dict(run_folder=run_folder, run_name=run_name, resume=resume),
        daemon=True,
    )
    slot.thread = t

    with _crawls_lock:
        if project_slug in _active_crawls:
            return False
        _active_crawls[project_slug] = slot

    t.start()
    return True


# ── CSV / metrics helpers ────────────────────────────────────────────────

def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def _count_csv_rows(filepath: str) -> int:
    if not os.path.isfile(filepath):
        return 0
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return max(sum(1 for _ in f) - 1, 0)
    except Exception:
        return 0


def _output_csvs(run_dir: str) -> List[Dict[str, Any]]:
    if not os.path.isdir(run_dir):
        return []
    files = []
    for name in sorted(os.listdir(run_dir)):
        if not name.endswith(".csv"):
            continue
        path = os.path.join(run_dir, name)
        size = os.path.getsize(path)
        try:
            with open(path, "r", encoding="utf-8") as f:
                row_count = sum(1 for _ in f) - 1
        except Exception:
            row_count = 0
        files.append({
            "name": name,
            "rows": max(row_count, 0),
            "size": _human_size(size),
            "size_bytes": size,
        })
    return files


def _grouped_output_csvs(run_dir: str) -> Dict[str, List[Dict[str, Any]]]:
    """Group CSV files into semantic categories for the results page."""
    groups: Dict[str, List[Dict[str, Any]]] = {
        "pages": [],
        "assets": [],
        "links": [],
        "sitemaps": [],
        "metadata": [],
        "errors": [],
    }
    for f in _output_csvs(run_dir):
        name = f["name"]
        if name == "pages.csv":
            groups["pages"].append(f)
        elif name.startswith("assets_"):
            groups["assets"].append(f)
        elif name in ("edges.csv", "nav_links.csv", "link_checks.csv"):
            groups["links"].append(f)
        elif name == "sitemap_urls.csv":
            groups["sitemaps"].append(f)
        elif name == "tags.csv":
            groups["metadata"].append(f)
        elif name == "crawl_errors.csv":
            groups["errors"].append(f)
        else:
            groups["metadata"].append(f)
    return groups


def _read_csv_page(
    filepath: str, page: int = 1, per_page: int = 100
) -> Tuple[List[str], List[Dict[str, str]], int, int]:
    if not os.path.isfile(filepath):
        return [], [], 0, 0
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        all_rows = list(reader)
    total = len(all_rows)
    page = max(1, page)
    per_page = max(1, per_page)
    total_pages = max(1, (total + per_page - 1) // per_page)
    start = (page - 1) * per_page
    return headers, all_rows[start : start + per_page], total, total_pages


def _run_metrics(run_dir: str) -> Dict[str, Any]:
    """Compute aggregate metrics from a single run's CSVs."""
    m: Dict[str, Any] = {
        "has_data": False,
        "pages": 0,
        "domains": 0,
        "total_assets": 0,
        "total_errors": 0,
        "total_links": 0,
        "total_tags": 0,
        "domain_breakdown": [],
        "status_breakdown": [],
        "content_breakdown": [],
        "asset_breakdown": [],
        "error_breakdown": [],
        "avg_word_count": 0,
        "total_images": 0,
        "images_missing_alt": 0,
        "training_pages": 0,
        "lang_breakdown": [],
    }

    pages_path = os.path.join(run_dir, config.PAGES_CSV)
    if not os.path.isfile(pages_path):
        return m

    try:
        with open(pages_path, "r", encoding="utf-8") as f:
            pages_rows = list(csv.DictReader(f))
    except Exception:
        return m

    if not pages_rows:
        return m

    m["has_data"] = True
    m["pages"] = len(pages_rows)

    domain_ctr: Counter[str] = Counter()
    status_ctr: Counter[str] = Counter()
    kind_ctr: Counter[str] = Counter()
    lang_ctr: Counter[str] = Counter()
    total_words = 0
    total_imgs = 0
    imgs_no_alt = 0
    training = 0

    for r in pages_rows:
        domain_ctr[r.get("domain", "unknown")] += 1
        status_ctr[r.get("http_status", "")] += 1
        kind = r.get("content_kind_guess", "").strip()
        kind_ctr[kind if kind else "(unclassified)"] += 1
        lang = r.get("lang", "").strip() or "(not set)"
        lang_ctr[lang] += 1
        wc = r.get("word_count", "0")
        total_words += int(wc) if wc and wc.isdigit() else 0
        ic = r.get("img_count", "0")
        total_imgs += int(ic) if ic and ic.isdigit() else 0
        ma = r.get("img_missing_alt_count", "0")
        imgs_no_alt += int(ma) if ma and ma.isdigit() else 0
        if r.get("training_related_flag", "").strip():
            training += 1

    m["domains"] = len(domain_ctr)
    m["avg_word_count"] = round(total_words / len(pages_rows)) if pages_rows else 0
    m["total_images"] = total_imgs
    m["images_missing_alt"] = imgs_no_alt
    m["training_pages"] = training

    m["domain_breakdown"] = sorted(domain_ctr.items(), key=lambda x: (-x[1], x[0]))[:15]
    m["status_breakdown"] = sorted(status_ctr.items(), key=lambda x: (-x[1], x[0]))
    m["content_breakdown"] = sorted(kind_ctr.items(), key=lambda x: (-x[1], x[0]))
    m["lang_breakdown"] = sorted(lang_ctr.items(), key=lambda x: (-x[1], x[0]))

    asset_ctr: Counter[str] = Counter()
    for name in sorted(os.listdir(run_dir)):
        if name.startswith("assets_") and name.endswith(".csv"):
            cat = name[len("assets_"):-len(".csv")]
            count = _count_csv_rows(os.path.join(run_dir, name))
            if count > 0:
                asset_ctr[cat] = count
    m["total_assets"] = sum(asset_ctr.values())
    m["asset_breakdown"] = sorted(asset_ctr.items(), key=lambda x: (-x[1], x[0]))

    errors_path = os.path.join(run_dir, config.ERRORS_CSV)
    error_ctr: Counter[str] = Counter()
    if os.path.isfile(errors_path):
        try:
            with open(errors_path, "r", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    error_ctr[r.get("error_type", "unknown")] += 1
                    err_status = r.get("http_status", "").strip()
                    if err_status and err_status != "0":
                        status_ctr[err_status] += 1
        except Exception:
            pass
    m["total_errors"] = sum(error_ctr.values())
    m["error_breakdown"] = sorted(error_ctr.items(), key=lambda x: (-x[1], x[0]))

    m["total_links"] = _count_csv_rows(os.path.join(run_dir, config.EDGES_CSV))
    m["total_tags"] = _count_csv_rows(os.path.join(run_dir, config.TAGS_CSV))

    return m


def _project_overview_metrics(slug: str) -> Dict[str, Any]:
    """Aggregate metrics across all runs in a project."""
    runs_dir = storage_module.get_project_runs_dir(slug)
    m: Dict[str, Any] = {
        "total_pages": 0,
        "total_runs": 0,
        "total_assets": 0,
        "total_errors": 0,
        "recent_runs": [],
    }
    if not os.path.isdir(runs_dir):
        return m

    run_folders = sorted(
        [d for d in os.listdir(runs_dir)
         if d.startswith("run_") and os.path.isdir(os.path.join(runs_dir, d))],
        reverse=True,
    )
    m["total_runs"] = len(run_folders)

    for rf in run_folders:
        rd = os.path.join(runs_dir, rf)
        pages = _count_csv_rows(os.path.join(rd, config.PAGES_CSV))
        m["total_pages"] += pages
        errors = _count_csv_rows(os.path.join(rd, config.ERRORS_CSV))
        m["total_errors"] += errors
        for name in os.listdir(rd):
            if name.startswith("assets_") and name.endswith(".csv"):
                m["total_assets"] += _count_csv_rows(os.path.join(rd, name))

    for rf in run_folders[:5]:
        rd = os.path.join(runs_dir, rf)
        friendly = storage_module._read_run_name(rd)
        raw = rf.replace("run_", "", 1)
        parts = raw.split("_")
        date_part = parts[0] if parts else raw
        time_part = parts[1].replace("-", ":") if len(parts) > 1 else ""
        m["recent_runs"].append({
            "name": rf,
            "friendly_name": friendly or "",
            "timestamp_label": (date_part + " " + time_part).strip(),
            "page_count": _count_csv_rows(os.path.join(rd, config.PAGES_CSV)),
            "status": storage_module.get_run_status(rd),
        })

    return m


# ── Config form helpers ──────────────────────────────────────────────────

def _build_config_dict_from_form(form) -> Dict[str, Any]:
    seed_urls = [
        u for u in form.get("seed_urls", "").strip().splitlines() if u.strip()
    ]
    sitemap_urls = [
        u for u in form.get("sitemap_urls", "").strip().splitlines() if u.strip()
    ]
    allowed_domains = [
        d.strip()
        for d in form.get("allowed_domains", "").strip().splitlines()
        if d.strip()
    ]
    delay_min = float(form.get("delay_min", 3))
    delay_max = float(form.get("delay_max", 5))

    max_depth_raw = form.get("max_depth", "").strip()
    max_depth = int(max_depth_raw) if max_depth_raw else None

    return {
        "SEED_URLS": seed_urls,
        "SITEMAP_URLS": sitemap_urls,
        "LOAD_SITEMAPS_FROM_ROBOTS": "load_sitemaps_from_robots" in form,
        "RESPECT_ROBOTS_TXT": "respect_robots_txt" in form,
        "MAX_SITEMAP_URLS": int(form.get("max_sitemap_urls", 1_000_000)),
        "MAX_PAGES_TO_CRAWL": int(form.get("max_pages", 1_000_000)),
        "MAX_DEPTH": max_depth,
        "REQUEST_DELAY_SECONDS": [delay_min, delay_max],
        "REQUEST_TIMEOUT_SECONDS": int(form.get("request_timeout", 20)),
        "MAX_RETRIES": int(form.get("max_retries", 3)),
        "STATE_SAVE_INTERVAL": int(form.get("state_save_interval", 50)),
        "WRITE_EDGES_CSV": "write_edges" in form,
        "WRITE_TAGS_CSV": "write_tags" in form,
        "ASSET_HEAD_METADATA": "asset_head" in form,
        "HEAD_TIMEOUT_SECONDS": int(form.get("head_timeout", 10)),
        "CAPTURE_RESPONSE_HEADERS": "capture_headers" in form,
        "WRITE_SITEMAP_URLS_CSV": "write_sitemap_urls" in form,
        "WRITE_NAV_LINKS_CSV": "write_nav_links" in form,
        "CHECK_OUTBOUND_LINKS": "check_outbound" in form,
        "MAX_LINK_CHECKS_PER_PAGE": 50,
        "LINK_CHECK_DELAY_SECONDS": 0.5,
        "CAPTURE_READABILITY": "capture_readability" in form,
        "ALLOWED_DOMAINS": allowed_domains,
        "USER_AGENT": form.get("user_agent", config.USER_AGENT).strip(),
        "LOG_LEVEL": form.get("log_level", "INFO").upper(),
    }


# ══════════════════════════════════════════════════════════════════════════
#  ROUTES: Projects
# ══════════════════════════════════════════════════════════════════════════

@app.route("/")
def projects_list():
    projects = storage_module.list_projects()
    return render_template("projects.html", projects=projects)


@app.route("/projects/create", methods=["POST"])
def create_project_route():
    name = request.form.get("name", "").strip()
    description = request.form.get("description", "").strip()
    if not name:
        return redirect(url_for("projects_list"))
    slug = storage_module.create_project(name, description)
    return redirect(url_for("project_overview", slug=slug))


@app.route("/projects/<slug>/delete", methods=["POST"])
def delete_project_route(slug: str):
    storage_module.delete_project(slug)
    return redirect(url_for("projects_list"))


# ══════════════════════════════════════════════════════════════════════════
#  ROUTES: Project pages
# ══════════════════════════════════════════════════════════════════════════

@app.route("/p/<slug>")
def project_overview(slug: str):
    project = storage_module.load_project(slug)
    if not project:
        return "Project not found", 404
    project["slug"] = slug
    metrics = _project_overview_metrics(slug)
    return render_template(
        "project_overview.html",
        project=project, m=metrics, status=_project_status(slug),
    )


@app.route("/p/<slug>/defaults", methods=["GET"])
def project_defaults(slug: str):
    project = storage_module.load_project(slug)
    if not project:
        return "Project not found", 404
    project["slug"] = slug
    cfg = storage_module.load_project_defaults(slug) or storage_module.snapshot_config()
    return render_template("project_defaults.html", project=project, cfg=cfg)


@app.route("/p/<slug>/defaults", methods=["POST"])
def save_project_defaults_route(slug: str):
    project = storage_module.load_project(slug)
    if not project:
        return "Project not found", 404
    cfg = _build_config_dict_from_form(request.form)
    storage_module.save_project_defaults(slug, cfg)
    logging.info("Saved defaults for project %s", slug)
    return redirect(url_for("project_defaults", slug=slug))


@app.route("/p/<slug>/runs")
def project_runs(slug: str):
    project = storage_module.load_project(slug)
    if not project:
        return "Project not found", 404
    project["slug"] = slug
    storage_module.activate_project(slug)
    run_list = storage_module.list_run_dirs()
    return render_template(
        "runs.html", project=project, runs=run_list, status=_project_status(slug),
    )


@app.route("/p/<slug>/runs/create", methods=["POST"])
def create_run_route(slug: str):
    storage_module.activate_project(slug)
    defaults = storage_module.load_project_defaults(slug)
    if defaults:
        storage_module.apply_run_config(defaults)
    name = request.form.get("run_name", "").strip() or None
    folder = storage_module.create_run(name)
    return redirect(url_for("run_config", slug=slug, run_name=folder))


# ══════════════════════════════════════════════════════════════════════════
#  ROUTES: Run pages
# ══════════════════════════════════════════════════════════════════════════

@app.route("/p/<slug>/runs/<run_name>/config", methods=["GET"])
def run_config(slug: str, run_name: str):
    project = storage_module.load_project(slug)
    if not project:
        return "Project not found", 404
    project["slug"] = slug
    storage_module.activate_project(slug)
    run_dir = os.path.join(config.OUTPUT_DIR, run_name)
    if not os.path.isdir(run_dir):
        return "Run not found", 404
    cfg = storage_module.load_run_config(run_dir) or storage_module.snapshot_config()
    friendly = storage_module._read_run_name(run_dir) or ""
    run_st = storage_module.get_run_status(run_dir)
    return render_template(
        "run_config.html",
        project=project, run_name=run_name,
        friendly_name=friendly, cfg=cfg, run_status=run_st,
        status=_project_status(slug),
    )


@app.route("/p/<slug>/runs/<run_name>/config", methods=["POST"])
def save_run_config_route(slug: str, run_name: str):
    storage_module.activate_project(slug)
    run_dir = os.path.join(config.OUTPUT_DIR, run_name)
    if not os.path.isdir(run_dir):
        return "Run not found", 404

    form = request.form
    friendly = form.get("friendly_name", "").strip()
    if friendly:
        storage_module._write_run_name(run_dir, friendly)

    cfg = _build_config_dict_from_form(form)
    storage_module.save_run_config(run_dir, cfg)
    logging.info("Saved config for run %s", run_name)
    return redirect(url_for("run_config", slug=slug, run_name=run_name))


@app.route("/p/<slug>/runs/<run_name>/monitor")
def run_monitor(slug: str, run_name: str):
    project = storage_module.load_project(slug)
    if not project:
        return "Project not found", 404
    project["slug"] = slug
    storage_module.activate_project(slug)
    run_dir = os.path.join(config.OUTPUT_DIR, run_name)
    if not os.path.isdir(run_dir):
        return "Run not found", 404
    run_status = storage_module.get_run_status(run_dir)
    friendly = storage_module._read_run_name(run_dir) or ""
    return render_template(
        "run_monitor.html",
        project=project, run_name=run_name, friendly_name=friendly,
        run_status=run_status, status=_project_status(slug),
    )


@app.route("/p/<slug>/runs/<run_name>/start", methods=["POST"])
def start_run_route(slug: str, run_name: str):
    with _crawls_lock:
        if slug in _active_crawls:
            return redirect(url_for("run_monitor", slug=slug, run_name=run_name))
    storage_module.activate_project(slug)
    run_dir = os.path.join(config.OUTPUT_DIR, run_name)
    rs = storage_module.get_run_status(run_dir)
    resume = rs == "interrupted"
    _start_crawl_thread(slug, run_folder=run_name, resume=resume)
    return redirect(url_for("run_monitor", slug=slug, run_name=run_name))


@app.route("/p/<slug>/runs/<run_name>/resume", methods=["POST"])
def resume_run_route(slug: str, run_name: str):
    with _crawls_lock:
        if slug in _active_crawls:
            return redirect(url_for("run_monitor", slug=slug, run_name=run_name))
    _start_crawl_thread(slug, run_folder=run_name, resume=True)
    return redirect(url_for("run_monitor", slug=slug, run_name=run_name))


@app.route("/p/<slug>/runs/<run_name>/stop", methods=["POST"])
def stop_run_route(slug: str, run_name: str):
    with _crawls_lock:
        slot = _active_crawls.get(slug)
        if not slot:
            return redirect(url_for("run_monitor", slug=slug, run_name=run_name))
        slot.stop_event.set()
        slot.status["stopping"] = True
    return redirect(url_for("run_monitor", slug=slug, run_name=run_name))


@app.route("/p/<slug>/runs/<run_name>/results")
def run_results(slug: str, run_name: str):
    project = storage_module.load_project(slug)
    if not project:
        return "Project not found", 404
    project["slug"] = slug
    storage_module.activate_project(slug)
    run_dir = os.path.join(config.OUTPUT_DIR, run_name)
    if not os.path.isdir(run_dir):
        return "Run not found", 404
    friendly = storage_module._read_run_name(run_dir) or ""
    groups = _grouped_output_csvs(run_dir)
    metrics = _run_metrics(run_dir)
    return render_template(
        "run_results.html",
        project=project, run_name=run_name, friendly_name=friendly,
        groups=groups, m=metrics,
    )


@app.route("/p/<slug>/runs/<run_name>/results/<filename>")
def run_results_detail(slug: str, run_name: str, filename: str):
    if not filename.endswith(".csv"):
        return "Not found", 404
    project = storage_module.load_project(slug)
    if not project:
        return "Project not found", 404
    project["slug"] = slug
    storage_module.activate_project(slug)
    run_dir = os.path.join(config.OUTPUT_DIR, run_name)
    filepath = os.path.join(run_dir, filename)
    if not os.path.isfile(filepath):
        return "Not found", 404
    try:
        page = int(request.args.get("page", 1))
    except (ValueError, TypeError):
        page = 1
    try:
        per_page = int(request.args.get("per_page", 100))
    except (ValueError, TypeError):
        per_page = 100
    headers, rows, total, total_pages = _read_csv_page(filepath, page, per_page)
    friendly = storage_module._read_run_name(run_dir) or ""
    return render_template(
        "results_detail.html",
        project=project, run_name=run_name, friendly_name=friendly,
        filename=filename, headers=headers, rows=rows,
        total=total, page=page, per_page=per_page, total_pages=total_pages,
    )


def _is_run_active(slug: str, run_name: str) -> bool:
    """True when the given run is the one currently being crawled."""
    with _crawls_lock:
        slot = _active_crawls.get(slug)
        if not slot:
            return False
        return (
            slot.status["running"]
            and slot.status.get("run_folder") == run_name
        )


@app.route("/p/<slug>/runs/<run_name>/download/<filename>")
def run_download(slug: str, run_name: str, filename: str):
    if not filename.endswith(".csv"):
        return "Not found", 404
    if _is_run_active(slug, run_name):
        return (
            "This run is still in progress. Please stop the crawl before "
            "downloading files to avoid incomplete or corrupted data."
        ), 409
    storage_module.activate_project(slug)
    run_dir = os.path.join(config.OUTPUT_DIR, run_name)
    abs_dir = os.path.abspath(run_dir)
    return send_from_directory(abs_dir, filename, as_attachment=True)


@app.route("/p/<slug>/runs/<run_name>/download-all")
def run_download_all(slug: str, run_name: str):
    if _is_run_active(slug, run_name):
        return (
            "This run is still in progress. Please stop the crawl before "
            "downloading files to avoid incomplete or corrupted data."
        ), 409
    storage_module.activate_project(slug)
    run_dir = os.path.join(config.OUTPUT_DIR, run_name)
    abs_dir = os.path.abspath(run_dir)
    if not os.path.isdir(abs_dir):
        return "No output directory", 404

    csv_names = sorted(n for n in os.listdir(abs_dir) if n.endswith(".csv"))
    if not csv_names:
        return "No CSV files to download", 404

    zip_label = run_name
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in csv_names:
            zf.write(os.path.join(abs_dir, name), arcname=name)
    buf.seek(0)

    return Response(
        buf.getvalue(),
        mimetype="application/zip",
        headers={"Content-Disposition": f"attachment; filename={zip_label}.zip"},
    )


@app.route("/p/<slug>/runs/<run_name>/delete", methods=["POST"])
def delete_run_route(slug: str, run_name: str):
    import shutil
    if not run_name.startswith("run_"):
        return "Cannot delete this entry", 400
    storage_module.activate_project(slug)
    target = os.path.join(config.OUTPUT_DIR, run_name)
    real_base = os.path.realpath(config.OUTPUT_DIR) + os.sep
    if not os.path.realpath(target).startswith(real_base):
        return "Invalid path", 400
    if os.path.isdir(target):
        shutil.rmtree(target)
        logging.info("Deleted run: %s", run_name)
    return redirect(url_for("project_runs", slug=slug))


@app.route("/p/<slug>/runs/<run_name>/rename", methods=["POST"])
def rename_run_route(slug: str, run_name: str):
    new_name = request.form.get("friendly_name", "").strip()
    storage_module.activate_project(slug)
    storage_module.rename_run(run_name, new_name)
    logging.info("Renamed run %s → %s", run_name, new_name or "(cleared)")
    return redirect(url_for("project_runs", slug=slug))


# ══════════════════════════════════════════════════════════════════════════
#  API: SSE streams
# ══════════════════════════════════════════════════════════════════════════

@app.route("/api/progress/<slug>")
def progress_stream(slug: str):
    def generate():
        while True:
            with _crawls_lock:
                slot = _active_crawls.get(slug)
                if slot:
                    snapshot = dict(slot.status)
                    mono = slot.start_mono
                else:
                    snapshot = dict(_EMPTY_STATUS, project_slug=slug)
                    mono = None
            running = snapshot["running"]
            if running and mono is not None:
                elapsed = time.monotonic() - mono
                m, s = divmod(int(elapsed), 60)
                h, m = divmod(m, 60)
                snapshot["elapsed"] = f"{h:02d}:{m:02d}:{s:02d}"
            yield f"data: {json.dumps(snapshot)}\n\n"
            if not running:
                break
            time.sleep(1)

    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/logs")
def logs_stream():
    def generate():
        sent = 0
        heartbeat = 0
        try:
            while True:
                buf_list = list(_log_buffer)
                new_entries = buf_list[sent:]
                for entry in new_entries:
                    yield f"data: {json.dumps(entry)}\n\n"
                sent = len(buf_list)
                heartbeat += 1
                if heartbeat >= 30:
                    yield ": keepalive\n\n"
                    heartbeat = 0
                time.sleep(0.5)
        except GeneratorExit:
            return

    return Response(generate(), mimetype="text/event-stream")


# ── Main ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    storage_module.migrate_legacy_data()
    print("The Crawl Street Journal: http://localhost:5001")
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True)
