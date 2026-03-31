"""
CSV writers for Collector: pages inventory, assets by type, edges, tags, errors.

Each crawl run is a self-contained project living in a timestamped subfolder
under OUTPUT_DIR.  The folder holds:

    _config.json   – snapshot of all crawler settings for this run
    _state.json    – crawl progress & serialised queue (for resume)
    .name          – optional human-friendly label
    .latest        – (in OUTPUT_DIR root) points to the most recent run

The ``get_latest_run_dir()`` helper resolves the marker for readers.
"""

import csv
import json
import logging
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import config

logger = logging.getLogger(__name__)

_active_run_dir: Optional[str] = None

# ── CSV field definitions ─────────────────────────────────────────────────

PAGES_FIELDS = (
    "requested_url",
    "final_url",
    "domain",
    "http_status",
    "content_type",
    "title",
    "meta_description",
    "lang",
    "canonical_url",
    "og_title",
    "og_type",
    "og_description",
    "twitter_card",
    "json_ld_types",
    "tags_all",
    "url_content_hint",
    "content_kind_guess",
    "h1_joined",
    "word_count",
    "http_last_modified",
    "etag",
    "sitemap_lastmod",
    "referrer_sitemap_url",
    "heading_outline",
    "date_published",
    "date_modified",
    "visible_dates",
    "link_count_internal",
    "link_count_external",
    "link_count_total",
    "img_count",
    "img_missing_alt_count",
    "readability_fk_grade",
    "privacy_policy_url",
    "analytics_signals",
    "training_related_flag",
    "nav_link_count",
    "referrer_url",
    "depth",
    "discovered_at",
)

ASSET_FIELDS = (
    "referrer_page_url",
    "asset_url",
    "link_text",
    "category",
    "head_content_type",
    "head_content_length",
    "discovered_at",
)

EDGE_FIELDS = ("from_url", "to_url", "link_text", "discovered_at")

TAG_ROW_FIELDS = ("page_url", "tag_value", "tag_source", "discovered_at")

ERROR_FIELDS = ("url", "error_type", "message", "http_status", "discovered_at")

SITEMAP_URL_FIELDS = (
    "url",
    "lastmod",
    "source_sitemap",
    "discovered_at",
)

NAV_LINK_FIELDS = (
    "page_url",
    "nav_href",
    "nav_text",
    "discovered_at",
)

LINK_CHECK_FIELDS = (
    "from_url",
    "to_url",
    "check_status",
    "check_final_url",
    "discovered_at",
)


def _output_path(name: str) -> str:
    """Resolve *name* inside the active run directory (or OUTPUT_DIR fallback)."""
    base = (_active_run_dir or config.OUTPUT_DIR).rstrip("/") or "."
    return os.path.join(base, name)


def ensure_output_dir() -> None:
    target = _active_run_dir or config.OUTPUT_DIR
    os.makedirs(target, exist_ok=True)


# ── Run name helpers ──────────────────────────────────────────────────────

def _write_run_name(run_dir: str, name: str) -> None:
    with open(os.path.join(run_dir, ".name"), "w", encoding="utf-8") as f:
        f.write(name.strip())


def _read_run_name(run_dir: str) -> Optional[str]:
    path = os.path.join(run_dir, ".name")
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            val = f.read().strip()
        return val or None
    return None


def rename_run(folder_name: str, new_name: str) -> bool:
    """Set or update the friendly name of an existing run."""
    full = os.path.join(config.OUTPUT_DIR, folder_name)
    if not os.path.isdir(full):
        return False
    _write_run_name(full, new_name)
    return True


# ── Latest-run marker ────────────────────────────────────────────────────

def _write_latest_marker(run_folder_name: str) -> None:
    marker = os.path.join(config.OUTPUT_DIR, ".latest")
    with open(marker, "w", encoding="utf-8") as f:
        f.write(run_folder_name)


def get_latest_run_dir() -> str:
    """Return the absolute path of the most recent run folder.

    Falls back to OUTPUT_DIR itself when no run has been recorded.
    """
    marker = os.path.join(config.OUTPUT_DIR, ".latest")
    if os.path.isfile(marker):
        with open(marker, "r", encoding="utf-8") as f:
            name = f.read().strip()
        candidate = os.path.join(config.OUTPUT_DIR, name)
        if os.path.isdir(candidate):
            return candidate
    return config.OUTPUT_DIR


def get_active_run_dir() -> str:
    """Return the run directory being written to right now, or latest."""
    return _active_run_dir or get_latest_run_dir()


# ── Config snapshot ──────────────────────────────────────────────────────

_SNAPSHOT_KEYS = (
    "SEED_URLS",
    "SITEMAP_URLS",
    "LOAD_SITEMAPS_FROM_ROBOTS",
    "RESPECT_ROBOTS_TXT",
    "MAX_SITEMAP_URLS",
    "MAX_PAGES_TO_CRAWL",
    "REQUEST_DELAY_SECONDS",
    "REQUEST_TIMEOUT_SECONDS",
    "MAX_RETRIES",
    "WRITE_EDGES_CSV",
    "WRITE_TAGS_CSV",
    "ASSET_HEAD_METADATA",
    "HEAD_TIMEOUT_SECONDS",
    "CAPTURE_RESPONSE_HEADERS",
    "WRITE_SITEMAP_URLS_CSV",
    "WRITE_NAV_LINKS_CSV",
    "CHECK_OUTBOUND_LINKS",
    "MAX_LINK_CHECKS_PER_PAGE",
    "LINK_CHECK_DELAY_SECONDS",
    "CAPTURE_READABILITY",
    "ALLOWED_DOMAINS",
    "USER_AGENT",
    "LOG_LEVEL",
)


def snapshot_config() -> Dict[str, Any]:
    """Return a JSON-safe dict of all crawl-relevant config values."""
    d: Dict[str, Any] = {}
    for key in _SNAPSHOT_KEYS:
        val = getattr(config, key, None)
        if isinstance(val, tuple):
            val = list(val)
        d[key] = val
    return d


def save_run_config(run_dir: str, cfg: Dict[str, Any]) -> None:
    path = os.path.join(run_dir, "_config.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def load_run_config(run_dir: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(run_dir, "_config.json")
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def apply_run_config(cfg: Dict[str, Any]) -> None:
    """Write *cfg* values into the live ``config`` module."""
    for key, val in cfg.items():
        if key not in _SNAPSHOT_KEYS:
            continue
        if key == "ALLOWED_DOMAINS" and isinstance(val, list):
            val = tuple(val)
        if key == "REQUEST_DELAY_SECONDS" and isinstance(val, list) and len(val) == 2:
            val = tuple(val)
        setattr(config, key, val)


# ── Project management ───────────────────────────────────────────────────

def _slugify(text: str) -> str:
    """Turn a project name into a filesystem-safe slug."""
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9\-]", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    slug = slug.strip("-")[:60]
    if not slug:
        slug = f"project-{uuid.uuid4().hex[:8]}"
    return slug


def get_project_dir(slug: str) -> str:
    return os.path.join(config.PROJECTS_DIR, slug)


def get_project_runs_dir(slug: str) -> str:
    return os.path.join(config.PROJECTS_DIR, slug, "runs")



def create_project(name: str, description: str = "") -> str:
    """Create a new project directory with metadata and defaults. Returns slug."""
    slug = _slugify(name)
    base = config.PROJECTS_DIR
    if os.path.isdir(os.path.join(base, slug)):
        slug = f"{slug}-{uuid.uuid4().hex[:6]}"
    project_dir = os.path.join(base, slug)
    os.makedirs(project_dir, exist_ok=False)
    os.makedirs(os.path.join(project_dir, "runs"), exist_ok=True)

    meta = {
        "name": name,
        "description": description,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(os.path.join(project_dir, "_project.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    save_project_defaults(slug, snapshot_config())
    logger.info("Created project: %s (%s)", slug, name)
    return slug


def list_projects() -> List[Dict[str, Any]]:
    """Return metadata for every project, sorted by creation date descending."""
    base = config.PROJECTS_DIR
    if not os.path.isdir(base):
        return []
    projects: List[Dict[str, Any]] = []
    for name in sorted(os.listdir(base)):
        pdir = os.path.join(base, name)
        if not os.path.isdir(pdir):
            continue
        meta_path = os.path.join(pdir, "_project.json")
        if not os.path.isfile(meta_path):
            continue
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            continue
        runs_dir = os.path.join(pdir, "runs")
        run_count = 0
        total_pages = 0
        latest_status = ""
        if os.path.isdir(runs_dir):
            run_folders = sorted(
                [
                    d for d in os.listdir(runs_dir)
                    if d.startswith("run_")
                    and os.path.isdir(os.path.join(runs_dir, d))
                ],
                reverse=True,
            )
            run_count = len(run_folders)
            for rf in run_folders:
                total_pages += _count_pages_in(os.path.join(runs_dir, rf))
            if run_folders:
                latest_status = get_run_status(os.path.join(runs_dir, run_folders[0]))
        projects.append({
            "slug": name,
            "name": meta.get("name", name),
            "description": meta.get("description", ""),
            "created_at": meta.get("created_at", ""),
            "run_count": run_count,
            "total_pages": total_pages,
            "latest_run_status": latest_status,
        })
    projects.sort(key=lambda p: p["created_at"], reverse=True)
    return projects


def load_project(slug: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(get_project_dir(slug), "_project.json")
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_project(slug: str, data: Dict[str, Any]) -> None:
    path = os.path.join(get_project_dir(slug), "_project.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def delete_project(slug: str) -> None:
    pdir = get_project_dir(slug)
    real_base = os.path.realpath(config.PROJECTS_DIR) + os.sep
    if not os.path.realpath(pdir).startswith(real_base):
        return
    if os.path.isdir(pdir):
        shutil.rmtree(pdir)
        logger.info("Deleted project: %s", slug)


def load_project_defaults(slug: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(get_project_dir(slug), "_defaults.json")
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_project_defaults(slug: str, cfg: Dict[str, Any]) -> None:
    path = os.path.join(get_project_dir(slug), "_defaults.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def activate_project(slug: str) -> None:
    """Point config.OUTPUT_DIR at this project's runs/ directory so all
    existing run functions (create_run, list_run_dirs, etc.) are scoped."""
    runs_dir = get_project_runs_dir(slug)
    os.makedirs(runs_dir, exist_ok=True)
    config.OUTPUT_DIR = runs_dir


def migrate_legacy_data() -> Optional[str]:
    """One-time migration: if projects/ is empty but output/ has run data,
    create a default project and relocate everything. Returns slug or None."""
    projects_dir = config.PROJECTS_DIR
    if os.path.isdir(projects_dir) and any(
        os.path.isfile(os.path.join(projects_dir, d, "_project.json"))
        for d in os.listdir(projects_dir)
        if os.path.isdir(os.path.join(projects_dir, d))
    ):
        return None

    old_output = "output"
    if not os.path.isdir(old_output):
        return None

    has_runs = any(
        d.startswith("run_")
        for d in os.listdir(old_output)
        if os.path.isdir(os.path.join(old_output, d))
    )
    has_csvs = any(
        d.endswith(".csv")
        for d in os.listdir(old_output)
        if not d.startswith("run_")
    )
    if not has_runs and not has_csvs:
        return None

    slug = "default"
    project_dir = os.path.join(projects_dir, slug)
    os.makedirs(project_dir, exist_ok=True)

    meta = {
        "name": "Default Project",
        "description": "Migrated from legacy output directory.",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(os.path.join(project_dir, "_project.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    save_project_defaults(slug, snapshot_config())

    runs_dest = os.path.join(project_dir, "runs")
    os.makedirs(runs_dest, exist_ok=True)
    for item in os.listdir(old_output):
        src = os.path.join(old_output, item)
        dst = os.path.join(runs_dest, item)
        if not os.path.exists(dst):
            shutil.move(src, dst)

    logger.info("Migrated legacy data to project: %s", slug)
    return slug


# ── Crawl state (for resume) ─────────────────────────────────────────────

def save_crawl_state(
    run_dir: str,
    *,
    status: str,
    pages_crawled: int,
    assets_from_pages: int,
    queue: List[Any],
    started_at: str = "",
    stopped_at: str = "",
) -> None:
    state = {
        "status": status,
        "pages_crawled": pages_crawled,
        "assets_from_pages": assets_from_pages,
        "queue": queue,
        "started_at": started_at,
        "stopped_at": stopped_at,
    }
    path = os.path.join(run_dir, "_state.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)


def load_crawl_state(run_dir: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(run_dir, "_state.json")
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_run_status(run_dir: str) -> str:
    """Return the status of a run: new, running, interrupted, completed."""
    state = load_crawl_state(run_dir)
    if state is None:
        return "new"
    return state.get("status", "new")


def rebuild_visited_from_csvs(run_dir: str) -> set:
    """Reconstruct the set of already-visited URLs from existing CSV data."""
    visited: set = set()
    pages_path = os.path.join(run_dir, config.PAGES_CSV)
    if os.path.isfile(pages_path):
        try:
            with open(pages_path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    url = row.get("requested_url", "").strip()
                    if url:
                        visited.add(url)
        except Exception:
            pass
    errors_path = os.path.join(run_dir, config.ERRORS_CSV)
    if os.path.isfile(errors_path):
        try:
            with open(errors_path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    url = row.get("url", "").strip()
                    if url:
                        visited.add(url)
        except Exception:
            pass
    return visited


def rebuild_sitemap_meta_from_csv(run_dir: str) -> Dict[str, Dict[str, str]]:
    """Reconstruct the sitemap_meta lookup from sitemap_urls.csv."""
    meta: Dict[str, Dict[str, str]] = {}
    path = os.path.join(run_dir, config.SITEMAP_URLS_CSV)
    if not os.path.isfile(path):
        return meta
    try:
        with open(path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                url = row.get("url", "").strip()
                if url:
                    meta[url] = {
                        "sitemap_lastmod": row.get("lastmod", ""),
                        "source_sitemap": row.get("source_sitemap", ""),
                    }
    except Exception:
        pass
    return meta


# ── Run-directory lifecycle ──────────────────────────────────────────────

def create_run(run_name: Optional[str] = None) -> str:
    """Create a new run folder with a config snapshot. Returns the folder name."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S_%f")
    folder_name = f"run_{stamp}"
    run_dir = os.path.join(config.OUTPUT_DIR, folder_name)
    os.makedirs(run_dir, exist_ok=False)

    if run_name:
        _write_run_name(run_dir, run_name)

    save_run_config(run_dir, snapshot_config())
    save_crawl_state(
        run_dir,
        status="new",
        pages_crawled=0,
        assets_from_pages=0,
        queue=[],
    )
    logger.info("Created run: %s (%s)", folder_name, run_name or "unnamed")
    return folder_name


def initialise_outputs(run_folder: Optional[str] = None, run_name: Optional[str] = None) -> None:
    """Prepare a new run folder with CSV headers.

    If *run_folder* is given, use that existing folder; otherwise create a
    fresh timestamped folder.  The run's ``_config.json`` is written (or
    overwritten) with the current live config so it's always in sync.
    """
    global _active_run_dir

    if run_folder:
        _active_run_dir = os.path.join(config.OUTPUT_DIR, run_folder)
        if not os.path.isdir(_active_run_dir):
            os.makedirs(_active_run_dir, exist_ok=True)
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S_%f")
        folder_name = f"run_{stamp}"
        _active_run_dir = os.path.join(config.OUTPUT_DIR, folder_name)
        os.makedirs(_active_run_dir, exist_ok=False)
        if run_name:
            _write_run_name(_active_run_dir, run_name)

    _write_latest_marker(os.path.basename(_active_run_dir))
    save_run_config(_active_run_dir, snapshot_config())
    logger.info("Run output directory: %s", _active_run_dir)

    _write_header(_output_path(config.PAGES_CSV), PAGES_FIELDS)
    if config.WRITE_EDGES_CSV:
        _write_header(_output_path(config.EDGES_CSV), EDGE_FIELDS)
    if config.WRITE_TAGS_CSV:
        _write_header(_output_path(config.TAGS_CSV), TAG_ROW_FIELDS)
    _write_header(_output_path(config.ERRORS_CSV), ERROR_FIELDS)
    if config.WRITE_SITEMAP_URLS_CSV:
        _write_header(_output_path(config.SITEMAP_URLS_CSV), SITEMAP_URL_FIELDS)
    if config.WRITE_NAV_LINKS_CSV:
        _write_header(_output_path(config.NAV_LINKS_CSV), NAV_LINK_FIELDS)
    if config.CHECK_OUTBOUND_LINKS:
        _write_header(_output_path(config.LINK_CHECKS_CSV), LINK_CHECK_FIELDS)
    seen: set[str] = set()
    for cat in set(config.ASSET_CATEGORY_BY_EXT.values()):
        if cat not in seen:
            seen.add(cat)
            _write_header(_assets_path_for_category(cat), ASSET_FIELDS)
    if "other" not in seen:
        _write_header(_assets_path_for_category("other"), ASSET_FIELDS)


def resume_outputs(run_folder: str) -> None:
    """Point the writer at an existing run folder without overwriting CSVs."""
    global _active_run_dir
    _active_run_dir = os.path.join(config.OUTPUT_DIR, run_folder)
    if not os.path.isdir(_active_run_dir):
        raise FileNotFoundError(f"Run folder not found: {_active_run_dir}")
    _write_latest_marker(run_folder)
    logger.info("Resuming run in: %s", _active_run_dir)


# ── Run listing ──────────────────────────────────────────────────────────

def _count_pages_in(directory: str) -> int:
    pages_path = os.path.join(directory, config.PAGES_CSV)
    if not os.path.isfile(pages_path):
        return 0
    try:
        with open(pages_path, "r", encoding="utf-8") as f:
            return max(sum(1 for _ in f) - 1, 0)
    except Exception:
        return 0


def list_run_dirs() -> List[Dict[str, Any]]:
    """Return metadata for every run (including legacy un-foldered data),
    newest first."""
    base = config.OUTPUT_DIR
    if not os.path.isdir(base):
        return []
    latest = os.path.basename(get_latest_run_dir())
    runs: List[Dict[str, Any]] = []

    for name in sorted(os.listdir(base), reverse=True):
        if not name.startswith("run_"):
            continue
        full = os.path.join(base, name)
        if not os.path.isdir(full):
            continue
        raw = name.replace("run_", "", 1)
        parts = raw.split("_")
        date_part = parts[0] if parts else raw
        time_part = parts[1].replace("-", ":") if len(parts) > 1 else ""
        timestamp_label = (date_part + " " + time_part).strip()
        friendly = _read_run_name(full)
        label = f"{friendly} ({timestamp_label})" if friendly else timestamp_label
        status = get_run_status(full)
        runs.append({
            "name": name,
            "friendly_name": friendly or "",
            "timestamp_label": timestamp_label,
            "label": label,
            "page_count": _count_pages_in(full),
            "status": status,
            "has_config": os.path.isfile(os.path.join(full, "_config.json")),
            "is_latest": name == latest,
        })

    legacy_pages = _count_pages_in(base)
    has_legacy_csvs = legacy_pages > 0 or any(
        n.endswith(".csv") and not os.path.isdir(os.path.join(base, n))
        for n in os.listdir(base)
        if not n.startswith("run_")
    )
    if has_legacy_csvs:
        runs.append({
            "name": "_legacy",
            "friendly_name": "",
            "timestamp_label": "",
            "label": "Pre-migration data",
            "page_count": legacy_pages,
            "status": "completed",
            "has_config": False,
            "is_latest": latest == os.path.basename(base),
        })

    return runs


# ── CSV writing ──────────────────────────────────────────────────────────

def _sanitise(value: Any) -> str:
    if value is None:
        return ""
    s = str(value)
    if "\x00" in s:
        s = s.replace("\x00", "")
    if len(s) > 32_000:
        s = s[:32_000] + "…[truncated]"
    return s


def _write_header(path: str, fieldnames: tuple) -> None:
    ensure_output_dir()
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(
            f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL,
        ).writeheader()


def _assets_path_for_category(category: str) -> str:
    fname = f"{config.ASSETS_CSV_PREFIX}{category}.csv"
    return _output_path(fname)


def append_row(path: str, fieldnames: tuple, row: Dict[str, Any]) -> None:
    ensure_output_dir()
    safe = {k: _sanitise(row.get(k, "")) for k in fieldnames}
    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f, fieldnames=fieldnames,
                extrasaction="ignore", quoting=csv.QUOTE_ALL,
            )
            w.writerow(safe)
    except Exception as e:
        url = safe.get("final_url") or safe.get("url") or safe.get("page_url") or "?"
        logger.warning("CSV write failed for %s → %s: %s", url, path, e)


def write_page(row: Dict[str, Any]) -> None:
    append_row(_output_path(config.PAGES_CSV), PAGES_FIELDS, row)


def write_asset(row: Dict[str, Any], category: str) -> None:
    known = set(config.ASSET_CATEGORY_BY_EXT.values()) | {"other"}
    cat = category if category in known else "other"
    append_row(_assets_path_for_category(cat), ASSET_FIELDS, row)


def write_edge(row: Dict[str, Any]) -> None:
    if not config.WRITE_EDGES_CSV:
        return
    append_row(_output_path(config.EDGES_CSV), EDGE_FIELDS, row)


def write_tag_row(row: Dict[str, Any]) -> None:
    if not config.WRITE_TAGS_CSV:
        return
    append_row(_output_path(config.TAGS_CSV), TAG_ROW_FIELDS, row)


def write_error(row: Dict[str, Any]) -> None:
    append_row(_output_path(config.ERRORS_CSV), ERROR_FIELDS, row)


def write_sitemap_url(row: Dict[str, Any]) -> None:
    if not config.WRITE_SITEMAP_URLS_CSV:
        return
    append_row(_output_path(config.SITEMAP_URLS_CSV), SITEMAP_URL_FIELDS, row)


def write_nav_link(row: Dict[str, Any]) -> None:
    if not config.WRITE_NAV_LINKS_CSV:
        return
    append_row(_output_path(config.NAV_LINKS_CSV), NAV_LINK_FIELDS, row)


def write_link_check(row: Dict[str, Any]) -> None:
    if not config.CHECK_OUTBOUND_LINKS:
        return
    append_row(_output_path(config.LINK_CHECKS_CSV), LINK_CHECK_FIELDS, row)
