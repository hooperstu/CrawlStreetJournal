"""
Filesystem-based storage layer for The Crawl Street Journal.

Handles all CSV writing (pages, assets, edges, tags, errors, nav links,
link checks, sitemap URLs), run-directory lifecycle, project management,
and crawl-state persistence for resume.

Run directory layout
--------------------
Each crawl run lives in a timestamped subfolder under the project's
``runs/`` directory::

    projects/<slug>/runs/run_2025-04-01_12-00-00_123456/
        _config.json       — snapshot of all crawler settings
        _state.json        — crawl progress & serialised queue (for resume)
        .name              — optional human-friendly label
        pages.csv           — one row per crawled HTML page
        edges.csv           — link-graph (from_url → to_url)
        tags.csv            — one row per tag/keyword per page
        crawl_errors.csv    — fetch failures and parse errors
        assets_pdf.csv      — discovered PDF links (+ other asset types)
        sitemap_urls.csv    — raw sitemap entries (loc + lastmod)
        nav_links.csv       — links inside <nav> elements
        link_checks.csv     — HEAD-check results for outbound links
        phone_numbers.csv   — tel: href links found on crawled pages

A ``.latest`` marker file in the ``runs/`` root points to the most recent
run folder; ``get_latest_run_dir()`` resolves it.

Concurrency model
-----------------
``StorageContext`` provides per-crawl isolation of output paths and CSV
writers.  The module also exposes a parallel set of module-level functions
(``write_page``, ``initialise_outputs``, etc.) that use a process-global
``_active_run_dir`` — these exist for backward compatibility with the CLI
and should not be used for concurrent crawls.
"""

import csv
import io
import json
import logging
import os
import re
import shutil
import tempfile
import threading
import uuid
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import config
import utils

logger = logging.getLogger(__name__)

# Module-level fallback for CLI / non-concurrent code paths.
# Set by ``initialise_outputs()``; concurrent crawls use StorageContext instead.
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
    "wcag_lang_valid",
    "wcag_heading_order_valid",
    "wcag_title_present",
    "wcag_form_labels_pct",
    "wcag_landmarks_present",
    "wcag_vague_link_pct",
    "wcag_img_alt_pct",
    "wcag_empty_headings",
    "wcag_duplicate_ids",
    "wcag_empty_buttons",
    "wcag_empty_links",
    "wcag_tables_no_headers",
    "wcag_autocomplete_pct",
    "wcag_has_search",
    "wcag_has_nav",
    # Phase 4 — extended extraction
    "author",
    "publisher",
    "json_ld_id",
    "cms_generator",
    "robots_directives",
    "hreflang_links",
    "feed_urls",
    "pagination_next",
    "pagination_prev",
    "breadcrumb_schema",
    "microdata_types",
    "rdfa_types",
    "schema_price",
    "schema_currency",
    "schema_availability",
    "schema_rating",
    "schema_review_count",
    "schema_event_date",
    "schema_event_location",
    "schema_job_title",
    "schema_job_location",
    "schema_recipe_time",
    "extraction_coverage_pct",
    "content_hash",
    "content_changed",
    # common
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

PHONE_NUMBER_FIELDS = (
    "page_url",
    "raw_href",
    "phone_number",
    "link_text",
    "discovered_at",
)


# ── Per-crawl storage context ─────────────────────────────────────────────

class StorageContext:
    """Isolated storage state for a single crawl.

    Each concurrent crawl gets its own instance so that ``output_dir`` and
    ``active_run_dir`` never collide between projects.

    Lifecycle:
        1. Construct with the project's ``runs/`` directory and a ``CrawlConfig``.
        2. Call ``initialise_outputs()`` (new crawl) or ``resume_outputs()`` (resume).
        3. Use the ``write_*`` methods during the crawl.
        4. ``get_active_run_dir()`` returns the resolved path at any point.

    Attributes:
        output_dir: The project's ``runs/`` directory.
        cfg: ``CrawlConfig`` instance controlling feature toggles.
        active_run_dir: Absolute path to the current run folder (set after init).
    """

    def __init__(
        self,
        output_dir: str,
        cfg: "config.CrawlConfig",
        active_run_dir: Optional[str] = None,
    ) -> None:
        self.output_dir = output_dir
        self.cfg = cfg
        self.active_run_dir = active_run_dir
        self._csv_lock = threading.Lock()

    # -- path helpers -----------------------------------------------------

    def _output_path(self, name: str) -> str:
        base = (self.active_run_dir or self.output_dir).rstrip("/") or "."
        return os.path.join(base, name)

    def ensure_output_dir(self) -> None:
        target = self.active_run_dir or self.output_dir
        os.makedirs(target, exist_ok=True)

    def _assets_path_for_category(self, category: str) -> str:
        fname = f"{config.ASSETS_CSV_PREFIX}{category}.csv"
        return self._output_path(fname)

    # -- header / row writing ---------------------------------------------

    def _write_header(self, path: str, fieldnames: tuple) -> None:
        self.ensure_output_dir()
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(
                f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL,
            ).writeheader()

    def append_row(self, path: str, fieldnames: tuple, row: Dict[str, Any]) -> None:
        """Append a single CSV row, sanitising values and creating the file if needed.

        Thread-safe: uses ``_csv_lock`` to prevent interleaved writes when
        ``CONCURRENT_WORKERS > 1``.
        """
        self.ensure_output_dir()
        safe = {k: _sanitise(row.get(k, "")) for k in fieldnames}
        try:
            with self._csv_lock:
                with open(path, "a", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(
                        f, fieldnames=fieldnames,
                        extrasaction="ignore", quoting=csv.QUOTE_ALL,
                    )
                    w.writerow(safe)
        except Exception as e:
            url = safe.get("final_url") or safe.get("url") or safe.get("page_url") or "?"
            logger.warning("CSV write failed for %s → %s: %s", url, path, e)

    # -- typed CSV writers ------------------------------------------------

    def write_page(self, row: Dict[str, Any]) -> None:
        self.append_row(self._output_path(config.PAGES_CSV), PAGES_FIELDS, row)

    def write_asset(self, row: Dict[str, Any], category: str) -> None:
        known = set(config.ASSET_CATEGORY_BY_EXT.values()) | {"other"}
        cat = category if category in known else "other"
        self.append_row(self._assets_path_for_category(cat), ASSET_FIELDS, row)

    def write_edge(self, row: Dict[str, Any]) -> None:
        if not self.cfg.WRITE_EDGES_CSV:
            return
        self.append_row(self._output_path(config.EDGES_CSV), EDGE_FIELDS, row)

    def write_tag_row(self, row: Dict[str, Any]) -> None:
        if not self.cfg.WRITE_TAGS_CSV:
            return
        self.append_row(self._output_path(config.TAGS_CSV), TAG_ROW_FIELDS, row)

    def write_error(self, row: Dict[str, Any]) -> None:
        self.append_row(self._output_path(config.ERRORS_CSV), ERROR_FIELDS, row)

    def write_sitemap_url(self, row: Dict[str, Any]) -> None:
        if not self.cfg.WRITE_SITEMAP_URLS_CSV:
            return
        self.append_row(
            self._output_path(config.SITEMAP_URLS_CSV), SITEMAP_URL_FIELDS, row,
        )

    def write_nav_link(self, row: Dict[str, Any]) -> None:
        if not self.cfg.WRITE_NAV_LINKS_CSV:
            return
        self.append_row(
            self._output_path(config.NAV_LINKS_CSV), NAV_LINK_FIELDS, row,
        )

    def write_link_check(self, row: Dict[str, Any]) -> None:
        if not self.cfg.CHECK_OUTBOUND_LINKS:
            return
        self.append_row(
            self._output_path(config.LINK_CHECKS_CSV), LINK_CHECK_FIELDS, row,
        )

    def write_phone_number(self, row: Dict[str, Any]) -> None:
        self.append_row(
            self._output_path(config.PHONE_NUMBERS_CSV), PHONE_NUMBER_FIELDS, row,
        )

    # -- latest-run marker ------------------------------------------------

    def _write_latest_marker(self, run_folder_name: str) -> None:
        marker = os.path.join(self.output_dir, ".latest")
        with open(marker, "w", encoding="utf-8") as f:
            f.write(run_folder_name)

    def get_latest_run_dir(self) -> str:
        marker = os.path.join(self.output_dir, ".latest")
        if os.path.isfile(marker):
            with open(marker, "r", encoding="utf-8") as f:
                name = f.read().strip()
            candidate = os.path.join(self.output_dir, name)
            if os.path.isdir(candidate):
                return candidate
        return self.output_dir

    def get_active_run_dir(self) -> str:
        return self.active_run_dir or self.get_latest_run_dir()

    # -- run name helpers -------------------------------------------------

    def rename_run(self, folder_name: str, new_name: str) -> bool:
        full = os.path.join(self.output_dir, folder_name)
        if not os.path.isdir(full):
            return False
        _write_run_name(full, new_name)
        return True

    # -- run lifecycle ----------------------------------------------------

    def create_run(self, run_name: Optional[str] = None) -> str:
        os.makedirs(self.output_dir, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S_%f")
        folder_name = f"run_{stamp}"
        run_dir = os.path.join(self.output_dir, folder_name)
        os.makedirs(run_dir, exist_ok=False)
        if run_name:
            _write_run_name(run_dir, run_name)
        save_run_config(run_dir, self.cfg.to_dict())
        save_crawl_state(
            run_dir, status="new", pages_crawled=0,
            assets_from_pages=0, queue=[],
        )
        logger.info("Created run: %s (%s)", folder_name, run_name or "unnamed")
        return folder_name

    def initialise_outputs(
        self, run_folder: Optional[str] = None, run_name: Optional[str] = None,
    ) -> None:
        """Create (or adopt) a run folder and write CSV headers for all enabled outputs."""
        if run_folder:
            self.active_run_dir = os.path.join(self.output_dir, run_folder)
            if not os.path.isdir(self.active_run_dir):
                os.makedirs(self.active_run_dir, exist_ok=True)
        else:
            stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S_%f")
            folder_name = f"run_{stamp}"
            self.active_run_dir = os.path.join(self.output_dir, folder_name)
            os.makedirs(self.active_run_dir, exist_ok=False)
            if run_name:
                _write_run_name(self.active_run_dir, run_name)

        self._write_latest_marker(os.path.basename(self.active_run_dir))
        save_run_config(self.active_run_dir, self.cfg.to_dict())
        logger.info("Run output directory: %s", self.active_run_dir)

        self._write_header(self._output_path(config.PAGES_CSV), PAGES_FIELDS)
        if self.cfg.WRITE_EDGES_CSV:
            self._write_header(self._output_path(config.EDGES_CSV), EDGE_FIELDS)
        if self.cfg.WRITE_TAGS_CSV:
            self._write_header(self._output_path(config.TAGS_CSV), TAG_ROW_FIELDS)
        self._write_header(self._output_path(config.ERRORS_CSV), ERROR_FIELDS)
        if self.cfg.WRITE_SITEMAP_URLS_CSV:
            self._write_header(
                self._output_path(config.SITEMAP_URLS_CSV), SITEMAP_URL_FIELDS,
            )
        if self.cfg.WRITE_NAV_LINKS_CSV:
            self._write_header(
                self._output_path(config.NAV_LINKS_CSV), NAV_LINK_FIELDS,
            )
        if self.cfg.CHECK_OUTBOUND_LINKS:
            self._write_header(
                self._output_path(config.LINK_CHECKS_CSV), LINK_CHECK_FIELDS,
            )
        self._write_header(
            self._output_path(config.PHONE_NUMBERS_CSV), PHONE_NUMBER_FIELDS,
        )
        seen: set[str] = set()
        for cat in set(config.ASSET_CATEGORY_BY_EXT.values()):
            if cat not in seen:
                seen.add(cat)
                self._write_header(self._assets_path_for_category(cat), ASSET_FIELDS)
        if "other" not in seen:
            self._write_header(self._assets_path_for_category("other"), ASSET_FIELDS)

    def resume_outputs(self, run_folder: str) -> None:
        """Point this context at an existing run folder without overwriting CSVs."""
        self.active_run_dir = os.path.join(self.output_dir, run_folder)
        if not os.path.isdir(self.active_run_dir):
            raise FileNotFoundError(f"Run folder not found: {self.active_run_dir}")
        self._write_latest_marker(run_folder)
        logger.info("Resuming run in: %s", self.active_run_dir)

    # -- run listing ------------------------------------------------------

    def list_run_dirs(self) -> List[Dict[str, Any]]:
        """Return metadata dicts for every run under this context's output dir, newest first."""
        base = self.output_dir
        if not os.path.isdir(base):
            return []
        latest = os.path.basename(self.get_latest_run_dir())
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


# ── Module-level helpers (backward compat & CLI) ─────────────────────────

def _output_path(name: str) -> str:
    """Resolve *name* inside the active run directory (or OUTPUT_DIR fallback)."""
    base = (_active_run_dir or config.OUTPUT_DIR).rstrip("/") or "."
    return os.path.join(base, name)


def ensure_output_dir() -> None:
    target = _active_run_dir or config.OUTPUT_DIR
    os.makedirs(target, exist_ok=True)


# ── Run name helpers ──────────────────────────────────────────────────────

def _write_run_name(run_dir: str, name: str) -> None:
    """Persist a human-friendly label to the ``.name`` marker file."""
    with open(os.path.join(run_dir, ".name"), "w", encoding="utf-8") as f:
        f.write(name.strip())


def _read_run_name(run_dir: str) -> Optional[str]:
    """Read the friendly name from ``.name``, or ``None`` if absent."""
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
    "MAX_DEPTH",
    "REQUEST_DELAY_SECONDS",
    "REQUEST_TIMEOUT_SECONDS",
    "MAX_RETRIES",
    "CONCURRENT_WORKERS",
    "STATE_SAVE_INTERVAL",
    "CONTENT_DEDUP",
    "CHANGE_DETECTION",
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
    "RENDER_JAVASCRIPT",
    "ALLOWED_DOMAINS",
    "EXCLUDED_DOMAINS",
    "URL_EXCLUDE_PATTERNS",
    "URL_INCLUDE_PATTERNS",
    "DOMAIN_OWNERSHIP_RULES",
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
    """Write *cfg* as ``_config.json`` inside the run folder."""
    path = os.path.join(run_dir, "_config.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def load_run_config(run_dir: str) -> Optional[Dict[str, Any]]:
    """Load ``_config.json`` from a run folder, or ``None`` if missing."""
    path = os.path.join(run_dir, "_config.json")
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def apply_run_config(cfg: Dict[str, Any]) -> None:
    """Write *cfg* values into the live ``config`` module.

    Normalises types that JSON round-trips can change (lists → tuples,
    numeric strings → ints) so the live config always has the expected
    Python types.
    """
    _INT_KEYS = {
        "MAX_PAGES_TO_CRAWL", "MAX_SITEMAP_URLS", "MAX_RETRIES",
        "STATE_SAVE_INTERVAL", "MAX_LINK_CHECKS_PER_PAGE",
    }
    _FLOAT_KEYS = {
        "REQUEST_TIMEOUT_SECONDS", "HEAD_TIMEOUT_SECONDS",
        "LINK_CHECK_DELAY_SECONDS",
    }
    for key, val in cfg.items():
        if key not in _SNAPSHOT_KEYS:
            continue
        if key == "ALLOWED_DOMAINS" and isinstance(val, list):
            val = tuple(val)
        elif key == "REQUEST_DELAY_SECONDS" and isinstance(val, list) and len(val) == 2:
            val = tuple(val)
        elif key in _INT_KEYS:
            try:
                val = int(val)
            except (TypeError, ValueError):
                pass
        elif key in _FLOAT_KEYS:
            try:
                val = float(val)
            except (TypeError, ValueError):
                pass
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
    """Read ``_project.json`` for *slug*, or ``None`` if the project does not exist."""
    path = os.path.join(get_project_dir(slug), "_project.json")
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def delete_project(slug: str) -> None:
    """Remove the project directory tree.  Validates the path stays inside PROJECTS_DIR."""
    pdir = get_project_dir(slug)
    real_base = os.path.realpath(config.PROJECTS_DIR) + os.sep
    if not os.path.realpath(pdir).startswith(real_base):
        return
    if os.path.isdir(pdir):
        shutil.rmtree(pdir)
        logger.info("Deleted project: %s", slug)


def load_project_defaults(slug: str) -> Optional[Dict[str, Any]]:
    """Load ``_defaults.json`` for *slug* (project-level crawl config template)."""
    path = os.path.join(get_project_dir(slug), "_defaults.json")
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_project_defaults(slug: str, cfg: Dict[str, Any]) -> None:
    """Write (or overwrite) ``_defaults.json`` for *slug*."""
    path = os.path.join(get_project_dir(slug), "_defaults.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def activate_project(slug: str) -> StorageContext:
    """Point config.OUTPUT_DIR at this project's runs/ directory and return
    an isolated :class:`StorageContext` for the project.

    The module-level mutation of ``config.OUTPUT_DIR`` is kept for backward
    compatibility with the CLI and non-concurrent code paths.
    """
    runs_dir = get_project_runs_dir(slug)
    os.makedirs(runs_dir, exist_ok=True)
    config.OUTPUT_DIR = runs_dir
    return StorageContext(runs_dir, config.CrawlConfig.from_module())


def export_project(slug: str) -> io.BytesIO:
    """Create a ZIP archive of the entire project directory and return it as
    an in-memory buffer.  The archive root is the slug folder name so that
    the internal structure (``_project.json``, ``_defaults.json``, ``runs/``)
    is preserved on extraction.

    Raises ``FileNotFoundError`` if the project does not exist and
    ``ValueError`` if the path escapes ``PROJECTS_DIR``.
    """
    pdir = get_project_dir(slug)
    real_base = os.path.realpath(config.PROJECTS_DIR) + os.sep
    if not os.path.realpath(pdir).startswith(real_base):
        raise ValueError("Project path escapes PROJECTS_DIR")
    if not os.path.isdir(pdir):
        raise FileNotFoundError(f"Project directory not found: {pdir}")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for dirpath, _dirnames, filenames in os.walk(pdir):
            for fname in filenames:
                abs_path = os.path.join(dirpath, fname)
                arcname = os.path.join(slug, os.path.relpath(abs_path, pdir))
                zf.write(abs_path, arcname=arcname)
    buf.seek(0)
    logger.info("Exported project %s (%d bytes)", slug, buf.getbuffer().nbytes)
    return buf


def import_project(zip_fileobj) -> str:
    """Import a project from a ZIP archive file object.

    The archive must contain a single top-level directory with a valid
    ``_project.json`` inside it.  The slug is derived from that directory
    name, with a hex suffix appended when a collision is detected.

    Returns the slug of the newly imported project.

    Raises ``ValueError`` on validation failures (missing metadata, path
    traversal attempts, structural problems).
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_zip = os.path.join(tmpdir, "upload.zip")
        zip_fileobj.save(tmp_zip)

        with zipfile.ZipFile(tmp_zip, "r") as zf:
            for info in zf.infolist():
                if info.filename.startswith("/") or ".." in info.filename.split("/"):
                    raise ValueError(
                        f"Unsafe path in archive: {info.filename}"
                    )
            zf.extractall(tmpdir)

        entries = [
            e for e in os.listdir(tmpdir)
            if e != "upload.zip" and os.path.isdir(os.path.join(tmpdir, e))
        ]
        if len(entries) != 1:
            raise ValueError(
                "Archive must contain exactly one top-level project folder"
            )

        extracted_name = entries[0]
        extracted_dir = os.path.join(tmpdir, extracted_name)
        meta_path = os.path.join(extracted_dir, "_project.json")
        if not os.path.isfile(meta_path):
            raise ValueError(
                "Archive does not contain a valid project (_project.json missing)"
            )

        slug = _slugify(extracted_name)
        dest = os.path.join(config.PROJECTS_DIR, slug)
        if os.path.exists(dest):
            slug = f"{slug}-{uuid.uuid4().hex[:6]}"
            dest = os.path.join(config.PROJECTS_DIR, slug)

        os.makedirs(config.PROJECTS_DIR, exist_ok=True)
        shutil.move(extracted_dir, dest)

    logger.info("Imported project as %s", slug)
    return slug


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

    old_output = os.path.join(config.DATA_DIR, "output")
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


def recover_stale_running_states() -> int:
    """On startup, mark any run whose _state.json says 'running' as 'interrupted'.

    A 'running' state left on disk after a server restart means the crawl
    process was killed without a clean shutdown.  Leaving it as 'running'
    confuses the UI (the config page shows an inaccurate status banner).
    Returns the number of runs that were corrected.
    """
    projects_dir = config.PROJECTS_DIR
    if not os.path.isdir(projects_dir):
        return 0
    corrected = 0
    for slug in os.listdir(projects_dir):
        runs_dir = os.path.join(projects_dir, slug, "runs")
        if not os.path.isdir(runs_dir):
            continue
        for run_name in os.listdir(runs_dir):
            run_dir = os.path.join(runs_dir, run_name)
            state_path = os.path.join(run_dir, "_state.json")
            if not os.path.isfile(state_path):
                continue
            try:
                with open(state_path, "r", encoding="utf-8") as f:
                    state = json.load(f)
                if state.get("status") == "running":
                    state["status"] = "interrupted"
                    with open(state_path, "w", encoding="utf-8") as f:
                        json.dump(state, f, ensure_ascii=False)
                    corrected += 1
                    logger.info(
                        "Recovered stale 'running' state for run %s/%s → interrupted",
                        slug, run_name,
                    )
            except Exception as e:
                logger.warning("Could not check state for %s/%s: %s", slug, run_name, e)
    return corrected


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
    """Persist crawl progress and the serialised queue to ``_state.json``.

    Called periodically during the crawl (every ``STATE_SAVE_INTERVAL``
    pages) and once at completion/interruption.  The *queue* field is a
    list of ``(url, referrer, depth)`` triples that can be reloaded on resume.
    """
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
    """Load ``_state.json``, or ``None`` if the file does not exist."""
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
        except Exception as e:
            logger.warning("Skipping corrupt pages CSV %s: %s", pages_path, e)
    errors_path = os.path.join(run_dir, config.ERRORS_CSV)
    if os.path.isfile(errors_path):
        try:
            with open(errors_path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    url = row.get("url", "").strip()
                    if url:
                        visited.add(url)
        except Exception as e:
            logger.warning("Skipping corrupt errors CSV %s: %s", errors_path, e)
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
    except Exception as e:
        logger.warning("Skipping corrupt sitemap CSV %s: %s", path, e)
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
    _write_header(_output_path(config.PHONE_NUMBERS_CSV), PHONE_NUMBER_FIELDS)
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
    """Return the number of data rows in ``pages.csv`` (0 if missing)."""
    return utils.count_csv_rows(os.path.join(directory, config.PAGES_CSV))


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
    """Coerce *value* to a CSV-safe string (strip NULs, truncate at 32 KB).

    Delegates to ``utils.sanitise_csv_value``.
    """
    return utils.sanitise_csv_value(value)


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


def write_phone_number(row: Dict[str, Any]) -> None:
    append_row(_output_path(config.PHONE_NUMBERS_CSV), PHONE_NUMBER_FIELDS, row)


# ── Content hash persistence (for dedup + change detection) ──────────────

def save_content_hashes(run_dir: str, hashes: Dict[str, str]) -> None:
    """Persist content hashes to a JSON file in the run directory."""
    path = os.path.join(run_dir, "_content_hashes.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(hashes, f, ensure_ascii=False)


def load_content_hashes(run_dir: str) -> Dict[str, str]:
    """Load content hashes from a previous run, or return empty dict."""
    path = os.path.join(run_dir, "_content_hashes.json")
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}
