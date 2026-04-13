#!/usr/bin/env python3
"""
The Crawl Street Journal — Web GUI

Flask application providing a browser interface for managing projects,
configuring crawls, running them, and reviewing results.

    python gui.py          # http://localhost:5001 (bind: ``127.0.0.1`` by default)

Serves on port **5001** (override with ``CSJ_GUI_PORT``) with ``threaded=True``
so that SSE long-poll streams do not block other requests.

Threading model
~~~~~~~~~~~~~~~
Each crawl runs in its own daemon ``threading.Thread``.  Per-project state
is held in a ``CrawlSlot`` inside ``_active_crawls``, guarded by
``_crawls_lock``.  Only one crawl may be active per project slug at a time;
the lock is checked both before and after thread construction (double-check
pattern) to prevent races.  ``CrawlSlot.status`` is a plain dict mutated by
the worker thread and snapshot-copied by Flask request threads; individual
key writes are atomic under CPython's GIL so no additional lock is needed
for status reads.

Route map
~~~~~~~~~
Projects::

    GET  /                                        List all projects.
    POST /projects/create                         Create a new project.
    POST /projects/<slug>/delete                  Delete a project.

Project pages::

    GET  /p/<slug>                                Dashboard (reports).
    GET  /p/<slug>/defaults                       Redirects to Settings (project defaults).
    POST /p/<slug>/defaults                       Save project defaults (same form as Settings).
    GET  /p/<slug>/runs                           List runs.
    POST /p/<slug>/runs/create                    Create a new run (optional continue_from).

Run pages::

    GET  /p/<slug>/runs/<run>/config              View run config.
    POST /p/<slug>/runs/<run>/config              Save run config.
    GET  /p/<slug>/runs/<run>/monitor             Live crawl monitor.
    POST /p/<slug>/runs/<run>/start               Start / auto-resume crawl (optional continue_from).
    POST /p/<slug>/runs/<run>/resume              Explicitly resume crawl.
    POST /p/<slug>/runs/<run>/stop                Signal crawl to stop.
    GET  /p/<slug>/runs/<run>/results             Results dashboard.
    GET  /p/<slug>/runs/<run>/results/<file>      Paginated CSV viewer.
    GET  /p/<slug>/runs/<run>/download/<file>     Download single CSV.
    GET  /p/<slug>/runs/<run>/download-all        Download all CSVs as ZIP.
    POST /p/<slug>/runs/<run>/delete              Delete a run folder.
    POST /p/<slug>/runs/<run>/rename              Rename a run.

SSE streams (consumed by the front-end JavaScript)::

    GET  /api/progress/<slug>                     Crawl progress events.
    GET  /api/logs                                Global log tail.

``/api/progress`` JSON includes ``concurrent_workers``, ``active_worker_urls``
(one slot per parallel worker), and ``draining_workers`` when applicable.

Desktop / loopback only::

    POST /api/quit                                Graceful shutdown (stop crawls, exit app).

Additional visualisation endpoints are registered via the ``eco_bp``
blueprint from ``viz_api``.
"""
from __future__ import annotations

import copy
import csv
import io
import itertools
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
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)
from werkzeug.serving import make_server

import config
from config import CrawlConfig
import storage as storage_module
from storage import StorageContext

from error_pages import render_http_error
import utils

app = Flask(
    __name__,
    template_folder=os.path.join(config.BUNDLE_DIR, "templates"),
    static_folder=os.path.join(config.BUNDLE_DIR, "static"),
    static_url_path="/static",
)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0


def _env_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip()


def _gui_bind_address() -> str:
    return _env_str("CSJ_GUI_BIND", getattr(config, "GUI_BIND_ADDRESS", "127.0.0.1"))


def _load_secret_key() -> bytes:
    """Return a persistent secret key, generating one on first run.

    The key is stored in a ``secret_key`` file inside DATA_DIR so it
    survives process restarts (keeping signed cookies valid).  Falls back
    to a fresh random key only if the file cannot be read or written.
    """
    key_path = os.path.join(config.DATA_DIR, "secret_key")
    try:
        if os.path.isfile(key_path):
            with open(key_path, "rb") as _f:
                key = _f.read().strip()
            if key:
                return key
        key = os.urandom(32)
        os.makedirs(config.DATA_DIR, exist_ok=True)
        with open(key_path, "wb") as _f:
            _f.write(key)
        return key
    except OSError:
        return os.urandom(32)


app.secret_key = _load_secret_key()

from viz_api import eco_bp  # noqa: E402
# Visualisation endpoints (reports charts, etc.) live in viz_api.py.
app.register_blueprint(eco_bp)


@app.errorhandler(404)
def _handle_unmatched_url(_e):
    """Unknown URLs (no route) — never a bare Werkzeug page in the desktop shell."""
    return render_http_error(
        "That page does not exist, or the link may be out of date.",
        404,
        page_title="Page not found",
    )


@app.errorhandler(500)
def _handle_unexpected_server_error(_e):
    """Uncaught exceptions — navigable recovery instead of a blank error body."""
    logging.getLogger(__name__).exception("Unhandled server error")
    return render_http_error(
        "Something went wrong. Use the links below to get back to the app.",
        500,
        page_title="Server error",
    )


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
    "concurrent_workers": 1,
    "active_worker_urls": [],
    "draining_workers": 0,
}


class CrawlSlot:
    """Mutable state for a single in-progress crawl.

    One slot exists per actively-crawling project in ``_active_crawls``.
    The ``status`` dict is read by Flask request threads (via snapshot
    copy) and written by the crawl worker thread.  ``stop_event`` is the
    cooperative cancellation signal checked by :func:`scraper.crawl`.

    Attributes:
        thread: The daemon thread running the crawl.
        stop_event: Set by the ``/stop`` route to request graceful shutdown.
        status: Live progress dict whose keys mirror ``_EMPTY_STATUS``.
        cfg: Frozen crawl configuration for this run.
        ctx: Storage context scoped to the project's runs directory.
        start_mono: ``time.monotonic()`` timestamp used for elapsed-time display.
    """
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


# Keyed by project slug; only one crawl per project is permitted.
# All mutations must hold _crawls_lock.  Status dicts inside individual
# slots are an exception — see the module docstring for the rationale.
_active_crawls: Dict[str, CrawlSlot] = {}
_crawls_lock = threading.Lock()

_stale_recovery_lock = threading.Lock()
_stale_recovery_done = False


def ensure_stale_run_states_recovered() -> None:
    """Rewrite on-disk ``running`` run states to ``interrupted`` once per process.

    Invoked from every HTTP server entry point (``run_server``, ``app.run`` via
    :mod:`csjapp`, etc.) so desktop launchers and alternate starters match
    ``python gui.py`` behaviour.
    """
    global _stale_recovery_done
    with _stale_recovery_lock:
        if _stale_recovery_done:
            return
        storage_module.recover_stale_running_states()
        _stale_recovery_done = True


# Set by :func:`run_server` so :func:`quit_application` can stop the Werkzeug
# server and end the Flask thread (used by the desktop launcher).
_shutdown_server: Optional[Any] = None
_shutdown_lock = threading.Lock()


def _signal_all_crawls_stop() -> None:
    """Ask every active crawl to stop cooperatively (same as the per-run Stop button)."""
    with _crawls_lock:
        items = list(_active_crawls.items())
    for slug, slot in items:
        slot.stop_event.set()
        slot.status["stopping"] = True
        urls = slot.status.get("active_worker_urls") or []
        busy = [u for u in urls if u]
        logging.info(
            "Quit: signalling crawl to stop (project=%s, run=%s, %d worker URL(s) still active)",
            slug,
            slot.status.get("run_folder") or "",
            len(busy),
        )
        if busy:
            for i, u in enumerate(urls):
                if u:
                    logging.info("  Worker %d: %s", i + 1, u)


def run_server(host: str, port: int, threaded: bool = True) -> None:
    """Run the Flask app on a Werkzeug server that supports :func:`shutdown`.

    Used by ``launcher.py`` so **Quit application** can stop the server thread;
    ``app.run`` does not expose a shutdown hook.
    """
    ensure_stale_run_states_recovered()
    global _shutdown_server
    server = make_server(host, port, app, threaded=threaded)
    with _shutdown_lock:
        _shutdown_server = server
    try:
        server.serve_forever()
    finally:
        with _shutdown_lock:
            _shutdown_server = None


def _project_status(slug: str) -> Dict[str, Any]:
    """Return a snapshot of the crawl status for *slug*."""
    with _crawls_lock:
        slot = _active_crawls.get(slug)
        if slot:
            return dict(slot.status)
    return dict(_EMPTY_STATUS, project_slug=slug)


def _effective_run_status(slug: str, run_name: str, disk_status: str) -> str:
    """Map on-disk status for UI and start/resume when the crawl thread is gone.

    If ``_state.json`` still says *running* after an unclean exit but no live
    crawl owns this run folder, treat as *interrupted* so the monitor and
    runs list show **Resume** instead of a misleading **Idle** / **Running**.
    """
    if disk_status != "running":
        return disk_status
    with _crawls_lock:
        slot = _active_crawls.get(slug)
        live_same = (
            slot
            and slot.status.get("running")
            and slot.status.get("run_folder") == run_name
        )
    return "running" if live_same else "interrupted"


# ── In-memory log buffer ─────────────────────────────────────────────────

_log_buffer: deque[Dict[str, str]] = deque(maxlen=2000)


class _BufferHandler(logging.Handler):
    """Appends formatted log records to the in-memory ring buffer for the ``/api/logs`` SSE stream."""

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


# ── Crawl runner ──────────────────────────────────────────────────────────

def _run_crawl(
    slot: CrawlSlot,
    project_slug: str,
    run_folder: Optional[str] = None,
    run_name: Optional[str] = None,
    resume: bool = False,
    continue_from_run: Optional[str] = None,
) -> None:
    """Execute a crawl inside a worker thread.

    This is the target function for the daemon thread created by
    :func:`_start_crawl_thread`.  It delegates to :func:`scraper.crawl`,
    forwarding two callbacks (``on_progress`` and ``on_phase``) that mutate
    ``slot.status`` so the ``/api/progress`` SSE stream can relay live
    updates to the browser.

    On completion (normal or exception), the slot is removed from
    ``_active_crawls`` under the lock so the project becomes available
    for a new crawl.

    Args:
        slot: Pre-allocated crawl slot (thread, stop_event, status, cfg, ctx).
        project_slug: Identifies the project; used as the ``_active_crawls`` key.
        run_folder: Existing ``run_*`` directory name, or ``None`` for a new run.
        run_name: Optional human-friendly label forwarded to :func:`scraper.crawl`.
        resume: If ``True``, resume an interrupted crawl rather than starting fresh.
    """
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
        # Mirror phase updates into the global log buffer (Monitor → Logs pane).
        label = phase.replace("_", " ")
        logging.info("[crawl:%s] %s", label, detail)

    def on_worker_urls(urls: List[str]) -> None:
        """Parallel/sequential worker slot → URL currently being fetched (\"\" = idle)."""
        slot.status["active_worker_urls"] = list(urls)
        slot.status["draining_workers"] = len([u for u in urls if u])

    try:
        logging.info(
            "Crawl thread started (project=%s, run=%s, resume=%s)",
            project_slug,
            run_folder or "(new)",
            resume,
        )
        visited_seed = None
        seed_urls_kw: Optional[List[str]] = None
        cfg_live = slot.cfg
        if (
            not resume
            and getattr(slot.cfg, "REFETCH_MODE", False)
            and getattr(slot.cfg, "REFETCH_SOURCE_RUN", "")
        ):
            runs_root = storage_module.get_project_runs_dir(project_slug)
            src = slot.cfg.REFETCH_SOURCE_RUN
            source_dir = os.path.join(runs_root, src)
            cols = getattr(slot.cfg, "REFETCH_GAP_COLUMNS", None)
            gap_urls = storage_module.refetch_gap_requested_urls(
                source_dir,
                cols if cols else None,
            )
            if not gap_urls:
                logging.warning(
                    "Refetch mode: no gap URLs in %s",
                    source_dir,
                )
                slot.status["finished_message"] = (
                    "Refetch finished: no pages matched the gap-column rules in the source run."
                )
                return
            if on_phase:
                on_phase(
                    "crawling",
                    "Refetch mode — %s page URL(s) with at least one empty gap column queued"
                    % f"{len(gap_urls):,}",
                )
            cfg_live = copy.copy(slot.cfg)
            cfg_live.MAX_DEPTH = 0
            slot.ctx.cfg = cfg_live
            seed_urls_kw = gap_urls

        if run_folder and not resume and continue_from_run and not getattr(
            slot.cfg, "REFETCH_MODE", False,
        ):
            runs_root = storage_module.get_project_runs_dir(project_slug)
            if continue_from_run == storage_module.CONTINUE_FROM_ALL_PRIOR_RUNS:
                visited_seed = scraper.visited_keys_from_all_prior_runs(
                    runs_root, exclude_run=run_folder
                )
                logging.info(
                    "Continue-from (all prior runs): %d URL(s) will be skipped if re-queued",
                    len(visited_seed),
                )
                if on_phase and visited_seed:
                    on_phase(
                        "crawling",
                        "Continuing from all prior runs — %s URLs treated as already crawled"
                        % f"{len(visited_seed):,}",
                    )
            else:
                prior_dir = os.path.join(runs_root, continue_from_run)
                if os.path.isdir(prior_dir):
                    visited_seed = scraper.visited_keys_from_prior_run(prior_dir)
                    logging.info(
                        "Continue-from: %d URL(s) from run %s will be skipped if re-queued",
                        len(visited_seed),
                        continue_from_run,
                    )
                    if on_phase and visited_seed:
                        on_phase(
                            "crawling",
                            "Continuing from prior run — %s URLs treated as already crawled"
                            % f"{len(visited_seed):,}",
                        )

        pages, assets = scraper.crawl(
            on_progress=on_progress,
            on_phase=on_phase,
            on_worker_urls=on_worker_urls,
            should_stop=lambda: slot.stop_event.is_set(),
            run_name=run_name,
            run_folder=run_folder,
            resume=resume,
            seed_urls=seed_urls_kw,
            visited_seed=visited_seed,
            project_slug=project_slug,
            cfg=cfg_live,
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
        # Release the project slot so a new crawl can be started.
        with _crawls_lock:
            _active_crawls.pop(project_slug, None)


def _start_crawl_thread(
    project_slug: str,
    run_folder: Optional[str] = None,
    run_name: Optional[str] = None,
    resume: bool = False,
    continue_from_run: Optional[str] = None,
) -> bool:
    """Spin up a daemon thread to crawl *project_slug*.

    Uses a double-check locking pattern: the lock is acquired once to
    reject an already-running project, then again after the thread and
    slot are fully constructed, to atomically register the slot.  This
    avoids holding the lock during potentially expensive config loading.

    Args:
        project_slug: Project identifier (also the ``_active_crawls`` key).
        run_folder: Name of an existing ``run_*`` directory, or ``None``.
        run_name: Human-friendly label stored alongside the run.
        resume: If ``True``, resume from the last checkpoint.
        continue_from_run: When starting a **new** crawl (``resume`` is ``False``),
            optional prior ``run_*`` folder name, or
            ``storage.CONTINUE_FROM_ALL_PRIOR_RUNS`` to skip URLs from every other
            run in the project.

    Returns:
        ``True`` if the crawl was started, ``False`` if one was already active.
    """
    with _crawls_lock:
        if project_slug in _active_crawls:
            return False

    runs_dir = storage_module.get_project_runs_dir(project_slug)
    # Snapshot current module-level globals as the baseline configuration.
    cfg = CrawlConfig.from_module()
    if run_folder:
        run_dir = os.path.join(runs_dir, run_folder)
        saved = storage_module.load_run_config(run_dir)
        if saved:
            # Overlay the run's saved settings onto the baseline; any keys
            # absent from the saved dict fall back to the module defaults.
            cfg = CrawlConfig.from_dict(saved, base=cfg)
    if (
        continue_from_run
        and continue_from_run != storage_module.CONTINUE_FROM_ALL_PRIOR_RUNS
        and not resume
        and run_folder
    ):
        prior_dir = os.path.join(runs_dir, continue_from_run)
        cur_dir = os.path.join(runs_dir, run_folder)
        if (
            os.path.isdir(prior_dir)
            and os.path.isdir(cur_dir)
            and os.path.realpath(prior_dir) != os.path.realpath(cur_dir)
        ):
            prior_saved = storage_module.load_run_config(prior_dir)
            if prior_saved:
                cfg = CrawlConfig.from_dict(prior_saved, base=cfg)
                storage_module.save_run_config(cur_dir, cfg.to_dict())
    # Force output into the project-specific runs directory regardless of
    # what the module-level OUTPUT_DIR says.
    cfg.OUTPUT_DIR = runs_dir
    ctx = StorageContext(runs_dir, cfg)

    _workers_n = max(1, cfg.CONCURRENT_WORKERS)
    status: Dict[str, Any] = dict(
        _EMPTY_STATUS,
        running=True,
        run_folder=run_folder or "",
        project_slug=project_slug,
        concurrent_workers=_workers_n,
        active_worker_urls=[""] * _workers_n,
        draining_workers=0,
    )

    stop_event = threading.Event()
    # Thread is set to None initially and patched after creation because
    # the Thread target needs a reference to the slot (circular dependency).
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
        kwargs=dict(
            run_folder=run_folder,
            run_name=run_name,
            resume=resume,
            continue_from_run=continue_from_run,
        ),
        daemon=True,
    )
    slot.thread = t

    # Second lock acquisition: atomically register the slot only if no
    # other thread has started a crawl for this project in the meantime.
    with _crawls_lock:
        if project_slug in _active_crawls:
            return False
        _active_crawls[project_slug] = slot

    t.start()
    return True


def _resolve_continue_from(slug: str, run_name: str, form_value: str) -> Optional[str]:
    """Validate *continue_from* form value: must name another ``run_*`` folder in this project."""
    v = (form_value or "").strip()
    if not v or v == run_name:
        return None
    if v == storage_module.CONTINUE_FROM_ALL_PRIOR_RUNS:
        return v
    if not v.startswith("run_"):
        return None
    base = _runs_dir(slug)
    if v not in os.listdir(base):
        return None
    full = os.path.join(base, v)
    if not os.path.isdir(full):
        return None
    return v


def _resolve_continue_from_create(slug: str, form_value: str) -> Optional[str]:
    """Like :func:`_resolve_continue_from` but the new run folder does not exist yet."""
    v = (form_value or "").strip()
    if not v:
        return None
    if v == storage_module.CONTINUE_FROM_ALL_PRIOR_RUNS:
        return v
    if not v.startswith("run_"):
        return None
    base = _runs_dir(slug)
    if v not in os.listdir(base):
        return None
    full = os.path.join(base, v)
    if not os.path.isdir(full):
        return None
    return v


def _resolve_refetch_source_run(slug: str, form_value: str) -> Optional[str]:
    """Validate *refetch_source_run*: must name a ``run_*`` folder with a ``pages.csv``."""
    v = (form_value or "").strip()
    if not v.startswith("run_"):
        return None
    base = _runs_dir(slug)
    if v not in os.listdir(base):
        return None
    full = os.path.join(base, v)
    if not os.path.isdir(full):
        return None
    pages_csv = os.path.join(full, config.PAGES_CSV)
    if not os.path.isfile(pages_csv):
        return None
    return v


def _read_continue_from_marker(run_dir: str) -> str:
    p = os.path.join(run_dir, ".continue_from")
    if not os.path.isfile(p):
        return ""
    try:
        with open(p, encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return ""


# ── CSV / metrics helpers ────────────────────────────────────────────────

def _human_size(nbytes: float) -> str:
    """Format a byte count as a human-readable string (e.g. ``4.2 MB``)."""
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def _count_csv_rows(filepath: str) -> int:
    """Return the number of data rows in *filepath* (excluding the header), or 0 on any error.

    Delegates to ``utils.count_csv_rows``.
    """
    return utils.count_csv_rows(filepath)


def _output_csvs(run_dir: str) -> List[Dict[str, Any]]:
    """List every CSV in *run_dir* with row count and human-readable size."""
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
        elif name == "keyword_log.csv":
            groups["metadata"].append(f)
        elif name == "crawl_errors.csv":
            groups["errors"].append(f)
        else:
            groups["metadata"].append(f)
    return groups


def _read_csv_page(
    filepath: str, page: int = 1, per_page: int = 100
) -> Tuple[List[str], List[Dict[str, str]], int, int]:
    """Read a single page of rows from a CSV file without loading the whole file.

    Uses a two-pass approach: the first pass counts lines (cheap, no CSV
    parsing), the second reads only the needed slice via ``itertools.islice``.

    Returns:
        A 4-tuple of (headers, rows_on_page, total_rows, total_pages).
        All values are empty/zero when the file does not exist.
    """
    if not os.path.isfile(filepath):
        return [], [], 0, 0

    # Pass 1: count data rows (subtract 1 for header line).
    with open(filepath, "r", encoding="utf-8") as f:
        total = max(sum(1 for _ in f) - 1, 0)

    page = max(1, page)
    per_page = max(1, per_page)
    total_pages = max(1, (total + per_page - 1) // per_page)
    start = (page - 1) * per_page

    # Pass 2: read only the required slice.
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        rows = list(itertools.islice(
            itertools.islice(reader, start, None),
            per_page,
        ))
    return headers, rows, total, total_pages


def _metrics_from_pages(
    pages_rows: List[Dict[str, str]],
) -> Dict[str, Any]:
    """Aggregate counters from page rows. Returns a partial metrics dict."""
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

    return {
        "pages": len(pages_rows),
        "domains": len(domain_ctr),
        "avg_word_count": round(total_words / len(pages_rows)) if pages_rows else 0,
        "total_images": total_imgs,
        "images_missing_alt": imgs_no_alt,
        "training_pages": training,
        "domain_breakdown": sorted(domain_ctr.items(), key=lambda x: (-x[1], x[0]))[:15],
        "status_breakdown": sorted(status_ctr.items(), key=lambda x: (-x[1], x[0])),
        "content_breakdown": sorted(kind_ctr.items(), key=lambda x: (-x[1], x[0])),
        "lang_breakdown": sorted(lang_ctr.items(), key=lambda x: (-x[1], x[0])),
        "_status_ctr": status_ctr,
    }


def _metrics_from_assets(run_dir: str) -> Dict[str, Any]:
    """Count asset rows by category from per-type asset CSVs in *run_dir*."""
    asset_ctr: Counter[str] = Counter()
    for name in sorted(os.listdir(run_dir)):
        if name.startswith("assets_") and name.endswith(".csv"):
            cat = name[len("assets_"):-len(".csv")]
            count = _count_csv_rows(os.path.join(run_dir, name))
            if count > 0:
                asset_ctr[cat] = count
    return {
        "total_assets": sum(asset_ctr.values()),
        "asset_breakdown": sorted(asset_ctr.items(), key=lambda x: (-x[1], x[0])),
    }


def _metrics_from_errors(
    run_dir: str,
    status_ctr: "Counter[str]",
) -> Dict[str, Any]:
    """Count error rows by type from crawl_errors.csv in *run_dir*.

    Also folds error HTTP status codes into the provided *status_ctr* so the
    results dashboard can show a combined HTTP status breakdown.
    """
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
    return {
        "total_errors": sum(error_ctr.values()),
        "error_breakdown": sorted(error_ctr.items(), key=lambda x: (-x[1], x[0])),
    }


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

    page_metrics = _metrics_from_pages(pages_rows)
    status_ctr = page_metrics.pop("_status_ctr")
    m.update(page_metrics)
    m["has_data"] = True

    m.update(_metrics_from_assets(run_dir))
    m.update(_metrics_from_errors(run_dir, status_ctr))

    # Update status_breakdown to include error-page HTTP codes.
    m["status_breakdown"] = sorted(status_ctr.items(), key=lambda x: (-x[1], x[0]))

    m["total_links"] = _count_csv_rows(os.path.join(run_dir, config.EDGES_CSV))
    m["total_tags"] = _count_csv_rows(os.path.join(run_dir, config.TAGS_CSV))

    return m


def _runs_dir(slug: str) -> str:
    """Return the runs directory for a project without mutating globals.

    This is the thread-safe replacement for the former pattern of
    ``activate_project(slug); config.OUTPUT_DIR``.
    """
    rd = storage_module.get_project_runs_dir(slug)
    os.makedirs(rd, exist_ok=True)
    return rd


def _run_dir(slug: str, run_name: str) -> str:
    """Return the full path to a specific run folder."""
    return os.path.join(_runs_dir(slug), run_name)


def _project_overview_metrics(slug: str) -> Dict[str, Any]:
    """Aggregate metrics across all runs in a project."""
    runs_dir = storage_module.get_project_runs_dir(slug)
    m: Dict[str, Any] = {
        "total_pages": 0,
        "total_runs": 0,
        "total_assets": 0,
        "total_errors": 0,
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
        m["total_pages"] += _count_csv_rows(os.path.join(rd, config.PAGES_CSV))
        m["total_errors"] += _count_csv_rows(os.path.join(rd, config.ERRORS_CSV))
        for name in os.listdir(rd):
            if name.startswith("assets_") and name.endswith(".csv"):
                m["total_assets"] += _count_csv_rows(os.path.join(rd, name))

    return m


# ── Config form helpers ──────────────────────────────────────────────────

def _int_form(form, key: str, default: int) -> int:
    """Read an integer field from a form, falling back to *default* on blank/invalid input."""
    raw = form.get(key, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (ValueError, TypeError):
        return default


def _float_form(form, key: str, default: float) -> float:
    """Read a float field from a form, falling back to *default* on blank/invalid input."""
    raw = form.get(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except (ValueError, TypeError):
        return default


def _build_config_dict_from_form(form) -> Dict[str, Any]:
    """Translate a Flask form submission into a config dict for :class:`CrawlConfig`.

    Checkbox fields are detected by presence (``"field_name" in form``);
    text fields fall back to sensible defaults when blank.  The returned
    dict uses the same key names as :class:`config.CrawlConfig` so it can
    be persisted directly and later loaded with ``CrawlConfig.from_dict``.

    Args:
        form: ``request.form`` (a Werkzeug ``MultiDict``).

    Returns:
        Config dict ready for ``storage_module.save_run_config`` or
        ``storage_module.save_project_defaults``.
    """
    def _ensure_scheme(u: str) -> str:
        u = u.strip()
        if u and "://" not in u:
            u = "https://" + u
        return u

    seed_urls = [
        _ensure_scheme(u) for u in form.get("seed_urls", "").strip().splitlines() if u.strip()
    ]
    sitemap_urls = [
        _ensure_scheme(u) for u in form.get("sitemap_urls", "").strip().splitlines() if u.strip()
    ]
    allowed_domains = [
        d.strip()
        for d in form.get("allowed_domains", "").strip().splitlines()
        if d.strip()
    ]
    delay_min = _float_form(form, "delay_min", 3.0)
    delay_max = _float_form(form, "delay_max", 5.0)

    max_depth_raw = form.get("max_depth", "").strip()
    try:
        max_depth: Optional[int] = int(max_depth_raw) if max_depth_raw else None
    except (ValueError, TypeError):
        max_depth = None

    excluded_domains = [
        d.strip()
        for d in form.get("excluded_domains", "").strip().splitlines()
        if d.strip()
    ]
    url_exclude_patterns = [
        p.strip()
        for p in form.get("url_exclude_patterns", "").strip().splitlines()
        if p.strip()
    ]
    url_include_patterns = [
        p.strip()
        for p in form.get("url_include_patterns", "").strip().splitlines()
        if p.strip()
    ]
    keyword_log_terms = [
        t.strip()
        for t in form.get("keyword_log_terms", "").strip().splitlines()
        if t.strip()
    ]
    refetch_on = "refetch_mode" in form
    refetch_gap_columns = [
        c.strip()
        for c in form.get("refetch_gap_columns", "").strip().splitlines()
        if c.strip()
    ] if refetch_on else []

    # Domain ownership rules: "domain_suffix = label" per line
    ownership_rules = []
    for line in form.get("domain_ownership_rules", "").strip().splitlines():
        line = line.strip()
        if "=" in line:
            parts = line.split("=", 1)
            ownership_rules.append([parts[0].strip(), parts[1].strip()])

    _disc_mode = (form.get("sitemap_discovery_mode") or "refresh").strip().lower()
    if _disc_mode not in ("refresh", "reuse"):
        _disc_mode = "refresh"

    return {
        "SEED_URLS": seed_urls,
        "SITEMAP_URLS": sitemap_urls,
        "LOAD_SITEMAPS_FROM_ROBOTS": "load_sitemaps_from_robots" in form,
        "SITEMAP_DISCOVERY_MODE": _disc_mode,
        "RESPECT_ROBOTS_TXT": "respect_robots_txt" in form,
        "MAX_SITEMAP_URLS": _int_form(form, "max_sitemap_urls", 1_000_000),
        "MAX_PAGES_TO_CRAWL": _int_form(form, "max_pages", 1_000_000),
        "MAX_DEPTH": max_depth,
        "REQUEST_DELAY_SECONDS": [delay_min, delay_max],
        "REQUEST_TIMEOUT_SECONDS": _int_form(form, "request_timeout", 20),
        "HTTP_MAX_REDIRECTS": _int_form(form, "http_max_redirects", 30),
        "HTTP_VERIFY_SSL": "http_verify_ssl" in form,
        "MAX_RETRIES": _int_form(form, "max_retries", 3),
        "STATE_SAVE_INTERVAL": _int_form(form, "state_save_interval", 50),
        "WRITE_EDGES_CSV": "write_edges" in form,
        "WRITE_TAGS_CSV": "write_tags" in form,
        "ASSET_HEAD_METADATA": "asset_head" in form,
        "HEAD_TIMEOUT_SECONDS": _int_form(form, "head_timeout", 10),
        "CAPTURE_RESPONSE_HEADERS": "capture_headers" in form,
        "WRITE_SITEMAP_URLS_CSV": "write_sitemap_urls" in form,
        "WRITE_NAV_LINKS_CSV": "write_nav_links" in form,
        "WRITE_KEYWORD_LOG_CSV": "write_keyword_log" in form,
        "KEYWORD_LOG_TERMS": keyword_log_terms,
        "CHECK_OUTBOUND_LINKS": "check_outbound" in form,
        "MAX_LINK_CHECKS_PER_PAGE": _int_form(form, "max_link_checks", 50),
        "LINK_CHECK_DELAY_SECONDS": _float_form(form, "link_check_delay", 0.5),
        "LINK_CHECK_GET_FALLBACK": "link_check_get_fallback" in form,
        "CAPTURE_READABILITY": "capture_readability" in form,
        "RENDER_JAVASCRIPT": "render_javascript" in form,
        "CONCURRENT_WORKERS": _int_form(form, "concurrent_workers", 1),
        "CONTENT_DEDUP": "content_dedup" in form,
        "CHANGE_DETECTION": "change_detection" in form,
        "ALLOWED_DOMAINS": allowed_domains,
        "EXCLUDED_DOMAINS": excluded_domains,
        "URL_EXCLUDE_PATTERNS": url_exclude_patterns,
        "URL_INCLUDE_PATTERNS": url_include_patterns,
        "USER_AGENT": form.get("user_agent", config.USER_AGENT).strip(),
        "LOG_LEVEL": form.get("log_level", "INFO").upper(),
        "DOMAIN_OWNERSHIP_RULES": ownership_rules,
        "REFETCH_MODE": refetch_on,
        "REFETCH_SOURCE_RUN": (
            form.get("refetch_source_run", "").strip() if refetch_on else ""
        ),
        "REFETCH_GAP_COLUMNS": refetch_gap_columns,
    }


# ══════════════════════════════════════════════════════════════════════════
#  ROUTES: Projects
# ══════════════════════════════════════════════════════════════════════════

@app.route("/")
def projects_list():
    """``GET /`` — Render the top-level project listing page."""
    projects = storage_module.list_projects()
    for p in projects:
        lf = (p.get("latest_run_folder") or "").strip()
        ls = p.get("latest_run_status") or ""
        if lf and ls:
            p["latest_run_status"] = _effective_run_status(p["slug"], lf, ls)
    return render_template("projects.html", projects=projects)


@app.route("/projects/create", methods=["POST"])
def create_project_route():
    """``POST /projects/create`` — Create a new project from the form.

    Form fields:
        name: Required project display name; silently redirects home if blank.
        description: Optional free-text description.

    Redirects to the new project's overview page on success.
    """
    name = request.form.get("name", "").strip()
    description = request.form.get("description", "").strip()
    if not name:
        return redirect(url_for("projects_list"))
    slug = storage_module.create_project(name, description)
    return redirect(url_for("reports.reports_dashboard", slug=slug))


@app.route("/projects/<slug>/delete", methods=["POST"])
def delete_project_route(slug: str):
    """``POST /projects/<slug>/delete`` — Permanently delete a project and all its runs.

    Blocked while a crawl is running for this project to prevent deleting
    a directory that a worker thread is actively writing to.
    """
    with _crawls_lock:
        slot = _active_crawls.get(slug)
        if slot and slot.status.get("running"):
            proj = storage_module.load_project(slug)
            if proj:
                proj = dict(proj)
                proj["slug"] = slug
            return render_http_error(
                "Cannot delete a project while a crawl is running. "
                "Please stop the crawl first.",
                409,
                slug=slug,
                project=proj,
                page_title="Cannot delete project",
            )
    storage_module.delete_project(slug)
    return redirect(url_for("projects_list"))


@app.route("/p/<slug>/export")
def export_project_route(slug: str):
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    with _crawls_lock:
        slot = _active_crawls.get(slug)
        if slot and slot.status.get("running"):
            proj = dict(project)
            proj["slug"] = slug
            return render_http_error(
                "Cannot export while a crawl is running. Please stop the crawl first.",
                409,
                slug=slug,
                project=proj,
                page_title="Cannot export",
            )
    try:
        buf = storage_module.export_project(slug)
    except (FileNotFoundError, ValueError) as exc:
        proj = dict(project)
        proj["slug"] = slug
        return render_http_error(str(exc), 400, slug=slug, project=proj, page_title="Export failed")
    return Response(
        buf.getvalue(),
        mimetype="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename={slug}.zip",
        },
    )


@app.route("/projects/import", methods=["POST"])
def import_project_route():
    uploaded = request.files.get("zipfile")
    if not uploaded or not uploaded.filename:
        return redirect(url_for("projects_list"))
    if not uploaded.filename.lower().endswith(".zip"):
        return render_http_error(
            "Only .zip files are accepted. Choose a `.zip` export from Crawl Street Journal.",
            400,
            page_title="Import failed",
        )
    try:
        slug = storage_module.import_project(uploaded)
    except ValueError as exc:
        return render_http_error(str(exc), 400, page_title="Import failed")
    return redirect(url_for("reports.reports_dashboard", slug=slug))


# ══════════════════════════════════════════════════════════════════════════
#  ROUTES: Project pages
# ══════════════════════════════════════════════════════════════════════════

@app.route("/p/<slug>")
def project_overview(slug: str):
    """``GET /p/<slug>`` — Redirect to the Dashboard (reports)."""
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    return redirect(url_for("reports.reports_dashboard", slug=slug))


@app.route("/p/<slug>/defaults", methods=["GET"])
def project_defaults(slug: str):
    """``GET /p/<slug>/defaults`` — Redirect to Settings page."""
    return redirect(url_for("project_settings", slug=slug))


@app.route("/p/<slug>/settings")
def project_settings(slug: str):
    """``GET /p/<slug>/settings`` — Project settings: defaults, export, delete."""
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    project["slug"] = slug
    cfg = storage_module.load_project_defaults(slug) or storage_module.snapshot_config()
    return render_template("project_settings.html", project=project, cfg=cfg)


@app.route("/p/<slug>/settings/project", methods=["POST"])
def save_project_metadata_route(slug: str):
    """``POST /p/<slug>/settings/project`` — Update display name and description in ``_project.json``."""
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    name = (request.form.get("project_name") or "").strip()
    description = (request.form.get("project_description") or "").strip()
    try:
        storage_module.save_project_metadata(slug, name=name, description=description)
    except ValueError as e:
        return render_http_error(str(e), 400, slug=slug, page_title="Bad request")
    except FileNotFoundError:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    logging.info("Saved project metadata for %s", slug)
    return redirect(url_for("project_settings", slug=slug))


@app.route("/p/<slug>/audit")
def project_audit(slug: str):
    """``GET /p/<slug>/audit`` — Content audit findings."""
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    project["slug"] = slug
    return render_template("audit.html", project=project)


@app.route("/p/<slug>/wcag")
def project_wcag(slug: str):
    """``GET /p/<slug>/wcag`` — WCAG accessibility audit."""
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    project["slug"] = slug
    return render_template("wcag.html", project=project)


@app.route("/p/<slug>/api/wcag")
def api_wcag(slug: str):
    """``GET /p/<slug>/api/wcag`` — JSON WCAG audit report."""
    project = storage_module.load_project(slug)
    if not project:
        return jsonify({"error": "Project not found"}), 404
    import wcag_audit
    base = _runs_dir(slug)
    run_dirs = [
        os.path.join(base, n)
        for n in sorted(os.listdir(base))
        if n.startswith("run_") and os.path.isdir(os.path.join(base, n))
    ] if os.path.isdir(base) else []
    if not run_dirs:
        return jsonify({"total_pages": 0, "criteria": []})
    return jsonify(wcag_audit.run_wcag_audit(run_dirs))


@app.route("/p/<slug>/api/audit")
def api_audit(slug: str):
    """``GET /p/<slug>/api/audit`` — JSON audit report."""
    project = storage_module.load_project(slug)
    if not project:
        return jsonify({"error": "Project not found"}), 404
    import audit_data
    base = _runs_dir(slug)
    run_dirs = [
        os.path.join(base, n)
        for n in sorted(os.listdir(base))
        if n.startswith("run_") and os.path.isdir(os.path.join(base, n))
    ] if os.path.isdir(base) else []
    if not run_dirs:
        return jsonify({"summary": {"checks_run": 0, "total_findings": 0}, "checks": {}})
    return jsonify(audit_data.run_full_audit(run_dirs))


@app.route("/p/<slug>/defaults", methods=["POST"])
def save_project_defaults_route(slug: str):
    """``POST /p/<slug>/defaults`` — Persist the project-level default config.

    These defaults are applied as the baseline whenever a new run is
    created under this project (see :func:`create_run_route`).
    """
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    cfg = _build_config_dict_from_form(request.form)
    storage_module.save_project_defaults(slug, cfg)
    logging.info("Saved defaults for project %s", slug)
    return redirect(url_for("project_settings", slug=slug))


@app.route("/p/<slug>/runs")
def project_runs(slug: str):
    """``GET /p/<slug>/runs`` — List all run folders for this project.

    Side-effect: calls ``activate_project`` to point the module-level
    ``config.OUTPUT_DIR`` at this project's runs directory, which
    ``list_run_dirs`` depends on.
    """
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    project["slug"] = slug
    ctx = storage_module.StorageContext(_runs_dir(slug), config.CrawlConfig.from_module())
    run_list = ctx.list_run_dirs()
    for r in run_list:
        if r.get("name") == "_legacy":
            continue
        r["status"] = _effective_run_status(
            slug, r["name"], str(r.get("status") or "new")
        )
    cumulative_continue_available = any(
        r.get("name") != "_legacy" and int(r.get("page_count") or 0) > 0
        for r in run_list
    )
    return render_template(
        "runs.html",
        project=project,
        runs=run_list,
        status=_project_status(slug),
        continue_all_value=storage_module.CONTINUE_FROM_ALL_PRIOR_RUNS,
        cumulative_continue_available=cumulative_continue_available,
    )


@app.route("/p/<slug>/runs/create", methods=["POST"])
def create_run_route(slug: str):
    """``POST /p/<slug>/runs/create`` — Create a new timestamped run directory.

    If the project has saved defaults they are applied to the module-level
    config globals *before* the run is created, so that the initial
    ``_config.json`` snapshot written to the run folder inherits them.

    Form fields:
        run_name: Optional human-friendly label for the new run.
        continue_from: Optional ``run_*`` folder, or cumulative sentinel
            (``storage.CONTINUE_FROM_ALL_PRIOR_RUNS``) — copy config and skip URLs
            from that run or from all other runs on first start.
        refetch_gap_pages: When set, ``refetch_source_run`` must name a ``run_*``
            with ``pages.csv`` — copy that run’s config and enable refetch mode
            (ignored with ``continue_from``).
        refetch_source_run: Source ``run_*`` folder for gap scanning.
    """
    defaults = storage_module.load_project_defaults(slug) or storage_module.snapshot_config()
    cfg = config.CrawlConfig.from_dict(defaults)
    ctx = storage_module.StorageContext(_runs_dir(slug), cfg)
    name = request.form.get("run_name", "").strip() or None
    refetch_on = request.form.get("refetch_gap_pages") in ("1", "on", "yes", "true")
    if refetch_on:
        refetch_src = _resolve_refetch_source_run(
            slug, request.form.get("refetch_source_run", ""),
        )
        if not refetch_src:
            return render_http_error(
                "Refetch mode requires a source run that has a pages.csv file. "
                "Choose a run under “Source run for refetch”.",
                400,
                slug=slug,
                page_title="Invalid refetch source",
            )
        folder = ctx.create_run(name, refetch_from_folder=refetch_src)
    else:
        continue_from = _resolve_continue_from_create(
            slug, request.form.get("continue_from", ""),
        )
        folder = ctx.create_run(name, continue_from_folder=continue_from)
    return redirect(url_for("run_config", slug=slug, run_name=folder))


# ══════════════════════════════════════════════════════════════════════════
#  ROUTES: Run pages
# ══════════════════════════════════════════════════════════════════════════

@app.route("/p/<slug>/runs/<run_name>/config", methods=["GET"])
def run_config(slug: str, run_name: str):
    """``GET /p/<slug>/runs/<run_name>/config`` — Show the configuration editor for a run."""
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    project["slug"] = slug
    run_dir = _run_dir(slug, run_name)
    if not os.path.isdir(run_dir):
        return render_http_error("Run not found", 404, slug=slug, project=project, page_title="Not found")
    cfg = storage_module.load_run_config(run_dir) or storage_module.snapshot_config()
    friendly = storage_module._read_run_name(run_dir) or ""
    run_st = _effective_run_status(
        slug, run_name, storage_module.get_run_status(run_dir)
    )
    continue_from_marker = _read_continue_from_marker(run_dir)
    other_runs: List[Dict[str, str]] = []
    base = _runs_dir(slug)
    if os.path.isdir(base):
        for n in sorted(os.listdir(base), reverse=True):
            if not n.startswith("run_") or n == run_name:
                continue
            full = os.path.join(base, n)
            if not os.path.isdir(full):
                continue
            fn = storage_module._read_run_name(full) or ""
            other_runs.append({"name": n, "label": fn or n})
    show_continue_from = bool(other_runs) or (
        continue_from_marker == storage_module.CONTINUE_FROM_ALL_PRIOR_RUNS
    )
    ctx_list = storage_module.StorageContext(base, config.CrawlConfig.from_module())
    refetch_runs = [
        r for r in ctx_list.list_run_dirs()
        if r.get("name") not in ("_legacy", run_name)
        and int(r.get("page_count") or 0) > 0
    ]
    return render_template(
        "run_config.html",
        project=project, run_name=run_name,
        friendly_name=friendly, cfg=cfg, run_status=run_st,
        status=_project_status(slug),
        continue_from_marker=continue_from_marker,
        other_runs=other_runs,
        continue_all_value=storage_module.CONTINUE_FROM_ALL_PRIOR_RUNS,
        show_continue_from=show_continue_from,
        refetch_runs=refetch_runs,
        default_refetch_gap_columns=storage_module.DEFAULT_REFETCH_GAP_COLUMNS,
    )


@app.route("/p/<slug>/runs/<run_name>/config", methods=["POST"])
def save_run_config_route(slug: str, run_name: str):
    """``POST /p/<slug>/runs/<run_name>/config`` — Save run config and optional friendly name.

    Form fields:
        friendly_name: Optional display label persisted alongside the run.
        (remaining): Crawl settings parsed by :func:`_build_config_dict_from_form`.
    """
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    project["slug"] = slug
    run_dir = _run_dir(slug, run_name)
    if not os.path.isdir(run_dir):
        return render_http_error("Run not found", 404, slug=slug, project=project, page_title="Not found")

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
    """``GET /p/<slug>/runs/<run_name>/monitor`` — Live crawl monitor page.

    The page connects to the ``/api/progress/<slug>`` SSE stream via
    JavaScript to display real-time crawl metrics.
    """
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    project["slug"] = slug
    run_dir = _run_dir(slug, run_name)
    if not os.path.isdir(run_dir):
        return render_http_error("Run not found", 404, slug=slug, project=project, page_title="Not found")
    run_status = _effective_run_status(
        slug, run_name, storage_module.get_run_status(run_dir)
    )
    friendly = storage_module._read_run_name(run_dir) or ""
    # For completed/interrupted runs where no active slot exists, read the
    # true page and asset counts from the on-disk CSVs and compute elapsed
    # time from the run's _state.json timestamps so the monitor page shows
    # accurate figures rather than 0 / "—".
    pages_written = 0
    assets_written = 0
    elapsed_written = ""
    live_status = _project_status(slug)
    if live_status.get("run_folder") != run_name:
        pages_csv = os.path.join(run_dir, "pages.csv")
        pages_written = _count_csv_rows(pages_csv)
        assets_written = _metrics_from_assets(run_dir).get("total_assets", 0)
        state = storage_module.load_crawl_state(run_dir)
        if state:
            try:
                from datetime import datetime as _dt, timezone as _tz
                started = state.get("started_at", "")
                stopped = state.get("stopped_at", "")
                if started:
                    t0 = _dt.fromisoformat(started.replace("Z", "+00:00"))
                    if stopped:
                        t1 = _dt.fromisoformat(stopped.replace("Z", "+00:00"))
                    else:
                        # Crashed run: _finalise_run never wrote stopped_at.
                        # Use the _state.json mtime as a best-effort end time.
                        state_path = os.path.join(run_dir, "_state.json")
                        t1 = _dt.fromtimestamp(os.path.getmtime(state_path), tz=_tz.utc)
                    secs = int(abs((t1.replace(tzinfo=None) - t0.replace(tzinfo=None)).total_seconds()))
                    h, rem = divmod(secs, 3600)
                    m, s = divmod(rem, 60)
                    elapsed_written = f"{h:02d}:{m:02d}:{s:02d}"
            except Exception:
                pass
    return render_template(
        "run_monitor.html",
        project=project, run_name=run_name, friendly_name=friendly,
        run_status=run_status, status=live_status,
        pages_written=pages_written, assets_written=assets_written,
        elapsed_written=elapsed_written,
    )


@app.route("/p/<slug>/runs/<run_name>/start", methods=["POST"])
def start_run_route(slug: str, run_name: str):
    """``POST /p/<slug>/runs/<run_name>/start`` — Start (or auto-resume) a crawl.

    If the run's on-disk status is ``"interrupted"`` (or stale ``"running"``
    with no live crawl) the crawl resumes from its last checkpoint rather than
    starting from scratch.  Silently redirects to the monitor if a crawl is
    already running for this project.
    """
    with _crawls_lock:
        if slug in _active_crawls:
            return redirect(url_for("run_monitor", slug=slug, run_name=run_name))
    run_dir = _run_dir(slug, run_name)
    rs = _effective_run_status(
        slug, run_name, storage_module.get_run_status(run_dir)
    )
    resume = rs == "interrupted"
    continue_from = None
    if not resume:
        raw_cf = request.form.get("continue_from", "").strip()
        if raw_cf:
            continue_from = _resolve_continue_from(slug, run_name, raw_cf)
        elif _read_continue_from_marker(run_dir):
            continue_from = _resolve_continue_from(
                slug, run_name, _read_continue_from_marker(run_dir)
            )
    _start_crawl_thread(
        slug,
        run_folder=run_name,
        resume=resume,
        continue_from_run=continue_from,
    )
    return redirect(url_for("run_monitor", slug=slug, run_name=run_name))


@app.route("/p/<slug>/runs/<run_name>/resume", methods=["POST"])
def resume_run_route(slug: str, run_name: str):
    """``POST /p/<slug>/runs/<run_name>/resume`` — Explicitly resume an interrupted crawl.

    Unlike ``start_run_route``, this always passes ``resume=True``
    regardless of the on-disk run status.
    """
    with _crawls_lock:
        if slug in _active_crawls:
            return redirect(url_for("run_monitor", slug=slug, run_name=run_name))
    _start_crawl_thread(slug, run_folder=run_name, resume=True)
    return redirect(url_for("run_monitor", slug=slug, run_name=run_name))


@app.route("/p/<slug>/runs/<run_name>/stop", methods=["POST"])
def stop_run_route(slug: str, run_name: str):
    """``POST /p/<slug>/runs/<run_name>/stop`` — Signal the active crawl to stop gracefully.

    Sets the slot's ``stop_event`` so the crawler's ``should_stop``
    callback returns ``True`` on the next check.  The crawl thread will
    finish its current page and then exit.
    """
    with _crawls_lock:
        slot = _active_crawls.get(slug)
        if not slot:
            return redirect(url_for("run_monitor", slug=slug, run_name=run_name))
        slot.stop_event.set()
        slot.status["stopping"] = True
        urls = slot.status.get("active_worker_urls") or []
        busy = [u for u in urls if u]
        logging.info(
            "Stop crawl requested (project=%s, run=%s, %d worker URL(s) still active)",
            slug,
            run_name,
            len(busy),
        )
        for i, u in enumerate(urls):
            if u:
                logging.info("  Worker %d: %s", i + 1, u)
    return redirect(url_for("run_monitor", slug=slug, run_name=run_name))


@app.route("/p/<slug>/runs/<run_name>/results")
def run_results(slug: str, run_name: str):
    """``GET /p/<slug>/runs/<run_name>/results`` — Results dashboard with grouped CSVs and metrics."""
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    project["slug"] = slug
    run_dir = _run_dir(slug, run_name)
    if not os.path.isdir(run_dir):
        return render_http_error("Run not found", 404, slug=slug, project=project, page_title="Not found")
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
    """``GET /p/<slug>/runs/<run_name>/results/<filename>`` — Paginated CSV viewer.

    Query params:
        page: 1-based page number (default 1).
        per_page: Rows per page (default 100).
    """
    # Prevent path traversal: strip directory components
    filename = os.path.basename(filename)
    if not filename.endswith(".csv"):
        return render_http_error(
            "Only CSV files can be viewed here.", 404, slug=slug, page_title="Not found",
        )
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    project["slug"] = slug
    run_dir = _run_dir(slug, run_name)
    filepath = os.path.join(run_dir, filename)
    # Verify the resolved path is within the run directory
    if not os.path.realpath(filepath).startswith(os.path.realpath(run_dir)):
        return render_http_error("Not found", 404, slug=slug, project=project, page_title="Not found")
    if not os.path.isfile(filepath):
        return render_http_error(
            f"No file named “{filename}” in this run.", 404,
            slug=slug, project=project, page_title="Not found",
        )
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
    """``GET /p/<slug>/runs/<run_name>/download/<filename>`` — Download a single CSV.

    Returns HTTP 409 if the run is still active, to prevent serving
    partially-written files.
    """
    if not filename.endswith(".csv"):
        return render_http_error(
            "Only CSV files can be downloaded.", 404, slug=slug, page_title="Not found",
        )
    if _is_run_active(slug, run_name):
        project = storage_module.load_project(slug)
        if not project:
            return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
        project["slug"] = slug
        return (
            render_template(
                "download_blocked.html",
                project=project,
                run_name=run_name,
                filename=filename,
            ),
            409,
        )
    run_dir = _run_dir(slug, run_name)
    abs_dir = os.path.abspath(run_dir)
    return send_from_directory(abs_dir, filename, as_attachment=True)


@app.route("/p/<slug>/runs/<run_name>/download-all")
def run_download_all(slug: str, run_name: str):
    """``GET /p/<slug>/runs/<run_name>/download-all`` — Stream all CSVs as a single ZIP archive.

    The ZIP is built in-memory (``io.BytesIO``), so very large crawls
    may consume significant RAM.  Returns HTTP 409 while the crawl is
    still running.
    """
    if _is_run_active(slug, run_name):
        project = storage_module.load_project(slug)
        if not project:
            return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
        project["slug"] = slug
        return (
            render_template(
                "download_blocked.html",
                project=project,
                run_name=run_name,
                filename=None,
            ),
            409,
        )
    run_dir = _run_dir(slug, run_name)
    abs_dir = os.path.abspath(run_dir)
    project = storage_module.load_project(slug)
    if not project:
        return render_http_error("Project not found", 404, slug=slug, page_title="Not found")
    project["slug"] = slug
    if not os.path.isdir(abs_dir):
        return render_http_error(
            "No output directory for this run.", 404, slug=slug, project=project, page_title="Not found",
        )

    csv_names = sorted(n for n in os.listdir(abs_dir) if n.endswith(".csv"))
    if not csv_names:
        return render_http_error(
            "No CSV files to download for this run yet.", 404,
            slug=slug, project=project, page_title="Nothing to download",
        )

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
    """``POST /p/<slug>/runs/<run_name>/delete`` — Permanently delete a run directory.

    Guards against path-traversal by verifying the resolved path stays
    inside ``config.OUTPUT_DIR``, and rejects folders that do not start
    with the ``run_`` prefix.
    """
    import shutil
    project = storage_module.load_project(slug)
    proj = dict(project) if project else None
    if proj is not None:
        proj["slug"] = slug
    if not run_name.startswith("run_"):
        return render_http_error(
            "Only automated run folders (names starting with run_) can be deleted.",
            400, slug=slug, project=proj, page_title="Cannot delete",
        )
    runs_base = _runs_dir(slug)
    target = os.path.join(runs_base, run_name)
    real_base = os.path.realpath(runs_base) + os.sep
    if not os.path.realpath(target).startswith(real_base):
        return render_http_error("Invalid path", 400, slug=slug, project=proj, page_title="Cannot delete")
    if os.path.isdir(target):
        shutil.rmtree(target)
        logging.info("Deleted run: %s", run_name)
    return redirect(url_for("project_runs", slug=slug))


@app.route("/p/<slug>/runs/<run_name>/rename", methods=["POST"])
def rename_run_route(slug: str, run_name: str):
    """``POST /p/<slug>/runs/<run_name>/rename`` — Update the human-friendly label for a run.

    Form fields:
        friendly_name: New display name; an empty string clears it.
    """
    new_name = request.form.get("friendly_name", "").strip()
    run_path = _run_dir(slug, run_name)
    if os.path.isdir(run_path):
        storage_module._write_run_name(run_path, new_name)
    logging.info("Renamed run %s → %s", run_name, new_name or "(cleared)")
    return redirect(url_for("project_runs", slug=slug))


# ══════════════════════════════════════════════════════════════════════════
#  API: SSE streams
# ══════════════════════════════════════════════════════════════════════════

@app.route("/api/progress/<slug>")
def progress_stream(slug: str):
    """``GET /api/progress/<slug>`` — Server-Sent Events stream of crawl progress.

    Yields one JSON-encoded ``data:`` frame per second containing the
    current ``CrawlSlot.status`` snapshot.  The stream terminates when
    the crawl finishes (``running`` becomes ``False``), signalling the
    front-end to stop reconnecting.
    """
    # SSE generator — runs in its own thread courtesy of Flask's
    # ``threaded=True`` mode.  Each iteration snapshots the status dict
    # under the lock, then yields it as an SSE frame.
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
    """``GET /api/logs`` — Server-Sent Events stream of application log entries.

    Streams new entries from the in-memory ``_log_buffer`` ring buffer.
    Unlike ``progress_stream``, this stream never terminates on its own;
    the client is expected to close the connection when the page is left.
    A keepalive comment is sent every ~15 seconds to prevent proxy
    timeouts.
    """
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
                    # Proxies/load balancers may close idle connections without periodic activity.
                    yield ": keepalive\n\n"
                    heartbeat = 0
                time.sleep(0.5)
        except GeneratorExit:
            return

    return Response(generate(), mimetype="text/event-stream")


def _client_is_loopback() -> bool:
    """True if the request comes from this machine (desktop UI is always loopback)."""
    addr = request.remote_addr or ""
    if addr in ("127.0.0.1", "::1"):
        return True
    if addr.startswith("::ffff:") and addr.rsplit("::ffff:", 1)[-1] == "127.0.0.1":
        return True
    return False


def _quit_worker() -> None:
    """Stop the Werkzeug server, then close the pywebview window (order matters)."""
    log = logging.getLogger(__name__)
    time.sleep(0.1)
    with _shutdown_lock:
        srv = _shutdown_server
    if srv is not None:
        try:
            srv.shutdown()
        except Exception:
            log.exception("Flask server shutdown failed")
    try:
        import webview

        if webview.windows:
            webview.windows[0].destroy()
    except Exception:
        log.exception("Failed to destroy pywebview window")


@app.route("/api/quit", methods=["POST"])
def quit_application():
    """Quit the whole app: cooperative crawl stop, Flask shutdown, pywebview close.

    Only accepted from loopback addresses so a remote browser cannot POST quit.
    """
    if not _client_is_loopback():
        return jsonify({"ok": False, "error": "forbidden"}), 403

    _signal_all_crawls_stop()
    threading.Thread(target=_quit_worker, daemon=False).start()

    return jsonify({"ok": True})


@app.context_processor
def _inject_quit_button() -> Dict[str, bool]:
    """Hide Quit when the UI is not served over loopback (e.g. LAN browser)."""
    return {"show_quit_button": _client_is_loopback()}


# ── Main ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Move any flat-file data from pre-project-era layouts into the new structure.
    storage_module.migrate_legacy_data()
    bind = _gui_bind_address()
    port = int(os.environ.get("CSJ_GUI_PORT", "5001"))
    host_hint = "localhost" if bind in ("127.0.0.1", "::1") else bind
    print(f"The Crawl Street Journal: http://{host_hint}:{port}")
    if bind == "0.0.0.0":
        print(
            "Listening on all interfaces (0.0.0.0). Restrict access with a host firewall if needed.",
            file=sys.stderr,
        )
    run_server(host=bind, port=port, threaded=True)
