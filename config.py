"""
========================================================================
  THE CRAWL STREET JOURNAL — CONFIGURATION
========================================================================
  Edit this file to set seeds, allowed domains, crawl limits, and output
  paths. Run with: python main.py

  The crawler records every HTML page it successfully fetches in
  pages.csv (rich metadata). Linked files (PDF, Office, etc.) are
  written to separate assets_*.csv files. Optional: edges.csv (link graph),
  tags.csv (one row per tag), crawl_errors.csv.
========================================================================
"""

from __future__ import annotations

import copy
import os
import sys
from dataclasses import dataclass, field, fields as dc_fields
from typing import Any, Dict, Optional, Tuple, Union

# ── Platform helpers ──────────────────────────────────────────────────


def _is_android() -> bool:
    """Detect if running on Android (Briefcase / Chaquopy / Termux)."""
    return "ANDROID_DATA" in os.environ or (
        hasattr(sys, "getandroidapilevel")  # CPython built for Android
    )


# ── Path resolution ───────────────────────────────────────────────────
# When running inside a PyInstaller bundle the source tree is extracted
# to a temporary folder (sys._MEIPASS).  Read-only assets (templates)
# live there, but user data must go to a persistent writable location.
_FROZEN = getattr(sys, "frozen", False)
BUNDLE_DIR = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))

if _is_android():
    # On Android, use the app-private files directory.  Briefcase and
    # Chaquopy set ANDROID_FILES_DIR; Termux provides a home directory.
    DATA_DIR = os.environ.get(
        "ANDROID_FILES_DIR",
        os.path.join(os.path.expanduser("~"), "CrawlStreetJournal"),
    )
elif _FROZEN:
    if sys.platform == "win32":
        _appdata = os.environ.get("LOCALAPPDATA", os.path.expanduser("~"))
        DATA_DIR = os.path.join(_appdata, "CrawlStreetJournal")
    elif sys.platform == "darwin":
        DATA_DIR = os.path.join(
            os.path.expanduser("~"), "Documents", "CrawlStreetJournal"
        )
    else:
        _xdg = os.environ.get(
            "XDG_DATA_HOME", os.path.join(os.path.expanduser("~"), ".local", "share")
        )
        DATA_DIR = os.path.join(_xdg, "CrawlStreetJournal")
else:
    DATA_DIR = os.path.dirname(os.path.abspath(__file__))

os.makedirs(DATA_DIR, exist_ok=True)

# ── SEEDS ─────────────────────────────────────────────────────────────
# Starting pages for link following (must sit under ALLOWED_DOMAINS).
SEED_URLS = [
]

# Extra sitemap URLs (sitemap index or urlset). URLs discovered here are
# enqueued like seeds.
SITEMAP_URLS = [
]

# If True, fetch each seed origin's robots.txt and enqueue any Sitemap:
# lines found (in addition to SITEMAP_URLS).
LOAD_SITEMAPS_FROM_ROBOTS = True

# If True, the crawler obeys robots.txt Disallow rules. Set to False to
# ignore robots.txt restrictions (e.g. for internal auditing purposes).
RESPECT_ROBOTS_TXT = True

# Cap how many locations to read from sitemaps (per run), to bound memory.
MAX_SITEMAP_URLS = 1_000_000

# ── CRAWL LIMITS ────────────────────────────────────────────────────
MAX_PAGES_TO_CRAWL = 1_000_000
MAX_DEPTH = None  # None = unlimited; integer limits link-following depth
REQUEST_DELAY_SECONDS = (3, 5)
REQUEST_TIMEOUT_SECONDS = 20
MAX_RETRIES = 3

# Number of concurrent fetch workers. 1 = sequential (safest / most polite).
# Higher values fetch multiple pages in parallel across different domains.
# Per-domain rate limiting is still enforced regardless of worker count.
CONCURRENT_WORKERS = 1

# How often to persist _state.json during a crawl (every N pages).
STATE_SAVE_INTERVAL = 10

# Content-hash deduplication: skip pages whose visible-text hash matches
# a previously crawled page in the same run. Saves bandwidth on sites
# that serve identical content under multiple URLs.
CONTENT_DEDUP = True

# Change detection: when resuming or re-crawling, compare content hashes
# against the previous run to flag pages as changed/unchanged.
CHANGE_DETECTION = False

# ── OUTPUT / PROJECTS ────────────────────────────────────────────────
# Per-project runs are stored under PROJECTS_DIR/<slug>/runs/.
# OUTPUT_DIR is set dynamically by activate_project(); the default below
# is only used for backwards-compatible CLI invocations without --project.
PROJECTS_DIR = os.path.join(DATA_DIR, "projects")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")

PAGES_CSV = "pages.csv"
EDGES_CSV = "edges.csv"
TAGS_CSV = "tags.csv"
ERRORS_CSV = "crawl_errors.csv"
PHONE_NUMBERS_CSV = "phone_numbers.csv"

# Prefix for per-type asset files, e.g. output/assets_pdf.csv
ASSETS_CSV_PREFIX = "assets_"

# ── FEATURE TOGGLES ─────────────────────────────────────────────────
WRITE_EDGES_CSV = True
WRITE_TAGS_CSV = True
# Optional HEAD request on discovered asset links (size, content-type).
ASSET_HEAD_METADATA = True
HEAD_TIMEOUT_SECONDS = 10
# Persist Last-Modified / ETag from HTTP responses on pages.csv rows.
CAPTURE_RESPONSE_HEADERS = True
# Write sitemap_urls.csv (one row per <loc> found in sitemaps, including
# lastmod and source sitemap URL — useful for estate-size analysis even
# when MAX_PAGES_TO_CRAWL is lower than the sitemap total).
WRITE_SITEMAP_URLS_CSV = True
SITEMAP_URLS_CSV = "sitemap_urls.csv"
# Write nav_links.csv (one row per link inside <nav> / role=navigation).
WRITE_NAV_LINKS_CSV = True
NAV_LINKS_CSV = "nav_links.csv"
# After the crawl, HEAD-check outbound links to detect broken targets.
# Expensive at scale — disabled by default.
CHECK_OUTBOUND_LINKS = False
LINK_CHECKS_CSV = "link_checks.csv"
MAX_LINK_CHECKS_PER_PAGE = 50
LINK_CHECK_DELAY_SECONDS = 0.5

# ── JAVASCRIPT RENDERING (optional) ──────────────────────────────────
# When True, the crawler fetches pages via a headless Chromium browser
# (Playwright) so that client-side rendered content is available to the
# parser.  Requires: pip install playwright && playwright install chromium
RENDER_JAVASCRIPT = False

# ── DOMAIN SCOPE ─────────────────────────────────────────────────────
# A URL is allowed if its hostname equals any of these entries or is a
# subdomain of one (matched at the dot boundary).  For example,
# "example.com" matches both "example.com" and "www.example.com".
# Configure via the project defaults in the GUI, or edit directly here.
ALLOWED_DOMAINS = (
)

# Domains explicitly excluded from crawling even if they match
# ALLOWED_DOMAINS.  Same matching rules apply (exact or subdomain).
EXCLUDED_DOMAINS = []

# URL patterns to exclude — any URL containing one of these substrings
# (case-insensitive) is skipped.  Useful for excluding paths like
# /admin/, /login/, /search?, /?print=, /wp-json/, etc.
URL_EXCLUDE_PATTERNS = []

# URL patterns that must be present — if non-empty, only URLs containing
# at least one of these substrings are crawled.  Useful for restricting
# a crawl to e.g. /blog/ or /products/ paths only.
URL_INCLUDE_PATTERNS = []

# Extensions treated as non-HTML for crawling: we record the link in an
# assets_*.csv but do not fetch body as HTML.
SKIP_EXTENSIONS = (
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".odt", ".ods",
    ".zip", ".mp3", ".mp4", ".woff", ".woff2", ".ttf", ".eot",
    ".css", ".js", ".xml", ".json", ".atom", ".rss",
)

# Map file extension (lowercase, with dot) to asset CSV suffix.
# Keys must cover every SKIP_EXTENSIONS entry you want classified;
# anything else goes to "other".
ASSET_CATEGORY_BY_EXT = {
    ".pdf": "pdf",
    ".jpg": "image",
    ".jpeg": "image",
    ".png": "image",
    ".gif": "image",
    ".svg": "image",
    ".webp": "image",
    ".doc": "office",
    ".docx": "office",
    ".ppt": "office",
    ".pptx": "office",
    ".odt": "office",
    ".xls": "spreadsheet",
    ".xlsx": "spreadsheet",
    ".ods": "spreadsheet",
    ".zip": "archive",
    ".mp3": "audio",
    ".mp4": "video",
    ".woff": "font",
    ".woff2": "font",
    ".ttf": "font",
    ".eot": "font",
    ".css": "stylesheet",
    ".js": "script",
    ".xml": "xml",
    ".json": "json",
    ".atom": "xml",
    ".rss": "xml",
}

# ── DOMAIN OWNERSHIP ──────────────────────────────────────────────────
# Rules for classifying crawled domains into ownership categories
# (used in reports visualisations). Each rule is a (domain_suffix, label)
# tuple — first match wins. Populate via project defaults or edit here.
DOMAIN_OWNERSHIP_RULES = []
DOMAIN_OWNERSHIP_DEFAULT = "Uncategorised"

# ── CONTENT ANALYSIS ──────────────────────────────────────────────────
# Compute Flesch–Kincaid grade level via textstat.
CAPTURE_READABILITY = True

# URL path or title tokens that flag a page as training/events-related.
TRAINING_KEYWORDS = (
    "training",
    "course",
    "courses",
    "learning",
    "cpd",
    "workshop",
    "webinar",
    "event",
    "events",
    "conference",
    "seminar",
    "masterclass",
)

# ── LOGGING ───────────────────────────────────────────────────────────
# Python log-level name: DEBUG, INFO, WARNING, ERROR, CRITICAL.
LOG_LEVEL = "INFO"

# ── IDENTITY ──────────────────────────────────────────────────────────
USER_AGENT = (
    "CSJ/1.0 "
    "(research; public page metadata inventory; contact: configure in config)"
)


# ── Per-crawl configuration dataclass ─────────────────────────────────

@dataclass
class CrawlConfig:
    """Per-crawl configuration snapshot.

    Each concurrent crawl gets its own instance, isolating settings that
    were previously stored as mutable module-level globals.
    """
    OUTPUT_DIR: str = ""
    SEED_URLS: list = field(default_factory=list)
    SITEMAP_URLS: list = field(default_factory=list)
    LOAD_SITEMAPS_FROM_ROBOTS: bool = True
    RESPECT_ROBOTS_TXT: bool = True
    MAX_SITEMAP_URLS: int = 1_000_000
    MAX_PAGES_TO_CRAWL: int = 1_000_000
    MAX_DEPTH: Optional[int] = None
    REQUEST_DELAY_SECONDS: Union[float, Tuple[float, float]] = (3, 5)
    REQUEST_TIMEOUT_SECONDS: int = 20
    MAX_RETRIES: int = 3
    CONCURRENT_WORKERS: int = 1
    STATE_SAVE_INTERVAL: int = 10
    CONTENT_DEDUP: bool = True
    CHANGE_DETECTION: bool = False
    WRITE_EDGES_CSV: bool = True
    WRITE_TAGS_CSV: bool = True
    ASSET_HEAD_METADATA: bool = True
    HEAD_TIMEOUT_SECONDS: int = 10
    CAPTURE_RESPONSE_HEADERS: bool = True
    WRITE_SITEMAP_URLS_CSV: bool = True
    WRITE_NAV_LINKS_CSV: bool = True
    CHECK_OUTBOUND_LINKS: bool = False
    MAX_LINK_CHECKS_PER_PAGE: int = 50
    LINK_CHECK_DELAY_SECONDS: float = 0.5
    CAPTURE_READABILITY: bool = True
    RENDER_JAVASCRIPT: bool = False
    ALLOWED_DOMAINS: Union[tuple, list] = ()
    DOMAIN_OWNERSHIP_RULES: list = field(default_factory=list)
    EXCLUDED_DOMAINS: list = field(default_factory=list)
    URL_EXCLUDE_PATTERNS: list = field(default_factory=list)
    URL_INCLUDE_PATTERNS: list = field(default_factory=list)
    USER_AGENT: str = ""
    LOG_LEVEL: str = "INFO"

    @classmethod
    def from_module(cls) -> CrawlConfig:
        """Snapshot current module-level globals into a new instance.

        Iterates over every dataclass field, reading the matching attribute
        from the ``config`` module.  Lists are shallow-copied to prevent
        shared mutation between the snapshot and the live module.
        """
        import config as _cfg
        kwargs: Dict[str, Any] = {}
        for f in dc_fields(cls):
            val = getattr(_cfg, f.name, f.default)
            if isinstance(val, list):
                val = list(val)
            kwargs[f.name] = val
        return cls(**kwargs)

    @classmethod
    def from_dict(cls, d: Dict[str, Any],
                  base: Optional[CrawlConfig] = None) -> CrawlConfig:
        """Create from a JSON config dict, optionally layered on a *base*.

        Unknown keys are silently ignored.  ``ALLOWED_DOMAINS`` and
        ``REQUEST_DELAY_SECONDS`` are converted from JSON lists back to
        tuples so that downstream code can rely on a consistent type.
        """
        inst = copy.copy(base) if base else cls.from_module()
        for key, val in d.items():
            if not hasattr(inst, key):
                continue
            if key == "ALLOWED_DOMAINS" and isinstance(val, list):
                val = tuple(val)
            if key == "REQUEST_DELAY_SECONDS" and isinstance(val, list) and len(val) == 2:
                val = tuple(val)
            setattr(inst, key, val)
        return inst

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dict (matches the shape of _config.json)."""
        d: Dict[str, Any] = {}
        for f in dc_fields(self):
            if f.name == "OUTPUT_DIR":
                continue
            val = getattr(self, f.name)
            if isinstance(val, tuple):
                val = list(val)
            d[f.name] = val
        return d
