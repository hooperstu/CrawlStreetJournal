"""
========================================================================
  COLLECTOR — CONFIGURATION
========================================================================
  Edit this file to set seeds, allowed domains, crawl limits, and output
  paths. Run with: python main.py

  The crawler records every HTML page it successfully fetches in
  pages.csv (rich metadata). Linked files (PDF, Office, etc.) are
  written to separate assets_*.csv files. Optional: edges.csv (link graph),
  tags.csv (one row per tag), crawl_errors.csv.
========================================================================
"""

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
REQUEST_DELAY_SECONDS = (3, 5)
REQUEST_TIMEOUT_SECONDS = 20
MAX_RETRIES = 1

# ── OUTPUT / PROJECTS ────────────────────────────────────────────────
# Per-project runs are stored under PROJECTS_DIR/<slug>/runs/.
# OUTPUT_DIR is set dynamically by activate_project(); the default below
# is only used for backwards-compatible CLI invocations without --project.
PROJECTS_DIR = "projects"
OUTPUT_DIR = "output"

PAGES_CSV = "pages.csv"
EDGES_CSV = "edges.csv"
TAGS_CSV = "tags.csv"
ERRORS_CSV = "crawl_errors.csv"

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

# ── DOMAIN SCOPE ─────────────────────────────────────────────────────
# A URL is allowed if its host matches any of these substrings.
# Configure via the project defaults in the GUI, or edit directly here.
ALLOWED_DOMAINS = (
)

# Extensions treated as non-HTML for crawling: we record the link in an
# assets_*.csv but do not fetch body as HTML.
SKIP_EXTENSIONS = (
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".odt", ".ods",
    ".zip", ".mp3", ".mp4", ".woff", ".woff2", ".ttf", ".eot",
    ".css", ".js", ".xml", ".json",
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
}

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
    "Collector/1.0 "
    "(research; public page metadata inventory; contact: configure in config)"
)
