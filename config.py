"""
========================================================================
  NHS COLLECTOR — CONFIGURATION
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
    "https://www.england.nhs.uk/",
    "https://digital.nhs.uk/",
]

# Extra sitemap URLs (sitemap index or urlset). URLs discovered here are
# enqueued like seeds. Add ICS, trust, or GP site sitemaps as needed.
SITEMAP_URLS = [
    "https://www.england.nhs.uk/sitemap_index.xml",
    "https://digital.nhs.uk/sitemap.xml",
]

# If True, fetch each seed origin's robots.txt and enqueue any Sitemap:
# lines found (in addition to SITEMAP_URLS).
LOAD_SITEMAPS_FROM_ROBOTS = False

# Cap how many locations to read from sitemaps (per run), to bound memory.
MAX_SITEMAP_URLS = 25000

# ── CRAWL LIMITS ────────────────────────────────────────────────────
MAX_PAGES_TO_CRAWL = 500
REQUEST_DELAY_SECONDS = 1.5
REQUEST_TIMEOUT_SECONDS = 20
MAX_RETRIES = 1

# ── OUTPUT DIRECTORY ─────────────────────────────────────────────────
# All CSVs are written under this folder (created if missing).
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

# ── DOMAIN SCOPE ─────────────────────────────────────────────────────
# A URL is allowed if its host matches any of these substrings (same rule
# as before). Add explicit GP or supplier domains as needed.
ALLOWED_DOMAINS = (
    "nhs.uk",
    "nhs.net",
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

USER_AGENT = (
    "NHSInventoryCrawler/1.0 "
    "(research; public page metadata inventory; contact: configure in config)"
)
