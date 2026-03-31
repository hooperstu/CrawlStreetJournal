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
MAX_SITEMAP_URLS = 1_000_000

# ── CRAWL LIMITS ────────────────────────────────────────────────────
MAX_PAGES_TO_CRAWL = 1_000_000
REQUEST_DELAY_SECONDS = (3, 5)
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
# The first two cover the core NHS estate; the rest are NHS-affiliated
# partner/supplier domains identified during the pre-crawl analysis.
ALLOWED_DOMAINS = (
    "nhs.uk",
    "nhs.net",
    # NHS-affiliated partner & supplier domains
    "beta.digitisingsocialcare.co.uk",
    "careersinpharmacy.uk",
    "curriculumlibrary.nshcs.org.uk",
    "ftp.nshcs.org.uk",
    "gettingitrightfirsttime.co.uk",
    "girft-hubtoolkit.org.uk",
    "girft-interactivepathways.org.uk",
    "gpinsomerset.com",
    "hub.seschoolofpas.org",
    "londonpaediatrics.co.uk",
    "schoolofanaesthesia.co.uk",
    "seschoolofpas.org",
    "stage.digitisingsocialcare.co.uk",
    "stokeanaesthesia.org.uk",
    "studyinghealthcare.ac.uk",
    "thcepn.com",
    "webzang.gpinsomerset.com",
    "work-learn-live-blmk.co.uk",
    "www.autismcentral.org.uk",
    "www.capitalnurselondon.co.uk",
    "www.e-lfh.org.uk",
    "www.eintegrity.org",
    "www.nhsfindyourplace.co.uk",
    "www.oxsph.org",
    "www.skillsforhealth.org.uk",
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
# Compute Flesch–Kincaid grade level via textstat (requires pip install
# textstat). Disabled by default because the dependency is optional.
CAPTURE_READABILITY = False

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

# ── IDENTITY ──────────────────────────────────────────────────────────
USER_AGENT = (
    "NHSInventoryCrawler/1.0 "
    "(research; public page metadata inventory; contact: configure in config)"
)
