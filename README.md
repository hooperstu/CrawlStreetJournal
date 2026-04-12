# The Crawl Street Journal

The Crawl Street Journal (CSJ) is a **Python crawler** that builds a **structured inventory** of public web pages across any set of domains you configure. It is designed for **analysis and visualisation**: one row per HTML page with rich metadata, plus **separate CSV files** for linked downloads (PDF, Office, images, and so on), optional link edges, optional tag detail rows, and an error log.

It does **not** filter pages by keywords. Every HTML page the crawler successfully fetches is recorded in `pages.csv`.

---

## Download (no coding required)

Pre-built desktop apps are available from the [latest release](https://github.com/hooperstu/CrawlStreetJournal/releases/latest). Download the file for your operating system, extract it, and double-click to run — no Python installation needed.

| Platform | File | Notes |
|----------|------|-------|
| **macOS** | `The-Crawl-Street-Journal-macOS.zip` | Extract, then right-click the `.app` and choose **Open** on first launch (Gatekeeper prompt). |
| **Windows** | `The-Crawl-Street-Journal-Windows.zip` | Extract the folder, then run `The Crawl Street Journal.exe`. You may need to click **More info → Run anyway** on the SmartScreen prompt. |
| **Linux** | `The-Crawl-Street-Journal-Linux.tar.gz` | Extract with `tar -xzf`, then run the `The Crawl Street Journal` executable. |

The app opens in a **native desktop window** (via `pywebview`). If the native window backend is not available on your system, it falls back to opening your default browser automatically.

### Android

An Android APK can be built using [BeeWare Briefcase](https://beeware.org/). The APK runs the full crawl engine and Flask GUI inside the device's browser — the same interface as the desktop version.

```bash
pip install briefcase
briefcase create android
briefcase build android
briefcase run android          # launch in emulator
briefcase package android      # produce distributable APK
```

**Notes:**

- The `lxml` C extension is excluded from the Android build; the parser falls back to Python's built-in `html.parser` automatically.
- `pywebview` is excluded — the app opens in the device's default browser (typically Chrome).
- The APK requires **Internet** and **Access Network State** permissions (configured in `pyproject.toml`).
- Crawls should be run with the app in the foreground; Android may terminate background processes.
- Optional Playwright-based JS rendering (`render.py`) is not supported on Android.

### Progressive Web App (PWA)

The web GUI also supports **Progressive Web App** installation. When you visit the GUI from Chrome on Android (or any modern browser), you can tap **"Add to Home Screen"** to install it as an app-like shortcut. This works whether the server is running locally (via the APK or [Termux](https://termux.dev/)) or on a remote machine.

### Termux (advanced)

For users comfortable with the terminal, CSJ runs directly on Android via [Termux](https://termux.dev/) with no code changes:

```bash
pkg install python
pip install -r requirements.txt
python3 gui.py                 # open http://localhost:5001 in your browser
```

---

## What this tool is for

- Mapping **how much content** exists and **where** it lives (by domain, path hints, content kind).
- Feeding **spreadsheets, BI tools, or scripts** with consistent columns across different CMS platforms and templates.
- Listing **file assets** discovered in-page (PDFs, documents, media) in **type-specific** CSVs for distribution charts.
- Running **content audits** (duplicate content, thin pages, broken links, redirect chains) and **WCAG 2.1 accessibility assessments** against crawl data.
- Optional **link graph** data (`edges.csv`) and **tag-level** data (`tags.csv`) for deeper breakdowns.
- **Dashboard reports** — indexability (noindex / robots.txt blocks), competitor-style signals from crawl data (tags, schema commerce, cross-domain edges), on-site performance audit, technical performance (fetch time, viewport, assets), and crawl-proxy “metrics” — with optional **full list** JSON (`?full=1`) and **ZIP CSV exports** for spreadsheets.

---

## How it works

1. **Seeds** — You list starting URLs in `SEED_URLS`. Only hosts that match `ALLOWED_DOMAINS` are used.
2. **Sitemaps** — URLs listed under `SITEMAP_URLS` are fetched as sitemap XML (index or urlset). Each location is handled like a seed: **HTML URLs** are queued for crawling; **file URLs** (by extension) are written only to the relevant `assets_*.csv`.
3. **Optional robots sitemaps** — If `LOAD_SITEMAPS_FROM_ROBOTS` is `True`, the crawler reads `Sitemap:` lines from each seed site's `robots.txt` and expands those sitemaps too (subject to `MAX_SITEMAP_URLS` per expansion).
4. **URL normalisation** — Every URL is canonicalised before it enters the queue or the visited set: fragments stripped, trailing slashes removed, scheme normalised to HTTPS, default ports (`:80`, `:443`) dropped, and query parameters sorted. This prevents the same page being fetched twice under cosmetically different URLs.
5. **Crawl** — For each queued HTML URL the crawler checks `robots.txt`, **GET**s the page (with delay between requests), checks the response is HTML, then parses the document and writes one row to `pages.csv`.
6. **Link discovery** — Same-domain links are followed. Links that look like **downloads** (configured extensions) are **not** crawled as HTML; they are appended to **`assets_<category>.csv`**. Optional **HTTP HEAD** on those URLs can fill in content type and size.
7. **Outputs** — Each run **creates a fresh** set of CSVs under `OUTPUT_DIR` (previous files in that folder for those names are replaced). All CSV output uses `QUOTE_ALL` quoting and sanitises field values (stripping null bytes, truncating extreme lengths) so that special characters in scraped data never corrupt the output.

---

## Web GUI

The primary interface is a **browser-based GUI** served by Flask on port **5001**. It provides:

- **Projects** — create named projects to group related crawl runs together
- **Per-run configuration** — seeds, allowed domains, sitemaps, crawl limits, feature toggles, all editable in-browser; configuration is saved as JSON alongside the run's output
- **Live monitor** — real-time progress (pages crawled, assets found, elapsed time) and streaming log output via Server-Sent Events
- **Results viewer** — paginated in-browser CSV viewer, per-run download links, and a ZIP of all CSVs
- **Navigation** — Dashboard | Runs | Settings (with Content Audit and WCAG audit accessible from the Dashboard and Results pages)
- **Ecosystem dashboard** — interactive D3 (and HTML table) views across **~30 panels** in **eight** tab groups (Governance, Trust, Content & on-site performance, Findability, Technical performance, Technology & standards, Competitive intelligence, Provenance). Includes global filters (runs, domains, CMS, content kind, schema, coverage) and an optional **“Full list breakdowns”** mode for larger JSON payloads. Selected reports offer **Download full breakdown (ZIP of CSVs)** for offline analysis.

```bash
cd /path/to/CrawlStreetJournal
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python3 gui.py
```

Open **http://127.0.0.1:5001** or **http://localhost:5001** in your browser (the server binds to **127.0.0.1** by default). The desktop app (`launcher.py`) does this automatically.

---

## Quick start

**Requirements:** Python 3.9 or newer.

### GUI (recommended)

```bash
cd /path/to/CrawlStreetJournal
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python3 gui.py                     # opens http://localhost:5001
```

### CLI (terminal crawl)

```bash
source .venv/bin/activate
python3 main.py
```

**Stop early:** press **Ctrl+C**. The crawler finishes the current page; data already written to disk is kept.

### CLI with project scope

```bash
python3 main.py --project my-project --name "Run label"
```

Output is saved under `projects/my-project/runs/`.

### Development tools

```bash
# Full test suite (268 tests — unit, Playwright GUI, real-crawl integration)
python3 -m pytest tests/ -v

# Unit tests only (no browser E2E)
python3 -m pytest tests/ --ignore=tests/test_playwright.py --ignore=tests/test_real_crawl.py -v

# Linting
flake8 --max-line-length=120 *.py
```

`pytest` and `flake8` are not in `requirements.txt` but are installed in the `.venv` as dev extras. Test discovery defaults to `tests/` via `[tool.pytest.ini_options]` in `pyproject.toml`. Playwright tests auto-start the Flask app on **127.0.0.1:5001** unless that port is already in use (see `tests/conftest.py`).

### Long background run

Use **`run_background_crawl.py`** when you want a high page cap (default **1 000 000** HTML pages in that script, matching `config.py`) and a single log file to review later. It still respects `REQUEST_DELAY_SECONDS` and `robots.txt`.

```bash
cd /path/to/CrawlStreetJournal
source .venv/bin/activate    # if you use a venv
pip install -r requirements.txt   # once

nohup python3 -u run_background_crawl.py >> crawl_background.log 2>&1 &
echo $! > crawl_background.pid
```

- **Progress:** `tail -f crawl_background.log`
- **Stop:** `kill $(cat crawl_background.pid)` (waits until the current page finishes if you use SIGTERM; the script treats SIGTERM like Ctrl+C)
- **Results:** same `output/` CSVs as `main.py` (each run overwrites those CSVs at start — only one crawl should write to `output/` at a time)

Edit **`BACKGROUND_MAX_PAGES`** at the top of `run_background_crawl.py` if you need a higher or lower cap.

---

## Docker

A `Dockerfile` and `docker-compose.yml` are included for container-based deployment. The image is ~191 MB and runs `gui.py` directly (no desktop launcher or browser).

```bash
docker compose up            # build and start on http://localhost:5001
```

Crawl data is persisted in a named Docker volume (`csj-data`) mapped to `/app/projects` inside the container.

---

## Configuration (`config.py`)

Edit `config.py` before you run. You do not need to change Python code elsewhere for normal use.

### Seeds, scope, and crawl behaviour

| Setting | What it does |
|--------|----------------|
| `SEED_URLS` | List of starting pages. Each must be on a host that matches `ALLOWED_DOMAINS`. |
| `SITEMAP_URLS` | Sitemap index or urlset URLs. Discovered URLs are merged with seeds (de-duplicated). |
| `LOAD_SITEMAPS_FROM_ROBOTS` | If `True`, also discover sitemap URLs from each seed origin's `robots.txt`. |
| `MAX_SITEMAP_URLS` | Upper limit on how many page URLs to read from a single sitemap expansion (default 1 000 000). |
| `ALLOWED_DOMAINS` | A URL is allowed if the hostname **contains** any of these substrings. Configure via the project defaults in the GUI. |
| `EXCLUDED_DOMAINS` | Hostnames to reject even if they match `ALLOWED_DOMAINS`. Useful for skipping staging or third-party subdomains. |
| `URL_EXCLUDE_PATTERNS` | List of regex or substring patterns — URLs matching any pattern are skipped (e.g. `/admin/`, `/login/`, `?print=`). |
| `URL_INCLUDE_PATTERNS` | If non-empty, **only** URLs matching at least one pattern are crawled. Use to restrict a crawl to e.g. `/blog/` paths only. |
| `RESPECT_ROBOTS_TXT` | If `True` (default), obey robots.txt Disallow rules. Set to `False` for internal auditing purposes where robots restrictions should be bypassed. |
| `MAX_PAGES_TO_CRAWL` | Maximum number of **HTML pages** to fetch in one run (default 1 000 000). |
| `MAX_DEPTH` | Maximum link-following depth from seeds/sitemaps. `None` (default) means unlimited. |
| `REQUEST_DELAY_SECONDS` | Pause between requests — either a single number (fixed) or a `(min, max)` tuple for a random delay in that range (default `(3, 5)`). Keep at least 1 second in production to be polite. |
| `REQUEST_TIMEOUT_SECONDS` | How long to wait for a response before giving up. |
| `MAX_RETRIES` | Retries after transient network errors. |
| `CONCURRENT_WORKERS` | Number of parallel fetch workers (default 1). Per-domain rate limiting is still enforced. |
| `STATE_SAVE_INTERVAL` | How often to persist `_state.json` during a crawl, in pages (default 10). |
| `CONTENT_DEDUP` | Skip pages with identical visible text hash (default True). |
| `CHANGE_DETECTION` | Compare content hashes across runs to flag changes (default False). |

### Output and optional features

| Setting | What it does |
|--------|----------------|
| `OUTPUT_DIR` | Directory for all CSV output (created if missing). |
| `PAGES_CSV`, `EDGES_CSV`, `TAGS_CSV`, `ERRORS_CSV`, `PHONE_NUMBERS_CSV` | Filenames for those outputs (under `OUTPUT_DIR`). |
| `ASSETS_CSV_PREFIX` | Prefix for asset files, e.g. `assets_` → `assets_pdf.csv`. |
| `WRITE_EDGES_CSV` | If `True`, writes `edges.csv` (one row per hyperlink discovered on a crawled page). |
| `WRITE_TAGS_CSV` | If `True`, writes `tags.csv` (one row per tag/label extracted; see below). |
| `ASSET_HEAD_METADATA` | If `True`, sends **HEAD** requests for discovered asset links to capture `Content-Type` and `Content-Length` where supported. |
| `HEAD_TIMEOUT_SECONDS` | Timeout for those HEAD requests. |
| `CAPTURE_RESPONSE_HEADERS` | If `True`, persists `Last-Modified` and `ETag` from HTTP responses on each `pages.csv` row. |
| `WRITE_SITEMAP_URLS_CSV` | If `True`, writes `sitemap_urls.csv` — one row per `<loc>` discovered in sitemaps (with `<lastmod>` and source sitemap URL). Useful for estate-size analysis even when only a subset of URLs are actually crawled. |
| `WRITE_NAV_LINKS_CSV` | If `True`, writes `nav_links.csv` — one row per distinct link inside `<nav>` or `[role=navigation]` elements. |
| `WRITE_KEYWORD_LOG_CSV` | If `True`, writes `keyword_log.csv` — rows when configured terms appear in visible text (see `KEYWORD_LOG_TERMS`). |
| `KEYWORD_LOG_TERMS` | List of substrings to match for optional keyword logging. |
| `CHECK_OUTBOUND_LINKS` | If `True`, HEAD-checks outbound link targets per page and writes `link_checks.csv`. **Expensive** at scale — disabled by default. |
| `MAX_LINK_CHECKS_PER_PAGE` | Cap on outbound links checked per page (default 50). |
| `LINK_CHECK_DELAY_SECONDS` | Delay between HEAD checks (default 0.5s). |

### Content analysis

| Setting | What it does |
|--------|----------------|
| `CAPTURE_READABILITY` | Computes Flesch–Kincaid grade level per page. Enabled by default. |
| `TRAINING_KEYWORDS` | Tuple of tokens used to flag training/events content in the URL, title, or H1. |

### Downloads and file types

| Setting | What it does |
|--------|----------------|
| `SKIP_EXTENSIONS` | Path endings treated as **non-HTML**: not crawled as pages, but recorded as assets. |
| `ASSET_CATEGORY_BY_EXT` | Maps each extension to a **category** (and thus to `assets_<category>.csv`). Extensions not listed here fall back to **`assets_other.csv`**. |

### Domain ownership

| Setting | What it does |
|--------|----------------|
| `DOMAIN_OWNERSHIP_RULES` | List of `(domain_suffix, label)` pairs for classifying crawled domains into ownership categories in the reports dashboard. First match wins. |
| `DOMAIN_OWNERSHIP_DEFAULT` | Label applied to domains that do not match any rule (default `"Uncategorised"`). |

### Identity and logging

| Setting | What it does |
|--------|----------------|
| `USER_AGENT` | Identifies the client to web servers; customise with a contact or project URL if appropriate. |
| `LOG_LEVEL` | Python log-level name: `DEBUG`, `INFO` (default), `WARNING`, `ERROR`, `CRITICAL`. |

---

## Output files

All paths are relative to **`OUTPUT_DIR`** (default `output/`).

### CSV safety

All CSV output is written with `quoting=csv.QUOTE_ALL` — every field is wrapped in double quotes regardless of content. Before writing, each field value is sanitised:

- **Null bytes** (`\x00`) are stripped (these crash Python's csv module).
- **`None` values** are coerced to empty strings.
- **Fields longer than 32 000 characters** are truncated (prevents memory issues in downstream tooling such as Excel).

If a single row fails to write for any reason, the error is logged and the crawl continues.

### `pages.csv` — one row per HTML page

| Column | Meaning |
|--------|---------|
| `requested_url` | URL taken from the queue before redirects. |
| `final_url` | URL after HTTP redirects. |
| `domain` | Hostname of `final_url`. |
| `http_status` | HTTP status code of the successful response. |
| `content_type` | Response `Content-Type` (media type, without parameters). |
| `title` | **Page title** for display: document `<title>` text (including nested markup), else `og:title`, else first `h1`. |
| `meta_description` | `meta name="description"`, or `og:description`, or `twitter:description`, or first substantial `<p>` in the main content area as a final fallback. Case-insensitive meta tag matching handles sites that emit e.g. `NAME="DESCRIPTION"`. |
| `lang` | `html[lang]` if present. |
| `canonical_url` | `link[rel=canonical]` if present. |
| `og_title`, `og_type`, `og_description` | Open Graph meta where present. |
| `twitter_card` | `twitter:card` meta where present. |
| `json_ld_types` | `@type` values found in `application/ld+json` blocks (pipe-separated). |
| `tags_all` | All extracted tags/labels merged (pipe-separated); see **Tags and labels** below. |
| `url_content_hint` | Heuristic labels from the URL path (e.g. `blog_path`, `news_path`, `careers_path`, `guidance_path`, `statistics_path`). |
| `content_kind_guess` | Coarse classification (e.g. blog, news, service, guidance, webpage) from URL hint + JSON-LD + `og:type` + breadcrumb trail + body CSS classes (covers CMS-specific patterns for SilverStripe, Drupal, WordPress, etc.). |
| `h1_joined` | Up to five `h1` texts, joined with ` \| `. |
| `word_count` | Approximate word count of visible text (scripts/styles stripped). |
| `http_last_modified` | `Last-Modified` response header (when `CAPTURE_RESPONSE_HEADERS` is enabled). |
| `etag` | `ETag` response header (when `CAPTURE_RESPONSE_HEADERS` is enabled). |
| `sitemap_lastmod` | `<lastmod>` value from the sitemap entry that discovered this URL (empty for link-discovered pages). |
| `referrer_sitemap_url` | Which sitemap XML file this URL was found in. |
| `heading_outline` | Pipe-separated H2–H6 outline, e.g. `H2:About us\|H3:Team`. |
| `date_published` | Structured published date from JSON-LD `datePublished`, `article:published_time`, first `<time datetime>`, or elements with publication-related CSS classes. |
| `date_modified` | Structured modified date from JSON-LD `dateModified`, `article:modified_time`, `og:updated_time`, or elements with modification-related CSS classes. |
| `visible_dates` | Dates extracted from visible "Last updated …" / "Review date …" / "Date published …" patterns in page text and date-classed elements (pipe-separated). |
| `link_count_internal` | Number of same-host `<a>` links on the page. |
| `link_count_external` | Number of other-host `<a>` links on the page. |
| `link_count_total` | Total `<a>` links (internal + external). |
| `img_count` | Total `<img>` elements on the page. |
| `img_missing_alt_count` | `<img>` elements with empty or missing `alt` attribute. |
| `readability_fk_grade` | Flesch–Kincaid grade level (pages with ≥ 30 words of visible text). |
| `privacy_policy_url` | First link matching common privacy/cookie policy URL patterns. |
| `analytics_signals` | Pipe-separated analytics tokens found in raw HTML (e.g. `googletagmanager.com\|dataLayer`). |
| `training_related_flag` | Pipe-separated training/events keywords matched in URL, title, or H1. |
| `nav_link_count` | Number of distinct links inside `<nav>` / `[role=navigation]` elements. |
| `referrer_url` | Page URL this one was first discovered from (`seed`, `sitemap:…`, or an HTML page URL). |
| `depth` | Number of link hops from a seed/sitemap entry (0 for direct seeds). |
| `discovered_at` | UTC timestamp when the row was written. |
| `fetch_time_ms` | Wall time in milliseconds for the successful HTML GET (including retries) — used for technical performance reporting. |
| `has_viewport_meta` | `1` or `0` — whether a `<meta name="viewport">` tag is present (basic responsive signal). |

#### WCAG signals

| Column | Meaning |
|--------|---------|
| `wcag_lang_valid` | `True` if `html[lang]` is present and non-empty. |
| `wcag_heading_order_valid` | `True` if no heading level is skipped in the document outline. |
| `wcag_title_present` | `True` if a non-empty `<title>` element is found. |
| `wcag_form_labels_pct` | Percentage of `<input>` / `<textarea>` / `<select>` elements that have an associated `<label>`. |
| `wcag_landmarks_present` | `True` if at least one ARIA landmark or HTML5 sectioning element is found. |
| `wcag_vague_link_pct` | Percentage of links whose visible text is vague (e.g. "click here", "read more"). |
| `wcag_img_alt_pct` | Percentage of `<img>` elements that have a non-empty `alt` attribute. |
| `wcag_empty_headings` | Count of heading elements (`h1`–`h6`) with no visible text. |
| `wcag_duplicate_ids` | Count of `id` attribute values that appear more than once in the document. |
| `wcag_empty_buttons` | Count of `<button>` elements with no accessible label. |
| `wcag_empty_links` | Count of `<a>` elements with no visible text or `aria-label`. |
| `wcag_tables_no_headers` | Count of `<table>` elements that lack `<th>` header cells. |
| `wcag_autocomplete_pct` | Percentage of eligible form fields that have an `autocomplete` attribute. |
| `wcag_has_search` | `True` if the page contains a search landmark (`role="search"` or `<search>`). |
| `wcag_has_nav` | `True` if the page contains a navigation landmark (`<nav>` or `role="navigation"`). |

#### Phase 4 — extended signals

| Column | Meaning |
|--------|---------|
| `author` | Author name from JSON-LD `author.name`, `meta[name=author]`, or byline patterns. |
| `publisher` | Publisher name from JSON-LD `publisher.name` or `og:site_name`. |
| `json_ld_id` | `@id` value from the primary JSON-LD block. |
| `cms_generator` | CMS or platform detected from `meta[name=generator]`, CDN paths (e.g. Shopify), or HTML markers. |
| `robots_directives` | Pipe-separated crawl/index directives from `meta[name=robots]` and `X-Robots-Tag` response header. |
| `hreflang_links` | Pipe-separated `hreflang` alternate URLs found in `<link rel="alternate">` elements. |
| `feed_urls` | Pipe-separated RSS/Atom feed URLs from `<link rel="alternate" type="application/rss+xml">` etc. |
| `pagination_next` | `href` of `<link rel="next">` (pagination). |
| `pagination_prev` | `href` of `<link rel="prev">` (pagination). |
| `breadcrumb_schema` | Pipe-separated breadcrumb item names from `BreadcrumbList` JSON-LD. |
| `microdata_types` | Pipe-separated `itemtype` values from HTML Microdata. |
| `rdfa_types` | Pipe-separated `typeof` values from RDFa markup. |
| `schema_price` | Product price from `Product` JSON-LD or microdata. |
| `schema_currency` | Currency code associated with `schema_price`. |
| `schema_availability` | Product availability from `Product` JSON-LD (e.g. `InStock`). |
| `schema_rating` | Aggregate rating value from `aggregateRating.ratingValue`. |
| `schema_review_count` | Aggregate review count from `aggregateRating.reviewCount`. |
| `schema_event_date` | Event start date from `Event` JSON-LD. |
| `schema_event_location` | Event location name from `Event` JSON-LD. |
| `schema_job_title` | Job title from `JobPosting` JSON-LD. |
| `schema_job_location` | Job location from `JobPosting` JSON-LD. |
| `schema_recipe_time` | Total time from `Recipe` JSON-LD. |
| `extraction_coverage_pct` | Percentage of Phase 1–4 fields that are non-empty — a rough indicator of metadata richness. |
| `extraction_coverage_core_pct` | Same idea as above but excludes sparse optional schema columns so the figure reflects typical SEO/trust inventory. |
| `content_hash` | SHA-256 hash (16-char prefix) of visible text. Used for content deduplication and change detection across runs. |
| `content_changed` | Change detection flag when comparing against a previous run: `changed`, `unchanged`, `new`, or empty if change detection is disabled. |

---

### `keyword_log.csv` (optional)

When `WRITE_KEYWORD_LOG_CSV` is enabled and `KEYWORD_LOG_TERMS` is non-empty, the crawler writes one row per (page, keyword) match in visible text. Use it for configurable keyword inventories alongside `tags.csv`.

### `assets_<category>.csv` — linked files (not crawled as HTML)

Examples: `assets_pdf.csv`, `assets_office.csv`, `assets_image.csv`. Columns:

| Column | Meaning |
|--------|---------|
| `referrer_page_url` | Page where the link was found (or `seed` / `sitemap:…` when the URL came only from sitemap). |
| `asset_url` | Absolute URL of the file. |
| `link_text` | Anchor text, if any. |
| `category` | Internal category (matches the filename suffix). |
| `head_content_type`, `head_content_length` | From HEAD when `ASSET_HEAD_METADATA` is enabled. |
| `discovered_at` | UTC timestamp. |

The same file may appear on many rows if it is linked from many pages.

### `edges.csv` (optional)

| Column | Meaning |
|--------|---------|
| `from_url` | Page where the link appeared. |
| `to_url` | Target URL (HTML or asset). |
| `link_text` | Anchor text. |
| `discovered_at` | UTC timestamp. |

### `tags.csv` (optional)

One row per extracted tag (good for pivot tables). Columns: `page_url`, `tag_value`, `tag_source`, `discovered_at`. The `tag_source` field tells you **which extractor** produced the value (for example `meta:keywords`, `json_ld:articleSection`, or `href:category`).

### `sitemap_urls.csv` (optional)

One row per `<loc>` discovered in any sitemap, regardless of whether the URL was actually crawled. Useful for estimating total estate size and freshness from sitemap metadata alone.

| Column | Meaning |
|--------|---------|
| `url` | The `<loc>` value from the sitemap. |
| `lastmod` | The `<lastmod>` value (W3C date string), or empty if not present. |
| `source_sitemap` | URL of the sitemap XML file this entry came from. |
| `discovered_at` | UTC timestamp. |

### `nav_links.csv` (optional)

One row per distinct link inside `<nav>` or `[role=navigation]` elements, per crawled page.

| Column | Meaning |
|--------|---------|
| `page_url` | Page the navigation was extracted from. |
| `nav_href` | Resolved URL of the navigation link. |
| `nav_text` | Visible link text (truncated to 200 characters). |
| `discovered_at` | UTC timestamp. |

### `phone_numbers.csv`

One row per `tel:` link found on crawled pages.

| Column | Meaning |
|--------|---------|
| `page_url` | Page where the telephone link was found. |
| `raw_href` | The original `href` value (including `tel:` prefix). |
| `phone_number` | The telephone number extracted from the `href` (prefix stripped). |
| `link_text` | Visible anchor text. |
| `discovered_at` | UTC timestamp. |

### `link_checks.csv` (optional)

Results of HEAD-checking outbound link targets when `CHECK_OUTBOUND_LINKS` is enabled.

| Column | Meaning |
|--------|---------|
| `from_url` | Page where the link was found. |
| `to_url` | Target URL that was checked. |
| `check_status` | HTTP status code returned by HEAD (0 on connection failure). |
| `check_final_url` | Final URL after redirects. |
| `discovered_at` | UTC timestamp. |

### `crawl_errors.csv`

Failures and intentional skips: robots disallow, fetch errors, non-HTML responses, parse errors. Columns: `url`, `error_type`, `message`, `http_status`, `discovered_at`.

---

## Tags and labels (diverse sites)

The tool does **not** rely on a single CMS. It collects tags from **several standard or common patterns** in the HTML:

- **Meta:** `meta[name]` for `news_keywords`, `keywords`, and `subject` (values split on commas, semicolons, or pipes).
- **Open Graph:** repeated `meta[property="article:tag"]` and `meta[property="article:section"]`.
- **JSON-LD:** `keywords`, `articleSection`, and `genre` inside `application/ld+json` objects (string or list), alongside `@type` values (stored separately in `json_ld_types`).
- **Links:** `a[rel~=tag]` — the visible link text is stored as a tag.
- **Category/tag hrefs:** Links whose URL contains `/category/`, `/tag/`, or `/topic/` segments (WordPress convention) — the link text is stored with source `href:category`.
- **Topic elements:** Elements with a class matching `topics` — child `<a>`, `<span>`, and `<li>` text is stored with source `class:topics`.

These are **deduplicated** by `(tag text, source)` for `tags_all` and `tags.csv`.

**Limits:** labels that exist only as bespoke layout (random CSS classes, client-rendered UI with no tags in the initial HTML) are **not** inferred. Supporting those would require **per-site rules** or a rendering pipeline, which this project does not include.

---

## Content kind classification

The `content_kind_guess` column applies a multi-signal heuristic to classify each page:

1. **URL path** — tokens like `/blog`, `/news`, `/guidance`, `/statistics`, `/events`, `/jobs` etc.
2. **JSON-LD `@type`** — `BlogPosting`, `NewsArticle`, `FAQPage`, `Event`, etc.
3. **Open Graph `og:type`** — `article`, `profile`, `product`, etc.
4. **Breadcrumb trail** — text from the page's breadcrumb navigation (e.g. "News" → `news`).
5. **Body CSS classes** — CMS-specific patterns such as SilverStripe page-type classes (`AboutOverviewPage`, `NewsItemListingPage`) and Drupal node types (`page-node-type-article`).

If none of these signals produce a match, the page is classified as `webpage`.

---

## URL deduplication

The crawler normalises every URL before adding it to the queue or checking the visited set. Normalisation includes:

- Fragment removal (`#section` stripped)
- Trailing slash removal
- Scheme normalisation (`http` → `https`)
- Default port removal (`:80`, `:443`)
- Host case normalisation
- Query parameter sorting

This prevents the same page being fetched multiple times under cosmetically different URLs, which is important at scale.

---

## Robots and politeness

- The crawler reads **`robots.txt`** per host and **does not fetch** URLs disallowed for the configured `USER_AGENT`. It parses and respects **`Crawl-delay`** directives from `robots.txt` (as a floor for per-domain delay, together with `REQUEST_DELAY_SECONDS`).
- A **delay** is applied between page fetches (`REQUEST_DELAY_SECONDS`).
- On **HTTP 429**, the crawler backs off briefly before retrying (behaviour defined in `scraper.py`).

---

## Project structure

| File | Purpose |
|------|---------|
| `launcher.py` | Desktop app entry point — finds a free port, starts Flask, opens a native `pywebview` window (falls back to browser). Includes crash log handler. |
| `gui.py` | Flask web application (port 5001): projects, run management, live monitor, results viewer, ecosystem dashboard. |
| `main.py` | CLI entry point — run from the terminal with Ctrl+C to stop. |
| `run_background_crawl.py` | Headless entry point for long background runs with file logging. |
| `run_pre_crawl_analysis.py` | Pre-crawl sampler — fetches a diverse sample of pages per domain, detects tech stack, and reports field coverage before a full crawl. |
| `config.py` | All configuration defaults and the `CrawlConfig` dataclass. |
| `FUNCTIONAL_SPEC.md` | Detailed function-level specification of all internal workflows. |
| `scraper.py` | Crawl orchestrator: priority-queue scheduling, robots.txt, rate limiting, URL normalisation, Playwright fallback. Thread-safe data structures (`_ThreadSafeSet`, `_ThreadSafeDict`) for concurrent mode. DNS cache with 5-minute TTL. |
| `parser.py` | HTML extraction: Phase 1–4 metadata, content classification, tags, WCAG signals, structured data. |
| `sitemap.py` | Sitemap XML parsing (index and urlset formats, namespace-agnostic). |
| `render.py` | Optional Playwright-based JS rendering (not installed by default; gated by `RENDER_JAVASCRIPT`). |
| `storage.py` | Filesystem persistence: CSV schemas, project/run lifecycle, config snapshots, resume state, export/import ZIP. |
| `viz_data.py` | Pure aggregation layer — reads crawl CSVs and returns JSON-serialisable structures for the dashboard (domains, graph, tags, technology, audits, indexability, competitor-style summaries, exports, etc.). |
| `viz_api.py` | Flask blueprint (`eco_bp`) — dashboard HTML route plus JSON under `/api/viz/…` and ZIP CSV exports under `/export/…`. |
| `viz_exports.py` | Builds ZIP archives of UTF-8 CSVs for full report breakdowns (used by dashboard download links). |
| `audit_data.py` | Content audit engine — 10 finding types (duplicate content, redirects, thin content, title/meta quality, orphan pages, link distribution, image accessibility, URL structure, content decay, broken links). Route: `/p/<slug>/audit`. |
| `wcag_audit.py` | WCAG 2.1 accessibility audit engine — 13 testable criteria (Level A + AA) organised by WCAG principles. Route: `/p/<slug>/wcag`. |
| `signals_audit.py` | Standalone research tool — full metadata signal inventory of a single page (not in the main crawl pipeline). |
| `utils.py` | Shared stateless helpers: JSON-LD flattening, URL domain checks, robots.txt sitemap parsing, CSV sanitisation, `read_csv`, `safe_int`, `safe_float`. |
| `Dockerfile` | Container image definition (~191 MB, runs `gui.py`, persists data via volume). |
| `docker-compose.yml` | Single-service Compose file — maps port 5001 and a named data volume. |
| `collector.spec` | PyInstaller spec for building the desktop app (macOS `.app`, Windows `.exe`, Linux binary). |
| `static/js/reports.js` | D3 v7 (and HTML tables) for the ecosystem dashboard panels and filters. |
| `templates/` | Jinja2 HTML templates for all GUI views. |
| `tests/` | **268** tests: parser, sitemap, outbound HTTP, continue-from, signals audit, viz_data, viz_api, quit API, Playwright GUI, real NHS crawl integration — see `tests/conftest.py` for shared Flask/Playwright fixtures. |
| `architecture.md` | System architecture overview (modules, dashboard, testing). |
| `requirements.txt` | Python dependencies: `requests`, `beautifulsoup4`, `lxml`, `urllib3`, `flask`, `textstat`, `tldextract`, `pywebview`. |

---

## Packaging for desktop (developer build)

The desktop app is built with **PyInstaller** using the spec file `collector.spec`.

```bash
source .venv/bin/activate
pip install pyinstaller
pyinstaller collector.spec --noconfirm
```

**What the spec does:**

- Entry point: `launcher.py`
- Bundles `templates/` and `static/` as data trees so Flask can resolve them in the frozen environment
- `hiddenimports` pulls in `gui`, the full crawl stack, Flask/Jinja/Werkzeug, `bs4`, `lxml`, `textstat`, `tldextract`, and SSL-related modules
- `tldextract` offline suffix list is bundled so the frozen binary can resolve domains without network access
- UPX compression is disabled for build reliability
- `console=False` on Windows/macOS; `console=True` on Linux so tracebacks are visible in the terminal
- macOS: produces `The Crawl Street Journal.app` with bundle ID `io.csj.crawlstreetjournal`, `.icns` icon, and ATS exception for local HTTP
- Windows: produces `The Crawl Street Journal.exe` with `.ico` icon
- `launcher.py` writes a crash log (`crash.log`) to `DATA_DIR` if the app encounters an unhandled exception
- The crawl engine's `finally` block calls `render.close()` so the Playwright browser is properly shut down on crawl completion
- Output is in `dist/The Crawl Street Journal/`

To update the app version, edit the `version` key in the `info_plist` dict inside `collector.spec`.

---

## Limitations

- **Discovery:** Link-following finds only what is linked from pages you reach. Large sites need **good seeds and sitemaps** so you do not under-represent sections that are poorly interlinked.
- **JavaScript:** By default, only the HTML from a normal GET is parsed. Content inserted solely by client-side scripts may be **missing** from titles, tags, and body text unless it is already present in the raw HTML or JSON-LD. An optional Playwright-based renderer (`render.py`) is included in the codebase for cases where JS rendering is needed; enable it by setting `RENDER_JAVASCRIPT = True` in `config.py` and running `pip install playwright && playwright install chromium`. It is **not** installed by default.
- **Legal and ethical use:** You are responsible for complying with each site's terms, **robots.txt**, applicable law (including data protection), and your organisation's policies. Use this tool only for **lawful** access to **public** information.

---

## Pre-crawl analysis

`run_pre_crawl_analysis.py` is a lightweight sampler you can run **before** a full crawl to understand a target estate:

```bash
source .venv/bin/activate
python3 run_pre_crawl_analysis.py --limit 5
```

For each domain in `TARGET_URLS` (configured in the script or via the GUI), it:

1. Fetches the homepage and detects the tech stack (WordPress, Drupal, SharePoint, Next.js, Gatsby, etc.)
2. Discovers URLs from `robots.txt`, `/sitemap.xml`, and shallow link-following
3. Fetches a diverse sample of up to 20 pages (varied by first path segment)
4. Runs the full parser pipeline on each page, including Phase 4 extraction
5. Writes per-domain `pages.csv` and `errors.csv` under `pre_crawl_analysis/<netloc>/`
6. Produces a cross-domain `summary.csv` (counts, tech, coverage) and `field_coverage.csv` (fill rate per `pages.csv` field)

Use the results to tune your seeds, allowed domains, and feature flags before committing to a large crawl.

---

## Troubleshooting

| Problem | Things to check |
|--------|------------------|
| Empty or tiny `pages.csv` | Raise `MAX_PAGES_TO_CRAWL`, add more `SEED_URLS` and `SITEMAP_URLS`, confirm hosts match `ALLOWED_DOMAINS`. |
| Many rows in `crawl_errors.csv` | Network issues, blocking, or non-HTML responses; read `message` and `error_type`. |
| Missing tags on some sites | See **Tags and labels** — the page may not expose tags in meta, OG, JSON-LD, `rel=tag`, category hrefs, or topic elements. |
| `ModuleNotFoundError` | Activate the virtual environment and run `pip install -r requirements.txt`. |
| CSV appears corrupted | Should not happen — all output uses `QUOTE_ALL` quoting and sanitises values. If it does, check `crawl_background.log` for write-error warnings. |

---

## Licence

Use in line with each site's terms of use and your own legal and ethical constraints.
