# NHS Collector

NHS Collector is a **Python crawler** that builds a **structured inventory** of public web pages across NHS-related sites (and any other hosts you allow in configuration). It is designed for **analysis and visualisation**: one row per HTML page with rich metadata, plus **separate CSV files** for linked downloads (PDF, Office, images, and so on), optional link edges, optional tag detail rows, and an error log.

It does **not** filter pages by keywords. Every HTML page the crawler successfully fetches is recorded in `pages.csv`.

---

## What this tool is for

- Mapping **how much content** exists and **where** it lives (by domain, path hints, content kind).
- Feeding **spreadsheets, BI tools, or scripts** with consistent columns across different CMS platforms and templates.
- Listing **file assets** discovered in-page (PDFs, documents, media) in **type-specific** CSVs for distribution charts.
- Optional **link graph** data (`edges.csv`) and **tag-level** data (`tags.csv`) for deeper breakdowns.

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

## Quick start

**Requirements:** Python 3.9 or newer.

```bash
cd /path/to/NHSE-Collector
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python3 main.py
```

**Stop early:** press **Ctrl+C**. The crawler finishes the current page; data already written to disk is kept.

### Long background run

Use **`run_background_crawl.py`** when you want a high page cap (default **1 000 000** HTML pages in that script, matching `config.py`) and a single log file to review later. It still respects `REQUEST_DELAY_SECONDS` and `robots.txt`.

```bash
cd /path/to/NHSE-Collector
source .venv/bin/activate    # if you use a venv
pip install -r requirements.txt   # once

nohup python3 -u run_background_crawl.py >> crawl_background.log 2>&1 &
echo $! > crawl_background.pid
```

- **Progress:** `tail -f crawl_background.log`
- **Stop:** `kill $(cat crawl_background.pid)` (waits until the current page finishes if you use SIGTERM; the script treats SIGTERM like Ctrl+C)
- **Results:** same `output/` CSVs as `main.py` (each run overwrites those CSVs at start — only one crawl should write to `output/` at a time)

Edit **`BACKGROUND_MAX_PAGES`** at the top of `run_background_crawl.py` if you need a higher or lower cap.

### Pre-crawl analysis

**`run_pre_crawl_analysis.py`** samples up to 20 pages from each target domain, detects the tech stack (WordPress, Drupal, SilverStripe, etc.), and writes per-domain results to `pre_crawl_analysis/`. It produces:

- Per-domain `pages.csv` and `errors.csv` in `pre_crawl_analysis/<domain>/`
- `summary.csv` — one row per domain with tech stack, page counts, and feature flags
- `field_coverage.csv` — fill-rate percentages for every `pages.csv` column, grouped by tech stack

This is useful for verifying that the parser captures data consistently across different CMS platforms before committing to a full crawl.

```bash
python3 run_pre_crawl_analysis.py
```

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
| `ALLOWED_DOMAINS` | A URL is allowed if the hostname **contains** any of these substrings. The default list covers `nhs.uk`, `nhs.net`, and 25 NHS-affiliated partner/supplier domains identified during the pre-crawl analysis. |
| `MAX_PAGES_TO_CRAWL` | Maximum number of **HTML pages** to fetch in one run (default 1 000 000). |
| `REQUEST_DELAY_SECONDS` | Pause between requests — either a single number (fixed) or a `(min, max)` tuple for a random delay in that range (default `(3, 5)`). Keep at least 1 second in production to be polite. |
| `REQUEST_TIMEOUT_SECONDS` | How long to wait for a response before giving up. |
| `MAX_RETRIES` | Retries after transient network errors. |

### Output and optional features

| Setting | What it does |
|--------|----------------|
| `OUTPUT_DIR` | Directory for all CSV output (created if missing). |
| `PAGES_CSV`, `EDGES_CSV`, `TAGS_CSV`, `ERRORS_CSV` | Filenames for those outputs (under `OUTPUT_DIR`). |
| `ASSETS_CSV_PREFIX` | Prefix for asset files, e.g. `assets_` → `assets_pdf.csv`. |
| `WRITE_EDGES_CSV` | If `True`, writes `edges.csv` (one row per hyperlink discovered on a crawled page). |
| `WRITE_TAGS_CSV` | If `True`, writes `tags.csv` (one row per tag/label extracted; see below). |
| `ASSET_HEAD_METADATA` | If `True`, sends **HEAD** requests for discovered asset links to capture `Content-Type` and `Content-Length` where supported. |
| `HEAD_TIMEOUT_SECONDS` | Timeout for those HEAD requests. |
| `CAPTURE_RESPONSE_HEADERS` | If `True`, persists `Last-Modified` and `ETag` from HTTP responses on each `pages.csv` row. |
| `WRITE_SITEMAP_URLS_CSV` | If `True`, writes `sitemap_urls.csv` — one row per `<loc>` discovered in sitemaps (with `<lastmod>` and source sitemap URL). Useful for estate-size analysis even when only a subset of URLs are actually crawled. |
| `WRITE_NAV_LINKS_CSV` | If `True`, writes `nav_links.csv` — one row per distinct link inside `<nav>` or `[role=navigation]` elements. |
| `CHECK_OUTBOUND_LINKS` | If `True`, HEAD-checks outbound link targets per page and writes `link_checks.csv`. **Expensive** at scale — disabled by default. |
| `MAX_LINK_CHECKS_PER_PAGE` | Cap on outbound links checked per page (default 50). |
| `LINK_CHECK_DELAY_SECONDS` | Delay between HEAD checks (default 0.5s). |

### Content analysis

| Setting | What it does |
|--------|----------------|
| `CAPTURE_READABILITY` | If `True`, computes Flesch–Kincaid grade level per page (requires `pip install textstat`). Disabled by default. |
| `TRAINING_KEYWORDS` | Tuple of tokens used to flag training/events content in the URL, title, or H1. |

### Downloads and file types

| Setting | What it does |
|--------|----------------|
| `SKIP_EXTENSIONS` | Path endings treated as **non-HTML**: not crawled as pages, but recorded as assets. |
| `ASSET_CATEGORY_BY_EXT` | Maps each extension to a **category** (and thus to `assets_<category>.csv`). Extensions not listed here fall back to **`assets_other.csv`**. |

### Identity

| Setting | What it does |
|--------|----------------|
| `USER_AGENT` | Identifies the client to web servers; customise with a contact or project URL if appropriate. |

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
| `readability_fk_grade` | Flesch–Kincaid grade level (when `CAPTURE_READABILITY` is enabled). |
| `privacy_policy_url` | First link matching common privacy/cookie policy URL patterns. |
| `analytics_signals` | Pipe-separated analytics tokens found in raw HTML (e.g. `googletagmanager.com\|dataLayer`). |
| `training_related_flag` | Pipe-separated training/events keywords matched in URL, title, or H1. |
| `nav_link_count` | Number of distinct links inside `<nav>` / `[role=navigation]` elements. |
| `referrer_url` | Page URL this one was first discovered from (`seed`, `sitemap:…`, or an HTML page URL). |
| `depth` | Number of link hops from a seed/sitemap entry (0 for direct seeds). |
| `discovered_at` | UTC timestamp when the row was written. |

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
- **Topic elements:** Elements with a class matching `topics` (england.nhs.uk convention) — child `<a>`, `<span>`, and `<li>` text is stored with source `class:topics`.

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

- The crawler reads **`robots.txt`** per host and **does not fetch** URLs disallowed for the configured `USER_AGENT`.
- A **delay** is applied between page fetches (`REQUEST_DELAY_SECONDS`).
- On **HTTP 429**, the crawler backs off briefly before retrying (behaviour defined in `scraper.py`).

---

## Project structure

| File | Purpose |
|------|---------|
| `main.py` | Interactive entry point — run from the terminal with Ctrl+C to stop. |
| `run_background_crawl.py` | Headless entry point for long background runs with file logging. |
| `run_pre_crawl_analysis.py` | Standalone pre-crawl sampler — 20 pages per domain, tech stack detection, field coverage matrix. |
| `config.py` | All configuration: seeds, domains, limits, feature toggles, file types. |
| `scraper.py` | Crawl loop: queue management, robots.txt, fetching, URL normalisation, rate limiting. |
| `parser.py` | HTML parsing: metadata extraction, tag collection, content classification, date extraction. |
| `sitemap.py` | Sitemap XML parsing (index and urlset formats). |
| `storage.py` | CSV schemas and write functions with sanitisation and `QUOTE_ALL` quoting. |
| `tests/` | Unit tests for parser and sitemap modules. |
| `requirements.txt` | Python dependencies (`requests`, `beautifulsoup4`, `lxml`). |

---

## Limitations

- **Discovery:** Link-following finds only what is linked from pages you reach. Large sites need **good seeds and sitemaps** so you do not under-represent sections that are poorly interlinked.
- **JavaScript:** Only the HTML from a normal GET is parsed. Content inserted solely by client-side scripts may be **missing** from titles, tags, and body text unless it is already present in the raw HTML or JSON-LD.
- **Legal and ethical use:** You are responsible for complying with each site's terms, **robots.txt**, applicable law (including data protection), and your organisation's policies. Use this tool only for **lawful** access to **public** information.

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
