# NHS Collector

NHS Collector is a **Python crawler** that builds a **structured inventory** of public web pages across NHS-related sites (and any other hosts you allow in configuration). It is designed for **analysis and visualisation**: one row per HTML page with metadata, plus **separate CSV files** for linked downloads (PDF, Office, images, and so on), optional link edges, optional tag detail rows, and an error log.

It does **not** filter pages by keywords. Every HTML page the crawler successfully fetches is recorded in `pages.csv`.

---

## What this tool is for

- Mapping **how much content** exists and **where** it lives (by domain, path hints, content kind).
- Feeding **spreadsheets, BI tools, or scripts** with consistent columns across different CMS and templates.
- Listing **file assets** discovered in-page (PDFs, documents, media) in **type-specific** CSVs for distribution charts.
- Optional **link graph** data (`edges.csv`) and **tag-level** data (`tags.csv`) for deeper breakdowns.

---

## How it works

1. **Seeds** — You list starting URLs in `SEED_URLS`. Only hosts that match `ALLOWED_DOMAINS` are used.
2. **Sitemaps** — URLs listed under `SITEMAP_URLS` are fetched as sitemap XML (index or urlset). Each location is handled like a seed: **HTML URLs** are queued for crawling; **file URLs** (by extension) are written only to the relevant `assets_*.csv`.
3. **Optional robots sitemaps** — If `LOAD_SITEMAPS_FROM_ROBOTS` is `True`, the crawler reads `Sitemap:` lines from each seed site’s `robots.txt` and expands those sitemaps too (subject to `MAX_SITEMAP_URLS` per expansion).
4. **Crawl** — For each queued HTML URL it checks `robots.txt`, **GET**s the page (with delay between requests), checks the response is HTML, then parses the document and writes one row to `pages.csv`.
5. **Link discovery** — Same-domain links are followed. Links that look like **downloads** (configured extensions) are **not** crawled as HTML; they are appended to **`assets_<category>.csv`**. Optional **HTTP HEAD** on those URLs can fill in content type and size.
6. **Outputs** — Each run **creates a fresh** set of CSVs under `OUTPUT_DIR` (previous files in that folder for those names are replaced).

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

### Long background run (several hours)

Use **`run_background_crawl.py`** when you want a higher page cap (default **20 000** HTML pages in that script) and a single log file to review later. It still respects `REQUEST_DELAY_SECONDS` and `robots.txt`.

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

---

## Configuration (`config.py`)

Edit `config.py` before you run. You do not need to change Python code elsewhere for normal use.

### Seeds, scope, and crawl behaviour

| Setting | What it does |
|--------|----------------|
| `SEED_URLS` | List of starting pages. Each must be on a host that matches `ALLOWED_DOMAINS`. |
| `SITEMAP_URLS` | Sitemap index or urlset URLs. Discovered URLs are merged with seeds (de-duplicated). |
| `LOAD_SITEMAPS_FROM_ROBOTS` | If `True`, also discover sitemap URLs from each seed origin’s `robots.txt`. |
| `MAX_SITEMAP_URLS` | Upper limit on how many page URLs to read from a single sitemap expansion (guards memory). |
| `ALLOWED_DOMAINS` | A URL is allowed if the hostname **contains** any of these substrings (e.g. `nhs.uk`). Add trust, ICS, GP supplier, or other domains you are permitted to crawl. |
| `MAX_PAGES_TO_CRAWL` | Maximum number of **HTML pages** to fetch in one run. |
| `REQUEST_DELAY_SECONDS` | Pause between requests. Keep this **at least 1 second** in production to be polite to remote servers. |
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

### `pages.csv` — one row per HTML page

| Column | Meaning |
|--------|---------|
| `requested_url` | URL taken from the queue before redirects. |
| `final_url` | URL after HTTP redirects. |
| `domain` | Hostname of `final_url`. |
| `http_status` | HTTP status code of the successful response. |
| `content_type` | Response `Content-Type` (media type, without parameters). |
| `title` | **Page title** for display: document `<title>` text (including nested markup), else `og:title`, else first `h1`. |
| `meta_description` | `meta name="description"`, or `og:description` if absent. |
| `lang` | `html[lang]` if present. |
| `canonical_url` | `link[rel=canonical]` if present. |
| `og_title`, `og_type`, `og_description` | Open Graph meta where present. |
| `twitter_card` | `twitter:card` meta where present. |
| `json_ld_types` | `@type` values found in `application/ld+json` blocks (pipe-separated). |
| `tags_all` | All extracted tags/labels merged (pipe-separated); see **Tags and labels** below. |
| `url_content_hint` | Heuristic labels from the URL path (e.g. blog/news segments). |
| `content_kind_guess` | Coarse classification (e.g. blog, news, webpage) from URL hint + JSON-LD + `og:type`. |
| `h1_joined` | Up to five `h1` texts, joined with ` \| `. |
| `word_count` | Approximate word count of visible text (scripts/styles stripped). |
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

One row per extracted tag (good for pivot tables). Columns: `page_url`, `tag_value`, `tag_source`, `discovered_at`. The `tag_source` field tells you **which extractor** produced the value (for example `meta:keywords` or `json_ld:keywords`).

### `crawl_errors.csv`

Failures and intentional skips: robots disallow, fetch errors, non-HTML responses, parse errors. Columns: `url`, `error_type`, `message`, `http_status`, `discovered_at`.

---

## Tags and labels (diverse sites)

The tool does **not** rely on a single CMS. It collects tags from **several standard or common patterns** in the HTML:

- **Meta:** `meta[name]` for `news_keywords`, `keywords`, and `subject` (values split on commas, semicolons, or pipes).
- **Open Graph:** repeated `meta[property="article:tag"]` and `meta[property="article:section"]`.
- **JSON-LD:** `keywords` inside `application/ld+json` objects (string or list), alongside `@type` values (stored separately in `json_ld_types`).
- **Links:** `a[rel~=tag]` — the visible link text is stored as a tag.

These are **deduplicated** by `(tag text, source)` for `tags_all` and `tags.csv`.  

**Limits:** labels that exist only as bespoke layout (random CSS classes, client-rendered UI with no tags in the initial HTML) are **not** inferred. Supporting those would require **per-site rules** or a rendering pipeline, which this project does not include.

---

## Robots and politeness

- The crawler reads **`robots.txt`** per host and **does not fetch** URLs disallowed for the configured `USER_AGENT`.
- A **delay** is applied between page fetches (`REQUEST_DELAY_SECONDS`).
- On **HTTP 429**, the crawler backs off briefly before retrying behaviour defined in `scraper.py`.

---

## Limitations

- **Discovery:** Link-following finds only what is linked from pages you reach. Large sites need **good seeds and sitemaps** so you do not under-represent sections that are poorly interlinked.
- **JavaScript:** Only the HTML from a normal GET is parsed. Content inserted solely by client-side scripts may be **missing** from titles, tags, and body text unless it is already present in the raw HTML or JSON-LD.
- **Legal and ethical use:** You are responsible for complying with each site’s terms, **robots.txt**, applicable law (including data protection), and your organisation’s policies. Use this tool only for **lawful** access to **public** information.

---

## Troubleshooting

| Problem | Things to check |
|--------|------------------|
| Empty or tiny `pages.csv` | Raise `MAX_PAGES_TO_CRAWL`, add more `SEED_URLS` and `SITEMAP_URLS`, confirm hosts match `ALLOWED_DOMAINS`. |
| Many rows in `crawl_errors.csv` | Network issues, blocking, or non-HTML responses; read `message` and `error_type`. |
| Missing tags on some sites | See **Tags and labels** — the page may not expose tags in meta, OG, JSON-LD, or `rel=tag`. |
| `ModuleNotFoundError` | Activate the virtual environment and run `pip install -r requirements.txt`. |

---

## Licence

Use in line with each site’s terms of use and your own legal and ethical constraints.
