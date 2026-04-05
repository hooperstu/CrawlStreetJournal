# Functional Specification — The Crawl Street Journal

This document details every function within the internal workflows, organised by module. It serves as a reference for developers understanding the crawl pipeline, data flow, and feature interactions.

---

## Table of contents

1. [Crawl Pipeline](#1-crawl-pipeline)
2. [HTML Parser](#2-html-parser)
3. [Storage Layer](#3-storage-layer)
4. [Visualisation & Reporting](#4-visualisation--reporting)
5. [Audit Engines](#5-audit-engines)
6. [GUI Routes](#6-gui-routes)
7. [Configuration System](#7-configuration-system)
8. [Desktop Packaging](#8-desktop-packaging)

---

## 1. Crawl Pipeline

### Module: `scraper.py`

The crawl engine orchestrates fetching, scope enforcement, rate limiting, and output writing.

#### URL normalisation

| Function | Purpose |
|----------|---------|
| `normalise_url(url)` | Canonicalise URL for deduplication: strip fragments, trailing slashes, empty queries; normalise scheme (`http→https`), lowercase netloc, remove default ports (`:80`, `:443`), sort query parameters. |

#### URL priority scoring

| Function | Purpose |
|----------|---------|
| `_score_url(url, depth, is_seed)` | Compute priority score (lower = higher priority). Factors: seed bonus (−100), depth penalty (×10), homepage bonus (−5), low-value path penalty (+20 for `/tag/`, `/page/`, `/wp-content/`, etc.). |
| `_PriorityQueue` | Thread-safe priority queue backed by `heapq`. Items are `(score, counter, url, referrer, depth)`. Counter breaks ties in FIFO order. Methods: `push()`, `pop()`, `to_list()` (for state serialisation). |

#### Thread-safe data structures

| Class | Purpose |
|-------|---------|
| `_ThreadSafeSet` | Lock-guarded `set` wrapper for `visited` and `queued` URL sets in concurrent mode. |
| `_ThreadSafeDict` | Lock-guarded `dict` wrapper for `content_hashes` in concurrent mode. |

#### DNS caching

| Function | Purpose |
|----------|---------|
| `_cached_getaddrinfo(*args)` | Process-global DNS cache wrapping `socket.getaddrinfo`. Thread-safe via `_dns_lock`. Entries expire after a 5-minute TTL (`_DNS_TTL = 300`). Installed as `socket.getaddrinfo` on module import. |

#### Robots.txt & Crawl-delay

| Function | Purpose |
|----------|---------|
| `_origin_of(url)` | Extract `scheme://netloc` as cache key. |
| `_robots_for_url(url)` | Return cached `RobotFileParser` for the URL's origin. Fetches and parses `robots.txt` on first access. Also extracts `Crawl-delay` directive into `_crawl_delay_cache`. Thread-safe. |
| `_get_crawl_delay(url)` | Return the `Crawl-delay` value (seconds) for the URL's origin, or `None` if not specified. Triggers `_robots_for_url` to populate cache. |
| `can_fetch(url, cfg)` | Check if the URL is permitted by robots.txt. Uses `_blocked_origins` as a fast-path for fully blocked origins. |

#### Domain scope

| Function | Purpose |
|----------|---------|
| `is_allowed_domain(url, cfg)` | Check hostname against `ALLOWED_DOMAINS` at dot boundary. |
| `_is_excluded_domain(url, cfg)` | Check hostname against `EXCLUDED_DOMAINS`. |
| `_matches_url_patterns(url, patterns)` | Substring match against a list of patterns (case-insensitive). |
| `is_url_allowed(url, cfg)` | Combined scope check: allowed domain AND not excluded domain AND not matching exclude patterns AND (if include patterns set) matching at least one include pattern. Used at both seed queueing and link discovery. |

#### Rate limiting

| Function | Purpose |
|----------|---------|
| `_resolve_delay(value)` | Return a sleep duration — fixed float or random within `(min, max)` range. |
| `_per_domain_delay(hostname, base_delay)` | Compute delay with adaptive backoff: base delay + exponential penalty for domains with repeated failures (`_domain_fail_count`). Capped at 60s extra. |
| `_wait_for_domain(hostname, delay_cfg, url)` | Sleep to enforce per-domain rate limiting. Uses the **maximum** of: configured delay, adaptive backoff, and `Crawl-delay` from robots.txt. All shared-state reads/writes are thread-safe via `_global_state_lock`. |
| `_record_domain_success(hostname)` | Clear failure count for domain. |
| `_record_domain_failure(hostname)` | Increment failure count for domain (drives adaptive backoff). |

#### Fetching

| Function | Purpose |
|----------|---------|
| `fetch_page(url, cfg)` | HTTP GET with exponential backoff retries. Returns `(body, status, final_url, content_type, response_meta, error_detail)`. Captures `Last-Modified`, `ETag`, `X-Robots-Tag`, `Server`, `X-Powered-By` headers. |
| `head_asset(url, cfg)` | HTTP HEAD for asset metadata. Returns `(content_type, content_length)`. |

#### Content deduplication

Content hash dedup is performed inside `_process_one_url` when `CONTENT_DEDUP` is enabled:
1. Extract visible text via `parser_module.get_visible_text()`
2. Compute SHA-256 hash (truncated to 16 hex chars)
3. Check against `content_hashes` dict (shared across the crawl)
4. If match found: skip page, log `content_duplicate` error
5. If new: add to dict, continue processing

Hashes are persisted to `_content_hashes.json` at end of crawl via `storage.save_content_hashes()`.

#### Change detection

When `CHANGE_DETECTION` is enabled and the crawl resumes:
1. Load hashes from the previous run via `storage.load_content_hashes()`
2. During crawl, new hashes are compared against previous
3. Foundation for flagging pages as changed/unchanged across runs

#### Page processing

| Function | Purpose |
|----------|---------|
| `_process_one_url(url, referrer, depth, ...)` | Fetch and process one URL. Workflow: check visited → check robots → wait for domain → fetch → JS render fallback → check content type → content dedup → parse inventory → write page row → write tags → write nav links → extract links → write edges → write assets → write phone numbers → queue new URLs. Returns `(page_written, new_assets, final_url)`. |

#### Sitemap handling

| Function | Purpose |
|----------|---------|
| `_sitemaps_from_robots(origin, cfg)` | Fetch `robots.txt` and extract `Sitemap:` lines. |
| `collect_start_items(cfg, ctx, on_phase)` | Gather all seed + sitemap URLs into a deduplicated list. Writes sitemap URL rows. Returns `(items, sitemap_meta)`. |
| `_seed_queues(cfg, ctx, seed_urls, ...)` | Populate seed queue from seeds or sitemap discovery. Asset-only URLs are written directly to CSV, not queued. |

#### Main loop

| Function | Purpose |
|----------|---------|
| `_init_run(cfg, ctx, ...)` | Set up run directory, initialise or resume outputs, build queues, load state. Returns all loop variables. |
| `_preflight_robots_report(queue_items, cfg)` | Pre-check robots.txt for every unique origin in the queue. Logs blocked origins. |
| `_persist_state_if_needed(...)` | Save crawl state every `STATE_SAVE_INTERVAL` pages. |
| `_finalise_run(...)` | Write terminal state record (completed or interrupted). |
| `crawl(seed_urls, max_pages, delay, ...)` | Main entry point. Initialises run, executes crawl loop, handles interrupts. Sequential when `CONCURRENT_WORKERS ≤ 1`; concurrent via `ThreadPoolExecutor` otherwise. Returns `(pages_crawled, assets_from_pages)`. |

---

## 2. HTML Parser

### Module: `parser.py`

Extracts metadata from raw HTML. Organised in extraction phases.

#### Core extraction

| Function | Purpose |
|----------|---------|
| `get_visible_text(soup)` | Strip scripts/styles, return visible text. |
| `_meta_content(soup, attrs)` | Extract `content` from a `<meta>` tag matching attrs. Case-insensitive retry for ASP.NET sites. |
| `_all_meta_properties(soup, prop)` | Collect all `content` values for repeated meta properties (e.g. `article:tag`). |
| `_document_title_from_soup(soup)` | Text inside `<title>`, including nested elements. |
| `_extract_first_paragraph(soup)` | Best-effort description from first substantial `<p>` inside `<main>`/`<article>`/`#content`. |

#### JSON-LD

| Function | Purpose |
|----------|---------|
| `_collect_json_ld_nodes(obj)` | Recursively flatten JSON-LD `@graph` structures into a list of node dicts. |
| `_extract_json_ld(soup)` | Return `(types, keywords, sections)` from all JSON-LD blocks. Deduplicates. |

#### Tags & classification

| Function | Purpose |
|----------|---------|
| `_rel_tag_hrefs(soup, base_url)` | Extract tag text from `a[rel=tag]` links. |
| `_collect_all_tags(soup, base_url)` | Collect tags from meta keywords, OG, JSON-LD, `rel=tag`, category/tag/topic hrefs, `.topics` class elements. Returns `(value, source)` pairs, deduplicated. |
| `url_content_hint(url)` | Heuristic labels from URL path segments (50+ patterns: blog, news, product, recipe, FAQ, etc.). |
| `guess_content_kind(url_hint, json_ld_types, og_type, path, breadcrumb, body_classes)` | Multi-signal content classification. Priority: JSON-LD type → URL hint → breadcrumb → body CSS class → CMS patterns. 22+ content kinds. |

#### Dates

| Function | Purpose |
|----------|---------|
| `_extract_structured_dates(soup)` | `date_published` and `date_modified` from JSON-LD, OG, `<time>`, CSS class containers. |
| `_extract_visible_dates(html, soup)` | Regex extraction of dates from visible text and date-class elements. |

#### Quality signals

| Function | Purpose |
|----------|---------|
| `_extract_heading_outline(soup)` | Pipe-separated H2–H6 outline. |
| `_count_links(soup, page_url)` | Internal, external, total link counts. |
| `_count_images(soup)` | Total images and missing-alt count. |
| `_compute_readability(visible_text)` | Flesch-Kincaid grade level via textstat. |
| `_find_privacy_policy_url(soup, page_url)` | First link matching privacy/cookie policy URL patterns. |
| `_detect_analytics(html)` | Analytics tokens (Google Tag Manager, GA, dataLayer) in raw HTML. |
| `_detect_training_keywords(url, title, h1)` | Training/events keyword flags. |
| `_count_nav_links(soup)` | Distinct links inside `<nav>` or `[role=navigation]`. |

#### WCAG static checks

| Function | Purpose |
|----------|---------|
| `_assess_wcag_static(soup, lang, title)` | 15 WCAG 2.1 checks: language declaration, heading hierarchy, page title, form labels, bypass blocks, vague links, image alt, empty headings, duplicate IDs, empty buttons/links, table headers, autocomplete, search/nav presence. Returns dict with string values for CSV. |

#### Phase 4 extraction

| Function | Purpose |
|----------|---------|
| `_extract_author(soup)` | Author from meta, JSON-LD `author`, or byline CSS patterns. |
| `_extract_publisher(soup)` | Publisher from JSON-LD `publisher` or `og:site_name`. |
| `_extract_json_ld_id(soup)` | First `@id` from JSON-LD nodes. |
| `_detect_cms_generator(soup)` | CMS from `meta[name=generator]` or HTML signals (Shopify CDN, Wix, Squarespace, Ghost, Webflow, HubSpot, AEM). |
| `_extract_robots_directives(soup, response_meta)` | Combine `meta[name=robots]` and `X-Robots-Tag` header. |
| `_extract_hreflang_links(soup, base_url)` | `lang=url` pairs from `link[rel=alternate][hreflang]`. |
| `_extract_feed_urls(soup, base_url)` | RSS/Atom feed URLs. |
| `_extract_pagination(soup, base_url)` | `next` and `prev` from `link[rel]`. |
| `_extract_breadcrumb_schema(soup)` | Breadcrumb items from `BreadcrumbList` JSON-LD. |
| `_extract_microdata(soup)` | Top-level Microdata `itemscope` types. |
| `_extract_rdfa_types(soup)` | RDFa `typeof` values. |
| `_extract_schema_specific(soup)` | Domain-specific fields: Product (price, currency, availability, rating, reviews), Event (date, location), JobPosting (title, location), Recipe (time). |
| `_compute_extraction_coverage(page_row)` | Percentage of non-empty content fields. |

#### Link extraction

| Function | Purpose |
|----------|---------|
| `extract_classified_links(html, base_url, discovered_at, allowed_domains=None)` | Separate `<a>` links into HTML URLs (to crawl), asset rows, edge rows, and phone number rows (`tel:` hrefs). Accepts explicit `allowed_domains` for thread safety. Returns `(html_urls, asset_rows, edge_rows, phone_rows)`. |
| `asset_category_for_url(url)` | Return the asset category string for a URL based on its file extension, or `None` if the URL should be crawled as HTML. |
| `extract_inline_assets(html, base_url, discovered_at)` | Extract assets from `<img>`, `<link>`, `<script>`, `<video>`, `<audio>`, `<source>`. |
| `extract_nav_links(soup, page_url, discovered_at)` | Distinct nav links for `nav_links.csv`. |

#### Main entry point

| Function | Purpose |
|----------|---------|
| `build_page_inventory_row(html, requested_url, final_url, ...)` | Orchestrates all extraction. Returns `(page_row_dict, tag_rows_list)`. Calls every function above in sequence. |

---

## 3. Storage Layer

### Module: `storage.py`

Filesystem persistence: CSV writing, project/run lifecycle, config snapshots, resume state.

#### CSV writing

| Function | Purpose |
|----------|---------|
| `_sanitise(value)` | Strip null bytes, coerce None to empty string, truncate >32K chars. |
| `append_row(path, fieldnames, row)` | Append one sanitised row to a CSV file with `QUOTE_ALL`. Thread-safe via `_csv_lock` when using `StorageContext`. |
| `write_page(row)` | Write to `pages.csv`. |
| `write_asset(row, category)` | Write to `assets_<category>.csv`. |
| `write_edge(row)` | Write to `edges.csv` (if enabled). |
| `write_tag_row(row)` | Write to `tags.csv` (if enabled). |
| `write_error(row)` | Write to `crawl_errors.csv`. |
| `write_sitemap_url(row)` | Write to `sitemap_urls.csv`. |
| `write_nav_link(row)` | Write to `nav_links.csv`. |
| `write_link_check(row)` | Write to `link_checks.csv`. |
| `write_phone_number(row)` | Write to `phone_numbers.csv`. |

#### Project lifecycle

| Function | Purpose |
|----------|---------|
| `create_project(name, description)` | Create project directory with `_project.json` and `_defaults.json`. Returns slug. |
| `list_projects()` | Return metadata for every project, sorted by creation date. |
| `activate_project(slug)` | Point `config.OUTPUT_DIR` at this project's runs directory. Returns `StorageContext`. |
| `delete_project(slug)` | Remove project directory with safety check. |
| `export_project(slug)` | Create ZIP of project directory. |
| `import_project(uploaded)` | Extract ZIP into projects directory. |

#### Run lifecycle

| Function | Purpose |
|----------|---------|
| `create_run(run_name)` | Create timestamped run folder with config snapshot and initial state. |
| `initialise_outputs(run_folder, run_name)` | Create CSV files with headers. |
| `resume_outputs(run_folder)` | Point writer at existing run without overwriting CSVs. |
| `save_crawl_state(run_dir, status, ...)` | Persist crawl progress and serialised queue to `_state.json`. |
| `load_crawl_state(run_dir)` | Load crawl state for resume. |
| `rebuild_visited_from_csvs(run_dir)` | Reconstruct visited URL set from `pages.csv` and `crawl_errors.csv`. |
| `rebuild_sitemap_meta_from_csv(run_dir)` | Reconstruct the sitemap metadata lookup from `sitemap_urls.csv` for crawl resume. |
| `get_run_status(run_dir)` | Return the status of a run (`new`, `running`, `interrupted`, `completed`) from `_state.json`. |
| `recover_stale_running_states()` | On startup, mark any run left in `running` state as `interrupted` (indicates unclean shutdown). |
| `load_project(slug)` | Read `_project.json` for a project, or `None` if missing. |
| `load_project_defaults(slug)` | Load `_defaults.json` for a project. |
| `save_project_defaults(slug, cfg)` | Write (or overwrite) `_defaults.json` for a project. |
| `snapshot_config()` | Return a JSON-safe dict of all crawl-relevant module-level config values. |
| `save_run_config(run_dir, cfg)` | Write config dict as `_config.json` inside a run folder. |
| `load_run_config(run_dir)` | Load `_config.json` from a run folder. |
| `apply_run_config(cfg)` | Write config dict values into the live `config` module (CLI backward compat). |

#### Content hash persistence

| Function | Purpose |
|----------|---------|
| `save_content_hashes(run_dir, hashes)` | Write content hash dict to `_content_hashes.json`. |
| `load_content_hashes(run_dir)` | Load content hashes from a previous run. |

### Module: `utils.py`

Shared stateless helpers used across the codebase.

| Function | Purpose |
|----------|---------|
| `flatten_json_ld(obj)` | Recursively flatten JSON-LD `@graph` structures into a list of node dicts. |
| `now_iso()` | Return the current UTC time as a `YYYY-MM-DD HH:MM:SS` string for `discovered_at` fields. |
| `sanitise_csv_value(value)` | Strip null bytes, coerce None to empty string, truncate >32K chars. |
| `count_csv_rows(filepath)` | Return the number of data rows (excluding header) in a CSV file, or 0 on error. |
| `is_allowed_domain(url, domains)` | Check if a URL's hostname matches any of the given domain substrings. |
| `parse_robots_for_sitemaps(text)` | Parse `Sitemap:` lines from a robots.txt body. |
| `read_csv(path)` | Read a CSV file and return a list of row dicts. |
| `safe_int(val, default=0)` | Parse a string to int, returning `default` on failure. |
| `safe_float(val, default=0.0)` | Parse a string to float, returning `default` on failure. |

---

## 4. Visualisation & Reporting

### Module: `viz_data.py`

Pure aggregation layer — reads crawl CSVs and returns JSON-serialisable structures.

| Function | Purpose |
|----------|---------|
| `filter_pages(rows, filters)` | Cross-cutting filter: domains, CMS, content kinds, schema formats/types, date range, min coverage. |
| `aggregate_domains(run_dirs, filters)` | Per-domain summary with 40+ metrics including Phase 4 fields. |
| `aggregate_domain_graph(run_dirs, filters)` | Force/chord/sankey node + link data from `edges.csv`. |
| `aggregate_tags(run_dirs, filters)` | Tag frequency and co-occurrence from `tags.csv`. |
| `aggregate_navigation(run_dirs, domain, filters)` | Hierarchical navigation tree from `nav_links.csv`. |
| `aggregate_freshness(run_dirs, filters)` | Per-domain date ranges and staleness classification. |
| `aggregate_chord(run_dirs, top_n, filters)` | Square matrix of inter-domain link counts for d3-chord. |
| `aggregate_technology(run_dirs, filters)` | CMS distribution, structured data adoption, schema types, SEO readiness, coverage histogram. |
| `aggregate_authorship(run_dirs, filters)` | Author-domain network, publisher landscape. |
| `aggregate_schema_insights(run_dirs, filters)` | Conditional Product/Event/Job/Recipe summaries. |
| `get_filter_options(run_dirs)` | Available values for filter dropdowns. |

### Module: `viz_api.py`

Flask blueprint exposing JSON API endpoints for the dashboard. All endpoints accept filter query parameters.

---

## 5. Audit Engines

### Module: `audit_data.py`

Content audit — 10 finding types with severity ratings and drill-down tables.

| Function | Checks | Data source |
|----------|--------|-------------|
| `audit_duplicate_content` | Identical titles, descriptions, or both across URLs | `pages.csv` title + meta_description |
| `audit_redirects` | `requested_url ≠ final_url`, cross-domain redirects | `pages.csv` |
| `audit_thin_content` | <50 words, no headings, zero internal links | `pages.csv` word_count + heading_outline + link_count_internal |
| `audit_title_meta` | Missing/short/long titles and descriptions | `pages.csv` title + meta_description lengths |
| `audit_orphan_pages` | Zero inbound links, sitemap-only discovery | `pages.csv` + `edges.csv` cross-reference |
| `audit_link_distribution` | Link equity: most-linked, zero-inbound, sinks | `edges.csv` inbound/outbound counts |
| `audit_image_accessibility` | Missing alt text per page and domain | `pages.csv` img_count + img_missing_alt_count |
| `audit_url_structure` | Path depth, URL length, query parameters | `pages.csv` final_url parsed |
| `audit_content_decay` | Never-updated, 3+ years stale | `pages.csv` date_published + date_modified |
| `audit_broken_links` | Error type and domain breakdown | `crawl_errors.csv` |
| `run_full_audit(run_dirs)` | Orchestrates all 10 checks, returns combined report with severity summary |

### Module: `wcag_audit.py`

WCAG 2.1 accessibility audit — 13 criteria across 4 principles.

| Criterion | Level | What's tested |
|-----------|-------|---------------|
| 1.1.1 Non-text Content | A | Image alt text coverage |
| 1.3.1a Headings | A | Heading hierarchy (no skipped levels) |
| 1.3.1b Form Labels | A | Input/label association |
| 1.3.1c Data Tables | A | `<th>` elements in tables |
| 1.3.5 Input Purpose | AA | `autocomplete` attributes |
| 2.4.1 Bypass Blocks | A | `<main>` landmark or skip link |
| 2.4.2 Page Titled | A | Non-empty `<title>` |
| 2.4.4 Link Purpose | A | Vague link text (≤5% threshold) |
| 2.4.5 Multiple Ways | AA | `<nav>` or search facility |
| 2.4.6 Headings and Labels | AA | Empty heading elements |
| 3.1.1 Language of Page | A | Valid `html[lang]` |
| 4.1.1 Parsing | A | Duplicate `id` attributes |
| 4.1.2 Name, Role, Value | A | Buttons/links without accessible names |

---

## 6. GUI Routes

### Module: `gui.py`

Flask application serving the web interface. All routes resolve run and project paths via pure helper functions (`_runs_dir`, `_run_dir`, `get_project_runs_dir`) rather than mutating `config.OUTPUT_DIR`.

| Route | Method | Purpose |
|-------|--------|---------|
| `/` | GET | Project list (homepage) |
| `/projects/create` | POST | Create new project → redirect to Dashboard |
| `/projects/<slug>/delete` | POST | Delete project (blocked if crawl running) |
| `/projects/import` | POST | Import project from ZIP |
| `/p/<slug>` | GET | Redirect to Dashboard |
| `/p/<slug>/reports` | GET | Dashboard (stats + visualisations) |
| `/p/<slug>/audit` | GET | Content Audit page |
| `/p/<slug>/wcag` | GET | WCAG Audit page |
| `/p/<slug>/runs` | GET | Runs list |
| `/p/<slug>/settings` | GET | Settings page |
| `/p/<slug>/defaults` | GET | Redirect to Settings |
| `/p/<slug>/defaults` | POST | Save project defaults |
| `/p/<slug>/export` | GET | Download project as ZIP |
| `/p/<slug>/runs/create` | POST | Create new run |
| `/p/<slug>/runs/<run>/config` | GET/POST | Run configuration |
| `/p/<slug>/runs/<run>/monitor` | GET | Live crawl monitor (SSE) |
| `/p/<slug>/runs/<run>/start` | POST | Start crawl |
| `/p/<slug>/runs/<run>/resume` | POST | Resume interrupted crawl |
| `/p/<slug>/runs/<run>/stop` | POST | Stop running crawl |
| `/p/<slug>/runs/<run>/results` | GET | Results file listing |
| `/p/<slug>/runs/<run>/results/<file>` | GET | CSV viewer (paginated) |
| `/p/<slug>/runs/<run>/download/<file>` | GET | Download single CSV |
| `/p/<slug>/runs/<run>/download-all` | GET | Download all CSVs as ZIP |
| `/p/<slug>/runs/<run>/delete` | POST | Delete run |
| `/p/<slug>/runs/<run>/rename` | POST | Rename run |
| `/api/progress/<slug>` | GET | SSE crawl progress stream |
| `/api/logs` | GET | SSE log stream |
| `/p/<slug>/api/audit` | GET | JSON content audit |
| `/p/<slug>/api/wcag` | GET | JSON WCAG audit |

---

## 7. Configuration System

### Module: `config.py`

All configuration is defined as module-level constants with a `CrawlConfig` dataclass for per-crawl isolation.

#### Scope settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `SEED_URLS` | `[]` | Starting pages |
| `SITEMAP_URLS` | `[]` | Extra sitemap index or urlset URLs |
| `LOAD_SITEMAPS_FROM_ROBOTS` | True | Also discover sitemaps from each seed origin's `robots.txt` |
| `MAX_SITEMAP_URLS` | 1,000,000 | Cap on locations read from sitemaps per run |
| `ALLOWED_DOMAINS` | `()` | Hostname allowlist (dot-boundary match) |
| `EXCLUDED_DOMAINS` | `[]` | Hostname blocklist (overrides allowed) |
| `URL_EXCLUDE_PATTERNS` | `[]` | URL substring blocklist |
| `URL_INCLUDE_PATTERNS` | `[]` | URL substring allowlist (if set, only matching URLs are crawled) |

#### Crawl limits

| Setting | Default | Purpose |
|---------|---------|---------|
| `MAX_PAGES_TO_CRAWL` | 1,000,000 | HTML page cap per run |
| `MAX_DEPTH` | None | Link-following depth (None = unlimited) |
| `REQUEST_DELAY_SECONDS` | (3, 5) | Random delay between requests |
| `REQUEST_TIMEOUT_SECONDS` | 20 | Per-request timeout |
| `MAX_RETRIES` | 3 | Retries on transient errors |
| `CONCURRENT_WORKERS` | 1 | Parallel fetch workers |
| `STATE_SAVE_INTERVAL` | 10 | Persist `_state.json` every N pages |

#### Feature flags

| Setting | Default | Purpose |
|---------|---------|---------|
| `CONTENT_DEDUP` | True | Skip pages with identical visible text hash |
| `CHANGE_DETECTION` | False | Compare hashes across runs |
| `RENDER_JAVASCRIPT` | False | Playwright headless browser fallback |
| `RESPECT_ROBOTS_TXT` | True | Obey robots.txt |
| `CAPTURE_READABILITY` | True | Flesch-Kincaid grade level |
| `CHECK_OUTBOUND_LINKS` | False | HEAD-check outbound targets |
| `WRITE_EDGES_CSV` | True | Write `edges.csv` link graph |
| `WRITE_TAGS_CSV` | True | Write `tags.csv` per-tag rows |
| `WRITE_SITEMAP_URLS_CSV` | True | Write `sitemap_urls.csv` |
| `WRITE_NAV_LINKS_CSV` | True | Write `nav_links.csv` |
| `ASSET_HEAD_METADATA` | True | HEAD-request discovered asset links |
| `CAPTURE_RESPONSE_HEADERS` | True | Persist `Last-Modified`/`ETag` on page rows |

#### Domain ownership

| Setting | Default | Purpose |
|---------|---------|---------|
| `DOMAIN_OWNERSHIP_RULES` | `[]` | List of `(domain_suffix, label)` rules for classifying domains in reports |
| `DOMAIN_OWNERSHIP_DEFAULT` | `"Uncategorised"` | Fallback label for domains matching no rule |

#### Identity and logging

| Setting | Default | Purpose |
|---------|---------|---------|
| `USER_AGENT` | `"CSJ/1.0 …"` | HTTP User-Agent string |
| `LOG_LEVEL` | `"INFO"` | Python log level |

---

## 8. Desktop Packaging

### Module: `launcher.py`

Desktop entry point. Opens a native `pywebview` window (WebKit/Edge/WebKitGTK) with Flask running in a daemon thread. Falls back to default browser if pywebview backend is unavailable.

Crash handler writes `crash.log` to `DATA_DIR` with full traceback.

### Spec: `collector.spec`

PyInstaller recipe. Bundles templates, static assets, `tldextract` suffix list, and all Python modules. `console=True` on Linux (visible tracebacks), `False` on macOS/Windows. UPX disabled.

### Docker: `Dockerfile` + `docker-compose.yml`

191MB image based on `python:3.12-slim`. Runs `gui.py` directly, exposes port 5001, persists data via named volume.
