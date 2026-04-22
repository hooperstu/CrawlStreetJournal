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
| `fetch_page(url, cfg)` | HTTP GET via a configured `requests.Session` (see `HTTP_VERIFY_SSL`, `HTTP_MAX_REDIRECTS`) with exponential backoff retries. Returns `(body, status, final_url, content_type, response_meta, error_detail)`. Captures `Last-Modified`, `ETag`, `X-Robots-Tag`, `Server`, `X-Powered-By` headers when enabled. |
| `head_asset(url, cfg)` | HTTP HEAD for asset metadata with the same TLS/redirect settings. Returns `(content_type, content_length)`. |
| `_http_session(cfg)` | Builds a session: `verify` from `HTTP_VERIFY_SSL`, `max_redirects` from `HTTP_MAX_REDIRECTS` (clamped 1–1000). Logs a one-time warning if verification is disabled. |
| `_check_outbound_links(...)` | HEAD-checks unique outbound targets (up to `MAX_LINK_CHECKS_PER_PAGE`). Writes `check_message` on failure; optional `LINK_CHECK_GET_FALLBACK` retries with a small ranged GET after HEAD errors or HTTP 403/405/501. |

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
| `_process_one_url(url, referrer, depth, ...)` | Fetch and process one URL. Workflow: check visited → check robots → wait for domain → fetch (records **wall time in ms** for `fetch_time_ms`) → JS render fallback → check content type → content dedup → parse inventory → write page row → write tags → optional keyword-log rows → write nav links → extract links → write edges → write assets → write phone numbers → queue new URLs. Returns `(page_written, new_assets, final_url)`. |

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
| `crawl(seed_urls, max_pages, delay, ...)` | Main entry point. Initialises run, executes crawl loop, handles interrupts. Sequential when `CONCURRENT_WORKERS ≤ 1`; concurrent via `ThreadPoolExecutor` otherwise (each worker pops independently; multi-seed or multi-host initial queue ⇒ at most one in-flight fetch per hostname). Returns `(pages_crawled, assets_from_pages)`. |

**Resume exit semantics:** when `resume=True`, if the session ends without a user stop but **no new pages** were written (`pages_crawled` unchanged for the session), the terminal `_state.json` is written with **`interrupted`** — not **`completed`** — so empty-queue resumes and imported runs are not mis-classified as successfully finished crawls.

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
| `_assess_wcag_static(soup, lang, title)` | 15 WCAG 2.1 checks: language declaration, heading hierarchy, page title, form labels, bypass blocks, vague links, image alt, empty headings, duplicate IDs, empty buttons/links, table headers, autocomplete, search/nav presence; also sets **`has_viewport_meta`** from `<meta name="viewport">`. Returns dict with string values for CSV. |

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
| `_compute_extraction_coverage_full(page_row)` | Percentage of non-empty extractable columns (full row, including sparse schema.org slots). |
| `_compute_extraction_coverage_core(page_row)` | Same, but excludes optional Product/Event/Job/Recipe-specific schema columns so the figure reflects typical SEO/trust inventory. |

#### Keyword log (optional)

| Function | Purpose |
|----------|---------|
| `normalise_keyword_log_terms(terms)` | Normalise configured `KEYWORD_LOG_TERMS` for matching. |
| `keyword_hits_in_visible_text(visible_text, terms)` | Count matches of each term in visible text for `keyword_log.csv`. |

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
| `build_page_inventory_row(html, requested_url, final_url, http_status, content_type, referrer_url, depth, discovered_at, response_meta=…, sitemap_meta=…, fetch_time_ms=…)` | Orchestrates all extraction. Returns **3-tuple** `(page_row_dict, tag_rows_list, keyword_hits)` where *keyword_hits* is `[(term, match_count), …]` for `KEYWORD_LOG_TERMS` (empty when disabled). Passes **`fetch_time_ms`** into the page row for crawl-time performance reporting. |

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
| `write_error(row)` | Write to `crawl_errors.csv` (extended schema: requested URL in `url`, `final_url`, provenance, `failure_class`, fetch/redirect metadata, `robots_txt_rule` when applicable). |
| `write_sitemap_url(row)` | Write to `sitemap_urls.csv`. |
| `write_nav_link(row)` | Write to `nav_links.csv`. |
| `write_link_check(row)` | Write to `link_checks.csv`. |
| `write_phone_number(row)` | Write to `phone_numbers.csv`. |
| `write_keyword_log_row(row)` | Append to `keyword_log.csv` when keyword logging is enabled. |

#### Project lifecycle

| Function | Purpose |
|----------|---------|
| `create_project(name, description)` | Create project directory with `_project.json` and `_defaults.json`. Returns slug. |
| `list_projects()` | Return metadata for every project, sorted by creation date. |
| `activate_project(slug)` | Point `config.OUTPUT_DIR` at this project's runs directory. Returns `StorageContext`. |
| `delete_project(slug)` | Remove project directory with safety check. |
| `export_project(slug)` | Create ZIP of project directory. |
| `import_project(uploaded)` | Extract ZIP into projects directory. |

**Import ZIPs without `_state.json`:** if an extracted run has `pages.csv` rows but no `_state.json`, `get_run_status` still reports **`interrupted`** so the GUI does not offer **Start** (which would re-initialise CSVs). Optional: run `tools/fix_project_zip_resume_state.py` on an archive to embed explicit `_state.json` with counters before import.

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
| `get_run_status(run_dir)` | Return the status of a run (`new`, `running`, `interrupted`, `completed`). Reads `_state.json` when present; if **missing** but `pages.csv` has data rows, returns **`interrupted`**; if state says **`new`** but `pages.csv` has rows, returns **`interrupted`**. |
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

The ecosystem dashboard (`/p/<slug>/reports`) is read-only: it aggregates existing CSVs. Several endpoints support **`full_lists`** (query `full=1` / `true` / `yes` / `all`) to return uncapped row lists for exports or deep analysis; otherwise list fields may be sampled for UI performance.

When **`runs=`** includes multiple folders (or “all runs”), **`viz_data`** **merges** CSV rows so the same logical URL or edge does not appear twice: runs are sorted by `run_*` folder name (oldest first) and **newer run wins** on key collision (`requested_url` for pages, `(from_url, to_url)` for edges, `url` for errors with stale errors removed when the merged page shows HTTP success, plus keys for tags, nav links, and assets). This aligns gap-refetch runs with their baseline crawl in reports.

### Module: `viz_data.py`

Pure aggregation layer — reads crawl CSVs and returns JSON-serialisable structures.

| Function | Purpose |
|----------|---------|
| `merged_page_rows_for_runs(run_dirs)` | **Public:** merged `pages.csv` rows (used by `viz_api` overview and `audit_data`). |
| `merged_error_rows_for_runs(run_dirs)` | **Public:** merged `crawl_errors.csv` rows with stale-error suppression. |
| `merged_asset_rows_for_runs(run_dirs)` | **Public:** merged rows from all `assets_*.csv` files. |
| `merged_edge_rows_for_runs(run_dirs)` | **Public:** merged `edges.csv` rows. |
| `filter_pages(rows, filters)` | Cross-cutting filter: domains, CMS, content kinds, schema formats/types, date range, min coverage. |
| `aggregate_domains(run_dirs, filters)` | Per-domain summary with 40+ metrics including Phase 4 fields. |
| `aggregate_domain_graph(run_dirs, filters)` | Force-directed node + link data from `edges.csv`. |
| `aggregate_tags(run_dirs, filters)` | Tag frequency and co-occurrence from `tags.csv`. |
| `aggregate_navigation(run_dirs, domain, filters)` | Hierarchical navigation tree from `nav_links.csv`. |
| `aggregate_freshness(run_dirs, filters)` | Per-domain date ranges and staleness classification. |
| `aggregate_chord(run_dirs, top_n, filters)` | Square matrix of inter-domain link counts for d3-chord. |
| `aggregate_technology(run_dirs, filters)` | CMS distribution, structured data adoption, schema types, SEO readiness, coverage histogram. |
| `aggregate_authorship(run_dirs, filters)` | Author-domain network, publisher landscape. |
| `aggregate_schema_insights(run_dirs, filters)` | Conditional Product/Event/Job/Recipe summaries. |
| `aggregate_page_depth(run_dirs, filters)` | Depth histogram and quality-by-depth scatter inputs. |
| `aggregate_content_health(run_dirs, filters)` | Domain × signal matrix for content health heatmap. |
| `aggregate_competitor_intelligence(run_dirs, filters, full_lists=…)` | Tags, top pages by words/coverage, schema product/pricing rows, in-crawl cross-domain edges (not third-party backlinks). |
| `aggregate_content_performance_audit(run_dirs, filters, full_lists=…)` | Thin content, duplicate `content_hash` / shared `canonical_url`, internal link graph, `tags_all` vs title/H1 alignment. |
| `aggregate_technical_performance(run_dirs, filters, full_lists=…)` | Per-domain fetch time (`fetch_time_ms`), viewport meta coverage, asset categories, external scripts, large images (from `assets_*.csv` HEAD metadata when present). |
| `aggregate_key_metrics_snapshot(run_dirs, filters, full_lists=…)` | Crawl-proxy “traffic” (referrer buckets), structural engagement, schema commerce counts — **not** analytics sessions. |
| `aggregate_indexability(run_dirs, filters, full_lists=…)` | Pages with `noindex` in `robots_directives` plus `robots_disallowed` rows from `crawl_errors.csv`. |
| `get_filter_options(run_dirs)` | Available values for filter dropdowns; `total_pages` reflects **merged** page count when multiple runs exist. |

### Module: `viz_api.py`

Flask blueprint (`eco_bp`) registered in `gui.py`. Routes are prefixed with `/p/<slug>/`.

The **Reports** dashboard template receives an **`overview`** dict for the stat cards: **`total_pages`**, **`total_errors`**, and **`total_assets`** are computed from **merged** row lists (`viz_data` public helpers) across all non-legacy runs so figures match multi-run aggregation rather than summing per-run CSV row counts.

**JSON APIs** (all accept the shared filter query parameters plus optional `full` where noted above):

| Prefix | Examples |
|--------|----------|
| `/api/viz/` | `runs`, `filter_options`, `domains`, `graph`, `tags`, `navigation`, `freshness`, `chord`, `technology`, `authorship`, `schema_insights`, `page_depth`, `content_health`, `content_performance_audit`, `technical_performance`, `key_metrics_snapshot`, `competitor_intelligence`, `indexability` |

**ZIP exports** (UTF-8 CSV files inside `application/zip`; same filter query string as JSON):

| Route | Builder |
|-------|---------|
| `/export/content_performance_audit.zip` | `viz_exports.build_content_audit_zip` |
| `/export/technical_performance.zip` | `viz_exports.build_technical_performance_zip` |
| `/export/key_metrics_snapshot.zip` | `viz_exports.build_key_metrics_zip` |
| `/export/indexability.zip` | `viz_exports.build_indexability_zip` |
| `/export/competitor_intelligence.zip` | `viz_exports.build_competitor_intelligence_zip` |

Legacy per-run URL `/p/<slug>/runs/<run_name>/reports` redirects to the project-level dashboard with `?runs=` preset.

### Module: `viz_exports.py`

Builds in-memory ZIP archives by calling the corresponding `viz_data.aggregate_*` functions with `full_lists=True` and serialising rows to CSV. Used for spreadsheet analysis or print-to-PDF outside the app.

---

## 5. Audit Engines

### Module: `audit_data.py`

Content audit — 10 finding types with severity ratings and drill-down tables. **`audit_data`** loads pages, edges, and errors through **`viz_data.merged_*_for_runs`** helpers when multiple run directories are supplied, matching dashboard de-duplication (§4).

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
| `/api/quit` | POST | Cooperative shutdown: stop crawls, then background worker ends Flask / closes pywebview — **403 unless the client is loopback** |
| `/p/<slug>/api/audit` | GET | JSON content audit |
| `/p/<slug>/api/wcag` | GET | JSON WCAG audit |

Visualisation JSON and ZIP routes live on the **`viz_api`** blueprint (`register_blueprint(eco_bp)`), not inline in `gui.py` — see §4.

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
| `HTTP_MAX_REDIRECTS` | 30 | Maximum redirects per request (Requests default) |
| `HTTP_VERIFY_SSL` | True | Verify TLS certificates (`False` is insecure — for broken-chain audits only) |
| `CONCURRENT_WORKERS` | 1 | Parallel fetch workers; multi-seed or multi-host queue ⇒ no second concurrent fetch to the same hostname |
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
| `LINK_CHECK_GET_FALLBACK` | False | After failed HEAD or 403/405/501, try a small ranged GET for status |
| `WRITE_EDGES_CSV` | True | Write `edges.csv` link graph |
| `WRITE_TAGS_CSV` | True | Write `tags.csv` per-tag rows |
| `WRITE_SITEMAP_URLS_CSV` | True | Write `sitemap_urls.csv` |
| `WRITE_NAV_LINKS_CSV` | True | Write `nav_links.csv` |
| `WRITE_KEYWORD_LOG_CSV` | False | Write `keyword_log.csv` when terms match visible text |
| `KEYWORD_LOG_TERMS` | `[]` | Substrings to match for keyword logging (see `parser.keyword_hits_in_visible_text`) |
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
