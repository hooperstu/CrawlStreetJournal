# AGENTS.md

## Cloud agent / development environment notes

### Overview

The Crawl Street Journal (CSJ) is a single-process Python (3.9+) web crawler with a Flask GUI. There are no databases, Docker containers, or separate microservices — everything runs in one Python process using filesystem-based storage (JSON config + CSV output).

### Running the application

- **Web GUI:** `source .venv/bin/activate && python3 gui.py` — serves at `http://localhost:5001`.
- **CLI crawl:** `source .venv/bin/activate && python3 main.py` — interactive terminal crawl.
- See `README.md` for full details on all entry points.

### Testing

- **Unit tests:** `source .venv/bin/activate && python3 -m pytest tests/test_parser.py tests/test_signals_audit.py tests/test_sitemap.py tests/test_viz_data.py -v` (71 unit tests).
- **Playwright tests:** `source .venv/bin/activate && python3 -m pytest tests/test_playwright.py -v` (113 scenario tests — requires Flask running on port 5001 and `tests/seed_test_data.py` run first).
- **Real crawl tests:** `source .venv/bin/activate && python3 -m pytest tests/test_real_crawl.py -v` (41 tests — requires an `nhs-estate-crawl` project with crawl data).
- **Linting:** `source .venv/bin/activate && flake8 --max-line-length=120 *.py` — no linting config is committed; the repo has minor pre-existing style warnings.
- There is no dedicated test framework in `requirements.txt`; `pytest`, `flake8`, `playwright`, and `pytest-playwright` are installed as dev extras in the venv.
- Playwright tests need the Flask app running (`python3 gui.py` in background) and test data seeded (`python3 tests/seed_test_data.py`).

### Non-obvious caveats

- The venv requires `python3.12-venv` system package on Ubuntu (not installed by default in the Cloud Agent base image). The update script handles this.
- `pytest` and `flake8` are not listed in `requirements.txt` but are needed for running tests and lint. The update script installs them alongside the main dependencies.
- The crawler makes real HTTP requests to external websites. Crawl tests that target live sites will be affected by network conditions and SSL certificate handling in the VM environment.
- Project data (configs, crawl outputs) is stored under `projects/` in the working directory; this directory is created automatically by the GUI on first project creation.
- The Flask app binds to port **5001** (not the default 5000) and to **127.0.0.1** by default (set ``CSJ_GUI_BIND=0.0.0.0`` only when needed, e.g. Docker).
- `signals_audit.py` is a standalone research module — it inventories every metadata signal on a page for analysis. It has no effect on the main crawl pipeline.
- `render.py` provides optional Playwright-based JS rendering. It requires separate installation (`pip install playwright && playwright install chromium`) and is gated behind `RENDER_JAVASCRIPT = True` in config. It is **not** installed by default.
- The `pages.csv` schema includes Phase 4 columns (author, publisher, cms_generator, microdata_types, rdfa_types, schema_* fields, etc.). These are always populated but may be empty for pages that lack the relevant signals.
- The app navigation is **Dashboard | Runs | Settings**. The old `/p/<slug>` (overview) and `/p/<slug>/defaults` URLs redirect to Dashboard and Settings respectively.
- Crawl scope filtering: `EXCLUDED_DOMAINS`, `URL_EXCLUDE_PATTERNS`, and `URL_INCLUDE_PATTERNS` are enforced in `scraper.py` via `is_url_allowed()` — both at seed queueing and link discovery.
