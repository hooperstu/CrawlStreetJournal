#!/usr/bin/env python3
"""
Long-running crawl for background execution. Logs to crawl_background.log
and raises MAX_PAGES_TO_CRAWL for multi-hour runs (override in this file).
"""
import argparse
import logging
import signal
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LOG_FILE = ROOT / "crawl_background.log"

BACKGROUND_MAX_PAGES = 1_000_000

_interrupted = False


def _signal_handler(_signum, _frame):
    global _interrupted
    _interrupted = True
    logging.info("Signal received; will stop after current page.")


def _on_progress(crawled: int, assets: int, current_url: str) -> None:
    if crawled % 50 == 0 and crawled > 0:
        logging.info(
            "Progress: %s pages, %s asset link rows, last %s",
            crawled,
            assets,
            current_url,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Background Collector crawl")
    parser.add_argument("--name", default=None, help="Friendly name for a new run")
    parser.add_argument(
        "--run", default=None, metavar="FOLDER",
        help="Start or resume a specific run folder",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume an interrupted run (requires --run)",
    )
    parser.add_argument(
        "--project", default=None, metavar="SLUG",
        help="Project slug to run within (scopes output to projects/<slug>/runs/)",
    )
    args = parser.parse_args()

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
        force=True,
    )
    signal.signal(signal.SIGINT, _signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal_handler)

    import config
    import storage as storage_module

    if args.project:
        storage_module.activate_project(args.project)

    config.MAX_PAGES_TO_CRAWL = BACKGROUND_MAX_PAGES

    import scraper

    delay = config.REQUEST_DELAY_SECONDS
    delay_str = f"{delay[0]}-{delay[1]}s" if isinstance(delay, (list, tuple)) and len(delay) == 2 else f"{delay}s"
    logging.info(
        "Background crawl started (max %s pages, delay %s). Output: %s/",
        config.MAX_PAGES_TO_CRAWL,
        delay_str,
        config.OUTPUT_DIR,
    )
    logging.info("Log file: %s", LOG_FILE)

    try:
        pages, assets = scraper.crawl(
            on_progress=_on_progress,
            should_stop=lambda: _interrupted,
            run_name=args.name,
            run_folder=args.run,
            resume=args.resume,
        )
    except Exception:
        logging.exception("Crawl failed")
        return 1

    import storage
    logging.info(
        "Finished: %s HTML pages; %s asset rows from links. CSVs in %s/",
        pages,
        assets,
        storage.get_active_run_dir(),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
