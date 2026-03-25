#!/usr/bin/env python3
"""
Long-running crawl for background execution. Logs to crawl_background.log
and raises MAX_PAGES_TO_CRAWL for multi-hour runs (override in this file).
"""
import logging
import signal
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LOG_FILE = ROOT / "crawl_background.log"

# Raise cap so the job can run for hours (still stops early if the queue is exhausted).
BACKGROUND_MAX_PAGES = 20_000

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

    config.MAX_PAGES_TO_CRAWL = BACKGROUND_MAX_PAGES

    import scraper

    logging.info(
        "Background crawl started (max %s pages, delay %ss). Output: %s/",
        config.MAX_PAGES_TO_CRAWL,
        config.REQUEST_DELAY_SECONDS,
        config.OUTPUT_DIR,
    )
    logging.info("Log file: %s", LOG_FILE)

    try:
        pages, assets = scraper.crawl(
            on_progress=_on_progress,
            should_stop=lambda: _interrupted,
        )
    except Exception:
        logging.exception("Crawl failed")
        return 1

    logging.info(
        "Finished: %s HTML pages; %s asset rows from links. CSVs in %s/",
        pages,
        assets,
        config.OUTPUT_DIR,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
