#!/usr/bin/env python3
"""
NHS Collector — crawl allowed NHS-related hosts, record page metadata to CSV,
and write linked files to per-type asset CSVs.

Configure seeds, domains, and limits in config.py.
"""

import logging
import signal
import sys

import config
import scraper

_interrupted = False


def _signal_handler(_signum, _frame):
    global _interrupted
    _interrupted = True
    print("\nShutting down after current page...", file=sys.stderr)


def _on_progress(crawled: int, assets: int, current_url: str) -> None:
    if crawled % 10 == 0 and crawled > 0:
        logging.info(
            "Progress: %s pages crawled, %s asset link rows written, last %s",
            crawled,
            assets,
            current_url,
        )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)

    signal.signal(signal.SIGINT, _signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal_handler)

    delay = config.REQUEST_DELAY_SECONDS
    delay_str = f"{delay[0]}-{delay[1]}s" if isinstance(delay, (list, tuple)) and len(delay) == 2 else f"{delay}s"
    logger.info(
        "Starting NHS Collector (max %s HTML pages, delay %s). Output dir: %s",
        config.MAX_PAGES_TO_CRAWL,
        delay_str,
        config.OUTPUT_DIR,
    )
    logger.info("Allowed domain substrings: %s", ", ".join(config.ALLOWED_DOMAINS))

    try:
        pages, assets = scraper.crawl(
            on_progress=_on_progress,
            should_stop=lambda: _interrupted,
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0
    except Exception as e:
        logger.exception("Crawl failed: %s", e)
        return 1

    logger.info(
        "Finished: %s HTML pages recorded; %s asset rows from page links. "
        "See %s/",
        pages,
        assets,
        config.OUTPUT_DIR,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
