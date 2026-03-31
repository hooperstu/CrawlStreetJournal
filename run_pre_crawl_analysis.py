#!/usr/bin/env python3
"""
Pre-crawl analysis: sample 20 pages from each target domain, detect tech
stacks, and produce per-domain CSVs plus cross-domain summary and
field-coverage reports.

Usage:
    python run_pre_crawl_analysis.py            # all domains
    python run_pre_crawl_analysis.py --limit 3  # first 3 domains only (testing)
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import signal
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup

import config
import parser as parser_module
import sitemap as sitemap_module
import storage

# ---------------------------------------------------------------------------
# Override config so every target domain is reachable
# ---------------------------------------------------------------------------
config.ALLOWED_DOMAINS = (
    "nhs.uk",
    "nhs.net",
    ".co.uk",
    ".org.uk",
    ".org",
    ".com",
    ".ac.uk",
    ".uk",
    ".net",
)

logger = logging.getLogger("pre_crawl")

ANALYSIS_DIR = os.path.join(os.path.dirname(__file__) or ".", "pre_crawl_analysis")
SAMPLE_SIZE = 20
DELAY_SECONDS = 1.0
TIMEOUT_SECONDS = 15

PAGES_FIELDS_EXTENDED = storage.PAGES_FIELDS + ("tech_stack_detected",)

_interrupted = False

# ---------------------------------------------------------------------------
# Target domains
# ---------------------------------------------------------------------------
TARGET_URLS: List[str] = [
    "https://www.hee.nhs.uk",
    "https://digital.nhs.uk",
    "https://england.nhs.uk",
    "https://anro.wm.hee.nhs.uk",
    "https://www.bedfordshirehospitals.nhs.uk",
    "https://www.healthcareers.nhs.uk",
    "https://cptraininghub.nhs.uk",
    "https://dental.hee.nhs.uk",
    "https://dental.southwest.hee.nhs.uk",
    "https://digital-transformation.hee.nhs.uk",
    "https://editlab.this.nhs.uk",
    "https://emergency.peninsuladeanery.nhs.uk",
    "https://emergency.severndeanery.nhs.uk",
    "https://foundation.peninsuladeanery.nhs.uk",
    "https://foundation.severndeanery.nhs.uk",
    "https://foundationprogramme.nhs.uk",
    "https://www.genomicseducation.hee.nhs.uk",
    "https://global.hee.nhs.uk",
    "https://gpnursing.jobs.nhs.uk",
    "https://medical.hee.nhs.uk",
    "https://gp-training.hee.nhs.uk",
    "https://healthacademy.lancsteachinghospitals.nhs.uk",
    "https://icmnro.wm.hee.nhs.uk",
    "http://jobs.mtw.nhs.uk",
    "https://kss.hee.nhs.uk",
    "https://library.hee.nhs.uk",
    "https://library.nhs.uk",
    "https://madeinheene.hee.nhs.uk",
    "https://nshcs.hee.nhs.uk",
    "https://me.mtw.nhs.uk",
    "https://medicine.peninsuladeanery.nhs.uk",
    "https://medicine.severndeanery.nhs.uk",
    "https://monitoring.ops.data.digital.nhs.uk",
    "https://www.mtw.nhs.uk",
    "https://www.myplannedcare.nhs.uk",
    "https://www.nhsimas.nhs.uk",
    "https://nhs-pcse-staging.pcse.england.nhs.uk",
    "https://nursing-associates.hee.nhs.uk",
    "https://nwschoolofpsychiatry.hee.nhs.uk",
    "https://obsandgynae.peninsuladeanery.nhs.uk",
    "https://obsandgynae.severndeanery.nhs.uk",
    "https://ophthalmology.severndeanery.nhs.uk",
    "https://paediatrics.peninsuladeanery.nhs.uk",
    "https://paediatrics.severndeanery.nhs.uk",
    "https://pathology.peninsuladeanery.nhs.uk",
    "https://pathology.severndeanery.nhs.uk",
    "https://pcse.england.nhs.uk",
    "https://peninsuladeanery.nhs.uk",
    "https://www.uhsussex.nhs.uk",
    "https://ppn.nhs.uk",
    "https://primarycare.peninsuladeanery.nhs.uk",
    "https://primarycare.severndeanery.nhs.uk",
    "https://psychiatry.peninsuladeanery.nhs.uk",
    "https://psychiatry.severndeanery.nhs.uk",
    "https://publichealth.severndeanery.nhs.uk",
    "https://radiology.peninsuladeanery.nhs.uk",
    "https://radiology.severndeanery.nhs.uk",
    "https://remedy.this.nhs.uk",
    "https://rs.nhsprofessionals.nhs.uk",
    "https://rsspare.nhsprofessionals.nhs.uk",
    "https://scan4safety.nhs.uk",
    "https://severndeanery.nhs.uk",
    "https://www.nhsbsa.nhs.uk",
    "https://www.stepintothenhs.nhs.uk",
    "https://support.mtw.nhs.uk",
    "https://surgery.peninsuladeanery.nhs.uk",
    "https://surgery.severndeanery.nhs.uk",
    "https://telblog.hee.nhs.uk",
    "https://thamesvalley.hee.nhs.uk",
    "https://www.this.nhs.uk",
    "https://tis-support.hee.nhs.uk",
    "https://transparency.ndsp.gpconnect.nhs.uk",
    "https://vts.wm.hee.nhs.uk",
    "https://websiteadmin.southwest.hee.nhs.uk",
    "https://wessex.hee.nhs.uk",
    "https://wordpress-support.hee.nhs.uk",
    "https://wpms.hee.nhs.uk",
    # www.academic, www.accs, www.anaesthesia have no non-www counterparts
    "https://www.academic.peninsuladeanery.nhs.uk",
    "https://www.academic.severndeanery.nhs.uk",
    "https://www.accs.peninsuladeanery.nhs.uk",
    "https://www.accs.severndeanery.nhs.uk",
    "https://www.anaesthesia.peninsuladeanery.nhs.uk",
    "https://www.anaesthesia.severndeanery.nhs.uk",
    # www.ophthalmology.peninsuladeanery has no non-www counterpart
    "https://www.ophthalmology.peninsuladeanery.nhs.uk",
    "https://www.supplychain.nhs.uk",
    "https://beta.digitisingsocialcare.co.uk",
    "https://www.autismcentral.org.uk",
    "https://www.capitalnurselondon.co.uk",
    "https://careersinpharmacy.uk",
    "https://curriculumlibrary.nshcs.org.uk",
    "https://www.eintegrity.org",
    "https://www.e-lfh.org.uk",
    "https://ftp.nshcs.org.uk",
    "https://gettingitrightfirsttime.co.uk",
    "https://girft-hubtoolkit.org.uk",
    "https://girft-interactivepathways.org.uk",
    "https://gpinsomerset.com",
    "https://www.skillsforhealth.org.uk",
    "https://hub.seschoolofpas.org",
    "https://seschoolofpas.org",
    "https://londonpaediatrics.co.uk",
    "https://stokeanaesthesia.org.uk",
    "https://migrate.nhs.net",
    "https://www.nhsfindyourplace.co.uk",
    "https://www.oxsph.org",
    "https://stage.digitisingsocialcare.co.uk",
    "https://studyinghealthcare.ac.uk",
    "https://survey.nhs.net",
    "https://thcepn.com",
    "https://webzang.gpinsomerset.com",
    "https://work-learn-live-blmk.co.uk",
    "http://schoolofanaesthesia.co.uk",
    "https://swimsnetworknhs.uk",
]

# ---------------------------------------------------------------------------
# Tech-stack detection
# ---------------------------------------------------------------------------

_WP_MARKERS = ("wp-content", "wp-includes", "wp-block-", "wp-json")
_DRUPAL_MARKERS = ("drupal", "/sites/default/files/", "views-row", "drupal.js")
_SP_MARKERS = (
    "sharepoint", "s4-workspace", "ms-webpart", "_layouts/", "/_catalogs/",
    "x-sharepointheealthscore",
)
_NEXT_MARKERS = ("__next", "_next/static", "_next/data")
_GATSBY_MARKERS = ("__gatsby", "gatsby-")


def detect_tech_stack(
    html: str,
    resp_headers: Optional[Dict[str, str]] = None,
) -> str:
    """Return a short label for the detected CMS / framework."""
    html_lower = html[:500_000].lower()
    headers_lower = {
        k.lower(): v.lower()
        for k, v in (resp_headers or {}).items()
    }

    soup = BeautifulSoup(html[:100_000], "lxml")
    gen_tag = soup.find("meta", attrs={"name": "generator"})
    if not gen_tag:
        gen_tag = soup.find("meta", attrs={"name": re.compile(r"^generator$", re.I)})
    generator = ""
    if gen_tag and gen_tag.get("content"):
        generator = str(gen_tag["content"]).strip().lower()

    if "wordpress" in generator or "developer starter kit" in generator:
        return "WordPress"
    if "drupal" in generator:
        return "Drupal"
    if "joomla" in generator:
        return "Joomla"
    if "silverstripe" in generator:
        return "SilverStripe"
    if "concrete5" in generator or "concrete cms" in generator:
        return "Concrete5"
    if "typo3" in generator:
        return "TYPO3"

    if any(m in html_lower for m in _WP_MARKERS):
        return "WordPress"
    if any(m in html_lower for m in _DRUPAL_MARKERS):
        return "Drupal"
    if any(m in html_lower for m in _SP_MARKERS):
        return "SharePoint"
    if any(m in html_lower for m in _NEXT_MARKERS):
        return "Next.js"
    if any(m in html_lower for m in _GATSBY_MARKERS):
        return "Gatsby"

    if "x-drupal-cache" in headers_lower or "x-generator" in headers_lower:
        xgen = headers_lower.get("x-generator", "")
        if "drupal" in xgen:
            return "Drupal"
    powered_by = headers_lower.get("x-powered-by", "")
    if "asp.net" in powered_by:
        return "ASP.NET"
    if "express" in powered_by:
        return "Express/Node"
    if "php" in powered_by:
        return "PHP"

    root_div = soup.find("div", id="root") or soup.find("div", id="app")
    if root_div and len(root_div.get_text(strip=True)) < 50:
        return "JS SPA"

    # SilverStripe HTML markers (for sites without the generator tag)
    if "silverstripe" in html_lower or "themes/pgme" in html_lower:
        return "SilverStripe"

    if "squarespace" in html_lower:
        return "Squarespace"
    if "wix.com" in html_lower:
        return "Wix"
    if "umbraco" in html_lower:
        return "Umbraco"
    if "sitecore" in html_lower:
        return "Sitecore"
    if "sitefinity" in html_lower:
        return "Sitefinity"
    if "kentico" in html_lower:
        return "Kentico"
    if "episerver" in html_lower or "optimizely" in html_lower:
        return "Optimizely"
    if "contensis" in html_lower:
        return "Contensis"

    return "static/unknown"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _sanitise_domain(netloc: str) -> str:
    return re.sub(r"[^a-zA-Z0-9.\-]", "_", netloc)


def _domain_dir(netloc: str) -> str:
    return os.path.join(ANALYSIS_DIR, _sanitise_domain(netloc))


def _existing_page_count(domain_path: str) -> int:
    pages_path = os.path.join(domain_path, "pages.csv")
    if not os.path.isfile(pages_path):
        return 0
    try:
        with open(pages_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            return sum(1 for _ in reader)
    except Exception:
        return 0


def _backfill_summary_from_csv(domain_path: str, summary: Dict[str, Any]) -> None:
    """Re-derive summary stats from an already-written pages.csv (for resumed domains)."""
    pages_path = os.path.join(domain_path, "pages.csv")
    if not os.path.isfile(pages_path):
        return
    try:
        with open(pages_path, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        return
    if not rows:
        return

    tech_vals = {r.get("tech_stack_detected", "") for r in rows} - {""}
    if tech_vals:
        summary["tech_stack"] = sorted(tech_vals)[0]

    kinds: Set[str] = set()
    total_words = 0
    for r in rows:
        k = r.get("content_kind_guess", "")
        if k:
            kinds.add(k)
        wc = r.get("word_count", "0")
        total_words += int(wc) if wc and wc.isdigit() else 0
        if r.get("json_ld_types"):
            summary["has_json_ld"] = True
        if r.get("og_title") or r.get("og_type"):
            summary["has_og_tags"] = True
        nc = r.get("nav_link_count", "0")
        if nc and nc.isdigit() and int(nc) > 0:
            summary["has_nav"] = True

    summary["avg_word_count"] = round(total_words / len(rows)) if rows else 0
    summary["distinct_content_kinds"] = "|".join(sorted(kinds))


def _sanitise(value) -> str:
    """Coerce a field value to a safe CSV string."""
    if value is None:
        return ""
    s = str(value)
    if "\x00" in s:
        s = s.replace("\x00", "")
    if len(s) > 32_000:
        s = s[:32_000] + "…[truncated]"
    return s


def _write_csv_header(path: str, fieldnames: tuple) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(
            f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL,
        ).writeheader()


def _append_csv_row(path: str, fieldnames: tuple, row: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    safe = {k: _sanitise(row.get(k, "")) for k in fieldnames}
    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f, fieldnames=fieldnames,
                extrasaction="ignore", quoting=csv.QUOTE_ALL,
            )
            w.writerow(safe)
    except Exception as e:
        url = safe.get("final_url") or safe.get("url") or "?"
        logging.warning("CSV write failed for %s → %s: %s", url, path, e)


def _fetch_raw(url: str) -> Tuple[Optional[requests.Response], str]:
    """GET with retries. Returns (response, error_message)."""
    headers = {"User-Agent": config.USER_AGENT}
    for attempt in range(2):
        try:
            resp = requests.get(
                url, headers=headers, timeout=TIMEOUT_SECONDS,
                allow_redirects=True,
            )
            return resp, ""
        except requests.exceptions.Timeout:
            if attempt == 1:
                return None, "timeout"
        except requests.exceptions.SSLError:
            return None, "ssl_error"
        except requests.exceptions.ConnectionError:
            return None, "connection_error"
        except Exception as exc:
            return None, str(exc)[:200]
    return None, "max_retries"


# ---------------------------------------------------------------------------
# Sitemap URL collection (lightweight, per-domain)
# ---------------------------------------------------------------------------

def _discover_sitemap_urls(origin: str, max_urls: int = 200) -> List[str]:
    """Try robots.txt, then common sitemap locations. Return page URLs."""
    sitemap_locations: List[str] = []

    resp, _ = _fetch_raw(origin.rstrip("/") + "/robots.txt")
    if resp and resp.status_code < 400:
        for line in resp.text.splitlines():
            line = line.strip()
            if line.lower().startswith("sitemap:"):
                sitemap_locations.append(line.split(":", 1)[1].strip())

    if not sitemap_locations:
        for path in ("/sitemap.xml", "/sitemap_index.xml"):
            sitemap_locations.append(origin.rstrip("/") + path)

    page_urls: List[str] = []
    seen_maps: Set[str] = set()

    for sm_url in sitemap_locations:
        if len(page_urls) >= max_urls:
            break
        if sm_url in seen_maps:
            continue
        seen_maps.add(sm_url)
        try:
            entries = sitemap_module.collect_urls_from_sitemap(
                sm_url, max_urls=max_urls - len(page_urls),
                visited_maps=seen_maps,
            )
            for loc, _ in entries:
                page_urls.append(loc.strip())
        except Exception:
            continue

    return page_urls[:max_urls]


def _pick_diverse(urls: List[str], n: int) -> List[str]:
    """Select up to *n* URLs with diverse first path segments."""
    if len(urls) <= n:
        return urls

    by_segment: Dict[str, List[str]] = defaultdict(list)
    for u in urls:
        seg = urlparse(u).path.strip("/").split("/")[0] if urlparse(u).path.strip("/") else "/"
        by_segment[seg].append(u)

    picked: List[str] = []
    segments = list(by_segment.keys())
    idx = 0
    while len(picked) < n and segments:
        seg = segments[idx % len(segments)]
        pool = by_segment[seg]
        if pool:
            picked.append(pool.pop(0))
        if not pool:
            segments.remove(seg)
            if segments:
                idx = idx % len(segments)
            continue
        idx += 1

    return picked[:n]


# ---------------------------------------------------------------------------
# BFS fallback
# ---------------------------------------------------------------------------

def _bfs_collect(
    origin: str, homepage_html: str, n: int,
    allowed_netlocs: Optional[Set[str]] = None,
) -> List[str]:
    """Shallow BFS on same domain from homepage, returning up to *n* URLs.

    *allowed_netlocs* lets us accept both the seed and redirect-target hosts.
    """
    if allowed_netlocs is None:
        allowed_netlocs = {urlparse(origin).netloc.lower()}
    soup = BeautifulSoup(homepage_html, "lxml")
    found: List[str] = [origin.rstrip("/")]
    seen: Set[str] = {origin.rstrip("/")}
    queue: deque[str] = deque()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        full = urljoin(origin, href)
        parsed = urlparse(full)
        norm = parsed._replace(fragment="").geturl().rstrip("/")
        if parsed.netloc.lower() not in allowed_netlocs:
            continue
        ext = parser_module._path_extension_lower(parsed.path)
        if ext:
            continue
        if norm not in seen:
            seen.add(norm)
            queue.append(norm)

    while queue and len(found) < n:
        url = queue.popleft()
        found.append(url)

    return found[:n]


# ---------------------------------------------------------------------------
# Process one domain
# ---------------------------------------------------------------------------

def _process_domain(
    seed_url: str,
    domain_idx: int,
    total_domains: int,
) -> Dict[str, Any]:
    """Sample up to SAMPLE_SIZE pages from one domain. Returns summary dict."""
    parsed = urlparse(seed_url)
    netloc = parsed.netloc.lower()
    origin = f"{parsed.scheme}://{parsed.netloc}"
    ddir = _domain_dir(netloc)
    pages_csv = os.path.join(ddir, "pages.csv")
    errors_csv = os.path.join(ddir, "errors.csv")

    summary: Dict[str, Any] = {
        "domain": netloc,
        "seed_url": seed_url,
        "tech_stack": "",
        "pages_sampled": 0,
        "pages_failed": 0,
        "sitemap_found": False,
        "redirects_to": "",
        "has_json_ld": False,
        "has_og_tags": False,
        "has_nav": False,
        "avg_word_count": 0,
        "distinct_content_kinds": "",
        "notes": "",
    }

    existing = _existing_page_count(ddir)
    if existing >= SAMPLE_SIZE:
        logger.info(
            "[%d/%d] SKIP %s — already has %d pages",
            domain_idx, total_domains, netloc, existing,
        )
        summary["pages_sampled"] = existing
        summary["notes"] = "resumed:already_complete"
        _backfill_summary_from_csv(ddir, summary)
        return summary

    logger.info(
        "[%d/%d] Starting %s ...",
        domain_idx, total_domains, netloc,
    )

    # -- Fetch homepage -------------------------------------------------
    resp, err = _fetch_raw(seed_url)
    if resp is None:
        logger.warning("  homepage unreachable: %s", err)
        summary["notes"] = f"homepage_unreachable:{err}"
        os.makedirs(ddir, exist_ok=True)
        _write_csv_header(pages_csv, PAGES_FIELDS_EXTENDED)
        _write_csv_header(errors_csv, storage.ERROR_FIELDS)
        _append_csv_row(errors_csv, storage.ERROR_FIELDS, {
            "url": seed_url, "error_type": "homepage_unreachable",
            "message": err, "http_status": "", "discovered_at": _now_iso(),
        })
        return summary

    homepage_html = resp.text if resp.status_code < 400 else ""
    final_netloc = urlparse(resp.url).netloc.lower()
    if final_netloc != netloc:
        summary["redirects_to"] = final_netloc

    # -- Detect tech stack ----------------------------------------------
    resp_headers = dict(resp.headers) if resp else {}
    tech = detect_tech_stack(homepage_html, resp_headers) if homepage_html else "unreachable"
    summary["tech_stack"] = tech
    logger.info("  tech stack: %s", tech)

    # -- Discover URLs --------------------------------------------------
    # When the seed redirects (e.g. england.nhs.uk -> www.england.nhs.uk),
    # try sitemaps and BFS on the final origin too.
    final_origin = f"{urlparse(resp.url).scheme}://{urlparse(resp.url).netloc}"
    origins_to_try = [origin]
    if final_origin != origin:
        origins_to_try.append(final_origin)

    sm_urls: List[str] = []
    for o in origins_to_try:
        sm_urls.extend(_discover_sitemap_urls(o))
    summary["sitemap_found"] = len(sm_urls) > 0

    allowed_hosts = {urlparse(o).netloc.lower() for o in origins_to_try}

    if sm_urls:
        urls_to_sample = _pick_diverse(sm_urls, SAMPLE_SIZE)
        logger.info("  sitemap: %d URLs, picked %d", len(sm_urls), len(urls_to_sample))
    else:
        if homepage_html:
            urls_to_sample = _bfs_collect(
                final_origin, homepage_html, SAMPLE_SIZE,
                allowed_netlocs=allowed_hosts,
            )
            logger.info("  BFS: found %d on-domain links", len(urls_to_sample))
        else:
            urls_to_sample = [seed_url]

    # -- Initialise CSVs ------------------------------------------------
    _write_csv_header(pages_csv, PAGES_FIELDS_EXTENDED)
    _write_csv_header(errors_csv, storage.ERROR_FIELDS)

    # -- Fetch & parse each page ----------------------------------------
    page_rows: List[Dict[str, Any]] = []
    kind_set: Set[str] = set()
    total_words = 0
    failed = 0

    for i, url in enumerate(urls_to_sample):
        if _interrupted:
            summary["notes"] = "interrupted"
            break

        html, status, final_url, ctype, resp_meta = _safe_fetch(url)

        if html is None:
            _append_csv_row(errors_csv, storage.ERROR_FIELDS, {
                "url": url, "error_type": "fetch_failed",
                "message": f"status={status}",
                "http_status": status, "discovered_at": _now_iso(),
            })
            failed += 1
            time.sleep(DELAY_SECONDS)
            continue

        ct_lower = (ctype or "").lower()
        if ct_lower and "text/html" not in ct_lower and "application/xhtml" not in ct_lower:
            _append_csv_row(errors_csv, storage.ERROR_FIELDS, {
                "url": url, "error_type": "non_html",
                "message": f"Content-Type: {ctype}",
                "http_status": status, "discovered_at": _now_iso(),
            })
            failed += 1
            time.sleep(DELAY_SECONDS)
            continue

        now = _now_iso()
        try:
            page_row, _ = parser_module.build_page_inventory_row(
                html,
                requested_url=url,
                final_url=final_url,
                http_status=status,
                content_type=ctype,
                referrer_url=seed_url,
                depth=0,
                discovered_at=now,
                response_meta=resp_meta,
                sitemap_meta={},
            )
        except Exception as exc:
            _append_csv_row(errors_csv, storage.ERROR_FIELDS, {
                "url": url, "error_type": "parse_error",
                "message": str(exc)[:300],
                "http_status": status, "discovered_at": _now_iso(),
            })
            failed += 1
            time.sleep(DELAY_SECONDS)
            continue

        page_row["tech_stack_detected"] = tech
        _append_csv_row(pages_csv, PAGES_FIELDS_EXTENDED, page_row)
        page_rows.append(page_row)

        kind = page_row.get("content_kind_guess", "")
        if kind:
            kind_set.add(kind)
        wc = page_row.get("word_count", 0)
        total_words += int(wc) if wc else 0

        if page_row.get("json_ld_types"):
            summary["has_json_ld"] = True
        if page_row.get("og_title") or page_row.get("og_type"):
            summary["has_og_tags"] = True
        if page_row.get("nav_link_count") and int(page_row["nav_link_count"]) > 0:
            summary["has_nav"] = True

        time.sleep(DELAY_SECONDS)

    n_ok = len(page_rows)
    summary["pages_sampled"] = n_ok
    summary["pages_failed"] = failed
    summary["avg_word_count"] = round(total_words / n_ok) if n_ok else 0
    summary["distinct_content_kinds"] = "|".join(sorted(kind_set))

    logger.info(
        "  done: %d pages OK, %d failed, kinds=%s",
        n_ok, failed, summary["distinct_content_kinds"] or "(none)",
    )
    return summary


def _safe_fetch(url: str) -> Tuple[Optional[str], int, str, str, Dict[str, str]]:
    """Wrapper around fetch_page that catches SSL/connection errors."""
    try:
        return _fetch_with_headers(url)
    except Exception as exc:
        logger.debug("Fetch exception for %s: %s", url, exc)
        return None, 0, url, "", {}


def _fetch_with_headers(url: str) -> Tuple[Optional[str], int, str, str, Dict[str, str]]:
    """Like scraper.fetch_page but always captures response headers."""
    headers = {"User-Agent": config.USER_AGENT}
    for attempt in range(2):
        try:
            resp = requests.get(
                url, headers=headers, timeout=TIMEOUT_SECONDS,
                allow_redirects=True,
            )
            final = resp.url
            ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
            status = resp.status_code
            meta = {
                "last_modified": (resp.headers.get("Last-Modified") or "").strip(),
                "etag": (resp.headers.get("ETag") or "").strip(),
            }
            if status >= 400:
                return None, status, final, ctype, meta
            return resp.text, status, final, ctype, meta
        except requests.exceptions.Timeout:
            if attempt == 1:
                return None, 0, url, "", {}
        except Exception:
            if attempt == 1:
                return None, 0, url, "", {}
    return None, 0, url, "", {}


# ---------------------------------------------------------------------------
# Summary & field-coverage writers
# ---------------------------------------------------------------------------

SUMMARY_FIELDS = (
    "domain", "seed_url", "tech_stack",
    "pages_sampled", "pages_failed",
    "sitemap_found", "redirects_to",
    "has_json_ld", "has_og_tags", "has_nav",
    "avg_word_count", "distinct_content_kinds", "notes",
)


def _write_summary(rows: List[Dict[str, Any]]) -> str:
    path = os.path.join(ANALYSIS_DIR, "summary.csv")
    os.makedirs(ANALYSIS_DIR, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f, fieldnames=SUMMARY_FIELDS,
            extrasaction="ignore", quoting=csv.QUOTE_ALL,
        )
        w.writeheader()
        for r in rows:
            w.writerow({k: _sanitise(r.get(k, "")) for k in SUMMARY_FIELDS})
    return path


def _build_field_coverage(summary_rows: List[Dict[str, Any]]) -> str:
    """
    Read every per-domain pages.csv, group by tech_stack, and compute fill
    rates for each PAGES_FIELDS column. Write field_coverage.csv.
    """
    stack_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for sr in summary_rows:
        netloc = sr["domain"]
        ddir = _domain_dir(netloc)
        pages_path = os.path.join(ddir, "pages.csv")
        if not os.path.isfile(pages_path):
            continue
        try:
            with open(pages_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    tech = row.get("tech_stack_detected", "").strip() or "static/unknown"
                    stack_rows[tech].append(row)
        except Exception:
            continue

    coverage_fields = ("tech_stack", "total_pages") + storage.PAGES_FIELDS
    path = os.path.join(ANALYSIS_DIR, "field_coverage.csv")

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=coverage_fields, quoting=csv.QUOTE_ALL)
        w.writeheader()

        for tech in sorted(stack_rows.keys()):
            rows = stack_rows[tech]
            total = len(rows)
            if total == 0:
                continue
            out: Dict[str, Any] = {"tech_stack": tech, "total_pages": total}
            for field in storage.PAGES_FIELDS:
                filled = sum(
                    1 for r in rows
                    if str(r.get(field, "")).strip() not in ("", "0")
                )
                pct = round(100 * filled / total, 1)
                out[field] = f"{pct}%"
            w.writerow(out)

    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _signal_handler(_signum, _frame):
    global _interrupted
    _interrupted = True
    logger.info("Signal received — finishing current domain then stopping.")


def _dedup_targets(urls: List[str]) -> List[str]:
    """Deduplicate by netloc, keeping the first occurrence."""
    seen: Set[str] = set()
    out: List[str] = []
    for u in urls:
        netloc = urlparse(u).netloc.lower()
        if netloc not in seen:
            seen.add(netloc)
            out.append(u)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre-crawl domain sampler")
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Process only the first N domains (0 = all)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                os.path.join(ANALYSIS_DIR, "pre_crawl.log"),
                encoding="utf-8",
            ),
        ],
        force=True,
    )
    os.makedirs(ANALYSIS_DIR, exist_ok=True)

    signal.signal(signal.SIGINT, _signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal_handler)

    targets = _dedup_targets(TARGET_URLS)
    if args.limit > 0:
        targets = targets[:args.limit]

    logger.info(
        "Pre-crawl analysis: %d unique domains, %d pages each",
        len(targets), SAMPLE_SIZE,
    )

    summaries: List[Dict[str, Any]] = []

    for idx, seed_url in enumerate(targets, 1):
        if _interrupted:
            break
        summary = _process_domain(seed_url, idx, len(targets))
        summaries.append(summary)

    summary_path = _write_summary(summaries)
    logger.info("Summary written to %s", summary_path)

    coverage_path = _build_field_coverage(summaries)
    logger.info("Field coverage written to %s", coverage_path)

    ok = sum(s["pages_sampled"] for s in summaries)
    fail = sum(s["pages_failed"] for s in summaries)
    logger.info("Done: %d pages sampled, %d failures across %d domains", ok, fail, len(summaries))
    return 0


if __name__ == "__main__":
    sys.exit(main())
