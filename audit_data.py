"""
Content audit engine — generates structured findings from crawl data.

Each ``audit_*`` function reads CSVs from one or more run directories and
returns a dict with a severity summary, a count, and a list of finding
rows.  The top-level ``run_full_audit`` orchestrates all checks and
returns them keyed by finding type.
"""

from __future__ import annotations

import csv
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import config


# ── Helpers ──────────────────────────────────────────────────────────────

def _read_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _read_pages(run_dirs: List[str]) -> List[Dict[str, str]]:
    rows = []
    for rd in run_dirs:
        rows.extend(_read_csv(os.path.join(rd, config.PAGES_CSV)))
    return rows


def _read_edges(run_dirs: List[str]) -> List[Dict[str, str]]:
    rows = []
    for rd in run_dirs:
        rows.extend(_read_csv(os.path.join(rd, config.EDGES_CSV)))
    return rows


def _read_errors(run_dirs: List[str]) -> List[Dict[str, str]]:
    rows = []
    for rd in run_dirs:
        rows.extend(_read_csv(os.path.join(rd, config.ERRORS_CSV)))
    return rows


def _severity(pct: float) -> str:
    """Map a percentage of affected pages to a severity label."""
    if pct >= 25:
        return "high"
    if pct >= 10:
        return "medium"
    if pct > 0:
        return "low"
    return "none"


def _safe_int(val: str, default: int = 0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


# ═══════════════════════════════════════════════════════════════════════════
# 1. DUPLICATE CONTENT
# ═══════════════════════════════════════════════════════════════════════════

def audit_duplicate_content(
    run_dirs: List[str],
) -> Dict[str, Any]:
    pages = _read_pages(run_dirs)
    total = len(pages)
    if not total:
        return {"id": "duplicate_content", "title": "Duplicate Content",
                "count": 0, "severity": "none", "findings": [], "total": 0}

    title_map: Dict[str, List[str]] = defaultdict(list)
    desc_map: Dict[str, List[str]] = defaultdict(list)
    both_map: Dict[Tuple[str, str], List[str]] = defaultdict(list)

    for r in pages:
        url = r.get("final_url", "")
        title = r.get("title", "").strip()
        desc = r.get("meta_description", "").strip()
        if title:
            title_map[title].append(url)
        if desc and len(desc) > 20:
            desc_map[desc].append(url)
        if title and desc:
            both_map[(title, desc)].append(url)

    findings = []

    for (title, desc), urls in sorted(
        both_map.items(), key=lambda x: -len(x[1])
    ):
        if len(urls) > 1:
            findings.append({
                "type": "title_and_description",
                "title": title[:100],
                "description": desc[:120],
                "urls": urls[:20],
                "count": len(urls),
            })

    dup_title_only = []
    for title, urls in sorted(title_map.items(), key=lambda x: -len(x[1])):
        if len(urls) > 1:
            dup_title_only.append({
                "type": "title_only",
                "title": title[:100],
                "urls": urls[:20],
                "count": len(urls),
            })

    affected = sum(len(f["urls"]) for f in findings) + sum(
        len(f["urls"]) for f in dup_title_only
    )

    return {
        "id": "duplicate_content",
        "title": "Duplicate Content",
        "count": len(findings) + len(dup_title_only),
        "affected_pages": min(affected, total),
        "severity": _severity(affected / total * 100 if total else 0),
        "total": total,
        "findings": findings[:50],
        "title_duplicates": dup_title_only[:50],
    }


# ═══════════════════════════════════════════════════════════════════════════
# 2. REDIRECT MAPPING
# ═══════════════════════════════════════════════════════════════════════════

def audit_redirects(run_dirs: List[str]) -> Dict[str, Any]:
    pages = _read_pages(run_dirs)
    total = len(pages)

    redirects = []
    cross_domain = 0
    for r in pages:
        req = r.get("requested_url", "").strip()
        final = r.get("final_url", "").strip()
        if req and final and req != final:
            req_host = urlparse(req).hostname or ""
            final_host = urlparse(final).hostname or ""
            is_cross = req_host.lower() != final_host.lower()
            if is_cross:
                cross_domain += 1
            redirects.append({
                "from_url": req,
                "to_url": final,
                "from_domain": req_host,
                "to_domain": final_host,
                "cross_domain": is_cross,
            })

    return {
        "id": "redirects",
        "title": "Redirect Mapping",
        "count": len(redirects),
        "cross_domain_count": cross_domain,
        "severity": _severity(len(redirects) / total * 100 if total else 0),
        "total": total,
        "findings": sorted(redirects, key=lambda x: -x["cross_domain"])[:100],
    }


# ═══════════════════════════════════════════════════════════════════════════
# 3. THIN CONTENT
# ═══════════════════════════════════════════════════════════════════════════

def audit_thin_content(run_dirs: List[str]) -> Dict[str, Any]:
    pages = _read_pages(run_dirs)
    total = len(pages)

    findings = []
    for r in pages:
        words = _safe_int(r.get("word_count", "0"))
        headings = r.get("heading_outline", "").strip()
        int_links = _safe_int(r.get("link_count_internal", "0"))
        url = r.get("final_url", "")

        issues = []
        if words < 50:
            issues.append(f"{words} words")
        if not headings:
            issues.append("no headings")
        if int_links == 0:
            issues.append("no internal links")

        if issues:
            findings.append({
                "url": url,
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
                "word_count": words,
                "has_headings": bool(headings),
                "internal_links": int_links,
                "issues": issues,
            })

    findings.sort(key=lambda x: len(x["issues"]), reverse=True)

    by_domain = Counter(f["domain"] for f in findings)

    return {
        "id": "thin_content",
        "title": "Thin Content",
        "count": len(findings),
        "severity": _severity(len(findings) / total * 100 if total else 0),
        "total": total,
        "by_domain": [{"domain": d, "count": c} for d, c in by_domain.most_common(20)],
        "findings": findings[:100],
    }


# ═══════════════════════════════════════════════════════════════════════════
# 4. TITLE & META QUALITY
# ═══════════════════════════════════════════════════════════════════════════

def audit_title_meta(run_dirs: List[str]) -> Dict[str, Any]:
    pages = _read_pages(run_dirs)
    total = len(pages)

    findings = []
    for r in pages:
        url = r.get("final_url", "")
        title = r.get("title", "").strip()
        desc = r.get("meta_description", "").strip()

        issues = []
        if not title:
            issues.append("missing title")
        elif len(title) < 30:
            issues.append(f"short title ({len(title)} chars)")
        elif len(title) > 60:
            issues.append(f"long title ({len(title)} chars)")

        if not desc:
            issues.append("missing description")
        elif len(desc) < 70:
            issues.append(f"short description ({len(desc)} chars)")
        elif len(desc) > 160:
            issues.append(f"long description ({len(desc)} chars)")

        if issues:
            findings.append({
                "url": url,
                "domain": r.get("domain", ""),
                "title": title[:80] or "(empty)",
                "title_length": len(title),
                "description_length": len(desc),
                "issues": issues,
            })

    summary = {
        "missing_title": sum(1 for f in findings if "missing title" in f["issues"]),
        "short_title": sum(1 for f in findings if any("short title" in i for i in f["issues"])),
        "long_title": sum(1 for f in findings if any("long title" in i for i in f["issues"])),
        "missing_description": sum(1 for f in findings if "missing description" in f["issues"]),
        "short_description": sum(1 for f in findings if any("short description" in i for i in f["issues"])),
        "long_description": sum(1 for f in findings if any("long description" in i for i in f["issues"])),
    }

    return {
        "id": "title_meta",
        "title": "Title & Meta Description Quality",
        "count": len(findings),
        "severity": _severity(len(findings) / total * 100 if total else 0),
        "total": total,
        "summary": summary,
        "findings": findings[:100],
    }


# ═══════════════════════════════════════════════════════════════════════════
# 5. ORPHAN PAGES
# ═══════════════════════════════════════════════════════════════════════════

def audit_orphan_pages(run_dirs: List[str]) -> Dict[str, Any]:
    pages = _read_pages(run_dirs)
    edges = _read_edges(run_dirs)
    total = len(pages)

    linked_urls: set = set()
    for e in edges:
        linked_urls.add(e.get("to_url", "").strip())

    findings = []
    for r in pages:
        url = r.get("final_url", "")
        referrer = r.get("referrer_url", "")
        is_sitemap_only = referrer.startswith("sitemap:")
        is_linked = url in linked_urls

        if not is_linked:
            findings.append({
                "url": url,
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
                "referrer": referrer[:100],
                "sitemap_only": is_sitemap_only,
                "depth": _safe_int(r.get("depth", "0")),
            })

    by_domain = Counter(f["domain"] for f in findings)

    return {
        "id": "orphan_pages",
        "title": "Orphan Pages",
        "count": len(findings),
        "severity": _severity(len(findings) / total * 100 if total else 0),
        "total": total,
        "sitemap_only_count": sum(1 for f in findings if f["sitemap_only"]),
        "by_domain": [{"domain": d, "count": c} for d, c in by_domain.most_common(20)],
        "findings": findings[:100],
    }


# ═══════════════════════════════════════════════════════════════════════════
# 6. INTERNAL LINK DISTRIBUTION
# ═══════════════════════════════════════════════════════════════════════════

def audit_link_distribution(run_dirs: List[str]) -> Dict[str, Any]:
    pages = _read_pages(run_dirs)
    edges = _read_edges(run_dirs)

    inbound: Counter = Counter()
    outbound: Counter = Counter()
    for e in edges:
        from_url = e.get("from_url", "").strip()
        to_url = e.get("to_url", "").strip()
        if from_url:
            outbound[from_url] += 1
        if to_url:
            inbound[to_url] += 1

    page_urls = {r.get("final_url", "") for r in pages}
    page_lookup = {r.get("final_url", ""): r for r in pages}

    zero_inbound = []
    for url in page_urls:
        if inbound[url] == 0:
            r = page_lookup.get(url, {})
            zero_inbound.append({
                "url": url,
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
            })

    top_linked = []
    for url, count in inbound.most_common(30):
        r = page_lookup.get(url, {})
        top_linked.append({
            "url": url,
            "domain": r.get("domain", ""),
            "title": r.get("title", "")[:80],
            "inbound_links": count,
        })

    link_sinks = []
    for url in page_urls:
        if inbound[url] >= 5 and outbound[url] <= 1:
            r = page_lookup.get(url, {})
            link_sinks.append({
                "url": url,
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
                "inbound": inbound[url],
                "outbound": outbound[url],
            })
    link_sinks.sort(key=lambda x: -x["inbound"])

    all_inbound = [inbound.get(url, 0) for url in page_urls]
    avg = sum(all_inbound) / len(all_inbound) if all_inbound else 0
    median_idx = len(all_inbound) // 2
    sorted_inbound = sorted(all_inbound)
    median = sorted_inbound[median_idx] if sorted_inbound else 0

    return {
        "id": "link_distribution",
        "title": "Internal Link Distribution",
        "count": len(zero_inbound),
        "severity": _severity(len(zero_inbound) / len(pages) * 100 if pages else 0),
        "total": len(pages),
        "total_edges": len(edges),
        "avg_inbound": round(avg, 1),
        "median_inbound": median,
        "zero_inbound_count": len(zero_inbound),
        "zero_inbound": zero_inbound[:50],
        "top_linked": top_linked,
        "link_sinks": link_sinks[:30],
    }


# ═══════════════════════════════════════════════════════════════════════════
# 7. IMAGE ACCESSIBILITY
# ═══════════════════════════════════════════════════════════════════════════

def audit_image_accessibility(run_dirs: List[str]) -> Dict[str, Any]:
    pages = _read_pages(run_dirs)

    findings = []
    total_images = 0
    total_missing = 0

    for r in pages:
        imgs = _safe_int(r.get("img_count", "0"))
        missing = _safe_int(r.get("img_missing_alt_count", "0"))
        total_images += imgs
        total_missing += missing

        if missing > 0:
            findings.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
                "img_count": imgs,
                "missing_alt": missing,
                "pct_missing": round(missing / imgs * 100, 1) if imgs else 0,
            })

    findings.sort(key=lambda x: -x["missing_alt"])

    by_domain: Dict[str, Dict[str, int]] = {}
    for f in findings:
        d = f["domain"]
        if d not in by_domain:
            by_domain[d] = {"images": 0, "missing": 0, "pages": 0}
        by_domain[d]["images"] += f["img_count"]
        by_domain[d]["missing"] += f["missing_alt"]
        by_domain[d]["pages"] += 1

    domain_summary = [
        {
            "domain": d,
            "pages_affected": v["pages"],
            "total_images": v["images"],
            "missing_alt": v["missing"],
            "pct_missing": round(v["missing"] / v["images"] * 100, 1) if v["images"] else 0,
        }
        for d, v in sorted(by_domain.items(), key=lambda x: -x[1]["missing"])
    ]

    return {
        "id": "image_accessibility",
        "title": "Image Accessibility",
        "count": len(findings),
        "severity": _severity(total_missing / total_images * 100 if total_images else 0),
        "total": len(pages),
        "total_images": total_images,
        "total_missing_alt": total_missing,
        "pct_missing": round(total_missing / total_images * 100, 1) if total_images else 0,
        "by_domain": domain_summary[:20],
        "findings": findings[:100],
    }


# ═══════════════════════════════════════════════════════════════════════════
# 8. URL STRUCTURE
# ═══════════════════════════════════════════════════════════════════════════

def audit_url_structure(run_dirs: List[str]) -> Dict[str, Any]:
    pages = _read_pages(run_dirs)

    depth_dist: Counter = Counter()
    length_dist: Counter = Counter()
    query_params: Counter = Counter()
    deep_urls = []

    for r in pages:
        url = r.get("final_url", "")
        parsed = urlparse(url)
        segments = [s for s in parsed.path.strip("/").split("/") if s]
        depth = len(segments)
        depth_dist[depth] += 1

        url_len = len(url)
        bucket = (url_len // 25) * 25
        length_dist[bucket] += 1

        for key in parse_qs(parsed.query):
            query_params[key] += 1

        if depth >= 5:
            deep_urls.append({
                "url": url,
                "domain": r.get("domain", ""),
                "depth": depth,
                "title": r.get("title", "")[:80],
            })

    deep_urls.sort(key=lambda x: -x["depth"])

    return {
        "id": "url_structure",
        "title": "URL Structure Analysis",
        "count": len(deep_urls),
        "severity": _severity(len(deep_urls) / len(pages) * 100 if pages else 0),
        "total": len(pages),
        "depth_distribution": [
            {"depth": d, "count": c}
            for d, c in sorted(depth_dist.items())
        ],
        "length_distribution": [
            {"bucket": f"{b}-{b+24}", "count": c}
            for b, c in sorted(length_dist.items())
        ],
        "top_query_params": [
            {"param": p, "count": c}
            for p, c in query_params.most_common(20)
        ],
        "deep_urls": deep_urls[:50],
    }


# ═══════════════════════════════════════════════════════════════════════════
# 9. CONTENT DECAY
# ═══════════════════════════════════════════════════════════════════════════

_ISO_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _extract_year(raw: str) -> Optional[int]:
    m = _ISO_DATE_RE.search(raw)
    if m:
        try:
            return int(m.group(1)[:4])
        except ValueError:
            pass
    return None


def audit_content_decay(run_dirs: List[str]) -> Dict[str, Any]:
    pages = _read_pages(run_dirs)
    today = datetime.now()
    current_year = today.year

    findings = []
    never_updated = 0
    stale_count = 0

    for r in pages:
        pub = r.get("date_published", "").strip()
        mod = r.get("date_modified", "").strip()
        url = r.get("final_url", "")

        if not pub:
            continue

        pub_year = _extract_year(pub)
        mod_year = _extract_year(mod) if mod else None
        age_years = (current_year - pub_year) if pub_year else None

        issues = []
        if pub and not mod:
            never_updated += 1
            issues.append("never updated")
        if age_years and age_years >= 3 and not mod:
            stale_count += 1
            issues.append(f"published {age_years}+ years ago, never modified")
        elif age_years and age_years >= 3 and mod_year and (current_year - mod_year) >= 2:
            stale_count += 1
            issues.append(f"published {age_years}+ years ago, last modified {current_year - mod_year}+ years ago")

        if issues:
            findings.append({
                "url": url,
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
                "date_published": pub[:10],
                "date_modified": mod[:10] if mod else "",
                "age_years": age_years,
                "issues": issues,
            })

    findings.sort(key=lambda x: x.get("age_years") or 0, reverse=True)

    by_domain = Counter(f["domain"] for f in findings)

    return {
        "id": "content_decay",
        "title": "Content Decay",
        "count": len(findings),
        "severity": _severity(stale_count / len(pages) * 100 if pages else 0),
        "total": len(pages),
        "never_updated": never_updated,
        "stale_3plus_years": stale_count,
        "by_domain": [{"domain": d, "count": c} for d, c in by_domain.most_common(20)],
        "findings": findings[:100],
    }


# ═══════════════════════════════════════════════════════════════════════════
# 10. BROKEN LINK HOTSPOTS
# ═══════════════════════════════════════════════════════════════════════════

def audit_broken_links(run_dirs: List[str]) -> Dict[str, Any]:
    pages = _read_pages(run_dirs)
    total_pages = len(pages)
    errors = _read_errors(run_dirs)

    by_type: Counter = Counter()
    by_domain: Counter = Counter()
    by_status: Counter = Counter()

    findings = []
    for e in errors:
        url = e.get("url", "")
        err_type = e.get("error_type", "")
        message = e.get("message", "")[:200]
        status = e.get("http_status", "")
        domain = urlparse(url).hostname or ""

        by_type[err_type] += 1
        by_domain[domain] += 1
        if status and status != "0":
            by_status[status] += 1

        findings.append({
            "url": url,
            "domain": domain,
            "error_type": err_type,
            "http_status": status,
            "message": message,
        })

    return {
        "id": "broken_links",
        "title": "Broken Links & Errors",
        "count": len(findings),
        "severity": _severity(len(findings) / max(total_pages, 1) * 100) if findings else "none",
        "total": total_pages,
        "by_type": [{"type": t, "count": c} for t, c in by_type.most_common(10)],
        "by_domain": [{"domain": d, "count": c} for d, c in by_domain.most_common(20)],
        "by_status": [{"status": s, "count": c} for s, c in by_status.most_common(10)],
        "findings": findings[:100],
    }


# ═══════════════════════════════════════════════════════════════════════════
# FULL AUDIT
# ═══════════════════════════════════════════════════════════════════════════

def run_full_audit(run_dirs: List[str]) -> Dict[str, Any]:
    """Execute all audit checks and return a combined report."""
    checks = [
        audit_duplicate_content(run_dirs),
        audit_redirects(run_dirs),
        audit_thin_content(run_dirs),
        audit_title_meta(run_dirs),
        audit_orphan_pages(run_dirs),
        audit_link_distribution(run_dirs),
        audit_image_accessibility(run_dirs),
        audit_url_structure(run_dirs),
        audit_content_decay(run_dirs),
        audit_broken_links(run_dirs),
    ]

    high = sum(1 for c in checks if c["severity"] == "high")
    medium = sum(1 for c in checks if c["severity"] == "medium")
    low = sum(1 for c in checks if c["severity"] == "low")
    total_findings = sum(c["count"] for c in checks)

    return {
        "summary": {
            "checks_run": len(checks),
            "high": high,
            "medium": medium,
            "low": low,
            "total_findings": total_findings,
        },
        "checks": {c["id"]: c for c in checks},
    }
