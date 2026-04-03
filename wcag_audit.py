"""
WCAG 2.1 accessibility audit engine.

Analyses crawl data against testable Level A and AA success criteria,
organised by the four WCAG principles: Perceivable, Operable,
Understandable, and Robust.

Each criterion returns a result dict with pass/fail counts, a
conformance percentage, and a list of failing page URLs.
"""

from __future__ import annotations

import csv
import os
from collections import Counter, defaultdict
from typing import Any, Dict, List

import config


def _read_pages(run_dirs: List[str]) -> List[Dict[str, str]]:
    rows = []
    for rd in run_dirs:
        path = os.path.join(rd, config.PAGES_CSV)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            rows.extend(csv.DictReader(f))
    return rows


def _safe_int(val: str, default: int = 0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(val: str, default: float = 0.0) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _criterion(
    cid: str, name: str, level: str, principle: str,
    total: int, passing: int, failing_pages: List[Dict[str, str]],
    description: str = "",
) -> Dict[str, Any]:
    pct = round(passing / total * 100, 1) if total else 100.0
    return {
        "id": cid,
        "name": name,
        "level": level,
        "principle": principle,
        "description": description,
        "total_pages": total,
        "passing": passing,
        "failing": total - passing,
        "conformance_pct": pct,
        "status": "pass" if pct == 100.0 else "partial" if pct >= 80 else "fail",
        "failing_pages": failing_pages[:50],
    }


def run_wcag_audit(run_dirs: List[str]) -> Dict[str, Any]:
    """Run all WCAG 2.1 Level A + AA checks and return structured results."""
    pages = _read_pages(run_dirs)
    total = len(pages)
    if not total:
        return {"total_pages": 0, "principles": {}, "summary": {}, "criteria": []}

    criteria = []

    # ════════════════════════════════════════════════════════════════
    # PRINCIPLE 1: PERCEIVABLE
    # ════════════════════════════════════════════════════════════════

    # 1.1.1 Non-text Content (Level A)
    fails_111 = []
    pass_111 = 0
    for r in pages:
        pct = _safe_float(r.get("wcag_img_alt_pct", "1"))
        if pct >= 1.0 or _safe_int(r.get("img_count", "0")) == 0:
            pass_111 += 1
        else:
            fails_111.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": f"{_safe_int(r.get('img_missing_alt_count', '0'))} of {_safe_int(r.get('img_count', '0'))} images missing alt",
            })
    criteria.append(_criterion(
        "1.1.1", "Non-text Content", "A", "perceivable", total, pass_111, fails_111,
        "All images must have text alternatives (alt attributes).",
    ))

    # 1.3.1 Info and Relationships — heading hierarchy (Level A)
    fails_131h = []
    pass_131h = 0
    for r in pages:
        if r.get("wcag_heading_order_valid", "") == "1":
            pass_131h += 1
        else:
            fails_131h.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": "Heading levels skip (e.g. H2 → H4)",
            })
    criteria.append(_criterion(
        "1.3.1a", "Info and Relationships — Headings", "A", "perceivable",
        total, pass_131h, fails_131h,
        "Heading levels must not skip (H1 → H3 without H2).",
    ))

    # 1.3.1 Info and Relationships — form labels (Level A)
    fails_131f = []
    pass_131f = 0
    for r in pages:
        pct = _safe_float(r.get("wcag_form_labels_pct", "1"))
        if pct >= 1.0:
            pass_131f += 1
        else:
            fails_131f.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": f"{round(pct * 100)}% of form inputs have labels",
            })
    criteria.append(_criterion(
        "1.3.1b", "Info and Relationships — Form Labels", "A", "perceivable",
        total, pass_131f, fails_131f,
        "Every form input must have an associated label.",
    ))

    # 1.3.1 Info and Relationships — data tables (Level A)
    fails_131t = []
    pass_131t = 0
    for r in pages:
        if _safe_int(r.get("wcag_tables_no_headers", "0")) == 0:
            pass_131t += 1
        else:
            fails_131t.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": f"{r.get('wcag_tables_no_headers', '0')} table(s) without header cells",
            })
    criteria.append(_criterion(
        "1.3.1c", "Info and Relationships — Data Tables", "A", "perceivable",
        total, pass_131t, fails_131t,
        "Data tables must use <th> elements to identify headers.",
    ))

    # 1.3.5 Identify Input Purpose (Level AA)
    fails_135 = []
    pass_135 = 0
    for r in pages:
        pct = _safe_float(r.get("wcag_autocomplete_pct", "1"))
        if pct >= 1.0:
            pass_135 += 1
        else:
            fails_135.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": f"{round(pct * 100)}% of inputs have autocomplete attribute",
            })
    criteria.append(_criterion(
        "1.3.5", "Identify Input Purpose", "AA", "perceivable",
        total, pass_135, fails_135,
        "Form inputs collecting user data should have autocomplete attributes.",
    ))

    # ════════════════════════════════════════════════════════════════
    # PRINCIPLE 2: OPERABLE
    # ════════════════════════════════════════════════════════════════

    # 2.4.1 Bypass Blocks (Level A)
    fails_241 = []
    pass_241 = 0
    for r in pages:
        if r.get("wcag_landmarks_present", "") == "1":
            pass_241 += 1
        else:
            fails_241.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": "No <main> landmark or skip link found",
            })
    criteria.append(_criterion(
        "2.4.1", "Bypass Blocks", "A", "operable", total, pass_241, fails_241,
        "Pages must provide a way to skip repeated navigation (landmark regions or skip links).",
    ))

    # 2.4.2 Page Titled (Level A)
    fails_242 = []
    pass_242 = 0
    for r in pages:
        if r.get("wcag_title_present", "") == "1":
            pass_242 += 1
        else:
            fails_242.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": "Page has no <title> element",
            })
    criteria.append(_criterion(
        "2.4.2", "Page Titled", "A", "operable", total, pass_242, fails_242,
        "Every page must have a descriptive title.",
    ))

    # 2.4.4 Link Purpose (Level A)
    fails_244 = []
    pass_244 = 0
    for r in pages:
        vague = _safe_float(r.get("wcag_vague_link_pct", "0"))
        if vague <= 0.05:
            pass_244 += 1
        else:
            fails_244.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": f"{round(vague * 100)}% of links have vague text (click here, read more, etc.)",
            })
    criteria.append(_criterion(
        "2.4.4", "Link Purpose (In Context)", "A", "operable",
        total, pass_244, fails_244,
        "Link text must describe the destination — avoid 'click here', 'read more'.",
    ))

    # 2.4.5 Multiple Ways (Level AA)
    fails_245 = []
    pass_245 = 0
    for r in pages:
        has_nav = r.get("wcag_has_nav", "") == "1"
        has_search = r.get("wcag_has_search", "") == "1"
        if has_nav or has_search:
            pass_245 += 1
        else:
            fails_245.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": "No navigation (<nav>) or search facility found",
            })
    criteria.append(_criterion(
        "2.4.5", "Multiple Ways", "AA", "operable", total, pass_245, fails_245,
        "More than one way to locate a page — e.g. navigation, search, or sitemap.",
    ))

    # 2.4.6 Headings and Labels (Level AA)
    fails_246 = []
    pass_246 = 0
    for r in pages:
        if _safe_int(r.get("wcag_empty_headings", "0")) == 0:
            pass_246 += 1
        else:
            fails_246.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": f"{r.get('wcag_empty_headings', '0')} empty heading element(s)",
            })
    criteria.append(_criterion(
        "2.4.6", "Headings and Labels", "AA", "operable",
        total, pass_246, fails_246,
        "Headings must be descriptive — empty heading tags fail this criterion.",
    ))

    # ════════════════════════════════════════════════════════════════
    # PRINCIPLE 3: UNDERSTANDABLE
    # ════════════════════════════════════════════════════════════════

    # 3.1.1 Language of Page (Level A)
    fails_311 = []
    pass_311 = 0
    for r in pages:
        if r.get("wcag_lang_valid", "") == "1":
            pass_311 += 1
        else:
            fails_311.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": f"lang attribute: '{r.get('lang', '')}'",
            })
    criteria.append(_criterion(
        "3.1.1", "Language of Page", "A", "understandable",
        total, pass_311, fails_311,
        "The <html> element must have a valid lang attribute.",
    ))

    # ════════════════════════════════════════════════════════════════
    # PRINCIPLE 4: ROBUST
    # ════════════════════════════════════════════════════════════════

    # 4.1.1 Parsing — duplicate IDs (Level A)
    fails_411 = []
    pass_411 = 0
    for r in pages:
        if _safe_int(r.get("wcag_duplicate_ids", "0")) == 0:
            pass_411 += 1
        else:
            fails_411.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": f"{r.get('wcag_duplicate_ids', '0')} duplicate id attribute(s)",
            })
    criteria.append(_criterion(
        "4.1.1", "Parsing", "A", "robust", total, pass_411, fails_411,
        "Elements must have unique ID attributes.",
    ))

    # 4.1.2 Name, Role, Value — empty buttons and links (Level A)
    fails_412 = []
    pass_412 = 0
    for r in pages:
        empty_btns = _safe_int(r.get("wcag_empty_buttons", "0"))
        empty_lnks = _safe_int(r.get("wcag_empty_links", "0"))
        if empty_btns == 0 and empty_lnks == 0:
            pass_412 += 1
        else:
            parts = []
            if empty_btns:
                parts.append(f"{empty_btns} button(s)")
            if empty_lnks:
                parts.append(f"{empty_lnks} link(s)")
            fails_412.append({
                "url": r.get("final_url", ""),
                "domain": r.get("domain", ""),
                "detail": f"{' and '.join(parts)} without accessible names",
            })
    criteria.append(_criterion(
        "4.1.2", "Name, Role, Value", "A", "robust",
        total, pass_412, fails_412,
        "All interactive elements (buttons, links) must have accessible names.",
    ))

    # ════════════════════════════════════════════════════════════════
    # SUMMARY
    # ════════════════════════════════════════════════════════════════

    principles = {}
    for c in criteria:
        p = c["principle"]
        if p not in principles:
            principles[p] = {"criteria_count": 0, "passing": 0, "failing": 0}
        principles[p]["criteria_count"] += 1
        if c["status"] == "pass":
            principles[p]["passing"] += 1
        else:
            principles[p]["failing"] += 1

    level_a = [c for c in criteria if c["level"] == "A"]
    level_aa = [c for c in criteria if c["level"] == "AA"]

    overall_pct = round(
        sum(c["conformance_pct"] for c in criteria) / len(criteria), 1
    ) if criteria else 0

    by_domain: Dict[str, Dict[str, int]] = defaultdict(lambda: {"issues": 0, "pages": set()})
    for c in criteria:
        for fp in c["failing_pages"]:
            d = fp.get("domain", "")
            by_domain[d]["issues"] += 1
            by_domain[d]["pages"].add(fp.get("url", ""))

    domain_summary = [
        {"domain": d, "issues": v["issues"], "pages_affected": len(v["pages"])}
        for d, v in sorted(by_domain.items(), key=lambda x: -x[1]["issues"])
    ][:20]

    return {
        "total_pages": total,
        "criteria_count": len(criteria),
        "overall_conformance_pct": overall_pct,
        "level_a_pass": sum(1 for c in level_a if c["status"] == "pass"),
        "level_a_total": len(level_a),
        "level_aa_pass": sum(1 for c in level_aa if c["status"] == "pass"),
        "level_aa_total": len(level_aa),
        "principles": {
            k: v for k, v in sorted(principles.items())
        },
        "by_domain": domain_summary,
        "criteria": criteria,
    }
