"""
Ecosystem mapping — data aggregation helpers.

Pure functions that read crawl CSVs from a run directory and return
JSON-serialisable dicts / lists consumed by the viz_api Blueprint.
"""
from __future__ import annotations

import csv
import math
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import tldextract

import config


# ── Helpers ──────────────────────────────────────────────────────────────

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


def _read_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""


def _parse_date(raw: str) -> Optional[str]:
    """Return YYYY-MM-DD from various date formats, or None."""
    raw = raw.strip()
    if not raw:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S+00:00",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S GMT",
    ):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
    if m:
        return m.group(1)
    return None


def _build_ownership_map(all_domains: List[str]) -> Dict[str, str]:
    """Derive ownership categories from the crawl's domain set.

    Priority order:
    1. Manual ``DOMAIN_OWNERSHIP_RULES`` (first match wins).
    2. Registered domain via ``tldextract`` — groups subdomains of the
       same organisation together (e.g. ``www.nhsbsa.nhs.uk`` and
       ``learning.nhsbsa.nhs.uk`` both map to ``nhsbsa.nhs.uk``) while
       keeping distinct organisations separate.
    """
    ownership: Dict[str, str] = {}

    for dom in all_domains:
        d = dom.lower()
        matched = False
        for suffix, label in config.DOMAIN_OWNERSHIP_RULES:
            if d.endswith(suffix.lower()) or d == suffix.lower():
                ownership[dom] = label
                matched = True
                break
        if not matched:
            ext = tldextract.extract(dom)
            ownership[dom] = ext.registered_domain or dom

    return ownership


def _ownership_fallback(domain: str) -> str:
    """Single-domain ownership lookup for domains outside the pre-built map."""
    d = domain.lower()
    for suffix, label in config.DOMAIN_OWNERSHIP_RULES:
        if d.endswith(suffix.lower()) or d == suffix.lower():
            return label
    return tldextract.extract(domain).registered_domain or domain


# ── Aggregate: per-domain summary ────────────────────────────────────────

def aggregate_domains(run_dir: str) -> List[Dict[str, Any]]:
    """
    Return one dict per domain with counts, quality metrics, freshness,
    analytics signals, status breakdown, and ownership category.
    """
    pages = _read_csv(os.path.join(run_dir, config.PAGES_CSV))
    if not pages:
        return []

    all_doms = list({r.get("domain", "unknown") for r in pages})
    ownership_map = _build_ownership_map(all_doms)

    domains: Dict[str, Dict[str, Any]] = {}

    for r in pages:
        dom = r.get("domain", "unknown")
        if dom not in domains:
            domains[dom] = {
                "domain": dom,
                "ownership": ownership_map.get(dom, config.DOMAIN_OWNERSHIP_DEFAULT),
                "page_count": 0,
                "total_words": 0,
                "total_images": 0,
                "images_missing_alt": 0,
                "readability_sum": 0.0,
                "readability_n": 0,
                "training_pages": 0,
                "status_codes": Counter(),
                "content_kinds": Counter(),
                "analytics_tools": set(),
                "analytics_tool_pages": Counter(),
                "has_privacy_policy": False,
                "privacy_policy_pages": 0,
                "dates_modified": [],
                "dates_published": [],
                "max_depth": 0,
                "link_internal_sum": 0,
                "link_external_sum": 0,
                "titles": [],
                "wcag_lang_valid_count": 0,
                "wcag_heading_order_valid_count": 0,
                "wcag_title_present_count": 0,
                "wcag_form_labels_sum": 0.0,
                "wcag_form_labels_n": 0,
                "wcag_landmarks_present_count": 0,
                "wcag_vague_link_sum": 0.0,
                "wcag_vague_link_n": 0,
            }
        d = domains[dom]
        d["page_count"] += 1
        d["total_words"] += _safe_int(r.get("word_count", "0"))
        d["total_images"] += _safe_int(r.get("img_count", "0"))
        d["images_missing_alt"] += _safe_int(r.get("img_missing_alt_count", "0"))

        rk = _safe_float(r.get("readability_fk_grade", ""))
        if rk > 0:
            d["readability_sum"] += rk
            d["readability_n"] += 1

        if r.get("training_related_flag", "").strip():
            d["training_pages"] += 1

        d["status_codes"][r.get("http_status", "?")] += 1
        kind = r.get("content_kind_guess", "").strip() or "(unclassified)"
        d["content_kinds"][kind] += 1

        for sig in r.get("analytics_signals", "").split("|"):
            sig = sig.strip()
            if sig:
                d["analytics_tools"].add(sig)
                d["analytics_tool_pages"][sig] += 1

        if r.get("privacy_policy_url", "").strip():
            d["has_privacy_policy"] = True
            d["privacy_policy_pages"] += 1

        dm = _parse_date(r.get("date_modified", ""))
        if dm:
            d["dates_modified"].append(dm)
        dp = _parse_date(r.get("date_published", ""))
        if dp:
            d["dates_published"].append(dp)

        sm = _parse_date(r.get("sitemap_lastmod", ""))
        if sm:
            d["dates_modified"].append(sm)

        depth = _safe_int(r.get("depth", "0"))
        if depth > d["max_depth"]:
            d["max_depth"] = depth

        d["link_internal_sum"] += _safe_int(r.get("link_count_internal", "0"))
        d["link_external_sum"] += _safe_int(r.get("link_count_external", "0"))

        title = r.get("title", "").strip()
        if title and len(d["titles"]) < 3:
            d["titles"].append(title)

        if r.get("wcag_lang_valid", "") == "1":
            d["wcag_lang_valid_count"] += 1
        if r.get("wcag_heading_order_valid", "") == "1":
            d["wcag_heading_order_valid_count"] += 1
        if r.get("wcag_title_present", "") == "1":
            d["wcag_title_present_count"] += 1
        fl = _safe_float(r.get("wcag_form_labels_pct", ""))
        if fl >= 0:
            d["wcag_form_labels_sum"] += fl
            d["wcag_form_labels_n"] += 1
        if r.get("wcag_landmarks_present", "") == "1":
            d["wcag_landmarks_present_count"] += 1
        vl = _safe_float(r.get("wcag_vague_link_pct", ""))
        if vl >= 0:
            d["wcag_vague_link_sum"] += vl
            d["wcag_vague_link_n"] += 1

    errors = _read_csv(os.path.join(run_dir, config.ERRORS_CSV))
    error_ctr: Dict[str, int] = Counter()
    for e in errors:
        edom = _extract_domain(e.get("url", ""))
        if not edom:
            edom = e.get("url", "").split("/")[2] if "/" in e.get("url", "") else "unknown"
        error_ctr[edom] += 1
        err_status = e.get("http_status", "").strip()
        if err_status and err_status != "0" and edom in domains:
            domains[edom]["status_codes"][err_status] += 1

    asset_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for name in sorted(os.listdir(run_dir)):
        if name.startswith("assets_") and name.endswith(".csv"):
            cat = name[len("assets_"):-len(".csv")]
            rows = _read_csv(os.path.join(run_dir, name))
            for ar in rows:
                ref = ar.get("referrer_page_url", "")
                adom = _extract_domain(ref)
                if adom:
                    asset_counts[adom][cat] += 1

    result = []
    for dom, d in domains.items():
        pc = d["page_count"]
        avg_words = round(d["total_words"] / pc) if pc else 0
        avg_readability = round(d["readability_sum"] / d["readability_n"], 1) if d["readability_n"] else 0
        alt_pct = round(d["images_missing_alt"] / d["total_images"] * 100, 1) if d["total_images"] else 0

        all_dates = sorted(d["dates_modified"] + d["dates_published"])
        latest_date = all_dates[-1] if all_dates else None
        oldest_date = all_dates[0] if all_dates else None

        result.append({
            "domain": dom,
            "ownership": d["ownership"],
            "page_count": pc,
            "avg_word_count": avg_words,
            "avg_readability": avg_readability,
            "total_images": d["total_images"],
            "images_missing_alt": d["images_missing_alt"],
            "alt_missing_pct": alt_pct,
            "training_pages": d["training_pages"],
            "error_count": error_ctr.get(dom, 0),
            "status_codes": dict(d["status_codes"]),
            "content_kinds": dict(d["content_kinds"]),
            "analytics_tools": sorted(d["analytics_tools"]),
            "analytics_tool_pages": dict(d["analytics_tool_pages"]),
            "has_privacy_policy": d["has_privacy_policy"],
            "privacy_policy_pages": d["privacy_policy_pages"],
            "latest_date": latest_date,
            "oldest_date": oldest_date,
            "date_count": len(all_dates),
            "max_depth": d["max_depth"],
            "avg_internal_links": round(d["link_internal_sum"] / pc, 1) if pc else 0,
            "avg_external_links": round(d["link_external_sum"] / pc, 1) if pc else 0,
            "titles": d["titles"],
            "assets": dict(asset_counts.get(dom, {})),
            "total_assets": sum(asset_counts.get(dom, {}).values()),
            "wcag_lang_pct": round(d["wcag_lang_valid_count"] / pc * 100, 1) if pc else 0,
            "wcag_heading_order_pct": round(d["wcag_heading_order_valid_count"] / pc * 100, 1) if pc else 0,
            "wcag_title_pct": round(d["wcag_title_present_count"] / pc * 100, 1) if pc else 0,
            "wcag_form_labels_pct": round(d["wcag_form_labels_sum"] / d["wcag_form_labels_n"] * 100, 1) if d["wcag_form_labels_n"] else 100.0,
            "wcag_landmarks_pct": round(d["wcag_landmarks_present_count"] / pc * 100, 1) if pc else 0,
            "wcag_vague_link_pct": round(d["wcag_vague_link_sum"] / d["wcag_vague_link_n"] * 100, 1) if d["wcag_vague_link_n"] else 0.0,
        })

    result.sort(key=lambda x: -x["page_count"])
    return result


# ── Aggregate: domain-to-domain graph ────────────────────────────────────

def aggregate_domain_graph(run_dir: str) -> Dict[str, Any]:
    """
    Return {nodes: [...], links: [...]} for force / chord / sankey layouts.
    Edges from edges.csv are collapsed from page-level to domain-level with
    weights. Self-links (intra-domain) are excluded.
    """
    pages = _read_csv(os.path.join(run_dir, config.PAGES_CSV))
    all_doms = list({r.get("domain", "unknown") for r in pages})
    ownership_map = _build_ownership_map(all_doms)

    domain_pages: Counter[str] = Counter()
    for r in pages:
        dom = r.get("domain", "unknown")
        domain_pages[dom] += 1

    edge_weights: Counter[Tuple[str, str]] = Counter()
    edges_path = os.path.join(run_dir, config.EDGES_CSV)
    if os.path.isfile(edges_path):
        try:
            with open(edges_path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    src = _extract_domain(row.get("from_url", ""))
                    tgt = _extract_domain(row.get("to_url", ""))
                    if src and tgt and src != tgt:
                        if src in domain_pages or tgt in domain_pages:
                            edge_weights[(src, tgt)] += 1
        except Exception:
            pass

    all_domains = set(domain_pages.keys())
    for s, t in edge_weights:
        all_domains.add(s)
        all_domains.add(t)

    domain_idx = {d: i for i, d in enumerate(sorted(all_domains))}

    nodes = []
    for dom in sorted(all_domains):
        nodes.append({
            "id": dom,
            "index": domain_idx[dom],
            "pages": domain_pages.get(dom, 0),
            "ownership": ownership_map.get(dom, _ownership_fallback(dom)),
        })

    links = []
    for (src, tgt), weight in edge_weights.most_common():
        links.append({
            "source": src,
            "target": tgt,
            "weight": weight,
        })

    return {"nodes": nodes, "links": links}


# ── Aggregate: tags ──────────────────────────────────────────────────────

def aggregate_tags(run_dir: str) -> Dict[str, Any]:
    """
    Return tag frequencies and co-occurrence pairs for word cloud and
    tag network visualisations.
    """
    tags_rows = _read_csv(os.path.join(run_dir, config.TAGS_CSV))

    freq: Counter[str] = Counter()
    source_freq: Counter[str] = Counter()
    page_tags: Dict[str, List[str]] = defaultdict(list)

    for r in tags_rows:
        tag = r.get("tag_value", "").strip()
        if not tag or len(tag) > 80:
            continue
        tag_lower = tag.lower()
        freq[tag_lower] += 1
        source_freq[r.get("tag_source", "")] += 1
        page_tags[r.get("page_url", "")].append(tag_lower)

    top_tags = freq.most_common(150)

    top_set = {t for t, _ in top_tags}
    cooccur: Counter[Tuple[str, str]] = Counter()
    for url, tag_list in page_tags.items():
        filtered = list(set(t for t in tag_list if t in top_set))
        for i in range(len(filtered)):
            for j in range(i + 1, len(filtered)):
                a, b = sorted([filtered[i], filtered[j]])
                cooccur[(a, b)] += 1

    top_pairs = cooccur.most_common(200)

    return {
        "tags": [{"tag": t, "count": c} for t, c in top_tags],
        "sources": dict(source_freq.most_common(20)),
        "cooccurrence": [
            {"source": a, "target": b, "weight": w}
            for (a, b), w in top_pairs
        ],
    }


# ── Aggregate: navigation hierarchy ─────────────────────────────────────

def aggregate_navigation(run_dir: str, domain: Optional[str] = None) -> Dict[str, Any]:
    """
    Build hierarchical navigation tree from nav_links.csv.

    When *domain* is given the tree has three levels:
        root (source domain)
        ├─ Internal Links
        │   ├─ /section-a
        │   │   ├─ Link Label …
        │   └─ /section-b
        │       └─ …
        └─ External Links
            ├─ target-domain-1
            │   └─ Link Label …
            └─ target-domain-2

    Path prefixes are the first non-empty segment of each internal URL;
    external links are grouped by their target domain.
    """
    nav_rows = _read_csv(os.path.join(run_dir, config.NAV_LINKS_CSV))
    if not nav_rows:
        return {"domains": [], "tree": None}

    by_domain: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for r in nav_rows:
        page_dom = _extract_domain(r.get("page_url", ""))
        nav_text = r.get("nav_text", "").strip()
        nav_href = r.get("nav_href", "").strip()
        if page_dom and nav_text:
            by_domain[page_dom][nav_text].add(nav_href)

    if domain and domain in by_domain:
        items = by_domain[domain]

        internal_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        external_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for text, hrefs in sorted(items.items()):
            href = next(iter(hrefs), "")
            target_dom = _extract_domain(href) if href else ""
            is_external = bool(target_dom and target_dom != domain)

            leaf = {
                "name": text,
                "href": href,
                "external": is_external,
                "size": len(hrefs),
            }

            if is_external:
                group_key = target_dom or "other"
                external_groups[group_key].append(leaf)
            else:
                parsed = urlparse(href)
                segments = [s for s in parsed.path.strip("/").split("/") if s]
                group_key = "/" + segments[0] if segments else "/"
                internal_groups[group_key].append(leaf)

        children = []

        if internal_groups:
            int_children = []
            for prefix in sorted(internal_groups):
                leaves = internal_groups[prefix]
                if len(leaves) == 1:
                    int_children.append(leaves[0])
                else:
                    int_children.append({
                        "name": prefix,
                        "group": True,
                        "children": sorted(leaves, key=lambda l: l["name"]),
                    })
            children.append({
                "name": "Internal",
                "group": True,
                "children": int_children,
            })

        if external_groups:
            ext_children = []
            for tgt_dom in sorted(external_groups):
                leaves = external_groups[tgt_dom]
                if len(leaves) == 1:
                    ext_children.append(leaves[0])
                else:
                    ext_children.append({
                        "name": tgt_dom,
                        "group": True,
                        "external": True,
                        "children": sorted(leaves, key=lambda l: l["name"]),
                    })
            children.append({
                "name": "External",
                "group": True,
                "external": True,
                "children": ext_children,
            })

        return {
            "domains": list(sorted(by_domain.keys())),
            "tree": {"name": domain, "children": children},
        }

    summaries = []
    for dom in sorted(by_domain.keys()):
        items = by_domain[dom]
        ext_count = 0
        for text, hrefs in items.items():
            for h in hrefs:
                if _extract_domain(h) != dom:
                    ext_count += 1
        summaries.append({
            "domain": dom,
            "nav_items": len(items),
            "external_links": ext_count,
        })
    summaries.sort(key=lambda x: -x["nav_items"])
    return {"domains": summaries, "tree": None}


# ── Aggregate: freshness timeline ────────────────────────────────────────

def aggregate_freshness(run_dir: str) -> Dict[str, Any]:
    """
    Return per-domain freshness data: latest modification dates, date
    distribution buckets, and stale/active classification.
    """
    pages = _read_csv(os.path.join(run_dir, config.PAGES_CSV))
    domain_dates: Dict[str, List[str]] = defaultdict(list)
    today = datetime.now().strftime("%Y-%m-%d")

    for r in pages:
        dom = r.get("domain", "unknown")
        for field in ("date_modified", "sitemap_lastmod", "date_published"):
            d = _parse_date(r.get(field, ""))
            if d:
                domain_dates[dom].append(d)

    result = []
    for dom, dates in sorted(domain_dates.items()):
        if not dates:
            continue
        dates_sorted = sorted(dates)
        latest = dates_sorted[-1]
        oldest = dates_sorted[0]

        buckets: Counter[str] = Counter()
        for d in dates_sorted:
            ym = d[:7]
            buckets[ym] += 1

        result.append({
            "domain": dom,
            "latest": latest,
            "oldest": oldest,
            "total_dates": len(dates_sorted),
            "buckets": dict(sorted(buckets.items())),
        })

    result.sort(key=lambda x: x.get("latest", ""), reverse=True)
    return {"today": today, "domains": result}


# ── Aggregate: chord matrix ──────────────────────────────────────────────

def aggregate_chord(run_dir: str, top_n: int = 30) -> Dict[str, Any]:
    """
    Return a square matrix of inter-domain link counts for the top N
    domains by page count, suitable for d3-chord.
    """
    pages = _read_csv(os.path.join(run_dir, config.PAGES_CSV))
    domain_pages: Counter[str] = Counter()
    for r in pages:
        domain_pages[r.get("domain", "unknown")] += 1

    top_domains = [d for d, _ in domain_pages.most_common(top_n)]
    dom_set = set(top_domains)
    dom_idx = {d: i for i, d in enumerate(top_domains)}
    n = len(top_domains)
    matrix = [[0] * n for _ in range(n)]

    edges_path = os.path.join(run_dir, config.EDGES_CSV)
    if os.path.isfile(edges_path):
        try:
            with open(edges_path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    src = _extract_domain(row.get("from_url", ""))
                    tgt = _extract_domain(row.get("to_url", ""))
                    if src in dom_set and tgt in dom_set and src != tgt:
                        matrix[dom_idx[src]][dom_idx[tgt]] += 1
        except Exception:
            pass

    return {"domains": top_domains, "matrix": matrix}
