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


# ── Global filter ─────────────────────────────────────────────────────────

def filter_pages(
    rows: List[Dict[str, str]],
    filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    """Apply cross-cutting filters to page rows.

    Supported filter keys (all optional, additive AND logic):
      domains        — list of domain strings
      ownership      — list of ownership labels (matched via ownership map)
      cms            — list of CMS generator substrings (case-insensitive)
      content_kinds  — list of content_kind_guess values
      schema_formats — list from {"json_ld", "microdata", "rdfa"}
      schema_types   — list of schema type substrings
      date_from      — YYYY-MM-DD lower bound on date_published / date_modified
      date_to        — YYYY-MM-DD upper bound
      min_coverage   — float 0-100 minimum extraction_coverage_pct
    """
    if not filters:
        return rows

    result = rows

    if filters.get("domains"):
        allowed = {d.lower() for d in filters["domains"]}
        result = [r for r in result if r.get("domain", "").lower() in allowed]

    if filters.get("cms"):
        cms_lower = [c.lower() for c in filters["cms"]]
        result = [
            r for r in result
            if any(c in r.get("cms_generator", "").lower() for c in cms_lower)
        ]

    if filters.get("content_kinds"):
        allowed_kinds = {k.lower() for k in filters["content_kinds"]}
        result = [
            r for r in result
            if r.get("content_kind_guess", "").lower() in allowed_kinds
        ]

    if filters.get("schema_formats"):
        fmts = {f.lower() for f in filters["schema_formats"]}

        def _has_format(row: Dict[str, str]) -> bool:
            if "json_ld" in fmts and row.get("json_ld_types", "").strip():
                return True
            if "microdata" in fmts and row.get("microdata_types", "").strip():
                return True
            if "rdfa" in fmts and row.get("rdfa_types", "").strip():
                return True
            return False
        result = [r for r in result if _has_format(r)]

    if filters.get("schema_types"):
        types_lower = [t.lower() for t in filters["schema_types"]]

        def _has_type(row: Dict[str, str]) -> bool:
            combined = (
                row.get("json_ld_types", "") + "|" +
                row.get("microdata_types", "") + "|" +
                row.get("rdfa_types", "")
            ).lower()
            return any(t in combined for t in types_lower)
        result = [r for r in result if _has_type(r)]

    if filters.get("date_from"):
        df = filters["date_from"]
        result = [
            r for r in result
            if (r.get("date_published", "") >= df or
                r.get("date_modified", "") >= df)
        ]

    if filters.get("date_to"):
        dt = filters["date_to"]
        result = [
            r for r in result
            if (r.get("date_published", "")[:10] <= dt or
                r.get("date_modified", "")[:10] <= dt or
                (not r.get("date_published") and not r.get("date_modified")))
        ]

    if filters.get("min_coverage") is not None:
        mc = float(filters["min_coverage"])
        result = [
            r for r in result
            if _safe_float(r.get("extraction_coverage_pct", "0")) >= mc
        ]

    return result


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

def aggregate_domains(
    run_dir: str,
    filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Return one dict per domain with counts, quality metrics, freshness,
    analytics signals, status breakdown, ownership, and Phase 4 fields.
    """
    pages = filter_pages(
        _read_csv(os.path.join(run_dir, config.PAGES_CSV)), filters,
    )
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
                # Phase 4 accumulators
                "cms_generators": Counter(),
                "authors": Counter(),
                "publishers": Counter(),
                "has_json_ld_count": 0,
                "has_microdata_count": 0,
                "has_rdfa_count": 0,
                "has_hreflang_count": 0,
                "has_feed_count": 0,
                "has_pagination_count": 0,
                "has_breadcrumb_schema_count": 0,
                "robots_noindex_count": 0,
                "schema_types": Counter(),
                "extraction_coverage_sum": 0.0,
                "extraction_coverage_n": 0,
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

        # Phase 4 accumulation
        cms = r.get("cms_generator", "").strip()
        if cms:
            d["cms_generators"][cms] += 1
        author = r.get("author", "").strip()
        if author:
            d["authors"][author] += 1
        publisher = r.get("publisher", "").strip()
        if publisher:
            d["publishers"][publisher] += 1
        if r.get("json_ld_types", "").strip():
            d["has_json_ld_count"] += 1
            for t in r["json_ld_types"].split("|"):
                t = t.strip()
                if t:
                    d["schema_types"][t] += 1
        if r.get("microdata_types", "").strip():
            d["has_microdata_count"] += 1
            for t in r["microdata_types"].split("|"):
                t = t.strip()
                if t:
                    d["schema_types"][t] += 1
        if r.get("rdfa_types", "").strip():
            d["has_rdfa_count"] += 1
        if r.get("hreflang_links", "").strip():
            d["has_hreflang_count"] += 1
        if r.get("feed_urls", "").strip():
            d["has_feed_count"] += 1
        if r.get("pagination_next", "").strip() or r.get("pagination_prev", "").strip():
            d["has_pagination_count"] += 1
        if r.get("breadcrumb_schema", "").strip():
            d["has_breadcrumb_schema_count"] += 1
        robots = r.get("robots_directives", "").lower()
        if "noindex" in robots:
            d["robots_noindex_count"] += 1
        cov = _safe_float(r.get("extraction_coverage_pct", ""))
        if cov > 0:
            d["extraction_coverage_sum"] += cov
            d["extraction_coverage_n"] += 1

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

        top_cms = d["cms_generators"].most_common(1)
        primary_cms = top_cms[0][0] if top_cms else ""

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
            # Phase 4 fields
            "cms_generator": primary_cms,
            "cms_generators": dict(d["cms_generators"]),
            "top_authors": [a for a, _ in d["authors"].most_common(5)],
            "top_publishers": [p for p, _ in d["publishers"].most_common(3)],
            "has_json_ld_pct": round(d["has_json_ld_count"] / pc * 100, 1) if pc else 0,
            "has_microdata_pct": round(d["has_microdata_count"] / pc * 100, 1) if pc else 0,
            "has_rdfa_pct": round(d["has_rdfa_count"] / pc * 100, 1) if pc else 0,
            "has_hreflang_pct": round(d["has_hreflang_count"] / pc * 100, 1) if pc else 0,
            "has_feed_pct": round(d["has_feed_count"] / pc * 100, 1) if pc else 0,
            "has_pagination_pct": round(d["has_pagination_count"] / pc * 100, 1) if pc else 0,
            "has_breadcrumb_schema_pct": round(d["has_breadcrumb_schema_count"] / pc * 100, 1) if pc else 0,
            "robots_noindex_pct": round(d["robots_noindex_count"] / pc * 100, 1) if pc else 0,
            "schema_types": dict(d["schema_types"].most_common(20)),
            "avg_extraction_coverage": round(d["extraction_coverage_sum"] / d["extraction_coverage_n"], 1) if d["extraction_coverage_n"] else 0,
        })

    result.sort(key=lambda x: -x["page_count"])
    return result


# ── Aggregate: domain-to-domain graph ────────────────────────────────────

def aggregate_domain_graph(
    run_dir: str, filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Return {nodes: [...], links: [...]} for force / chord / sankey layouts.
    Edges from edges.csv are collapsed from page-level to domain-level with
    weights. Self-links (intra-domain) are excluded.
    """
    pages = filter_pages(
        _read_csv(os.path.join(run_dir, config.PAGES_CSV)), filters,
    )
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

def aggregate_tags(
    run_dir: str, filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Return tag frequencies and co-occurrence pairs for word cloud and
    tag network visualisations.
    """
    # When filters are active, restrict to tags from filtered page URLs
    allowed_urls: Optional[set] = None
    if filters:
        pages = filter_pages(
            _read_csv(os.path.join(run_dir, config.PAGES_CSV)), filters,
        )
        allowed_urls = {r.get("final_url", "") for r in pages}

    tags_rows = _read_csv(os.path.join(run_dir, config.TAGS_CSV))

    freq: Counter[str] = Counter()
    source_freq: Counter[str] = Counter()
    page_tags: Dict[str, List[str]] = defaultdict(list)

    for r in tags_rows:
        if allowed_urls is not None and r.get("page_url", "") not in allowed_urls:
            continue
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

def aggregate_navigation(
    run_dir: str, domain: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
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

def aggregate_freshness(
    run_dir: str, filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Return per-domain freshness data: latest modification dates, date
    distribution buckets, and stale/active classification.
    """
    pages = filter_pages(
        _read_csv(os.path.join(run_dir, config.PAGES_CSV)), filters,
    )
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

def aggregate_chord(
    run_dir: str, top_n: int = 30,
    filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Return a square matrix of inter-domain link counts for the top N
    domains by page count, suitable for d3-chord.
    """
    pages = filter_pages(
        _read_csv(os.path.join(run_dir, config.PAGES_CSV)), filters,
    )
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


# ── Aggregate: technology & standards ────────────────────────────────────

def aggregate_technology(
    run_dir: str, filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """CMS distribution, structured data adoption, schema type frequencies,
    and SEO readiness per domain."""
    pages = filter_pages(
        _read_csv(os.path.join(run_dir, config.PAGES_CSV)), filters,
    )
    if not pages:
        return {
            "cms_distribution": [], "structured_data_adoption": {},
            "schema_type_frequency": [], "seo_readiness": [],
            "coverage_histogram": [],
        }

    # CMS distribution
    cms_domains: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    cms_pages: Counter = Counter()
    for r in pages:
        cms = r.get("cms_generator", "").strip() or "(undetected)"
        dom = r.get("domain", "unknown")
        cms_domains[cms][dom] += 1
        cms_pages[cms] += 1

    cms_distribution = []
    for cms, doms in sorted(cms_domains.items(), key=lambda x: -cms_pages[x[0]]):
        cms_distribution.append({
            "cms": cms,
            "page_count": cms_pages[cms],
            "domain_count": len(doms),
            "domains": [
                {"domain": d, "pages": c}
                for d, c in sorted(doms.items(), key=lambda x: -x[1])[:20]
            ],
        })

    # Structured data adoption
    total = len(pages)
    has_jld = sum(1 for r in pages if r.get("json_ld_types", "").strip())
    has_md = sum(1 for r in pages if r.get("microdata_types", "").strip())
    has_rdfa = sum(1 for r in pages if r.get("rdfa_types", "").strip())
    has_any = sum(
        1 for r in pages
        if (r.get("json_ld_types", "").strip() or
            r.get("microdata_types", "").strip() or
            r.get("rdfa_types", "").strip())
    )

    structured_data_adoption = {
        "total_pages": total,
        "json_ld": has_jld,
        "microdata": has_md,
        "rdfa": has_rdfa,
        "any": has_any,
        "none": total - has_any,
    }

    # Schema type frequency
    type_freq: Counter = Counter()
    for r in pages:
        for field in ("json_ld_types", "microdata_types"):
            for t in r.get(field, "").split("|"):
                t = t.strip()
                if t:
                    type_freq[t] += 1

    schema_type_frequency = [
        {"type": t, "count": c} for t, c in type_freq.most_common(40)
    ]

    # SEO readiness per domain
    dom_seo: Dict[str, Dict[str, int]] = {}
    for r in pages:
        dom = r.get("domain", "unknown")
        if dom not in dom_seo:
            dom_seo[dom] = {
                "domain": dom, "pages": 0,
                "has_canonical": 0, "has_hreflang": 0, "has_feed": 0,
                "has_pagination": 0, "has_breadcrumb_schema": 0,
                "has_robots": 0, "has_structured_data": 0,
            }
        s = dom_seo[dom]
        s["pages"] += 1
        if r.get("canonical_url", "").strip():
            s["has_canonical"] += 1
        if r.get("hreflang_links", "").strip():
            s["has_hreflang"] += 1
        if r.get("feed_urls", "").strip():
            s["has_feed"] += 1
        if r.get("pagination_next", "").strip() or r.get("pagination_prev", "").strip():
            s["has_pagination"] += 1
        if r.get("breadcrumb_schema", "").strip():
            s["has_breadcrumb_schema"] += 1
        if r.get("robots_directives", "").strip():
            s["has_robots"] += 1
        if (r.get("json_ld_types", "").strip() or
                r.get("microdata_types", "").strip()):
            s["has_structured_data"] += 1

    seo_readiness = sorted(dom_seo.values(), key=lambda x: -x["pages"])

    # Coverage histogram (10% buckets)
    cov_buckets: Counter = Counter()
    for r in pages:
        cov = _safe_float(r.get("extraction_coverage_pct", "0"))
        bucket = min(int(cov // 10) * 10, 90)
        cov_buckets[bucket] += 1
    coverage_histogram = [
        {"bucket": f"{b}-{b + 9}%", "count": cov_buckets.get(b, 0)}
        for b in range(0, 100, 10)
    ]

    return {
        "cms_distribution": cms_distribution,
        "structured_data_adoption": structured_data_adoption,
        "schema_type_frequency": schema_type_frequency,
        "seo_readiness": seo_readiness,
        "coverage_histogram": coverage_histogram,
    }


# ── Aggregate: authorship & provenance ───────────────────────────────────

def aggregate_authorship(
    run_dir: str, filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Author-domain relationships and publisher landscape."""
    pages = filter_pages(
        _read_csv(os.path.join(run_dir, config.PAGES_CSV)), filters,
    )
    if not pages:
        return {"authors": [], "publishers": [], "author_domains": []}

    author_doms: Dict[str, Counter] = defaultdict(Counter)
    publisher_doms: Dict[str, Counter] = defaultdict(Counter)

    for r in pages:
        author = r.get("author", "").strip()
        pub = r.get("publisher", "").strip()
        dom = r.get("domain", "unknown")
        if author:
            author_doms[author][dom] += 1
        if pub:
            publisher_doms[pub][dom] += 1

    authors = []
    for name, doms in sorted(
        author_doms.items(),
        key=lambda x: -sum(x[1].values()),
    )[:50]:
        authors.append({
            "author": name,
            "total_pages": sum(doms.values()),
            "domain_count": len(doms),
            "domains": [
                {"domain": d, "pages": c}
                for d, c in doms.most_common(10)
            ],
        })

    publishers = []
    for name, doms in sorted(
        publisher_doms.items(),
        key=lambda x: -sum(x[1].values()),
    )[:30]:
        publishers.append({
            "publisher": name,
            "total_pages": sum(doms.values()),
            "domain_count": len(doms),
            "domains": [
                {"domain": d, "pages": c}
                for d, c in doms.most_common(10)
            ],
        })

    # Author-domain network (nodes + links for force layout)
    author_nodes = []
    author_links = []
    top_authors = authors[:30]
    all_doms_in_network: set = set()
    for a in top_authors:
        for d in a["domains"]:
            all_doms_in_network.add(d["domain"])

    for a in top_authors:
        author_nodes.append({
            "id": "author:" + a["author"],
            "label": a["author"],
            "type": "author",
            "pages": a["total_pages"],
        })
    for dom in sorted(all_doms_in_network):
        author_nodes.append({
            "id": "domain:" + dom,
            "label": dom,
            "type": "domain",
            "pages": sum(
                d["pages"] for a in top_authors
                for d in a["domains"] if d["domain"] == dom
            ),
        })
    for a in top_authors:
        for d in a["domains"]:
            author_links.append({
                "source": "author:" + a["author"],
                "target": "domain:" + d["domain"],
                "weight": d["pages"],
            })

    return {
        "authors": authors,
        "publishers": publishers,
        "author_network": {"nodes": author_nodes, "links": author_links},
    }


# ── Aggregate: domain-specific schema insights ──────────────────────────

def aggregate_schema_insights(
    run_dir: str, filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Conditional insights for Product, Event, Job, Recipe schemas."""
    pages = filter_pages(
        _read_csv(os.path.join(run_dir, config.PAGES_CSV)), filters,
    )

    products = []
    events = []
    jobs = []
    recipes = []

    for r in pages:
        price = r.get("schema_price", "").strip()
        if price:
            products.append({
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
                "price": _safe_float(price),
                "currency": r.get("schema_currency", ""),
                "availability": r.get("schema_availability", ""),
                "rating": _safe_float(r.get("schema_rating", "")),
                "review_count": _safe_int(r.get("schema_review_count", "")),
                "url": r.get("final_url", ""),
            })

        event_date = r.get("schema_event_date", "").strip()
        if event_date:
            events.append({
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
                "date": event_date,
                "location": r.get("schema_event_location", ""),
                "url": r.get("final_url", ""),
            })

        job_title = r.get("schema_job_title", "").strip()
        if job_title:
            jobs.append({
                "domain": r.get("domain", ""),
                "job_title": job_title,
                "location": r.get("schema_job_location", ""),
                "url": r.get("final_url", ""),
            })

        recipe_time = r.get("schema_recipe_time", "").strip()
        if recipe_time:
            recipes.append({
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
                "time": recipe_time,
                "url": r.get("final_url", ""),
            })

    # Product summary
    product_summary = None
    if len(products) >= 3:
        prices = [p["price"] for p in products if p["price"] > 0]
        avail = Counter(p["availability"] for p in products if p["availability"])
        dom_counts = Counter(p["domain"] for p in products)
        product_summary = {
            "count": len(products),
            "price_min": round(min(prices), 2) if prices else 0,
            "price_max": round(max(prices), 2) if prices else 0,
            "price_avg": round(sum(prices) / len(prices), 2) if prices else 0,
            "availability": dict(avail.most_common(10)),
            "by_domain": [
                {"domain": d, "count": c} for d, c in dom_counts.most_common(20)
            ],
            "top_rated": sorted(
                [p for p in products if p["rating"] > 0],
                key=lambda x: -x["rating"],
            )[:10],
        }

    event_summary = None
    if len(events) >= 3:
        dom_counts = Counter(e["domain"] for e in events)
        event_summary = {
            "count": len(events),
            "by_domain": [
                {"domain": d, "count": c} for d, c in dom_counts.most_common(20)
            ],
            "events": sorted(events, key=lambda x: x["date"])[:50],
        }

    job_summary = None
    if len(jobs) >= 3:
        dom_counts = Counter(j["domain"] for j in jobs)
        loc_counts = Counter(j["location"] for j in jobs if j["location"])
        job_summary = {
            "count": len(jobs),
            "by_domain": [
                {"domain": d, "count": c} for d, c in dom_counts.most_common(20)
            ],
            "by_location": [
                {"location": loc, "count": c} for loc, c in loc_counts.most_common(20)
            ],
            "jobs": jobs[:50],
        }

    recipe_summary = None
    if len(recipes) >= 3:
        dom_counts = Counter(rec["domain"] for rec in recipes)
        recipe_summary = {
            "count": len(recipes),
            "by_domain": [
                {"domain": d, "count": c} for d, c in dom_counts.most_common(20)
            ],
            "recipes": recipes[:50],
        }

    return {
        "products": product_summary,
        "events": event_summary,
        "jobs": job_summary,
        "recipes": recipe_summary,
    }


# ── Filter options (for populating the global filter bar) ────────────────

def get_filter_options(run_dir: str) -> Dict[str, Any]:
    """Return available values for each filter dimension."""
    pages = _read_csv(os.path.join(run_dir, config.PAGES_CSV))

    domains: set = set()
    cms_values: set = set()
    content_kinds: set = set()
    schema_types: set = set()

    for r in pages:
        domains.add(r.get("domain", ""))
        cms = r.get("cms_generator", "").strip()
        if cms:
            cms_values.add(cms)
        kind = r.get("content_kind_guess", "").strip()
        if kind:
            content_kinds.add(kind)
        for field in ("json_ld_types", "microdata_types"):
            for t in r.get(field, "").split("|"):
                t = t.strip()
                if t:
                    schema_types.add(t)

    return {
        "domains": sorted(domains),
        "cms_values": sorted(cms_values),
        "content_kinds": sorted(content_kinds),
        "schema_types": sorted(schema_types),
        "total_pages": len(pages),
    }
