"""
ZIP exports for dashboard reports — full CSV breakdowns (not PDF).

The project does not generate PDF reports; exports are UTF-8 CSV inside a ZIP
archive suitable for spreadsheets or printing to PDF externally.
"""
from __future__ import annotations

import csv
import io
import zipfile
from typing import Any, Dict, List, Optional

import viz_data


def _csv_from_rows(
    fieldnames: List[str],
    rows: List[Dict[str, Any]],
) -> bytes:
    buf = io.StringIO(newline="")
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    for row in rows:
        w.writerow({k: row.get(k, "") for k in fieldnames})
    return buf.getvalue().encode("utf-8")


def build_content_audit_zip(
    run_dirs: List[str],
    filters: Optional[Dict[str, Any]] = None,
) -> bytes:
    data = viz_data.aggregate_content_performance_audit(
        run_dirs, filters=filters, full_lists=True,
    )
    files: List[tuple] = []

    summary = data.get("summary") or {}
    files.append((
        "summary.csv",
        _csv_from_rows(
            list(summary.keys()) if summary else ["page_count"],
            [summary] if summary else [{"page_count": 0}],
        ),
    ))

    thin = (data.get("thin_content") or {}).get("sample") or []
    if thin:
        files.append((
            "thin_content.csv",
            _csv_from_rows(list(thin[0].keys()), thin),
        ))

    dup_h = (data.get("duplicates") or {}).get("by_content_hash") or []
    dup_h_rows = []
    for cl in dup_h:
        for u in cl.get("urls") or []:
            dup_h_rows.append({
                "content_hash": cl.get("content_hash", ""),
                "url": u,
                "cluster_count": cl.get("count", ""),
            })
    if dup_h_rows:
        files.append((
            "duplicate_content_hash_urls.csv",
            _csv_from_rows(
                ["content_hash", "url", "cluster_count"],
                dup_h_rows,
            ),
        ))

    dup_c = (data.get("duplicates") or {}).get("by_canonical_url") or []
    dup_c_rows = []
    for g in dup_c:
        for p in g.get("pages") or []:
            dup_c_rows.append({
                "canonical_url": g.get("canonical_url", ""),
                "page_url": p.get("url", ""),
                "title": p.get("title", ""),
            })
    if dup_c_rows:
        files.append((
            "canonical_duplicate_pages.csv",
            _csv_from_rows(
                ["canonical_url", "page_url", "title"],
                dup_c_rows,
            ),
        ))

    il = data.get("internal_links") or {}
    top_in = il.get("top_inlinked_pages") or []
    if top_in:
        files.append((
            "internal_inlinks.csv",
            _csv_from_rows(list(top_in[0].keys()), top_in),
        ))
    top_out = il.get("top_outlinking_pages") or []
    if top_out:
        files.append((
            "internal_outlinks.csv",
            _csv_from_rows(list(top_out[0].keys()), top_out),
        ))
    by_dom = il.get("by_domain") or []
    if by_dom:
        files.append((
            "internal_edges_by_domain.csv",
            _csv_from_rows(list(by_dom[0].keys()), by_dom),
        ))

    km = data.get("keyword_mapping") or {}
    gaps = km.get("gap_sample") or []
    if gaps:
        files.append((
            "keyword_mapping_gaps.csv",
            _csv_from_rows(list(gaps[0].keys()), gaps),
        ))
    aligned = km.get("aligned_sample") or []
    if aligned:
        files.append((
            "keyword_mapping_aligned.csv",
            _csv_from_rows(list(aligned[0].keys()), aligned),
        ))

    return _zip_files(files)


def build_technical_performance_zip(
    run_dirs: List[str],
    filters: Optional[Dict[str, Any]] = None,
) -> bytes:
    data = viz_data.aggregate_technical_performance(
        run_dirs, filters=filters, full_lists=True,
    )
    files: List[tuple] = []
    thr = str(data.get("slow_fetch_threshold_ms", ""))
    lim = str(data.get("large_image_bytes_threshold", ""))

    for d in data.get("domains") or []:
        dom_safe = d.get("domain", "unknown").replace("/", "_")
        prefix = f"{dom_safe}_"

        slow = d.get("slow_pages_sample") or []
        if slow:
            files.append((
                prefix + "slow_pages.csv",
                _csv_from_rows(list(slow[0].keys()), slow),
            ))

        novp = d.get("no_viewport_sample") or []
        if novp:
            files.append((
                prefix + "no_viewport_meta.csv",
                _csv_from_rows(list(novp[0].keys()), novp),
            ))

        abc = d.get("assets_by_category") or {}
        abc_rows = [{"category": k, "count": v} for k, v in sorted(abc.items())]
        if abc_rows:
            files.append((
                prefix + "asset_counts_by_category.csv",
                _csv_from_rows(["category", "count"], abc_rows),
            ))

        ext = d.get("external_scripts_top") or []
        if ext:
            files.append((
                prefix + "external_scripts.csv",
                _csv_from_rows(list(ext[0].keys()), ext),
            ))

        large = d.get("large_images_sample") or []
        if large:
            files.append((
                prefix + "large_images.csv",
                _csv_from_rows(list(large[0].keys()), large),
            ))

        summ = {
            "domain": d.get("domain", ""),
            "page_count": d.get("page_count", ""),
            "avg_fetch_time_ms": d.get("avg_fetch_time_ms", ""),
            "p90_fetch_time_ms": d.get("p90_fetch_time_ms", ""),
            "slow_page_count": d.get("slow_page_count", ""),
            "viewport_meta_pct": d.get("viewport_meta_pct", ""),
            "slow_fetch_threshold_ms": thr,
            "large_image_bytes_threshold": lim,
        }
        files.append((
            prefix + "domain_summary.csv",
            _csv_from_rows(list(summ.keys()), [summ]),
        ))

    if not files:
        files.append(("readme.txt", b"No technical performance data in scope.\r\n"))

    return _zip_files(files)


def build_key_metrics_zip(
    run_dirs: List[str],
    filters: Optional[Dict[str, Any]] = None,
) -> bytes:
    data = viz_data.aggregate_key_metrics_snapshot(
        run_dirs, filters=filters, full_lists=True,
    )
    files: List[tuple] = []

    doms = data.get("domains") or []
    if doms:
        files.append((
            "domains_summary.csv",
            _csv_from_rows(list(doms[0].keys()), doms),
        ))

    br = data.get("page_breakdown") or []
    if br:
        files.append((
            "page_breakdown.csv",
            _csv_from_rows(list(br[0].keys()), br),
        ))

    if not files:
        files.append(("readme.txt", b"No key metrics data in scope.\r\n"))

    return _zip_files(files)


def _zip_files(files: List[tuple]) -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files:
            if isinstance(content, str):
                content = content.encode("utf-8")
            zf.writestr(name, content)
    return bio.getvalue()
