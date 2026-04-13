#!/usr/bin/env python3
"""Add or refresh ``_state.json`` in a CSJ project ZIP so runs with CSV data are resumable.

Use after merging crawl exports: archives without ``_state.json`` are treated as
``new`` by older app versions; starting a crawl would truncate CSVs. Current
``storage.get_run_status()`` also infers *interrupted* when page rows exist but
this script writes explicit state for clarity and correct counters.

Usage::

    python3 tools/fix_project_zip_resume_state.py /path/to/project.zip /path/to/out.zip
"""

from __future__ import annotations

import argparse
import glob
import io
import json
import os
import sys
import tempfile
import zipfile

# Repo root (parent of tools/)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import config  # noqa: E402
import utils  # noqa: E402


def _sum_asset_rows(run_dir: str) -> int:
    total = 0
    pattern = os.path.join(run_dir, f"{config.ASSETS_CSV_PREFIX}*.csv")
    for path in glob.glob(pattern):
        total += utils.count_csv_rows(path)
    return total


def _state_for_run(run_dir: str) -> dict:
    pages = utils.count_csv_rows(os.path.join(run_dir, config.PAGES_CSV))
    if pages == 0:
        return {}
    assets = _sum_asset_rows(run_dir)
    return {
        "status": "interrupted",
        "pages_crawled": pages,
        "assets_from_pages": assets,
        "queue": [],
        "started_at": "",
        "stopped_at": "",
    }


def fix_zip(src_zip: str, dst_zip: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(src_zip, "r") as zin:
            zin.extractall(tmp)

        top = [e for e in os.listdir(tmp) if os.path.isdir(os.path.join(tmp, e))]
        if len(top) != 1:
            raise ValueError("Archive must contain exactly one top-level project folder")
        proj = os.path.join(tmp, top[0])
        runs_root = os.path.join(proj, "runs")
        if not os.path.isdir(runs_root):
            raise ValueError("No runs/ directory in project")

        for name in os.listdir(runs_root):
            if not name.startswith("run_"):
                continue
            run_dir = os.path.join(runs_root, name)
            if not os.path.isdir(run_dir):
                continue
            state = _state_for_run(run_dir)
            if not state:
                continue
            path = os.path.join(run_dir, "_state.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False)

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
            for dirpath, _dirnames, filenames in os.walk(proj):
                for fname in filenames:
                    abs_path = os.path.join(dirpath, fname)
                    arc = os.path.join(top[0], os.path.relpath(abs_path, proj))
                    zout.write(abs_path, arcname=arc)
        buf.seek(0)
        with open(dst_zip, "wb") as out:
            out.write(buf.read())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("src_zip", help="Input project .zip")
    ap.add_argument("dst_zip", help="Output .zip path")
    args = ap.parse_args()
    fix_zip(args.src_zip, args.dst_zip)
    print("Wrote", args.dst_zip)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
