#!/usr/bin/env python3
"""
Rebuild PyInstaller desktop icons from ``static/img/favicon.svg`` so the
packaged app matches the web favicon (vector source → high-resolution rasters).

Dependencies:
  - ``rsvg-convert`` (macOS: brew install librsvg; Ubuntu: librsvg2-bin)
  - ``iconutil`` (macOS only) for ``assets/icon.icns``
  - ``pillow`` for multi-size ``assets/icon.ico``::

        pip install pillow

Usage (from repo root)::

        python3 scripts/render_desktop_icons.py
"""
from __future__ import annotations

import shutil
import struct
import subprocess
import sys
from pathlib import Path


def _rsvg(svg: Path, width: int, height: int, dest: Path) -> None:
    subprocess.run(
        [
            "rsvg-convert",
            "-w",
            str(width),
            "-h",
            str(height),
            str(svg),
            "-o",
            str(dest),
        ],
        check=True,
    )


def _require_rsvg_convert() -> None:
    if shutil.which("rsvg-convert") is None:
        sys.stderr.write(
            "rsvg-convert not found. Install librsvg (e.g. brew install librsvg).\n"
        )
        sys.exit(1)


def main() -> None:
    _require_rsvg_convert()

    try:
        from PIL import Image
    except ImportError:
        sys.stderr.write("pillow is required: pip install pillow\n")
        sys.exit(1)

    root = Path(__file__).resolve().parents[1]
    svg = root / "static" / "img" / "favicon.svg"
    if not svg.is_file():
        sys.stderr.write(f"Missing favicon source: {svg}\n")
        sys.exit(1)

    iconset = root / "assets" / "icon.iconset"
    iconset.mkdir(parents=True, exist_ok=True)

    # iconutil expects these exact filenames (width × height in pixels).
    jobs = [
        ("icon_16x16.png", 16, 16),
        ("icon_16x16@2x.png", 32, 32),
        ("icon_32x32.png", 32, 32),
        ("icon_32x32@2x.png", 64, 64),
        ("icon_128x128.png", 128, 128),
        ("icon_128x128@2x.png", 256, 256),
        ("icon_256x256.png", 256, 256),
        ("icon_256x256@2x.png", 512, 512),
        ("icon_512x512.png", 512, 512),
        ("icon_512x512@2x.png", 1024, 1024),
    ]
    for name, w, h in jobs:
        _rsvg(svg, w, h, iconset / name)

    if sys.platform == "darwin":
        icns_out = root / "assets" / "icon.icns"
        subprocess.run(
            ["iconutil", "-c", "icns", str(iconset), "-o", str(icns_out)],
            check=True,
        )
        print(f"Wrote {icns_out.relative_to(root)}")
    else:
        print(
            "Skipping icon.icns (iconutil is macOS-only). "
            "Run this script on a Mac to refresh that file.",
        )

    # Linux launcher / high-res reference
    png_out = root / "assets" / "icon.png"
    _rsvg(svg, 512, 512, png_out)
    print(f"Wrote {png_out.relative_to(root)}")

    # Windows / PyInstaller: base image must be 256×256 so Pillow emits all sizes.
    master = iconset / "icon_512x512@2x.png"
    ico_out = root / "assets" / "icon.ico"
    img = Image.open(master).convert("RGBA")
    base = img.resize((256, 256), Image.Resampling.LANCZOS)
    base.save(ico_out, format="ICO")
    data = ico_out.read_bytes()
    n = struct.unpack("<H", data[4:6])[0]
    print(f"Wrote {ico_out.relative_to(root)} ({n} embedded sizes)")


if __name__ == "__main__":
    main()
