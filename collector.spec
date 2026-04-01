# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec for The Crawl Street Journal.

Build with:
    pyinstaller collector.spec --noconfirm

Output:
    macOS  → dist/The Crawl Street Journal.app
    Windows → dist/The Crawl Street Journal/The Crawl Street Journal.exe
    Linux  → dist/The Crawl Street Journal/The Crawl Street Journal
"""

import sys
from pathlib import Path

block_cipher = None
ROOT = Path(SPECPATH)

# ── Per-platform icon ─────────────────────────────────────────────────
_icon_map = {
    "darwin": ROOT / "assets" / "icon.icns",
    "win32": ROOT / "assets" / "icon.ico",
    "linux": ROOT / "assets" / "icon.png",
}
_icon_path = _icon_map.get(sys.platform)
_icon = str(_icon_path) if _icon_path and _icon_path.exists() else None

a = Analysis(
    [str(ROOT / "launcher.py")],
    pathex=[str(ROOT)],
    binaries=[],
    datas=[
        (str(ROOT / "templates"), "templates"),
        (str(ROOT / "static"), "static"),
    ],
    hiddenimports=[
        "gui",
        "config",
        "scraper",
        "parser",
        "sitemap",
        "storage",
        "run_pre_crawl_analysis",
        "run_background_crawl",
        "flask",
        "jinja2",
        "markupsafe",
        "werkzeug",
        "requests",
        "urllib3",
        "charset_normalizer",
        "certifi",
        "idna",
        "bs4",
        "lxml",
        "lxml.etree",
        "lxml.html",
        "textstat",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "tkinter",
        "matplotlib",
        "numpy",
        "pandas",
        "scipy",
        "pytest",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="The Crawl Street Journal",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    icon=_icon,
    target_arch=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="The Crawl Street Journal",
)

if sys.platform == "darwin":
    app = BUNDLE(
        coll,
        name="The Crawl Street Journal.app",
        icon=_icon,
        bundle_identifier="io.csj.crawlstreetjournal",
        info_plist={
            "CFBundleDisplayName": "The Crawl Street Journal",
            "CFBundleShortVersionString": "1.0.0",
            "NSHighResolutionCapable": True,
            "LSBackgroundOnly": False,
        },
    )
