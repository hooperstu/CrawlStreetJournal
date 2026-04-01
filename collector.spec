# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec for The Crawl Street Journal.

Build with:
    pyinstaller collector.spec

Output lands in dist/The Crawl Street Journal.app  (macOS)
"""

import sys
from pathlib import Path

block_cipher = None
ROOT = Path(SPECPATH)

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
        # Third-party libraries that PyInstaller may miss
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

# macOS .app bundle
app = BUNDLE(
    coll,
    name="The Crawl Street Journal.app",
    icon=str(ROOT / "assets" / "icon.icns") if (ROOT / "assets" / "icon.icns").exists() else None,
    bundle_identifier="io.csj.crawlstreetjournal",
    info_plist={
        "CFBundleDisplayName": "The Crawl Street Journal",
        "CFBundleShortVersionString": "1.0.0",
        "NSHighResolutionCapable": True,
        "LSBackgroundOnly": False,
    },
)
