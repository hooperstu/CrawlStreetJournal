# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec for The Crawl Street Journal.

This file is the single build recipe: it tells PyInstaller which entry point to
analyse, what non-Python assets to copy, which modules to pull in explicitly,
and how to assemble the frozen executable (and on macOS, the .app bundle).
Downstream packaging steps should invoke PyInstaller against this spec only.

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
# Each platform expects a different raster/vector icon format for native shells
# (Dock and Finder on macOS, Explorer and the taskbar on Windows, desktop
# launchers on Linux). Map to the matching asset so the bootloader and BUNDLE
# step receive a path PyInstaller can embed; missing files degrade to no icon
# rather than failing the build.
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
        # Flask `render_template` and `TemplateNotFound` resolution look under a
        # `templates/` tree at runtime; the frozen app has no source tree, so the
        # HTML Jinja bundles must be copied next to the executable.
        (str(ROOT / "templates"), "templates"),
        # Same for `url_for('static', ...)` — CSS, client JS, and other assets
        # must exist on disk inside `static/` in the bundle.
        (str(ROOT / "static"), "static"),
    ],
    hiddenimports=[
        # `gui` imports `scraper` only inside route handlers, so Analysis from
        # `launcher.py` does not traverse into `scraper`, `parser`, or `sitemap`;
        # omitting them breaks the crawl on first use with ImportError.
        "gui",
        "config",
        "scraper",
        "parser",
        "sitemap",
        "storage",
        # Standalone scripts not imported by the GUI; listed so they remain
        # importable or discoverable in the frozen tree if you invoke them from
        # the packaged environment (they are not required for the browser UI).
        "run_pre_crawl_analysis",
        "run_background_crawl",
        # Flask and its template stack load submodules and data files via
        # package metadata and lazy imports; PyInstaller often omits pieces
        # (notably Jinja filters and Werkzeug utilities), which surfaces as
        # errors on first request rather than at startup.
        "flask",
        "jinja2",
        "markupsafe",
        "werkzeug",
        # `requests` pulls SSL and encoding stacks through optional branches;
        # without these, HTTPS crawls fail with missing modules or certificate
        # stores even though the interpreter starts.
        "requests",
        "urllib3",
        "charset_normalizer",
        "certifi",
        "idna",
        "bs4",
        # `lxml` registers extension modules; explicit submodule names ensure
        # HTML parsing on crawl matches the dev environment.
        "lxml",
        "lxml.etree",
        "lxml.html",
        # Imported only inside a try-block when readability capture is enabled;
        # static analysis never sees it, so the hook would drop it otherwise.
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
            "CFBundleShortVersionString": "2.0.0",
            "NSHighResolutionCapable": True,
            "LSBackgroundOnly": False,
        },
    )
