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

# ── tldextract offline suffix list ─────────────────────────────────────
# `tldextract` ships an offline `.tld_set_snapshot`; without it the library
# makes a network request on every invocation.  Locate it from the installed
# package so the build works in any venv or CI environment.
_tldextract_data = []
try:
    import tldextract as _tld_mod
    _tld_snapshot = Path(_tld_mod.__file__).parent / ".tld_set_snapshot"
    if _tld_snapshot.exists():
        _tldextract_data.append((str(_tld_snapshot), "tldextract"))
except ImportError:
    pass

# Windows: pywebview loads Edge WebView2 via interop DLLs under ``webview/lib/``.
# PyInstaller's hook-webview often pulls these in, but listing them explicitly
# keeps one-folder COLLECT builds working when analysis omits the package data.
_webview_win_binaries = []
_webview_win_datas = []
if sys.platform == "win32":
    from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs

    _webview_win_datas = collect_data_files("webview", subdir="lib") + collect_data_files(
        "webview", subdir="js"
    )
    _webview_win_binaries = collect_dynamic_libs("webview")

a = Analysis(
    [str(ROOT / "launcher.py")],
    pathex=[str(ROOT)],
    binaries=_webview_win_binaries,
    datas=[
        # Flask `render_template` and `TemplateNotFound` resolution look under a
        # `templates/` tree at runtime; the frozen app has no source tree, so the
        # HTML Jinja bundles must be copied next to the executable.
        (str(ROOT / "templates"), "templates"),
        # Same for `url_for('static', ...)` — CSS, client JS, and other assets
        # must exist on disk inside `static/` in the bundle.
        (str(ROOT / "static"), "static"),
    ]
    + _tldextract_data
    + _webview_win_datas,
    hiddenimports=[
        # Core application modules — `gui` imports these inside route handlers
        # or conditionally, so PyInstaller's static analysis from `launcher.py`
        # doesn't traverse into them. Every .py file that can be imported at
        # runtime must be listed here.
        "gui",
        "config",
        "utils",
        "scraper",
        "parser",
        "sitemap",
        "storage",
        "viz_api",
        "viz_data",
        "signals_audit",
        "render",
        "audit_data",
        "wcag_audit",
        # Standalone scripts not imported by the GUI; listed so they remain
        # importable in the frozen tree.
        "run_pre_crawl_analysis",
        "run_background_crawl",
        # Flask and its template/routing stack load submodules via package
        # metadata and lazy imports; PyInstaller often omits pieces that
        # surface as errors on first request rather than at startup.
        "flask",
        "jinja2",
        "markupsafe",
        "werkzeug",
        "werkzeug.serving",
        "werkzeug.debug",
        # `requests` pulls SSL and encoding stacks through optional branches.
        "requests",
        "urllib3",
        "charset_normalizer",
        "certifi",
        "idna",
        "bs4",
        # `lxml` extension modules for HTML parsing.
        "lxml",
        "lxml.etree",
        "lxml.html",
        # `tldextract` is used by viz_data for domain ownership grouping.
        # It lazy-loads a public suffix list; we include its dependencies
        # so the frozen binary can resolve domains correctly.
        "tldextract",
        "tldextract._version",
        "filelock",
        "requests_file",
        # Imported inside a try-block when readability capture is enabled.
        "textstat",
        # NLTK (textstat dependency) — needed submodules.
        "nltk",
        "nltk.corpus",
        # Native desktop window — pywebview uses the OS webview engine
        # (WebKit on macOS, Edge WebView2 on Windows, WebKitGTK on Linux).
        # The top-level `webview` package and its submodules are imported
        # statically, but the platform backends are loaded dynamically at
        # runtime in `webview.guilib.initialize()` based on the current OS.
        # PyInstaller's static analysis never reaches them, so every backend
        # submodule the current build platform might need must be listed here.
        "webview",
        "webview.guilib",
        "webview.http",
        "webview.util",
        "webview.window",
        "webview.event",
        "webview.menu",
        "webview.screen",
        "webview.localization",
        "webview.errors",
        "webview.models",
        "webview.state",
        "webview.dom",
        # pywebview runtime deps
        "proxy_tools",
        "bottle",
    ]
    # Platform-specific pywebview backends — only the modules for the current
    # OS are needed.  Including the wrong platform's imports (e.g. `clr` on
    # macOS) would cause Analysis to fail.
    + (
        [
            # Windows: pythonnet (.NET CLR bridge) + WinForms/EdgeChromium
            "clr",
            "pythonnet",
            "webview.platforms",
            "webview.platforms.winforms",
            "webview.platforms.edgechromium",
            "webview.platforms.mshtml",
        ]
        if sys.platform == "win32"
        else [
            "webview.platforms",
            "webview.platforms.cocoa",
        ]
        if sys.platform == "darwin"
        else [
            "webview.platforms",
            "webview.platforms.gtk",
        ]
    ),
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

# On Linux the executable is typically launched from a terminal; console=True
# ensures error tracebacks are visible if something goes wrong.  On macOS and
# Windows the app opens a browser window and a visible console is unwanted.
_console = sys.platform == "linux"

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="The Crawl Street Journal",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=_console,
    icon=_icon,
    target_arch=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
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
            "CFBundleShortVersionString": "3.3.1",
            "CFBundleVersion": "3.3.1",
            "NSHighResolutionCapable": True,
            "LSBackgroundOnly": False,
            # The app makes HTTP requests to crawl target sites and serves
            # a local web UI — allow arbitrary network loads.
            "NSAppTransportSecurity": {
                "NSAllowsArbitraryLoads": True,
            },
        },
    )
