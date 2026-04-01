#!/usr/bin/env bash
#
# Build The Crawl Street Journal for the current platform (macOS / Linux).
#
# Usage:
#     ./scripts/build-macos-app.sh          # uses existing venv / global Python
#     ./scripts/build-macos-app.sh --clean   # wipe build/ & dist/ first
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

# ── Options ───────────────────────────────────────────────────────────
if [[ "${1:-}" == "--clean" ]]; then
    echo "🧹  Cleaning previous build artefacts…"
    rm -rf build/ dist/
fi

# ── Virtual environment ──────────────────────────────────────────────
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
    if [[ -d .venv ]]; then
        echo "📦  Activating .venv…"
        source .venv/bin/activate
    else
        echo "📦  Creating .venv…"
        python3 -m venv .venv
        source .venv/bin/activate
    fi
fi

# ── Dependencies ─────────────────────────────────────────────────────
PIP="$ROOT/.venv/bin/python3 -m pip"
echo "📥  Installing dependencies…"
$PIP install --quiet --upgrade pip
$PIP install --quiet -r requirements.txt
$PIP install --quiet pyinstaller

# ── Build ────────────────────────────────────────────────────────────
echo "🔨  Building The Crawl Street Journal…"
"$ROOT/.venv/bin/pyinstaller" collector.spec --noconfirm

# ── Post-build ───────────────────────────────────────────────────────
if [[ "$(uname -s)" == "Darwin" ]]; then
    APP_PATH="dist/The Crawl Street Journal.app"
    if [[ -d "$APP_PATH" ]]; then
        echo ""
        echo "✅  Build complete!"
        echo "    $APP_PATH"
        echo ""
        echo "    To install:  drag the .app to /Applications"
        echo "    To distribute:  create a DMG or zip the .app"
        SIZE=$(du -sh "$APP_PATH" | cut -f1)
        echo "    Bundle size: $SIZE"
    else
        echo "❌  Build failed — .app not found."
        exit 1
    fi
else
    DIST_DIR="dist/The Crawl Street Journal"
    if [[ -d "$DIST_DIR" ]]; then
        echo ""
        echo "✅  Build complete!"
        echo "    $DIST_DIR"
        echo ""
        echo "    Run with:  $DIST_DIR/The Crawl Street Journal"
        SIZE=$(du -sh "$DIST_DIR" | cut -f1)
        echo "    Bundle size: $SIZE"
    else
        echo "❌  Build failed — dist folder not found."
        exit 1
    fi
fi
