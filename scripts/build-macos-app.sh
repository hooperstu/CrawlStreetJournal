#!/usr/bin/env bash
#
# Build The Crawl Street Journal macOS .app bundle.
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
echo "📥  Installing dependencies…"
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
pip install --quiet pyinstaller

# ── Build ────────────────────────────────────────────────────────────
echo "🔨  Building The Crawl Street Journal.app…"
pyinstaller collector.spec --noconfirm

# ── Post-build ───────────────────────────────────────────────────────
APP_PATH="dist/The Crawl Street Journal.app"
if [[ -d "$APP_PATH" ]]; then
    echo ""
    echo "✅  Build complete!"
    echo "    $APP_PATH"
    echo ""
    echo "    To install:  drag the .app to /Applications"
    echo "    To distribute:  create a DMG or zip the .app"
    echo ""
    SIZE=$(du -sh "$APP_PATH" | cut -f1)
    echo "    Bundle size: $SIZE"
else
    echo "❌  Build failed — dist/The Crawl Street Journal.app not found."
    exit 1
fi
