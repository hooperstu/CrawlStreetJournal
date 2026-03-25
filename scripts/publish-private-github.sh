#!/usr/bin/env bash
# Create a private GitHub repo "NHSE-Collector" and push the current branch.
# Requires: GitHub CLI (gh) and `gh auth login` completed once.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if ! command -v gh >/dev/null 2>&1; then
  echo "Install GitHub CLI: brew install gh" >&2
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "Run: gh auth login" >&2
  exit 1
fi

USER_LOGIN="$(gh api user -q .login)"
REPO_NAME="NHSE-Collector"
DEFAULT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

# Remove stale remote from the old project name if present
if git remote get-url origin >/dev/null 2>&1; then
  OLD_URL="$(git remote get-url origin)"
  if echo "$OLD_URL" | grep -qi 'cabinet-collector'; then
    echo "Removing old remote origin ($OLD_URL)"
    git remote remove origin
  fi
fi

if git remote get-url origin >/dev/null 2>&1; then
  echo "Remote origin already set to: $(git remote get-url origin)"
  echo "Pushing $DEFAULT_BRANCH..."
  git push -u origin "$DEFAULT_BRANCH"
  exit 0
fi

echo "Creating private repo $USER_LOGIN/$REPO_NAME and pushing..."
gh repo create "$REPO_NAME" \
  --private \
  --source=. \
  --remote=origin \
  --push \
  --description "NHS web inventory crawler (NHSE Collector)"

echo "Done. Remote: https://github.com/$USER_LOGIN/$REPO_NAME"
