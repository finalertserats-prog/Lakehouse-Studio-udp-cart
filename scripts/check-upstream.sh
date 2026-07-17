#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# check-upstream.sh — is there anything NEW in manaskiran/LakeHouse-Studio?
#
# This repo and manaskiran/LakeHouse-Studio share NO git history — our code was
# brought in by a manual tree overlay, not a merge. So "am I behind upstream?"
# can't be answered with commit ancestry (git status / merge-base are useless
# here). Instead we compare TREES against a recorded sync base.
#
# Usage:
#   scripts/check-upstream.sh                 # report whether upstream has new content
#   scripts/check-upstream.sh --mark-synced   # record current upstream/main as the new base
#                                             #   (run this right AFTER you finish a sync)
#
# Exit codes: 0 = up to date (or marked), 10 = upstream has new content, 1 = error.
# ---------------------------------------------------------------------------
set -euo pipefail

UPSTREAM_URL="https://github.com/manaskiran/LakeHouse-Studio.git"
ROOT="$(git rev-parse --show-toplevel)"
BASE_FILE="$ROOT/scripts/.upstream_sync_base"
DEFAULT_BASE="d819293d9f11238a37f2dd139ec0735ba456628f"   # first full-match sync commit

# 1. Ensure the 'upstream' remote points at manaskiran.
if ! git remote get-url upstream >/dev/null 2>&1; then
  echo "-> adding 'upstream' remote -> $UPSTREAM_URL"
  git remote add upstream "$UPSTREAM_URL"
fi

echo "-> fetching upstream ..."
git fetch --quiet upstream

# 2. Resolve the recorded sync base (falls back to the first sync commit).
BASE="$(tr -d '[:space:]' < "$BASE_FILE" 2>/dev/null || true)"
[ -n "$BASE" ] || BASE="$DEFAULT_BASE"

# 3. --mark-synced: record the current upstream/main as the new base and exit.
if [ "${1:-}" = "--mark-synced" ]; then
  git rev-parse upstream/main > "$BASE_FILE"
  echo "✅ marked upstream/main ($(git rev-parse --short upstream/main)) as the new sync base."
  echo "   ($BASE_FILE)"
  exit 0
fi

echo "-> comparing recorded base $(git rev-parse --short "$BASE") against upstream/main $(git rev-parse --short upstream/main)"
echo

# 4. Compare trees. Identical trees => nothing new, regardless of commit hash.
if git diff --quiet "$BASE" upstream/main; then
  echo "✅ UP TO DATE — upstream has no content we don't already have."
  echo "   (base tree == upstream/main tree; the commit hash may differ, the bytes don't)"
  exit 0
fi

echo "🔔 UPSTREAM HAS NEW CONTENT since our sync base."
echo
echo "Recent upstream commits:"
git log --oneline -5 upstream/main | sed 's/^/  /'
echo
echo "Files changed upstream (base -> upstream/main):"
git diff --stat "$BASE" upstream/main | sed 's/^/  /'
echo
echo "Files upstream has that we are MISSING entirely:"
comm -23 \
  <(git ls-tree -r --name-only upstream/main | sort) \
  <(git ls-tree -r --name-only main | sort) \
  | sed 's/^/  + /' || true
echo
echo "⚠  Do NOT blind-overlay — that would clobber our local security + catalog work."
echo "   Merge as a 3-way instead, then verify tests, then mark synced:"
echo "     git switch -c sync-upstream"
echo "     git diff $BASE upstream/main | git apply --3way   # resolve any conflicts"
echo "     .venv/Scripts/python -m pytest tests/ -q"
echo "     scripts/check-upstream.sh --mark-synced"
exit 10
