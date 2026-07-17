#!/bin/sh
# fetch-verified.sh — download an Apache artifact with mirror fallback + SHA-512 verify.
#
# Usage:
#   fetch-verified.sh <relative_path_under_apache_dist> <output_file>
#
# Mirror chain (in order):
#   1. ${LHS_APACHE_MIRROR} if set        — e.g. corporate Nexus / Artifactory
#   2. https://dlcdn.apache.org           — Apache official CDN, fast, current releases only
#   3. https://archive.apache.org/dist    — Apache archive, always has every release, slow
#
# SHA-512 verification:
#   Fetches <url>.sha512 from the same mirror as the binary and verifies before keeping.
#   If a mirror serves the binary but not the checksum, we move on to the next mirror.
#   If ALL mirrors fail to provide a verifiable download, exit non-zero.

set -eu

REL_PATH="$1"
OUT="$2"

# Mirror chain. User can prepend a custom mirror via LHS_APACHE_MIRROR.
MIRRORS="${LHS_APACHE_MIRROR:-} https://dlcdn.apache.org https://archive.apache.org/dist"

for MIRROR in $MIRRORS; do
  [ -z "$MIRROR" ] && continue
  URL="${MIRROR}/${REL_PATH}"

  echo "[fetch] ${URL}"

  if ! wget -q --tries=2 --timeout=120 "$URL" -O "$OUT.tmp"; then
    echo "[fetch]   binary fetch failed — trying next mirror"
    rm -f "$OUT.tmp"
    continue
  fi

  size=$(wc -c < "$OUT.tmp")
  if [ "$size" -lt 1000 ]; then
    echo "[fetch]   suspicious size (${size} bytes) — likely an error page, trying next mirror"
    rm -f "$OUT.tmp"
    continue
  fi

  # Fetch published checksum from the same mirror
  if ! wget -q --tries=2 --timeout=30 "${URL}.sha512" -O "$OUT.sha512.tmp"; then
    echo "[fetch]   .sha512 missing on this mirror — trying next mirror for verifiable copy"
    rm -f "$OUT.tmp" "$OUT.sha512.tmp"
    continue
  fi

  # Apache .sha512 files come in a few formats:
  #   "hadoop-3.4.1.tar.gz: AB CD EF ..."   (spaced hex, possibly multi-line)
  #   "<hash>  hadoop-3.4.1.tar.gz"          (standard sha512sum format)
  # Normalize: strip everything that isn't a hex digit, lowercase, take the first 128 chars.
  expected=$(tr -d ' \n\t:\r' < "$OUT.sha512.tmp" | tr 'A-Z' 'a-z' | grep -oE '[a-f0-9]{128}' | head -n1)
  actual=$(sha512sum "$OUT.tmp" | awk '{print $1}')

  if [ -z "$expected" ]; then
    echo "[fetch]   could not parse SHA-512 from $URL.sha512"
    rm -f "$OUT.tmp" "$OUT.sha512.tmp"
    continue
  fi

  if [ "$expected" != "$actual" ]; then
    echo "[fetch]   SHA-512 MISMATCH"
    echo "[fetch]     expected: $expected"
    echo "[fetch]     actual:   $actual"
    rm -f "$OUT.tmp" "$OUT.sha512.tmp"
    continue
  fi

  mv "$OUT.tmp" "$OUT"
  rm -f "$OUT.sha512.tmp"
  echo "[fetch]   OK (${size} bytes, SHA-512 verified)"
  exit 0
done

echo "[fetch] FAILED: no mirror provided a verifiable copy of $REL_PATH" >&2
exit 1
