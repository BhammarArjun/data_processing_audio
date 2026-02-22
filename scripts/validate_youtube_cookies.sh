#!/usr/bin/env bash
set -euo pipefail

# Validate a cookies.txt file with yt-dlp against one YouTube URL.
#
# Usage:
#   ./scripts/validate_youtube_cookies.sh <cookies_file> [test_url]

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <cookies_file> [test_url]" >&2
  exit 1
fi

COOKIE_FILE="$1"
TEST_URL="${2:-https://www.youtube.com/watch?v=PoT1MjnnTo4}"

if [[ ! -f "${COOKIE_FILE}" ]]; then
  echo "Cookie file not found: ${COOKIE_FILE}" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -x "${REPO_ROOT}/venv/bin/python" ]]; then
  PYTHON_BIN="${REPO_ROOT}/venv/bin/python"
else
  PYTHON_BIN="python3"
fi

echo "Testing yt-dlp metadata access with cookie file: ${COOKIE_FILE}"
echo "Test URL: ${TEST_URL}"

# Step 1: metadata/auth probe (no format selection)
"${PYTHON_BIN}" -m yt_dlp \
  --cookies "${COOKIE_FILE}" \
  --js-runtimes "node" \
  --ignore-no-formats-error \
  --no-warnings \
  --no-playlist \
  --skip-download \
  --print "id=%(id)s title=%(title)s" \
  --extractor-args "youtube:player_client=tv_downgraded,android,ios" \
  "${TEST_URL}"

# Step 2: lightweight format probe with fallbacks
FORMATS=(
  "bestaudio[acodec!=none]/bestaudio*/bestaudio/best*[acodec!=none]/best"
  "bestaudio*/bestaudio/best"
  "best"
)

for fmt in "${FORMATS[@]}"; do
  echo "Probing download format: ${fmt}"
  if "${PYTHON_BIN}" -m yt_dlp \
    --cookies "${COOKIE_FILE}" \
    --js-runtimes "node" \
    --no-warnings \
    --no-playlist \
    --skip-download \
    --extractor-args "youtube:player_client=tv_downgraded,android,ios" \
    -f "${fmt}" \
    "${TEST_URL}" >/dev/null 2>&1; then
    echo "Cookie validation passed with format: ${fmt}"
    exit 0
  fi
done

echo "Cookie auth looks valid (metadata probe passed), but format probe failed." >&2
echo "Run this for debug:" >&2
echo "  ${PYTHON_BIN} -m yt_dlp --cookies \"${COOKIE_FILE}\" -F \"${TEST_URL}\"" >&2
exit 1
