#!/usr/bin/env bash
set -euo pipefail

# Export fresh YouTube cookies from a local browser profile into Netscape format.
#
# Usage:
#   ./scripts/export_youtube_cookies.sh [browser_spec] [output_file]
# Example:
#   ./scripts/export_youtube_cookies.sh "brave" "./cookies.youtube.txt"
#   ./scripts/export_youtube_cookies.sh "brave:Default" "./cookies.youtube.txt"

BROWSER_SPEC="${1:-brave}"
OUT_FILE="${2:-./cookies.youtube.txt}"
TEST_URL="${TEST_URL:-https://www.youtube.com/watch?v=BaW_jenozKc}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -x "${REPO_ROOT}/venv/bin/python" ]]; then
  PYTHON_BIN="${REPO_ROOT}/venv/bin/python"
else
  PYTHON_BIN="python3"
fi

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "python not found: ${PYTHON_BIN}" >&2
  exit 1
fi

echo "Exporting cookies from browser spec: ${BROWSER_SPEC}"
echo "Output file: ${OUT_FILE}"

"${PYTHON_BIN}" -m yt_dlp \
  --cookies-from-browser "${BROWSER_SPEC}" \
  --cookies "${OUT_FILE}" \
  --js-runtimes "node" \
  --ignore-no-formats-error \
  --skip-download \
  --no-warnings \
  --no-playlist \
  "${TEST_URL}" >/dev/null

if [[ ! -s "${OUT_FILE}" ]]; then
  echo "Cookie export failed: empty file: ${OUT_FILE}" >&2
  exit 1
fi

if ! grep -q "youtube.com" "${OUT_FILE}"; then
  echo "Cookie export failed: no youtube.com cookies in ${OUT_FILE}" >&2
  exit 1
fi

echo "Cookie export complete: ${OUT_FILE}"
echo "Next: copy this file to your Linux VM and pass --cookies <path> to the pipeline."
