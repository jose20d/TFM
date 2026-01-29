#!/usr/bin/env bash
set -euo pipefail

# One-command local demo runner (no DB, no server).
# It cleans previous outputs, downloads approved sources, generates the HTML map, and opens it.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

VENV_DIR="${VENV_DIR:-.venv}"

MRDS_JSON="${MRDS_JSON:-./data/demo/usgs_mrds/records_100.json}"
WGI_JSON="${WGI_JSON:-./data/demo/worldbank_wgi/records_100.json}"
OUT_HTML="${OUT_HTML:-./output/demo_map.html}"

LIMIT="${LIMIT:-100}"
SOURCES="${SOURCES:-all}"

if [[ ! -d "$VENV_DIR" ]]; then
  python3 -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

python -m pip install -r requirements.txt

# Clean generated outputs
echo "[1/4] Cleaning previous generated outputs..."
python scripts/clean_data.py --yes
rm -rf ./output

# Download sources (does not stop on per-source failures; writes demo_report.json)
echo "[2/4] Downloading approved data sources (this may take a moment)..."
python scripts/demo_fetch.py --limit "$LIMIT" --sources "$SOURCES"

# Generate visible artifact
echo "[3/4] Generating local interactive HTML map..."
python scripts/generate_demo_map.py --mrds "$MRDS_JSON" --wgi "$WGI_JSON" --out "$OUT_HTML"

echo "[4/4] Done."
echo "HTML generated at: $OUT_HTML"

if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$OUT_HTML" >/dev/null 2>&1 &
fi

