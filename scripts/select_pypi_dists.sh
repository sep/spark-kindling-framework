#!/usr/bin/env bash
# Copy the distributions published to PyPI from a build directory into an
# upload directory: one wheel and one sdist per published package.
#
# Usage: scripts/select_pypi_dists.sh <dist-dir> <out-dir>
#
# Used by CI's publish-pypi job and by the one-time manual bootstrap upload
# (see docs/contributing/release_process.md). Packages not listed here (adx,
# databricks_autoloader, visualization) stay GitHub-release-only.
set -euo pipefail

DIST_DIR="${1:?dist dir}"
OUT_DIR="${2:?out dir}"
PUBLISHED=(
  spark_kindling spark_kindling_cli spark_kindling_sdk
  spark_kindling_ext_databricks spark_kindling_ext_sdp
  spark_kindling_ext_cosmos spark_kindling_ext_temporal
  spark_kindling_ext_otel_azure
)

mkdir -p "$OUT_DIR"
for name in "${PUBLISHED[@]}"; do
  shopt -s nullglob
  files=("$DIST_DIR"/"$name"-[0-9]*.whl "$DIST_DIR"/"$name"-[0-9]*.tar.gz)
  shopt -u nullglob
  if [ "${#files[@]}" -ne 2 ]; then
    echo "❌ Expected one wheel and one sdist for $name in $DIST_DIR, found: ${files[*]:-none}" >&2
    exit 1
  fi
  cp "${files[@]}" "$OUT_DIR"/
done
ls -l "$OUT_DIR"
