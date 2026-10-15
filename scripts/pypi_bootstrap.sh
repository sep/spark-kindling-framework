#!/usr/bin/env bash
# One-time PyPI bootstrap: upload the first release candidate by hand so the
# eight projects exist, then print the trusted-publisher forms to fill in.
# After that, CI publishes every release (see docs/contributing/release_process.md).
#
# Usage: poe pypi-bootstrap            # TestPyPI, then PyPI
#        poe pypi-bootstrap --links    # only print the publisher checklist
#
# Tokens: TESTPYPI_TOKEN and PYPI_TOKEN (account-wide API tokens), or typed at
# the prompt (not echoed). Revoke them once the trusted publishers are added.
set -euo pipefail

cd "$(dirname "$0")/.."

PROJECTS=(
  spark-kindling spark-kindling-cli spark-kindling-sdk
  spark-kindling-ext-databricks spark-kindling-ext-sdp
  spark-kindling-ext-cosmos spark-kindling-ext-temporal
  spark-kindling-ext-otel-azure
)

print_links() {
  echo ""
  echo "Add a trusted publisher to each project (16 forms):"
  echo "  Owner: sep   Repository: spark-kindling-framework   Workflow: ci.yml"
  echo ""
  echo "  pypi.org -- Environment: pypi"
  for project in "${PROJECTS[@]}"; do
    echo "    https://pypi.org/manage/project/${project}/settings/publishing/"
  done
  echo ""
  echo "  test.pypi.org -- Environment: testpypi"
  for project in "${PROJECTS[@]}"; do
    echo "    https://test.pypi.org/manage/project/${project}/settings/publishing/"
  done
  echo ""
  echo "Then revoke both API tokens."
}

if [ "${1:-}" = "--links" ]; then
  print_links
  exit 0
fi

# Same preconditions as `poe release`: clean, up-to-date main.
if [ "$(git branch --show-current)" != "main" ]; then
  echo "❌ Run this on main (currently on $(git branch --show-current))." >&2
  exit 1
fi
if [ -n "$(git status --porcelain)" ]; then
  echo "❌ Working tree is not clean." >&2
  git status --short >&2
  exit 1
fi
git fetch origin main --quiet
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
  echo "❌ Local main is not up to date with origin/main (git pull)." >&2
  exit 1
fi

VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' pyproject.toml | head -1)
# The bootstrap claims the names with a release candidate; prereleases are
# not installed unless asked for by version.
if ! [[ "$VERSION" =~ (a|b|rc)[0-9]+$ ]]; then
  echo "❌ Version $VERSION is not a release candidate. Run 'poe version --bump_type rc' first." >&2
  exit 1
fi

UPLOAD_DIR="dist/pypi-bootstrap-${VERSION}"
rm -rf "$UPLOAD_DIR"
uv sync --quiet
uv run poe build
bash scripts/select_pypi_dists.sh dist "$UPLOAD_DIR"

echo ""
echo "About to upload the files above (version $VERSION) to TestPyPI, then PyPI."
echo "Uploads are permanent: a file can never be replaced under the same version."
read -r -p "Type the version to continue: " CONFIRM
if [ "$CONFIRM" != "$VERSION" ]; then
  echo "Aborted." >&2
  exit 1
fi

if [ -z "${TESTPYPI_TOKEN:-}" ]; then
  read -r -s -p "test.pypi.org API token: " TESTPYPI_TOKEN
  echo ""
fi
if [ -z "${PYPI_TOKEN:-}" ]; then
  read -r -s -p "pypi.org API token: " PYPI_TOKEN
  echo ""
fi

# --check-url skips files already uploaded, so a rerun after a partial
# failure uploads only what is missing.
echo "Uploading to TestPyPI..."
UV_PUBLISH_TOKEN="$TESTPYPI_TOKEN" uv publish \
  --publish-url https://test.pypi.org/legacy/ \
  --check-url https://test.pypi.org/simple/ \
  "$UPLOAD_DIR"/*
echo "Uploading to PyPI..."
UV_PUBLISH_TOKEN="$PYPI_TOKEN" uv publish \
  --check-url https://pypi.org/simple/ \
  "$UPLOAD_DIR"/*

echo ""
echo "✅ Uploaded $VERSION to TestPyPI and PyPI."
print_links
echo "Next: poe release $VERSION (CI finds these files already uploaded and skips them)."
