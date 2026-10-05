#!/bin/bash
# scripts/build_platform_wheels.sh
#
# Thin wrapper around scripts/build.py kept for backwards-compatible callers.
# The underlying build now produces one combined wheel (spark-kindling) plus
# the design-time wheels, rather than three platform-specific wheels.

set -e

# Add user-installed tools (uv) to PATH
export PATH="/home/vscode/.local/bin:$PATH"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache}"
mkdir -p "$UV_CACHE_DIR"

exec python3 scripts/build.py "$@"
