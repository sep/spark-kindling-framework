#!/bin/bash
# scripts/build_platform_wheels.sh
#
# Thin wrapper around scripts/build.py kept for backwards-compatible callers.
# The underlying build now produces one combined wheel (spark-kindling) plus
# the design-time wheels, rather than three platform-specific wheels.

set -e

# Add Poetry to PATH
export PATH="/home/vscode/.local/bin:$PATH"
export POETRY_CACHE_DIR="${POETRY_CACHE_DIR:-/tmp/poetry-cache}"
export POETRY_VIRTUALENVS_PATH="${POETRY_VIRTUALENVS_PATH:-/tmp/poetry-virtualenvs}"
export VIRTUALENV_OVERRIDE_APP_DATA="${VIRTUALENV_OVERRIDE_APP_DATA:-/tmp/virtualenv-app-data}"
export XDG_DATA_HOME="${XDG_DATA_HOME:-/tmp/xdg-data}"
mkdir -p "$POETRY_CACHE_DIR" "$POETRY_VIRTUALENVS_PATH" "$VIRTUALENV_OVERRIDE_APP_DATA" "$XDG_DATA_HOME"

exec python3 scripts/build.py "$@"
