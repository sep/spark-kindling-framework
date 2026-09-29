#!/bin/sh
# Lazy Kindling CLI for the published domain devcontainer image.
#
# Installed as /usr/local/bin/kindling. The image bakes in no Kindling
# packages: a domain project pins its own spark-kindling-cli (and framework
# and SDK) in pyproject.toml, so the CLI it should run is the project's.
# This shim prefers that, and only when nothing provides a CLI does it
# install the latest published release into the system Python, then runs
# the requested command. pip's console script replaces this file on that
# first install, so the download happens at most once per container.
#
# This is what makes `kindling env bootstrap` (the scaffolded
# postCreateCommand) work in a fresh project without the image shipping a
# CLI whose version has nothing to do with the project's pin.
set -e

# 1. The project's own environment (uv/poetry in-project virtualenv).
if [ -x "./.venv/bin/kindling" ]; then
  exec "./.venv/bin/kindling" "$@"
fi

# 2. A CLI already importable by the system Python.
if python3 -c "import kindling_cli" >/dev/null 2>&1; then
  exec python3 -m kindling_cli.cli "$@"
fi

# 3. Nothing provides one: install the latest release (CLI plus the SDK it
#    requires, which is not on PyPI) and run it. The release JSON streams
#    straight into Python; passing it through the shell's echo mangles JSON
#    escape sequences in release bodies.
echo "kindling: no Kindling CLI in this project or the system Python; installing the latest release..." >&2
WHEEL_URLS="$(curl -fsSL https://api.github.com/repos/sep/spark-kindling-framework/releases/latest \
  | python3 -c 'import json, sys; assets = json.load(sys.stdin)["assets"]; print(" ".join(next(a["browser_download_url"] for a in assets if a["name"].startswith(prefix) and a["name"].endswith(".whl")) for prefix in ("spark_kindling_cli-", "spark_kindling_sdk-")))')"
pip install --no-cache-dir $WHEEL_URLS >&2
exec python3 -m kindling_cli.cli "$@"
