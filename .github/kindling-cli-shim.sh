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

# 1. The project's own environment (uv's in-project .venv).
if [ -x "./.venv/bin/kindling" ]; then
  exec "./.venv/bin/kindling" "$@"
fi

# 2. A CLI already importable by the system Python.
if python3 -c "import kindling_cli" >/dev/null 2>&1; then
  exec python3 -m kindling_cli.cli "$@"
fi

# 3. Nothing provides one: install the latest release and run it. The
#    latest GitHub release names the version (a release can reach PyPI later,
#    after its publish approval); that exact CLI and SDK come from PyPI, or
#    from the release's wheels while PyPI doesn't have them yet. If GitHub is
#    unreachable, PyPI's latest is used. The release JSON goes to a file and
#    straight into Python; passing it through the shell's echo mangles JSON
#    escape sequences in release bodies.
echo "kindling: no Kindling CLI in this project or the system Python; installing the latest release..." >&2
RELEASE_JSON="$(mktemp)"
trap 'rm -f "$RELEASE_JSON"' EXIT
if curl -fsSL https://api.github.com/repos/sep/spark-kindling-framework/releases/latest -o "$RELEASE_JSON"; then
  VERSION="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["tag_name"].lstrip("v"))' "$RELEASE_JSON")"
  if ! pip install --no-cache-dir "spark-kindling-cli==$VERSION" "spark-kindling-sdk==$VERSION" >&2; then
    echo "kindling: $VERSION is not on PyPI yet; installing it from the GitHub release..." >&2
    WHEEL_URLS="$(python3 -c 'import json, sys; assets = json.load(open(sys.argv[1]))["assets"]; print(" ".join(next(a["browser_download_url"] for a in assets if a["name"].startswith(prefix) and a["name"].endswith(".whl")) for prefix in ("spark_kindling_cli-", "spark_kindling_sdk-")))' "$RELEASE_JSON")"
    pip install --no-cache-dir $WHEEL_URLS >&2
  fi
else
  echo "kindling: GitHub is unreachable; installing the latest CLI from PyPI..." >&2
  pip install --no-cache-dir spark-kindling-cli >&2
fi
exec python3 -m kindling_cli.cli "$@"
