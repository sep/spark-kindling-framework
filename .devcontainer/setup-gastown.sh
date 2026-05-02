#!/usr/bin/env bash
# One-time Gastown HQ + rig bootstrap.
# Deletes itself on success so it never runs again.
set -euo pipefail

HQ_DIR="${HOME}/gt-hq"
KINDLING_DIR="/workspaces/kindling"
SELF="${KINDLING_DIR}/.devcontainer/setup-gastown.sh"

echo "⚙️  Bootstrapping Gastown HQ at ${HQ_DIR}..."

gt install "${HQ_DIR}" --name kindling-workspace --no-git

cd "${HQ_DIR}"
gt rig add kindling --adopt --url "$(git -C "${KINDLING_DIR}" remote get-url origin 2>/dev/null || true)"

cd "${KINDLING_DIR}"
bd init --stealth
bd setup claude --stealth
bd setup codex

echo "✅ Gastown bootstrap complete"
echo "   Run: git rm .devcontainer/setup-gastown.sh && git commit -m 'chore: Gastown bootstrap complete'"

# Self-destruct — file is gone from the mounted workspace so this never re-runs
rm -f "${SELF}"
