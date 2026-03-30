#!/bin/bash
# Load .env file if it exists
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$REPO_ROOT/.env"

if [ -f "$ENV_FILE" ]; then
    echo "Loading environment variables from .env..."
    set -a  # automatically export all variables
    source "$ENV_FILE"
    set +a  # turn off automatic export
    echo "✓ Environment variables loaded"
else
    echo "No .env file found at $ENV_FILE"
fi
