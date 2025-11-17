#!/bin/bash
# Load .env file if it exists
if [ -f "/workspace/.env" ]; then
    echo "Loading environment variables from .env..."
    set -a  # automatically export all variables
    source "/workspace/.env"
    set +a  # turn off automatic export
    echo "✓ Environment variables loaded"
else
    echo "No .env file found at /workspace/.env"
fi
