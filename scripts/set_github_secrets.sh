#!/bin/bash
# Script to set GitHub repository secrets from .env file
# Usage: ./scripts/set_github_secrets.sh

set -e

# Check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    echo "❌ GitHub CLI (gh) is not installed"
    echo "   Install from: https://cli.github.com/"
    exit 1
fi

# Check if authenticated
if ! gh auth status &> /dev/null; then
    echo "❌ Not authenticated with GitHub CLI"
    echo "   Run: gh auth login"
    exit 1
fi

# Check if .env exists
if [ ! -f .env ]; then
    echo "❌ .env file not found"
    exit 1
fi

echo "🔐 Setting GitHub repository secrets from .env file..."
echo ""

# Source the .env file to load environment variables
set -a  # automatically export all variables
source .env
set +a  # disable auto-export

# Extract secret names from .env (lines starting with export or just VAR=)
while IFS= read -r line || [ -n "$line" ]; do
    # Skip comments and empty lines
    if [[ "$line" =~ ^#.*$ ]] || [[ -z "$line" ]]; then
        continue
    fi

    # Extract variable name from export statements
    if [[ "$line" =~ ^export[[:space:]]+([A-Z_][A-Z0-9_]*)= ]]; then
        key="${BASH_REMATCH[1]}"
    # Extract variable name from regular KEY=VALUE statements
    elif [[ "$line" =~ ^([A-Z_][A-Z0-9_]*)= ]]; then
        key="${BASH_REMATCH[1]}"
    else
        continue
    fi

    # Get the actual value from the environment variable
    value="${!key}"

    if [ -n "$value" ]; then
        echo "Setting secret: $key"
        echo "$value" | gh secret set "$key" --body -
    else
        echo "⚠️  Skipping $key (empty value)"
    fi
done < .env

echo ""
echo "✅ GitHub secrets have been set successfully!"
echo ""
echo "To verify, run: gh secret list"
