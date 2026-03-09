#!/bin/bash
# Script to publish GitHub secrets from .env file
# Usage:
#   ./scripts/set_github_secrets.sh [--environment <env>] [--name <VAR_NAME>]

set -e

ENVIRONMENT=""
SECRET_NAME=""
VAR_NAME_PATTERN='[A-Za-z_][A-Za-z0-9_]*'

usage() {
    echo "Usage: $0 [--environment <env>] [--name <VAR_NAME>]"
    echo ""
    echo "Options:"
    echo "  --environment <env>  Publish as GitHub environment secrets (e.g. dev, test)"
    echo "  --name <VAR_NAME>    Publish only a single variable from .env"
    echo "  -h, --help           Show this help message"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --environment)
            if [[ -z "${2:-}" ]]; then
                echo "❌ Missing value for --environment"
                usage
                exit 1
            fi
            ENVIRONMENT="$2"
            shift 2
            ;;
        --name)
            if [[ -z "${2:-}" ]]; then
                echo "❌ Missing value for --name"
                usage
                exit 1
            fi
            SECRET_NAME="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "❌ Unknown argument: $1"
            usage
            exit 1
            ;;
    esac
done

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

TARGET_DESC="repository"
if [[ -n "$ENVIRONMENT" ]]; then
    TARGET_DESC="environment '$ENVIRONMENT'"
fi

echo "🔐 Setting GitHub ${TARGET_DESC} secrets from .env file..."
echo ""

# Source the .env file to load environment variables
set -a  # automatically export all variables
source .env
set +a  # disable auto-export

set_secret() {
    local key="$1"
    value="${!key}"

    if [ -n "$value" ]; then
        echo "Setting secret: $key"
        cmd=(gh secret set "$key" --body -)
        if [[ -n "$ENVIRONMENT" ]]; then
            cmd+=(--env "$ENVIRONMENT")
        fi
        echo "$value" | "${cmd[@]}"
    else
        echo "⚠️  Skipping $key (empty value)"
    fi
}

if [[ -n "$SECRET_NAME" ]]; then
    if ! grep -qE "^export[[:space:]]+${SECRET_NAME}=|^${SECRET_NAME}=" .env; then
        echo "❌ Variable '$SECRET_NAME' not found in .env"
        exit 1
    fi

    set_secret "$SECRET_NAME"
else
    # Extract secret names from .env (lines starting with export or just VAR=)
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip comments and empty lines
        if [[ "$line" =~ ^#.*$ ]] || [[ -z "$line" ]]; then
            continue
        fi

        # Extract variable name from export statements
        if [[ "$line" =~ ^export[[:space:]]+($VAR_NAME_PATTERN)= ]]; then
            key="${BASH_REMATCH[1]}"
        # Extract variable name from regular KEY=VALUE statements
        elif [[ "$line" =~ ^($VAR_NAME_PATTERN)= ]]; then
            key="${BASH_REMATCH[1]}"
        else
            continue
        fi

        set_secret "$key"
    done < .env
fi

echo ""
echo "✅ GitHub secrets have been set successfully!"
echo ""
if [[ -n "$ENVIRONMENT" ]]; then
    echo "To verify, run: gh secret list --env $ENVIRONMENT"
else
    echo "To verify, run: gh secret list"
fi
