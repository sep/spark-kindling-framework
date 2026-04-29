#!/bin/bash
# Upload runtime wheels to Azure Storage
# Preferred entrypoints:
#   poetry run poe upload
#   poetry run poe upload-release
# Direct script usage:
#   ./scripts/upload_to_storage.sh
#   ./scripts/upload_to_storage.sh --release
#   ./scripts/upload_to_storage.sh --release 0.2.0

set -e

# ============================================================================
# CONFIGURATION - Update these for your storage account
# ============================================================================
STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT}"
CONTAINER="${AZURE_CONTAINER:-artifacts}"
ROOT_BASE_PATH="${AZURE_BASE_PATH:-}"
ROOT_BASE_PATH="${ROOT_BASE_PATH%/}"
PACKAGES_PATH="${ROOT_BASE_PATH:+${ROOT_BASE_PATH}/}packages"

# ============================================================================
# Script Logic
# ============================================================================

USE_RELEASE=false
VERSION=""
COMBINED_PATTERN="spark_kindling-*.whl"
LEGACY_PATTERNS=("kindling_synapse-*.whl" "kindling_databricks-*.whl" "kindling_fabric-*.whl")

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --release|-r)
            USE_RELEASE=true
            shift
            ;;
        *)
            VERSION=$1
            shift
            ;;
    esac
done

echo "📦 Uploading wheels to Azure Storage"
echo "======================================"
echo ""

if [ "$USE_RELEASE" = true ]; then
    if [ -z "$VERSION" ]; then
        VERSION=$(git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//')
        if [ -z "$VERSION" ]; then
            echo "❌ Error: No version specified and no git tags found"
            echo "Usage: ./scripts/upload_to_storage.sh --release [version]"
            exit 1
        fi
        echo "📌 Detected latest version: ${VERSION}"
    else
        VERSION="${VERSION#v}"
        echo "📌 Using version: ${VERSION}"
    fi

    TAG="v${VERSION}"

    if ! git rev-parse "$TAG" >/dev/null 2>&1; then
        echo "❌ Error: Tag ${TAG} not found"
        exit 1
    fi

    if ! command -v gh &> /dev/null; then
        echo "❌ Error: GitHub CLI (gh) not found"
        echo "Please install it: https://cli.github.com/"
        exit 1
    fi

    REPO=$(git remote get-url origin 2>/dev/null | sed -E 's#.*github\.com[:/]([^[:space:]]+?)(\.git)?$#\1#')
    if [ -z "$REPO" ]; then
        echo "❌ Error: Could not detect GitHub repository"
        echo "Make sure you're in a git repository with a GitHub remote"
        exit 1
    fi

    echo "📥 Downloading wheels from GitHub release ${TAG}..."
    echo "📦 Repository: ${REPO}"

    TEMP_DIR=$(mktemp -d)
    trap "rm -rf $TEMP_DIR" EXIT

    gh release download "$TAG" --pattern "*.whl" --repo "$REPO" --dir "$TEMP_DIR" 2>/dev/null || {
        echo "❌ Error: Failed to download wheels from release ${TAG}"
        echo "Make sure the release exists and has wheel attachments"
        echo "Repository: ${REPO}"
        exit 1
    }

    WHEELS_DIR="$TEMP_DIR"
else
    VERSION=$(grep '^version = ' pyproject.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
    if [ -z "$VERSION" ]; then
        echo "❌ Error: Could not detect version from pyproject.toml"
        exit 1
    fi
    echo "📌 Using local build version: ${VERSION}"

    if [ ! -d "dist" ]; then
        echo "❌ Error: dist/ directory not found"
        echo "Run: poetry run poe build"
        exit 1
    fi

    WHEELS_DIR="dist"
    echo "📦 Using local wheels from: ${WHEELS_DIR}"
fi

echo ""

WHEELS_TO_UPLOAD=()
if ls "$WHEELS_DIR"/$COMBINED_PATTERN >/dev/null 2>&1; then
    for wheel in "$WHEELS_DIR"/$COMBINED_PATTERN; do
        [ -f "$wheel" ] && WHEELS_TO_UPLOAD+=("$wheel")
    done
else
    for pattern in "${LEGACY_PATTERNS[@]}"; do
        for wheel in "$WHEELS_DIR"/$pattern; do
            [ -f "$wheel" ] && WHEELS_TO_UPLOAD+=("$wheel")
        done
    done
fi

if [ ${#WHEELS_TO_UPLOAD[@]} -eq 0 ]; then
    echo "❌ Error: No runtime wheels found to upload in ${WHEELS_DIR}"
    echo "Expected ${COMBINED_PATTERN} or legacy kindling_<platform>-*.whl files"
    exit 1
fi

echo "📦 Runtime wheels to upload:"
ls -lh "${WHEELS_TO_UPLOAD[@]}"
echo ""

echo "🔑 Using Azure CLI authentication..."
if ! az account show &>/dev/null; then
    echo "❌ Error: Not logged in to Azure CLI"
    echo "Run: az login"
    exit 1
fi
echo "✓ Logged in as: $(az account show --query user.name -o tsv)"
echo ""

echo "☁️  Uploading to: ${STORAGE_ACCOUNT}/${CONTAINER}/${PACKAGES_PATH}/"
echo ""

UPLOAD_COUNT=0
for wheel in "${WHEELS_TO_UPLOAD[@]}"; do
    wheel_name=$(basename "$wheel")
    DEST_PATH="${PACKAGES_PATH}/${wheel_name}"

    echo "  Uploading: ${wheel_name}..."
    az storage blob upload         --account-name "$STORAGE_ACCOUNT"         --container-name "$CONTAINER"         --name "$DEST_PATH"         --file "$wheel"         --overwrite         --auth-mode login         --only-show-errors

    UPLOAD_COUNT=$((UPLOAD_COUNT + 1))
done

echo ""
echo "✅ Successfully uploaded ${UPLOAD_COUNT} wheel(s)"
echo ""
echo "🎉 Upload complete!"
echo ""
echo "Storage structure:"
echo "  ${STORAGE_ACCOUNT}/${CONTAINER}/${PACKAGES_PATH}/"
for wheel in "${WHEELS_TO_UPLOAD[@]}"; do
    echo "      ├── $(basename "$wheel")"
done
echo ""
echo "🔗 Bootstrap script can now install from:"
if [ -n "$ROOT_BASE_PATH" ]; then
    echo "   artifacts_storage_path: abfss://${CONTAINER}@${STORAGE_ACCOUNT}.dfs.core.windows.net/${ROOT_BASE_PATH}"
else
    echo "   artifacts_storage_path: abfss://${CONTAINER}@${STORAGE_ACCOUNT}.dfs.core.windows.net"
fi
