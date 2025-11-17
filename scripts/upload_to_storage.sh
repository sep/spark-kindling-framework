#!/bin/bash
# Upload wheels to Azure Storage
# Usage:
#   ./scripts/upload_to_storage.sh              # Upload from local dist/ (testing)
#   ./scripts/upload_to_storage.sh --release    # Upload from GitHub release (production)
#   ./scripts/upload_to_storage.sh --release 0.2.0  # Upload specific release version

set -e

# ============================================================================
# CONFIGURATION - Update these for your storage account
# ============================================================================
STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT}"
CONTAINER="${AZURE_CONTAINER:-artifacts}"
BASE_PATH="${AZURE_BASE_PATH:-packages}"  # Base path in storage: packages/...

# ============================================================================
# Script Logic
# ============================================================================

USE_RELEASE=false
VERSION=""

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
    # ========================================================================
    # RELEASE MODE: Download from GitHub release
    # ========================================================================

    # Get version from argument or detect latest tag
    if [ -z "$VERSION" ]; then
        VERSION=$(git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//')
        if [ -z "$VERSION" ]; then
            echo "❌ Error: No version specified and no git tags found"
            echo "Usage: ./scripts/upload_to_storage.sh --release [version]"
            exit 1
        fi
        echo "📌 Detected latest version: ${VERSION}"
    else
        # Remove 'v' prefix if provided
        VERSION="${VERSION#v}"
        echo "📌 Using version: ${VERSION}"
    fi

    TAG="v${VERSION}"

    # Check if release exists
    if ! git rev-parse "$TAG" >/dev/null 2>&1; then
        echo "❌ Error: Tag ${TAG} not found"
        exit 1
    fi

    # Check GitHub CLI is available
    if ! command -v gh &> /dev/null; then
        echo "❌ Error: GitHub CLI (gh) not found"
        echo "Please install it: https://cli.github.com/"
        exit 1
    fi

    # Detect GitHub repository
    REPO=$(git remote get-url origin 2>/dev/null | sed 's/.*github.com[:/]\(.*\)\.git/\1/' | sed 's/.*github.com[:/]\(.*\)/\1/')
    if [ -z "$REPO" ]; then
        echo "❌ Error: Could not detect GitHub repository"
        echo "Make sure you're in a git repository with a GitHub remote"
        exit 1
    fi

    # Download wheels from GitHub release
    echo "📥 Downloading wheels from GitHub release ${TAG}..."
    echo "📦 Repository: ${REPO}"

    TEMP_DIR=$(mktemp -d)
    trap "rm -rf $TEMP_DIR" EXIT

    # Download release assets to temp directory
    gh release download "$TAG" --pattern "*.whl" --repo "$REPO" --dir "$TEMP_DIR" 2>/dev/null || {
        echo "❌ Error: Failed to download wheels from release ${TAG}"
        echo "Make sure the release exists and has wheel attachments"
        echo "Repository: ${REPO}"
        exit 1
    }

    WHEELS_DIR="$TEMP_DIR"
else
    # ========================================================================
    # LOCAL MODE: Upload from dist/ (for testing)
    # ========================================================================

    # Detect version from pyproject.toml
    VERSION=$(grep '^version = ' pyproject.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
    if [ -z "$VERSION" ]; then
        echo "❌ Error: Could not detect version from pyproject.toml"
        exit 1
    fi
    echo "📌 Using local build version: ${VERSION}"

    # Check if dist/ has wheels
    if [ ! -d "dist" ] || [ -z "$(ls -A dist/*.whl 2>/dev/null)" ]; then
        echo "❌ Error: No wheels found in dist/"
        echo "Run: bash scripts/build_platform_wheels.sh"
        exit 1
    fi

    WHEELS_DIR="dist"
    echo "📦 Using local wheels from: ${WHEELS_DIR}"
fi

echo ""

# List wheels to upload
echo "📦 Wheels to upload:"
ls -lh "$WHEELS_DIR"/*.whl
echo ""

# Check Azure CLI authentication
echo "🔑 Using Azure CLI authentication..."
if ! az account show &>/dev/null; then
    echo "❌ Error: Not logged in to Azure CLI"
    echo "Run: az login"
    exit 1
fi
echo "✓ Logged in as: $(az account show --query user.name -o tsv)"
echo ""

# Upload wheels to storage
echo "☁️  Uploading to: ${STORAGE_ACCOUNT}/${CONTAINER}/${BASE_PATH}/"
echo ""

UPLOAD_COUNT=0
for wheel in "$WHEELS_DIR"/*.whl; do
    wheel_name=$(basename "$wheel")
    DEST_PATH="${BASE_PATH}/${wheel_name}"

    echo "  Uploading: ${wheel_name}..."
    az storage blob upload \
        --account-name "$STORAGE_ACCOUNT" \
        --container-name "$CONTAINER" \
        --name "$DEST_PATH" \
        --file "$wheel" \
        --overwrite \
        --auth-mode login \
        --only-show-errors

    UPLOAD_COUNT=$((UPLOAD_COUNT + 1))
done

echo ""
echo "✅ Successfully uploaded ${UPLOAD_COUNT} wheels"
echo ""
echo "🎉 Upload complete!"
echo ""
echo "Storage structure:"
echo "  ${STORAGE_ACCOUNT}/${CONTAINER}/${BASE_PATH}/"
echo "      ├── kindling_databricks-${VERSION}-py3-none-any.whl"
echo "      ├── kindling_fabric-${VERSION}-py3-none-any.whl"
echo "      └── kindling_synapse-${VERSION}-py3-none-any.whl"
echo ""
echo "🔗 Bootstrap script can now install from:"
echo "   artifacts_storage_path: abfss://${CONTAINER}@${STORAGE_ACCOUNT}.dfs.core.windows.net/${BASE_PATH}"
