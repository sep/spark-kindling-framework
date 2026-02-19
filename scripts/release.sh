#!/bin/bash
# Automated release script for Kindling Framework
# Works with existing GitHub Actions workflow to build and attach wheels
# Usage: ./scripts/release.sh <version>
# Example: ./scripts/release.sh 0.2.0

set -e  # Exit on any error

VERSION=$1

if [ -z "$VERSION" ]; then
    echo "❌ Error: Version required"
    echo "Usage: ./scripts/release.sh <version>"
    echo "Example: ./scripts/release.sh 0.2.0"
    exit 1
fi

# Remove 'v' prefix if provided
VERSION="${VERSION#v}"
TAG="v${VERSION}"

echo "🚀 Starting release process for version ${VERSION}"
echo ""

# Verify we're on main branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo "⚠️  Warning: Not on main branch (currently on ${CURRENT_BRANCH})"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check for uncommitted changes
if [[ -n $(git status -s) ]]; then
    echo "❌ Error: Uncommitted changes detected"
    git status -s
    exit 1
fi

echo "✅ Git status clean"
echo ""

# Check current version
CURRENT_VERSION=$(grep "^version = " build-configs/fabric.toml | sed 's/version = "\(.*\)"/\1/')
echo "📌 Current version: ${CURRENT_VERSION}"

if [ "${CURRENT_VERSION}" = "${VERSION}" ]; then
    echo "⚠️  Version is already ${VERSION}"
    read -p "Continue with release anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Release cancelled"
        exit 1
    fi
    VERSION_UPDATED=false
else
    # Update version in all build configs
    echo "📝 Updating version from ${CURRENT_VERSION} to ${VERSION}..."
    for config in build-configs/*.toml; do
        sed -i "s/^version = .*/version = \"${VERSION}\"/" "$config"
        echo "  ✓ Updated ${config}"
    done

    # Update version in main pyproject.toml if it exists
    if [ -f "pyproject.toml" ]; then
        sed -i "s/^version = .*/version = \"${VERSION}\"/" pyproject.toml
        echo "  ✓ Updated pyproject.toml"
    fi

    # Update version in design-time package pyproject files if present
    for pkg_pyproject in packages/kindling_sdk/pyproject.toml packages/kindling_cli/pyproject.toml; do
        if [ -f "$pkg_pyproject" ]; then
            sed -i "s/^version = .*/version = \"${VERSION}\"/" "$pkg_pyproject"
            echo "  ✓ Updated ${pkg_pyproject}"
        fi
    done
    VERSION_UPDATED=true

    echo ""

    # Show what changed
    echo "📋 Version changes:"
    git diff build-configs/*.toml pyproject.toml packages/kindling_sdk/pyproject.toml packages/kindling_cli/pyproject.toml 2>/dev/null || true
fi

echo ""

# Commit and push version changes if they were made
if [ "$VERSION_UPDATED" = true ]; then
    echo "💾 Committing version changes..."
    git add build-configs/*.toml pyproject.toml packages/kindling_sdk/pyproject.toml packages/kindling_cli/pyproject.toml 2>/dev/null || true
    git commit -m "Bump version to ${VERSION} [skip ci]"
    echo ""

    echo "⬆️  Pushing version bump to main..."
    git push origin main
    echo ""
else
    echo "ℹ️  No version changes to commit"
    echo ""
fi

# Check if gh CLI is available
if ! command -v gh &> /dev/null; then
    echo "❌ Error: GitHub CLI (gh) not found"
    echo "Please install it: https://cli.github.com/"
    echo ""
    echo "Or create release manually:"
    echo "1. Go to: https://github.com/sep/spark-kindling-framework/releases/new"
    echo "2. Tag: ${TAG}"
    echo "3. Click 'Publish release'"
    echo "4. GitHub Actions will automatically build and attach wheels"
    exit 1
fi

# Create GitHub release
echo "🎉 Creating GitHub release ${TAG}..."
echo "GitHub Actions will automatically build and attach wheels..."
echo ""

gh release create "${TAG}" \
    --title "Release ${TAG}" \
    --generate-notes

echo ""
echo "✅ Release ${TAG} created!"
echo ""
echo "🔄 GitHub Actions is now:"
echo "  1. Building wheels (runtime + kindling-sdk + kindling-cli)"
echo "  2. Running tests"
echo "  3. Attaching wheels to the release"
echo ""
echo "🔗 View release: https://github.com/sep/spark-kindling-framework/releases/tag/${TAG}"
echo "🔗 View workflow: https://github.com/sep/spark-kindling-framework/actions"
echo ""
echo "⏳ Wait a few minutes for wheels to be attached..."
