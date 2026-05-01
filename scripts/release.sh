#!/bin/bash
# Release script for Kindling Framework
#
# Usage: ./scripts/release.sh <version>
# Example: ./scripts/release.sh 0.9.4
#
# What this does:
#   1. Verifies you are on main with a clean working tree
#   2. Verifies the version in pyproject.toml matches <version>
#      (bump first with: poe version --bump_type patch)
#   3. Pushes a v<version> tag to origin
#
# From there, GitHub Actions takes over:
#   - Builds all wheels
#   - Runs system tests on all platforms
#   - Creates and publishes the GitHub release with wheels attached
#
# The release is never visible to the public until all tests pass and
# wheels are ready.

set -e

VERSION=$1

if [ -z "$VERSION" ]; then
    echo "❌ Error: Version required"
    echo "Usage: ./scripts/release.sh <version>"
    echo "Example: ./scripts/release.sh 0.9.4"
    exit 1
fi

# Remove 'v' prefix if provided
VERSION="${VERSION#v}"
TAG="v${VERSION}"

echo "🚀 Preparing release tag ${TAG}"
echo ""

# Verify we're on main branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo "❌ Error: Must be on main branch (currently on ${CURRENT_BRANCH})"
    exit 1
fi

# Check for uncommitted changes
if [[ -n $(git status -s) ]]; then
    echo "❌ Error: Uncommitted changes detected"
    git status -s
    exit 1
fi

# Verify local main is up to date with origin
git fetch origin main --quiet
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)
if [ "$LOCAL" != "$REMOTE" ]; then
    echo "❌ Error: Local main is not up to date with origin/main"
    echo "   Run: git pull origin main"
    exit 1
fi

echo "✅ Git status clean, on main, up to date with origin"
echo ""

# Verify the version in pyproject.toml matches what we're releasing
CURRENT_VERSION=$(grep "^version = " pyproject.toml | sed 's/version = "\(.*\)"/\1/')
if [ "${CURRENT_VERSION}" != "${VERSION}" ]; then
    echo "❌ Error: pyproject.toml version (${CURRENT_VERSION}) does not match release version (${VERSION})"
    echo ""
    echo "   Bump the version first, then re-run this script:"
    echo "   poe version --bump_type patch"
    exit 1
fi

echo "📌 Releasing version ${VERSION}"
echo ""

# Check tag doesn't already exist
if git rev-parse "${TAG}" >/dev/null 2>&1; then
    echo "❌ Error: Tag ${TAG} already exists locally"
    echo "   If you need to re-release, delete the tag first: git tag -d ${TAG}"
    exit 1
fi
if git ls-remote --tags origin "${TAG}" | grep -q "${TAG}"; then
    echo "❌ Error: Tag ${TAG} already exists on origin"
    exit 1
fi

# Push the tag — CI will build, test, and publish the release
echo "🏷️  Pushing tag ${TAG} to origin..."
git tag "${TAG}"
git push origin "${TAG}"

echo ""
echo "✅ Tag ${TAG} pushed."
echo ""
echo "GitHub Actions is now:"
echo "  1. Building wheels (runtime + kindling-sdk + kindling-cli)"
echo "  2. Running system tests on all platforms"
echo "  3. Creating the GitHub release with wheels attached (if tests pass)"
echo ""
echo "🔗 Watch: https://github.com/sep/spark-kindling-framework/actions"
