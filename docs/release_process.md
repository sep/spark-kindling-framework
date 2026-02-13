# Release Process Guide

This guide explains how to create releases for the Kindling framework and how wheels are automatically attached.

## 📦 What Happens on Release

When you publish a GitHub Release:

1. ✅ **All tests run** (unit, integration, KDA, system, security)
2. ✅ **Platform wheels are built** (synapse, databricks, fabric)
3. ✅ **Wheels are automatically attached** to the release as downloadable assets
4. ✅ **Release notes are generated** with download links

## 🚀 Creating a Release

### Step 1: Prepare the Release

```bash
# 1. Bump version in pyproject.toml
poetry run poe version --bump_type patch
# Or: --bump_type minor / --bump_type major

# 2. Add release notes file
vim docs/releases/v0.6.0.md

# 3. Commit and push
git add pyproject.toml docs/releases/v0.6.0.md
git commit -m "chore: prepare release v0.6.0"
git push origin main

# 4. Wait for CI to pass
# Check: https://github.com/sep/spark-kindling-framework/actions
```

### Step 2: Create the Release on GitHub

#### Option A: Via GitHub UI (Recommended for first time)

1. **Go to Releases Page**
   ```
   https://github.com/sep/spark-kindling-framework/releases
   ```

2. **Click "Draft a new release"**

3. **Fill in Release Details**
   - **Choose a tag**: `v0.6.0` (create new tag)
   - **Target**: `main` branch
   - **Release title**: `v0.6.0 - Brief description`
   - **Description**: Add release notes (or click "Generate release notes")

4. **Publish Release**
   - Click "Publish release"
   - GitHub Actions automatically triggers
   - Wheels are built and attached within ~5 minutes

#### Option B: Via GitHub CLI

```bash
# Install gh CLI if needed
# https://cli.github.com/

# Create release with auto-generated notes
gh release create v0.6.0 \
  --title "v0.6.0 - Feature Release" \
  --generate-notes

# Or with custom notes
gh release create v0.6.0 \
  --title "v0.6.0 - Feature Release" \
  --notes-file docs/releases/v0.6.0.md
```

### Step 3: Verify Release Assets

After the workflow completes:

1. **Check Release Page**
   ```
   https://github.com/sep/spark-kindling-framework/releases/tag/v0.6.0
   ```

2. **Verify Assets Section shows:**
   - ✅ `kindling_synapse-0.6.0-py3-none-any.whl`
   - ✅ `kindling_databricks-0.6.0-py3-none-any.whl`
   - ✅ `kindling_fabric-0.6.0-py3-none-any.whl`
   - ✅ Source code (zip)
   - ✅ Source code (tar.gz)

## 📥 Installing from Release

### Direct Download (Manual)

```bash
# 1. Download wheel from release page
# https://github.com/sep/spark-kindling-framework/releases/latest

# 2. Install locally
pip install kindling_synapse-0.6.0-py3-none-any.whl
```

### Direct Install from URL

```bash
# Install directly from GitHub Release
pip install https://github.com/sep/spark-kindling-framework/releases/download/v0.6.0/kindling_synapse-0.6.0-py3-none-any.whl

# Or use the "latest" release
pip install https://github.com/sep/spark-kindling-framework/releases/latest/download/kindling_synapse-0.6.0-py3-none-any.whl
```

### In requirements.txt

```txt
# requirements.txt

# Install specific version from release
kindling-synapse @ https://github.com/sep/spark-kindling-framework/releases/download/v0.6.0/kindling_synapse-0.6.0-py3-none-any.whl

# Or always use latest
kindling-synapse @ https://github.com/sep/spark-kindling-framework/releases/latest/download/kindling_synapse-0.6.0-py3-none-any.whl
```

### In Databricks/Synapse/Fabric

```python
# Databricks notebook
%pip install https://github.com/sep/spark-kindling-framework/releases/download/v0.6.0/kindling_databricks-0.6.0-py3-none-any.whl

# Or in cluster libraries
# UI: Libraries → Install New → PyPI
# Package: https://github.com/sep/spark-kindling-framework/releases/download/v0.6.0/kindling_databricks-0.6.0-py3-none-any.whl
```

## 🏷️ Release Types

### Semantic Versioning

Follow [Semantic Versioning](https://semver.org/):

```
MAJOR.MINOR.PATCH

Examples:
v0.1.0  - Initial release
v0.1.1  - Bug fix (patch)
v0.2.0  - New features (minor)
v1.0.0  - Breaking changes (major)
```

### Pre-releases

For beta/alpha versions:

```bash
# Create pre-release
gh release create v0.6.0-beta.1 \
  --prerelease \
  --title "v0.6.0 Beta 1" \
  --notes "Beta release for testing"

# Users can install with:
pip install https://github.com/sep/spark-kindling-framework/releases/download/v0.6.0-beta.1/kindling_synapse-0.6.0b1-py3-none-any.whl
```

## 📊 What Shows Up in a Release

When you navigate to a release page, users will see:

```
Release v0.6.0 - Feature Release
Published by @username on Oct 17, 2025

[Release notes here]

Assets

 kindling_synapse-0.6.0-py3-none-any.whl      76 KB
 kindling_databricks-0.6.0-py3-none-any.whl   76 KB
 kindling_fabric-0.6.0-py3-none-any.whl       72 KB
 Source code (zip)
 Source code (tar.gz)
```

## 🔄 Automated Version Bumping

Use the existing `poe version` task (defined in `pyproject.toml`):

```bash
poetry run poe version --bump_type patch   # 0.6.0 -> 0.6.1
poetry run poe version --bump_type minor   # 0.6.0 -> 0.7.0
poetry run poe version --bump_type major   # 0.6.0 -> 1.0.0
poetry run poe version --bump_type alpha   # 0.6.0 -> 0.6.1a1
```

This updates the version in `pyproject.toml` and can optionally trigger build/deploy.

## 🎯 Complete Release Workflow

```bash
# 1. Update version
poetry run poe version --bump_type patch

# 2. Update release notes
vim docs/releases/v0.6.0.md

# 3. Commit changes
git add .
git commit -m "chore: prepare release v0.6.0"
git push origin main

# 4. Wait for CI to pass (check Actions tab)

# 5. Create release
gh release create v0.6.0 \
  --title "v0.6.0 - Feature Release" \
  --notes-file docs/releases/v0.6.0.md

# 6. Monitor release build
# Go to: Actions → Wait for "Attach Wheels to Release" job

# 7. Verify release
gh release view v0.6.0

# 8. Test installation
pip install https://github.com/sep/spark-kindling-framework/releases/download/v0.6.0/kindling_synapse-0.6.0-py3-none-any.whl
```

## 🔐 Access Control for Releases

### Public Repository
- ✅ Anyone can view releases
- ✅ Anyone can download assets
- ❌ Only maintainers can create releases

### Private Repository
- ✅ Only org members can view releases
- ✅ Only org members can download assets
- ❌ Only maintainers can create releases
- 💡 Users need GitHub authentication to download

For private repos, users must authenticate:

```bash
# Option 1: Use GitHub CLI (automatic auth)
gh release download v0.6.0 --pattern "*.whl"

# Option 2: Use curl with token
curl -H "Authorization: token YOUR_PAT" \
  -L https://github.com/sep/spark-kindling-framework/releases/download/v0.6.0/kindling_synapse-0.6.0-py3-none-any.whl \
  -o kindling_synapse-0.6.0-py3-none-any.whl
```

## 📝 Release Notes Best Practices

### Good Release Notes

```markdown
## What's Changed

### 🚀 New Features
- Added support for Fabric OneLake paths (#123)
- Implemented automatic schema evolution (#125)

### 🐛 Bug Fixes
- Fixed Azure Key Vault authentication timeout (#130)
- Resolved memory leak in streaming pipelines (#132)

### 📚 Documentation
- Added comprehensive API documentation
- Updated deployment guides for all platforms

### ⚠️ Breaking Changes
- Renamed `app_framework` to `data_apps` - **migration required**
- Changed configuration format for Synapse - see migration guide

### 🔧 Maintenance
- Upgraded to PySpark 3.5.0
- Updated all dependencies for security patches

**Full Changelog**: https://github.com/sep/spark-kindling-framework/compare/v0.5.0...v0.6.0
```

### Use GitHub's Auto-Generated Notes

GitHub can automatically generate release notes from PR titles:

1. Click "Generate release notes" when creating a release
2. Review and edit as needed
3. Categorizes by labels (feature, bug, documentation, etc.)

## 🚨 Hotfix Releases

For urgent bug fixes:

```bash
# 1. Create hotfix branch from tag
git checkout -b hotfix/0.6.1 v0.6.0

# 2. Fix the bug
git add .
git commit -m "fix: critical bug in platform detection"

# 3. Update version
poetry run poe version --bump_type patch

# 4. Push and create PR
git push origin hotfix/0.6.1

# 5. After PR approval and merge
gh release create v0.6.1 \
  --title "v0.6.1 - Hotfix Release" \
  --notes "Critical bug fix: platform detection"
```

## 📊 Monitoring Releases

### View Download Statistics

GitHub tracks download counts for release assets:

1. Go to: `https://github.com/sep/spark-kindling-framework/releases`
2. Each asset shows download count
3. Use GitHub API for detailed stats:

```bash
# Get release download stats
curl -H "Authorization: token YOUR_PAT" \
  https://api.github.com/repos/sep/spark-kindling-framework/releases
```

### Release Notifications

- Users can "Watch" your repo → "Releases only"
- They'll get notified of new releases
- RSS feed available: `/releases.atom`

## 🔄 Comparison: Release Assets vs GitHub Packages vs PyPI

| Feature | Release Assets | GitHub Packages | PyPI |
|---------|---------------|-----------------|------|
| **Visibility** | Public/Private with repo | Public/Private with repo | Always public |
| **Installation** | Direct URL | `--extra-index-url` | Standard `pip install` |
| **Versioning** | Tag-based | Semantic versioning | Semantic versioning |
| **Storage** | Free unlimited | 500MB free (private) | Free unlimited |
| **Authentication** | GitHub token (private) | GitHub PAT (private) | None needed |
| **Discoverability** | Via repo | Via repo/org | Global search |
| **Best For** | Quick distribution | Internal packages | Public packages |

## 💡 Recommendations

For Kindling framework:

### Use Release Assets if:
✅ You want simple, direct downloads
✅ You don't need version resolution
✅ Users are comfortable with URLs
✅ You want zero setup beyond CI/CD

### Use GitHub Packages if:
✅ You want proper `pip install` workflow
✅ You need version management
✅ Users will have many dependencies
✅ You want organization-wide package registry

### Use PyPI if:
✅ You want public, global distribution
✅ Building an open-source framework
✅ Want maximum discoverability

**Current Setup**: Release assets are enabled! Wheels automatically attach to every release. You can add GitHub Packages or PyPI later if needed.

## 📚 Additional Resources

- [GitHub Releases Documentation](https://docs.github.com/en/repositories/releasing-projects-on-github)
- [Semantic Versioning](https://semver.org/)
- [GitHub CLI Releases](https://cli.github.com/manual/gh_release)
- [Kindling CI/CD Setup](./ci_cd_setup.md)
