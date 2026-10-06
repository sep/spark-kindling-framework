# Release Process Guide

This guide explains how to create releases for the Kindling framework, how wheels are validated and attached, and how they reach PyPI.

## 📦 What Happens on Release

When you push a `v*` tag with `poe release`, GitHub Actions classifies the files
changed since the previous version tag and chooses the required release lane:

- **Runtime release**: builds wheels, stages release-candidate artifacts, runs Synapse/Fabric/Databricks system tests, then publishes the GitHub release.
- **CLI release**: builds and smoke-tests wheels, then publishes after unit, integration, quality, and security gates pass.
- **SDK release**: builds and smoke-tests wheels, then publishes after unit, integration, quality, and security gates pass.
- **Docs/proposal-only release**: publishes release notes without wheel assets.

Workflow, build-system, runtime package, build-config, and system-test changes
take the runtime lane. Unknown paths also take the runtime lane.

Once the GitHub release is published, the `publish-pypi` job uploads the
packages to PyPI (prerelease tags go to TestPyPI instead); see
[Publishing to PyPI](#-publishing-to-pypi). Docs-only releases publish nothing
to PyPI.

## 🚀 Creating a Release

### Step 1: Prepare the Release

```bash
# 1. Bump version in pyproject.toml
poe version --bump_type patch
# Or: --bump_type minor / --bump_type major

# 2. Add release notes file
vim docs/releases/<version>.md

# 3. Commit and push
git add pyproject.toml packages/kindling_cli/pyproject.toml \
    packages/kindling_sdk/pyproject.toml uv.lock CHANGELOG.md docs/releases/<version>.md
git commit -m "chore: prepare release <version>"
git push origin main

# 4. Wait for CI to pass
# Check: https://github.com/sep/spark-kindling-framework/actions
```

### Step 2: Create the Release on GitHub

```bash
poe release <version>
```

The Poe task creates and pushes the release tag. CI owns the GitHub release
object and creates it only after the required validation lane passes.

### Fast-track releases

For an urgent release that has already been validated locally, include
`[fast-track release]` in the tagged commit message. This tag-only directive
skips unit, integration, code-quality, KDA, security, and cloud system-test
jobs. CI still builds the release wheels and runs their smoke tests before
publishing, so broken or missing artifacts cannot be released.

```bash
git commit --allow-empty -m "chore(release): fast-track <version> [fast-track release]"
git push origin main
poe release <version>
```

Use this only for time-sensitive releases; the normal validated release path
remains the default.

### Copilot review and fix-up commits

Copilot reviews a pull request once, at the head it first sees. Commits pushed
afterwards -- the fix-ups that address its findings -- are **not** re-reviewed
automatically, so in a fast-track flow the code that actually merges can be
unreviewed. After pushing fix-ups, request a re-review explicitly:

```bash
gh api -X POST repos/<org>/<repo>/pulls/<n>/requested_reviewers \
  -f 'reviewers[]=copilot-pull-request-reviewer[bot]'
```

Wait for the review that names the new head commit before merging.

### Step 3: Verify Release Assets

After the workflow completes:

1. **Check Release Page**
   ```
   https://github.com/sep/spark-kindling-framework/releases/tag/v<version>
   ```

2. **For runtime, CLI, or SDK releases, verify Assets shows:**
   - ✅ `spark_kindling-<version>-py3-none-any.whl` (combined runtime)
   - ✅ `spark_kindling_cli-<version>-py3-none-any.whl`
   - ✅ `spark_kindling_sdk-<version>-py3-none-any.whl`
   - ✅ `kindling_bootstrap.py`
   - ✅ `spark_kindling-current-url.txt`
   - ✅ `spark_kindling-current-install.txt`
   - ✅ Source code (zip)
   - ✅ Source code (tar.gz)

Docs/proposal-only releases are expected to have generated release notes and
source archives only.

3. **Check PyPI** (final releases; for a prerelease, the same paths on
   `https://test.pypi.org`)
   ```
   https://pypi.org/project/spark-kindling/
   ```
   It should list `<version>` with a wheel and an sdist; so should
   `spark-kindling-cli` and `spark-kindling-sdk`. An extension shows a new
   version only when its own version was bumped.

## 🐍 Publishing to PyPI

The `publish-pypi` job in `.github/workflows/ci.yml` runs after
`publish-release`, so it only ever uploads files that passed every gate,
cloud system tests included. It uploads the wheel and sdist built by
`poe build` (the wheel is byte-identical to the release asset; the sdist goes
to PyPI only).

| Published to PyPI | GitHub Release only |
|---|---|
| `spark-kindling`, `spark-kindling-cli`, `spark-kindling-sdk` | `spark-kindling-ext-adx` |
| `spark-kindling-ext-databricks`, `spark-kindling-ext-sdp` | `spark-kindling-ext-databricks-autoloader` |
| `spark-kindling-ext-cosmos`, `spark-kindling-ext-temporal`, `spark-kindling-ext-otel-azure` | `spark-kindling-ext-visualization` |

To publish another package, add its distribution name to `PUBLISHED` in the
job and set up its trusted publisher (below).

- **Where**: a prerelease tag (`a`, `b` or `rc`, e.g. `v0.14.0rc1`) goes to
  [TestPyPI](https://test.pypi.org/project/spark-kindling/) through the
  `testpypi` environment; a final tag (`v0.14.0`) goes to
  [PyPI](https://pypi.org/project/spark-kindling/) through the `pypi`
  environment. If the `pypi` environment requires a reviewer, the job waits
  for that approval under the workflow run.
- **Authentication**: trusted publishing. PyPI accepts the job's GitHub OIDC
  identity (this repository, `ci.yml`, the environment); no API token is
  stored anywhere.
- **Unchanged extensions**: extensions keep their version across Kindling
  releases, so an unchanged one is already on the index and is skipped
  (`skip-existing`), not an error. That also means a changed extension whose
  version was not bumped is silently not uploaded: bump the extension's
  version whenever its code changes.
- **If the job fails** after the GitHub release is published, fix the cause
  and re-run the failed job; the GitHub release is not affected. Until it
  succeeds, `kindling env update` pins that release by wheel URL.

### Release candidate dry run

Before a final release that changes packaging, tag a release candidate:
`poe version --bump_type rc` (0.13.1 -> 0.14.0rc1, or rc1 -> rc2), commit,
then `poe release 0.14.0rc1`. When the candidate checks out,
`poe version --bump_type release` (0.14.0rc1 -> 0.14.0) prepares the final. When it lands, install it from TestPyPI in a
scratch environment (TestPyPI does not carry the third-party dependencies, so
keep PyPI as an extra index):

```bash
pip install --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ \
    'spark-kindling[standalone]==0.14.0rc1' spark-kindling-cli==0.14.0rc1
```

`KINDLING_PYPI_URL=https://test.pypi.org` points the CLI's "is this version
on PyPI?" lookup at TestPyPI, so `kindling env update --version 0.14.0rc1`
writes version pins for the candidate; uv then needs TestPyPI as an index too
(`UV_INDEX=https://test.pypi.org/simple/ UV_INDEX_STRATEGY=unsafe-best-match`)
to resolve them.

### Uploads are permanent

PyPI never accepts a second upload of the same version, even after the file
is deleted. A bad release is not replaced: yank it on pypi.org (the project's
**Manage → Releases → Options → Yank**), which keeps exact `==` pins working
but stops resolvers from picking it otherwise, and fix forward with a new
patch version. This is another reason to dry-run packaging changes as an rc.

### One-time setup

For each published package, on both [pypi.org](https://pypi.org/manage/account/publishing/)
and [test.pypi.org](https://test.pypi.org/manage/account/publishing/), add a
trusted publisher (a *pending* publisher for a project that does not exist
yet; the first upload creates it):

| Field | PyPI | TestPyPI |
|---|---|---|
| Owner | `sep` | `sep` |
| Repository | `spark-kindling-framework` | `spark-kindling-framework` |
| Workflow | `ci.yml` | `ci.yml` |
| Environment | `pypi` | `testpypi` |

In the GitHub repository (**Settings → Environments**), create the `pypi`
and `testpypi` environments. Add required reviewers to `pypi` to gate every
upload behind an approval, and restrict it to `v*` tags if desired.

## 📥 Installing a Release

From 0.14.0 on, install a release from PyPI and pin the version:

```bash
pip install 'spark-kindling[synapse]==<version>'
pip install spark-kindling-cli==<version>   # brings spark-kindling-sdk
```

```txt
# requirements.txt
spark-kindling[databricks]==<version>
```

On Databricks, add `spark-kindling[databricks]==<version>` as a cluster
library (**Libraries → Install New → PyPI**) or `%pip install` it in a
notebook.

### Without PyPI access

Every release also attaches its wheels to the GitHub release. Use these where
PyPI is unreachable (locked-down workspaces) and for releases before 0.14.0,
which exist only on GitHub.

#### Direct Download (Manual)

```bash
# 1. Download wheel from release page
# https://github.com/sep/spark-kindling-framework/releases/latest

# 2. Install the downloaded wheel with the platform extras your environment needs
#    Use PEP 508 direct-reference form so extras resolve against the distribution name.
pip install 'spark-kindling[synapse] @ file:///path/to/spark_kindling-<version>-py3-none-any.whl'
```

#### Direct Install from URL

```bash
# Install directly from GitHub Release (one wheel, pick your extra)
pip install 'spark-kindling[synapse] @ https://github.com/sep/spark-kindling-framework/releases/download/v<version>/spark_kindling-<version>-py3-none-any.whl'

# Or resolve the latest release through the stable alias asset
CURRENT_RUNTIME_URL=$(curl -fsSL https://github.com/sep/spark-kindling-framework/releases/latest/download/spark_kindling-current-url.txt)
pip install "spark-kindling[databricks] @ ${CURRENT_RUNTIME_URL}"
```

#### In requirements.txt

```txt
# requirements.txt

# Install specific version from release with synapse extras
spark-kindling[synapse] @ https://github.com/sep/spark-kindling-framework/releases/download/v<version>/spark_kindling-<version>-py3-none-any.whl

# Or resolve latest from the stable alias asset before generating requirements
# (the wheel filename itself remains versioned)
```

#### In Databricks/Synapse/Fabric

```python
# Databricks notebook
%pip install 'spark-kindling[databricks] @ https://github.com/sep/spark-kindling-framework/releases/download/v<version>/spark_kindling-<version>-py3-none-any.whl'

# Or in cluster libraries, as a wheel from a location the cluster can reach
# UI: Libraries → Install New → File path/ADLS (upload the downloaded wheel first)
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

A version with a PEP 440 prerelease suffix (`a`, `b` or `rc`, no hyphen:
`0.14.0rc1`, `0.14.0a1`) is released like any other, with `poe release`. CI
marks the GitHub release as a prerelease and publishes it to TestPyPI, not
PyPI (see [Release candidate dry run](#release-candidate-dry-run)):

```bash
pip install --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ 'spark-kindling[synapse]==0.14.0rc1'
```

## 📊 What Shows Up in a Release

When you navigate to a release page, users will see:

```
Release v<version> - Brief Description
Published by @username on Oct 17, 2025

[Release notes here]

Assets

 spark_kindling-<version>-py3-none-any.whl         ~180 KB
 spark_kindling_cli-<version>-py3-none-any.whl     ~40 KB
 spark_kindling_sdk-<version>-py3-none-any.whl     ~60 KB
 Source code (zip)
 Source code (tar.gz)
```

## 🔄 Automated Version Bumping

Use the existing `poe version` task (defined in `pyproject.toml`):

```bash
poe version --bump_type patch   # X.Y.Z -> X.Y.(Z+1)
poe version --bump_type minor   # X.Y.Z -> X.(Y+1).0
poe version --bump_type major   # X.Y.Z -> (X+1).0.0
poe version --bump_type alpha   # X.Y.Z -> X.Y.(Z+1)a1
poe version --bump_type rc      # X.Y.Z -> X.(Y+1).0rc1; rcN -> rc(N+1)
poe version --bump_type release # X.Y.ZrcN -> X.Y.Z
```

This updates the version in `pyproject.toml`, runs `uv lock` to refresh the
workspace member versions in `uv.lock` (commit both files), and can optionally
trigger build/deploy.

## 🎯 Complete Release Workflow

```bash
# 1. Update version
poe version --bump_type patch

# 2. Update release notes
vim docs/releases/<version>.md

# 3. Commit changes
git add .
git commit -m "chore: prepare release <version>"
git push origin main

# 4. Wait for CI to pass (check Actions tab)

# 5. Create release (pushes the tag; CI creates the GitHub release)
poe release <version>

# 6. Monitor release build
# Go to: Actions → wait for staged-artifact deploy + system tests + "Publish Release"
# then "Publish to PyPI" (approve the `pypi` environment if it asks)

# 7. Verify release
gh release view v<version>
# and https://pypi.org/project/spark-kindling/

# 8. Test installation
pip install 'spark-kindling[synapse]==<version>'
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
gh release download v<version> --pattern "*.whl"

# Option 2: Use curl with token
curl -H "Authorization: token YOUR_PAT" \
  -L https://github.com/sep/spark-kindling-framework/releases/download/v<version>/spark_kindling-<version>-py3-none-any.whl \
  -o spark_kindling-<version>-py3-none-any.whl
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

**Full Changelog**: https://github.com/sep/spark-kindling-framework/compare/v<previous-version>...v<version>
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
git checkout -b hotfix/<version> v<previous-version>

# 2. Fix the bug
git add .
git commit -m "fix: critical bug in platform detection"

# 3. Update version
poe version --bump_type patch

# 4. Push and create PR
git push origin hotfix/<version>

# 5. After PR approval and merge, release from main like any other version
poe release <version>
```

A hotfix goes through the same gates and the same PyPI upload as a normal
release; CI owns the GitHub release object.

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

**Current Setup**: Both. Wheels attach to every GitHub release, and from 0.14.0 the published packages also go to PyPI (see [Publishing to PyPI](#-publishing-to-pypi)). The GitHub release stays the version catalog that `kindling env update` reads.

## 📚 Additional Resources

- [GitHub Releases Documentation](https://docs.github.com/en/repositories/releasing-projects-on-github)
- [PyPI Trusted Publishers](https://docs.pypi.org/trusted-publishers/)
- [Semantic Versioning](https://semver.org/)
- [GitHub CLI Releases](https://cli.github.com/manual/gh_release)
- [Kindling CI/CD Setup](./ci_cd_setup.md)
