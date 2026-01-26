# GitHub Packages for Python Distribution

GitHub Packages is GitHub's built-in package registry that allows you to host Python packages privately within your organization or publicly for the community.

## 🎯 Why Use GitHub Packages?

### Advantages over PyPI

✅ **Private by Default**: Packages stay within your GitHub organization
✅ **Access Control**: Use GitHub teams and permissions
✅ **No Extra Accounts**: Uses your existing GitHub authentication
✅ **Free for Public Repos**: Unlimited bandwidth for public packages
✅ **Integrated**: Shows up directly in your GitHub repo
✅ **Version Control Sync**: Package versions match your git tags

### When to Use GitHub Packages

**Use GitHub Packages if:**
- 🏢 Internal company framework (like Kindling)
- 🔒 Want to keep code private
- 👥 Already using GitHub for source control
- 💰 Want to avoid PyPI rate limits
- 🔐 Need organization-level access controls

**Use PyPI if:**
- 🌍 Want public, community-wide distribution
- 📦 Building open-source tools for everyone
- 🚀 Want maximum discoverability

## 📦 How GitHub Packages Works

### Package URL Format
```
https://pypi.pkg.github.com/OWNER
```

For your Kindling framework:
```
https://pypi.pkg.github.com/sep
```

### Package Installation
```bash
# Install from GitHub Packages
pip install kindling-synapse \
  --index-url https://pypi.pkg.github.com/sep/simple/
```

## 🔧 Setup GitHub Packages Publishing

### 1. Update CI/CD Workflow

Add this job to `.github/workflows/ci.yml`:

```yaml
  publish-to-github-packages:
    name: Publish to GitHub Packages
    runs-on: ubuntu-latest
    needs: [build-platform-wheels, security-scan]
    permissions:
      contents: read
      packages: write
    if: github.ref == 'refs/heads/main' || github.event_name == 'release'

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Install Poetry
        run: |
          python -m pip install --upgrade pip
          pip install poetry

      - name: Configure Poetry for GitHub Packages
        run: |
          poetry config repositories.github https://pypi.pkg.github.com/${{ github.repository_owner }}
          poetry config pypi-token.github ${{ secrets.GITHUB_TOKEN }}

      - name: Build and publish Synapse wheel
        run: |
          # Build Synapse wheel
          mkdir -p build
          cp -r packages/kindling build/
          cp build-configs/synapse.toml build/pyproject.toml
          cp README.md build/
          cd build
          poetry build
          poetry publish --repository github
          cd ..
          rm -rf build

      - name: Build and publish Databricks wheel
        run: |
          mkdir -p build
          cp -r packages/kindling build/
          cp build-configs/databricks.toml build/pyproject.toml
          cp README.md build/
          cd build
          poetry build
          poetry publish --repository github
          cd ..
          rm -rf build

      - name: Build and publish Fabric wheel
        run: |
          mkdir -p build
          cp -r packages/kindling build/
          cp build-configs/fabric.toml build/pyproject.toml
          cp README.md build/
          cd build
          poetry build
          poetry publish --repository github
          cd ..
          rm -rf build

      - name: Create publish summary
        run: |
          echo "# 📦 Packages Published" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo "Published to GitHub Packages at: https://github.com/${{ github.repository }}/packages" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo "Install with:" >> $GITHUB_STEP_SUMMARY
          echo '```bash' >> $GITHUB_STEP_SUMMARY
          echo "pip install kindling-synapse --index-url https://pypi.pkg.github.com/${{ github.repository_owner }}/simple/" >> $GITHUB_STEP_SUMMARY
          echo '```' >> $GITHUB_STEP_SUMMARY
```

### 2. No Additional Secrets Needed!

GitHub Packages uses the built-in `GITHUB_TOKEN` which is automatically available in workflows. No setup required!

## 📥 Installing from GitHub Packages

### For End Users

#### Option 1: Using pip with index-url
```bash
# Install specific platform
pip install kindling-synapse \
  --index-url https://pypi.pkg.github.com/sep/simple/

# Or set it globally
pip config set global.index-url https://pypi.pkg.github.com/sep/simple/
pip install kindling-synapse
```

#### Option 2: Using pip with extra-index-url (Recommended)
```bash
# Use both PyPI and GitHub Packages
pip install kindling-synapse \
  --extra-index-url https://pypi.pkg.github.com/sep/simple/
```

#### Option 3: Using requirements.txt
```txt
# requirements.txt
--extra-index-url https://pypi.pkg.github.com/sep/simple/
kindling-synapse==0.2.0
```

### For Private Packages (Authentication Required)

If your packages are private, users need to authenticate:

#### Method 1: Personal Access Token (PAT)
```bash
# 1. Create PAT: GitHub → Settings → Developer settings → Personal access tokens
#    Scopes needed: read:packages

# 2. Install with authentication
pip install kindling-synapse \
  --index-url https://USERNAME:TOKEN@pypi.pkg.github.com/sep/simple/
```

#### Method 2: Using .pypirc (More secure)
```bash
# Create ~/.pypirc
cat > ~/.pypirc << 'PYPIRC'
[distutils]
index-servers =
    github

[github]
repository = https://pypi.pkg.github.com/sep
username = YOUR_GITHUB_USERNAME
password = YOUR_PERSONAL_ACCESS_TOKEN
PYPIRC

# Install packages
pip install kindling-synapse --index-url https://pypi.pkg.github.com/sep/simple/
```

#### Method 3: Using keyring (Most secure)
```bash
# Install keyring
pip install keyring

# Store credentials
keyring set https://pypi.pkg.github.com/sep username
keyring set https://pypi.pkg.github.com/sep password

# Use with pip (automatically uses keyring)
pip install kindling-synapse \
  --index-url https://pypi.pkg.github.com/sep/simple/
```

## 🎨 Package Visibility

### Public Packages
```yaml
# In your pyproject.toml (or build-configs/*.toml)
[tool.poetry]
# No special config needed - packages are public by default if repo is public
```

### Private Packages
Packages are automatically private if:
- Your GitHub repository is private
- You set the package to private in GitHub Packages settings

To change visibility:
1. Go to: `https://github.com/sep/spark-kindling-framework/packages`
2. Select a package
3. Settings → Change visibility

## �� Access Control

### Organization-Level Access

GitHub Packages inherits permissions from your repository:

| Repository Access | Can Install | Can Publish |
|------------------|-------------|-------------|
| Public repo | ✅ Everyone | ❌ No one (except maintainers) |
| Private repo | ✅ Org members only | ❌ No one (except maintainers) |
| With write access | ✅ Yes | ✅ Yes (if configured) |

### Team-Based Access

```bash
# Give a team access to packages
# Settings → Packages → [Select package] → Manage Access → Add teams
```

### Fine-Grained Access Control

You can grant specific people or teams access:
1. Repository Settings → Packages
2. Select your package
3. "Manage Actions access" to control CI/CD publishing
4. "Manage access" to control who can install

## 🚀 Complete Example: Kindling on GitHub Packages

### 1. Update Your Workflow

Create or update `.github/workflows/publish-github-packages.yml`:

```yaml
name: Publish to GitHub Packages

on:
  push:
    branches: [main]
  release:
    types: [published]

jobs:
  publish:
    name: Publish Platform Wheels
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    strategy:
      matrix:
        platform: [synapse, databricks, fabric]

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Install Poetry
        run: pip install poetry

      - name: Configure Poetry for GitHub Packages
        run: |
          poetry config repositories.github https://pypi.pkg.github.com/${{ github.repository_owner }}
          poetry config pypi-token.github ${{ secrets.GITHUB_TOKEN }}

      - name: Build ${{ matrix.platform }} wheel
        run: |
          mkdir -p build
          cp -r packages/kindling build/
          cp build-configs/${{ matrix.platform }}.toml build/pyproject.toml
          cp README.md build/
          cd build

          # Update version if this is a release
          if [ "${{ github.event_name }}" == "release" ]; then
            poetry version ${{ github.event.release.tag_name }}
          fi

          poetry build
          poetry publish --repository github
```

### 2. User Installation Instructions

Create `docs/installation.md`:

```markdown
# Installing Kindling from GitHub Packages

## Prerequisites

- Python 3.10 or higher
- GitHub account with access to the `sep` organization

## Installation

### For Synapse
\`\`\`bash
pip install kindling-synapse \\
  --extra-index-url https://pypi.pkg.github.com/sep/simple/
\`\`\`

### For Databricks
\`\`\`bash
pip install kindling-databricks \\
  --extra-index-url https://pypi.pkg.github.com/sep/simple/
\`\`\`

### For Microsoft Fabric
\`\`\`bash
pip install kindling-fabric \\
  --extra-index-url https://pypi.pkg.github.com/sep/simple/
\`\`\`

## Authentication (for private packages)

If packages are private, authenticate with a Personal Access Token:

1. **Create a PAT**:
   - Go to: Settings → Developer settings → Personal access tokens → Tokens (classic)
   - Click "Generate new token"
   - Select scope: `read:packages`
   - Copy the token

2. **Install with authentication**:
\`\`\`bash
pip install kindling-synapse \\
  --index-url https://USERNAME:TOKEN@pypi.pkg.github.com/sep/simple/
\`\`\`

Or store credentials in `~/.pypirc` (more secure).
```

### 3. Add to README.md

```markdown
## Installation

Kindling is distributed via GitHub Packages. Install the platform-specific version:

\`\`\`bash
# For Azure Synapse
pip install kindling-synapse --extra-index-url https://pypi.pkg.github.com/sep/simple/

# For Databricks
pip install kindling-databricks --extra-index-url https://pypi.pkg.github.com/sep/simple/

# For Microsoft Fabric
pip install kindling-fabric --extra-index-url https://pypi.pkg.github.com/sep/simple/
\`\`\`

See [Installation Guide](docs/installation.md) for details.
```

## 💰 Pricing & Limits

### Free Tier (Public repos)
- ✅ Unlimited storage
- ✅ Unlimited bandwidth
- ✅ Unlimited packages

### Private Repos
- 📦 500MB storage free
- ⬇️ 1GB/month bandwidth free
- 💵 $0.25/GB for additional storage
- 💵 $0.50/GB for additional bandwidth

### Enterprise
- 📦 50GB storage included
- ⬇️ 100GB/month bandwidth included
- 💵 Same overage rates

For most internal frameworks like Kindling, you'll stay well within free limits.

## 🔄 Migrating from GitHub Packages to PyPI

If you decide to move to PyPI later, it's easy:

```yaml
  publish-to-pypi:
    name: Publish to PyPI
    runs-on: ubuntu-latest
    if: github.event_name == 'release'

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Download wheels (from previous build job)
        uses: actions/download-artifact@v3
        with:
          name: platform-wheels
          path: dist/

      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          password: ${{ secrets.PYPI_API_TOKEN }}
```

## 📊 Viewing Your Packages

### In GitHub UI
1. Go to your repository
2. Click "Packages" tab (right sidebar)
3. See all published versions
4. View download statistics

### Via API
```bash
# List packages
curl -H "Authorization: token YOUR_PAT" \
  https://api.github.com/orgs/sep/packages?package_type=pypi

# Package details
curl -H "Authorization: token YOUR_PAT" \
  https://api.github.com/orgs/sep/packages/pypi/kindling-synapse
```

## 🛠️ Troubleshooting

### "Could not find a version that satisfies the requirement"

**Solution**: Add `--extra-index-url`:
```bash
pip install kindling-synapse \
  --extra-index-url https://pypi.pkg.github.com/sep/simple/
```

### "403 Forbidden"

**Solution**: Check authentication:
```bash
# Test authentication
curl -H "Authorization: token YOUR_PAT" \
  https://pypi.pkg.github.com/sep/simple/kindling-synapse/
```

### "Package already exists"

**Solution**: Versions are immutable. Bump version number:
```bash
# In build-configs/*.toml
version = "0.1.1"  # Increment version
```

### Slow Installation

**Solution**: Use `--extra-index-url` instead of `--index-url` to check PyPI first for common dependencies:
```bash
pip install kindling-synapse \
  --extra-index-url https://pypi.pkg.github.com/sep/simple/
```

## 🎯 Recommendation for Kindling

Based on your project being an internal framework for Azure platforms:

✅ **Use GitHub Packages**

**Reasons:**
1. Keep proprietary code private within SEP organization
2. No additional account setup needed
3. Control access via GitHub teams
4. Free for private repos within limits
5. Integrates perfectly with your existing GitHub workflow

**Setup is simple:**
1. Add the publish job to your workflow (uses built-in `GITHUB_TOKEN`)
2. Packages automatically published on push to main or release
3. Team members install with `--extra-index-url`

This gives you all the benefits of a package registry while keeping everything within your organization's control.

## 📚 Additional Resources

- [GitHub Packages Documentation](https://docs.github.com/en/packages)
- [Publishing Python Packages](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-python-registry)
- [Configuring Poetry for GitHub Packages](https://python-poetry.org/docs/repositories/)
- [GitHub Packages Pricing](https://docs.github.com/en/billing/managing-billing-for-github-packages/about-billing-for-github-packages)
