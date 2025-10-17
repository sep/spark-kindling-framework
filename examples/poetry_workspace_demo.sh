#!/bin/bash
"""
Poetry Workspace Build Example

This script demonstrates how Poetry workspaces would replace 
the custom build_platform_wheels.py script.
"""

# =============================================================================
# SETUP: Initialize Poetry workspace
# =============================================================================

echo "🏗️  Setting up Poetry workspace..."

# Initialize workspace (run once)
poetry new kindling-workspace --name kindling-workspace
cd kindling-workspace

# Create package structure
mkdir -p packages/{kindling-core,kindling-synapse,kindling-fabric,kindling}

# Initialize each package
poetry new packages/kindling-core --name kindling-core
poetry new packages/kindling-synapse --name kindling-synapse  
poetry new packages/kindling-fabric --name kindling-fabric
poetry new packages/kindling --name kindling

# =============================================================================
# BUILD: All packages with one command
# =============================================================================

echo "📦 Building all packages..."

# Option 1: Build all packages individually
for package in packages/*/; do
    echo "Building $package..."
    poetry build -C "$package"
done

# Option 2: Use workspace build (if using poetry-multiproject-plugin)
# poetry build --all

# Option 3: Parallel builds
echo "Building packages in parallel..."
poetry build -C packages/kindling-core/ &
poetry build -C packages/kindling-synapse/ &  
poetry build -C packages/kindling-fabric/ &
poetry build -C packages/kindling/ &
wait

echo "✅ All packages built successfully"

# =============================================================================
# VALIDATION: Test installations
# =============================================================================

echo "🔍 Validating package installations..."

# Create temporary virtual environments for testing
python -m venv test-env-core
source test-env-core/bin/activate

# Test core package
pip install packages/kindling-core/dist/kindling_core-*.whl
python -c "import kindling; print('✅ Core package works')"
deactivate

# Test synapse package  
python -m venv test-env-synapse
source test-env-synapse/bin/activate
pip install packages/kindling-core/dist/kindling_core-*.whl
pip install packages/kindling-synapse/dist/kindling_synapse-*.whl
python -c "
from kindling.platform_provider_discovery import get_available_platforms
platforms = get_available_platforms()
print(f'✅ Available platforms: {platforms}')
assert 'synapse' in platforms, 'Synapse platform not found'
"
deactivate

# Test meta-package
python -m venv test-env-meta
source test-env-meta/bin/activate
pip install packages/kindling/dist/kindling-*.whl[synapse]
python -c "
from kindling.platform_provider_discovery import get_platform_provider
provider = get_platform_provider('synapse')
print(f'✅ Synapse provider: {provider.__class__.__name__}')
"
deactivate

# Cleanup
rm -rf test-env-*

# =============================================================================
# PUBLISH: Upload to PyPI
# =============================================================================

echo "🚀 Publishing packages..."

# Option 1: Publish individually
for package in packages/*/; do
    echo "Publishing $package..."
    poetry publish -C "$package" --dry-run  # Remove --dry-run for real publishing
done

# Option 2: Publish in dependency order
poetry publish -C packages/kindling-core/ --dry-run
poetry publish -C packages/kindling-synapse/ --dry-run
poetry publish -C packages/kindling-fabric/ --dry-run  
poetry publish -C packages/kindling/ --dry-run  # Meta-package last

echo "✅ All packages published"

# =============================================================================
# USAGE EXAMPLES
# =============================================================================

cat << 'EOF'

📚 USAGE EXAMPLES:

# Install core only
pip install kindling-core

# Install with Synapse support
pip install kindling-synapse  # Automatically includes kindling-core

# Install via meta-package
pip install kindling[synapse]

# Install multiple platforms
pip install kindling-synapse kindling-fabric

# Install for development
pip install kindling[dev]

# Check available platforms
python -c "
from kindling.platform_provider_discovery import get_available_platforms
print(f'Available: {get_available_platforms()}')
"

EOF

# =============================================================================
# COMPARISON WITH CUSTOM SCRIPT
# =============================================================================

cat << 'EOF'

📊 COMPARISON:

Custom build_platform_wheels.py:
❌ 400+ lines of hardcoded logic
❌ Manual dependency management  
❌ Custom pyproject.toml generation
❌ Platform-specific file copying
❌ Wheel validation logic
❌ Maintenance burden

Poetry workspace approach:
✅ Standard Poetry commands
✅ Declarative pyproject.toml files
✅ Automatic dependency resolution
✅ Entry point discovery
✅ Built-in validation
✅ Industry standard tooling

Lines of code reduction: ~400 lines → ~50 lines of TOML config

EOF