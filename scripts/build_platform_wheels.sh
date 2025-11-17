#!/bin/bash
# scripts/build_platform_wheels.sh
#
# Builds platform-specific wheels using Poetry with isolated build directories
# Creates pythonic wheels: kindling-synapse, kindling-databricks, kindling-fabric
# Never modifies source files - builds in isolation

set -e

# Add Poetry to PATH
export PATH="/home/vscode/.local/bin:$PATH"

PLATFORMS=("synapse" "databricks" "fabric")
DIST_DIR="dist"

# Detect version from pyproject.toml (single source of truth)
VERSION=$(grep '^version = ' pyproject.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
if [ -z "$VERSION" ]; then
    echo "❌ Error: Could not detect version from pyproject.toml"
    exit 1
fi
echo "📌 Detected version: $VERSION"

# Save the original working directory at script start
SCRIPT_DIR="$(pwd)"

echo "🔥 Building platform-specific kindling wheels (isolated builds)..."
echo "📅 Build time: $(date)"

# Clean previous builds
echo "🧹 Cleaning previous builds..."
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

# Build each platform wheel in isolation
for platform in "${PLATFORMS[@]}"; do
    echo ""
    echo "📦 Building kindling-$platform wheel..."

    # Create isolated build directory
    build_dir=$(mktemp -d)
    echo "   📁 Using build dir: $build_dir"

    # Copy source to isolated environment
    mkdir -p "$build_dir/packages"
    cp -r packages/kindling "$build_dir/packages/"

    # Generate platform-specific pyproject.toml from template
    python3 scripts/generate_platform_config.py "$platform" "$VERSION" > "$build_dir/pyproject.toml"

    cp README.md "$build_dir/"

    # Build in isolation
    cd "$build_dir"
    echo "   🔨 Running: poetry build --format wheel"
    poetry build --format wheel

    # Find the generated wheel
    wheel_file=$(ls dist/kindling*.whl | head -1)
    if [ ! -f "$wheel_file" ]; then
        echo "❌ Error: No wheel file generated for $platform"
        cd "$OLDPWD"
        rm -rf "$build_dir"
        exit 1
    fi

    # Post-process wheel (platform file filtering)
    echo "   🧹 Removing other platform files..."

    # Work directly with the wheel in the build directory
    original_wheel="$wheel_file"
    temp_dir=$(mktemp -d)

    # Extract wheel
    cd "$temp_dir"
    python -m zipfile -e "$build_dir/$original_wheel" .

    # Remove other platform files based on platform (keep only current platform)
    case $platform in
        "synapse")
            rm -f kindling/platform_databricks.py kindling/platform_fabric.py kindling/platform_local.py
            ;;
        "databricks")
            rm -f kindling/platform_synapse.py kindling/platform_fabric.py kindling/platform_local.py
            ;;
        "fabric")
            rm -f kindling/platform_synapse.py kindling/platform_databricks.py kindling/platform_local.py
            ;;
    esac

    # Repackage wheel with correct name
    wheel_name="kindling_${platform}-${VERSION}-py3-none-any.whl"
    output_path="$SCRIPT_DIR/$DIST_DIR/$wheel_name"
    echo "   📍 Creating wheel at: $output_path"
    python -m zipfile -c "$output_path" .

    # Verify the wheel was created
    if [ -f "$output_path" ]; then
        wheel_size=$(du -h "$output_path" | cut -f1)
        echo "   📦 Wheel created successfully: $wheel_size"
    else
        echo "   ❌ Error: Wheel not created at $output_path"
        ls -la "$SCRIPT_DIR/$DIST_DIR/"
    fi

    rm -rf "$temp_dir"

    echo "   ✅ Built: $wheel_name ($wheel_size)"

    # Return to original directory and cleanup
    cd "$SCRIPT_DIR"
    rm -rf "$build_dir"
done

echo ""
echo "🎉 All platform wheels built successfully!"
echo "📍 Output directory: $DIST_DIR"
echo "📦 Built packages:"
ls -la "$DIST_DIR"

echo ""
echo "📊 Build summary:"
for platform in "${PLATFORMS[@]}"; do
    # Poetry uses underscores in wheel names
    wheel_file="$DIST_DIR/kindling_${platform}-${VERSION}-py3-none-any.whl"
    if [ -f "$wheel_file" ]; then
        size=$(du -h "$wheel_file" | cut -f1)
        echo "   ✅ $platform: $size"
    else
        echo "   ❌ $platform: FAILED"
    fi
done

echo ""
echo "🚀 Ready for deployment! Each wheel contains:"
echo "   📁 Core kindling framework"
echo "   🎯 Platform-specific implementation"
echo "   📦 Platform-specific dependencies"
echo "   🏷️  Pythonic package names (kindling-{platform})"

echo ""
echo "💡 Usage:"
echo "   pip install $DIST_DIR/kindling_synapse-${VERSION}-py3-none-any.whl"
echo "   pip install $DIST_DIR/kindling_databricks-${VERSION}-py3-none-any.whl"
echo "   pip install $DIST_DIR/kindling_fabric-${VERSION}-py3-none-any.whl"
