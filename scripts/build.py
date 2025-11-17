#!/usr/bin/env python3
"""
Build platform-specific wheels using Poetry with isolated build directories.
Creates pythonic wheels: kindling-synapse, kindling-databricks, kindling-fabric
Never modifies source files - builds in isolation.
"""

import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List

PLATFORMS = ["synapse", "databricks", "fabric"]
DIST_DIR = Path("dist")

# Platform file filtering - remove other platform files
PLATFORM_FILES_TO_REMOVE = {
    "synapse": [
        "kindling/platform_databricks.py",
        "kindling/platform_fabric.py",
        "kindling/platform_standalone.py",
    ],
    "databricks": [
        "kindling/platform_synapse.py",
        "kindling/platform_fabric.py",
        "kindling/platform_standalone.py",
    ],
    "fabric": [
        "kindling/platform_synapse.py",
        "kindling/platform_databricks.py",
        "kindling/platform_standalone.py",
    ],
}


def get_version_from_pyproject() -> str:
    """Extract version from pyproject.toml (single source of truth)"""
    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        raise FileNotFoundError("pyproject.toml not found")

    content = pyproject_path.read_text()
    match = re.search(r'^version = "([^"]+)"', content, re.MULTILINE)
    if not match:
        raise ValueError("Could not find version in pyproject.toml")

    return match.group(1)


def ensure_poetry_installed() -> None:
    """Check if Poetry is available"""
    try:
        subprocess.run(["poetry", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Error: Poetry not found")
        print("Install it: pip install poetry")
        sys.exit(1)


def generate_platform_config(platform: str, version: str, build_dir: Path) -> None:
    """Generate platform-specific pyproject.toml"""
    subprocess.run(
        ["python3", "scripts/generate_platform_config.py", platform, version],
        stdout=open(build_dir / "pyproject.toml", "w"),
        check=True,
    )


def build_wheel(platform: str, version: str, build_dir: Path) -> Path:
    """Build wheel in isolated directory using Poetry"""
    print(f"\n📦 Building kindling-{platform} wheel...")
    print(f"   📁 Using build dir: {build_dir}")

    # Copy source to isolated environment
    packages_dir = build_dir / "packages"
    packages_dir.mkdir()
    shutil.copytree("packages/kindling", packages_dir / "kindling")

    # Generate platform-specific pyproject.toml
    generate_platform_config(platform, version, build_dir)

    # Copy README
    shutil.copy("README.md", build_dir / "README.md")

    # Build wheel
    print("   🔨 Running: poetry build --format wheel")
    result = subprocess.run(
        ["poetry", "build", "--format", "wheel"], cwd=build_dir, capture_output=True, text=True
    )

    if result.returncode != 0:
        print(f"❌ Error building {platform} wheel:")
        print(result.stderr)
        sys.exit(1)

    # Find generated wheel
    wheel_files = list((build_dir / "dist").glob("kindling*.whl"))
    if not wheel_files:
        print(f"❌ Error: No wheel file generated for {platform}")
        sys.exit(1)

    return wheel_files[0]


def filter_platform_files(wheel_path: Path, platform: str, output_path: Path) -> None:
    """Remove other platform files from wheel and save to output"""
    print("   🧹 Removing other platform files...")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Extract wheel
        with zipfile.ZipFile(wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_path)

        # Remove other platform files
        files_to_remove = PLATFORM_FILES_TO_REMOVE.get(platform, [])
        for file_path in files_to_remove:
            full_path = temp_path / file_path
            if full_path.exists():
                full_path.unlink()

        # Repackage wheel
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zip_ref:
            for file_path in temp_path.rglob("*"):
                if file_path.is_file():
                    arcname = file_path.relative_to(temp_path)
                    zip_ref.write(file_path, arcname)


def build_platform_wheel(platform: str, version: str) -> tuple[str, int]:
    """Build a single platform wheel and return (name, size)"""
    with tempfile.TemporaryDirectory() as temp_dir:
        build_dir = Path(temp_dir)

        # Build wheel in isolation
        original_wheel = build_wheel(platform, version, build_dir)

        # Filter platform files and save to dist
        wheel_name = f"kindling_{platform}-{version}-py3-none-any.whl"
        output_path = DIST_DIR / wheel_name
        filter_platform_files(original_wheel, platform, output_path)

        # Get size
        wheel_size = output_path.stat().st_size
        size_kb = wheel_size // 1024

        print(f"   ✅ Built: {wheel_name} ({size_kb}K)")

        return wheel_name, size_kb


def main():
    """Build all platform-specific wheels"""
    print("🔥 Building platform-specific kindling wheels (isolated builds)...")
    print(f"📅 Build time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Check Poetry is installed
    ensure_poetry_installed()

    # Get version
    try:
        version = get_version_from_pyproject()
        print(f"📌 Detected version: {version}")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

    # Clean previous builds
    print("\n🧹 Cleaning previous builds...")
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)
    DIST_DIR.mkdir()

    # Build each platform wheel
    results = []
    for platform in PLATFORMS:
        try:
            wheel_name, size_kb = build_platform_wheel(platform, version)
            results.append((platform, wheel_name, size_kb, True))
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            results.append((platform, None, 0, False))

    # Summary
    print("\n🎉 All platform wheels built successfully!")
    print(f"📍 Output directory: {DIST_DIR}")
    print(f"\n📦 Built packages:")

    for file_path in sorted(DIST_DIR.glob("*.whl")):
        size = file_path.stat().st_size // 1024
        print(f"   {file_path.name} ({size}K)")

    print("\n📊 Build summary:")
    for platform, wheel_name, size_kb, success in results:
        if success:
            print(f"   ✅ {platform}: {size_kb}K")
        else:
            print(f"   ❌ {platform}: FAILED")

    # Check if any failed
    if not all(result[3] for result in results):
        print("\n❌ Some builds failed")
        sys.exit(1)

    print("\n🚀 Ready for deployment! Each wheel contains:")
    print("   📁 Core kindling framework")
    print("   🎯 Platform-specific implementation")
    print("   📦 Platform-specific dependencies")
    print("   🏷️  Pythonic package names (kindling-{platform})")

    print("\n💡 Usage:")
    print(f"   pip install {DIST_DIR}/kindling_synapse-{version}-py3-none-any.whl")
    print(f"   pip install {DIST_DIR}/kindling_databricks-{version}-py3-none-any.whl")
    print(f"   pip install {DIST_DIR}/kindling_fabric-{version}-py3-none-any.whl")

    print("\n📤 Next step:")
    print("   poetry run poe deploy       # Deploy to Azure Storage (testing)")
    print("   poetry run poe deploy-release  # Deploy from GitHub release (production)")


if __name__ == "__main__":
    main()
