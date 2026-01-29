#!/usr/bin/env python3
"""
Build platform-specific extension wheels.
Usage: build_extensions.py <extension_name> [platforms...]
"""
import os
import shutil
import subprocess
import sys
from pathlib import Path

WORKSPACE = Path(__file__).parent.parent
EXTENSION_DIR = WORKSPACE / "packages"
DIST_DIR = WORKSPACE / "dist"


def get_version(extension_name: str) -> str:
    """Extract version from extension's pyproject.toml"""
    # Convert to underscore format for directory name
    dir_name = extension_name.replace("-", "_")
    pyproject_path = EXTENSION_DIR / dir_name / "pyproject.toml"

    if not pyproject_path.exists():
        raise FileNotFoundError(
            f"Extension not found: {extension_name} (looked in {pyproject_path})"
        )

    with open(pyproject_path) as f:
        for line in f:
            if line.startswith("version = "):
                return line.split('"')[1]

    raise ValueError(f"Could not find version in {pyproject_path}")


def build_platform_wheel(extension_name: str, platform: str, version: str) -> Path:
    """Build platform-specific extension wheel"""

    print(f"\n📦 Building {extension_name}-{platform} wheel (v{version})...")

    # Convert to underscore format for directory name
    dir_name = extension_name.replace("-", "_")
    extension_dir = EXTENSION_DIR / dir_name

    if not extension_dir.exists():
        raise FileNotFoundError(f"Extension directory not found: {extension_dir}")

    # Generate platform-specific pyproject.toml
    config_script = WORKSPACE / "scripts" / "generate_extension_config.py"
    result = subprocess.run(
        [sys.executable, str(config_script), extension_name, platform, version],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Failed to generate config: {result.stderr}")

    platform_config = result.stdout

    # Create temp build directory
    build_dir = WORKSPACE / f".build_{extension_name}_{platform}"
    if build_dir.exists():
        shutil.rmtree(build_dir)

    build_dir.mkdir(parents=True)

    try:
        # Copy source files
        package_name = extension_name.replace("-", "_")
        src_package = extension_dir / package_name
        dst_package = build_dir / package_name

        shutil.copytree(src_package, dst_package)

        # Copy additional files
        for file in ["README.md", "CHANGELOG.md"]:
            src_file = extension_dir / file
            if src_file.exists():
                shutil.copy(src_file, build_dir / file)

        # Write platform-specific pyproject.toml
        (build_dir / "pyproject.toml").write_text(platform_config)

        # Build wheel using poetry
        result = subprocess.run(
            ["poetry", "build", "--format", "wheel"],
            cwd=build_dir,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(f"Poetry build failed: {result.stderr}")

        # Move wheel to dist directory
        DIST_DIR.mkdir(exist_ok=True)

        built_wheels = list((build_dir / "dist").glob("*.whl"))
        if not built_wheels:
            raise RuntimeError("No wheel file found after build")

        wheel_file = built_wheels[0]
        dest_wheel = DIST_DIR / wheel_file.name

        shutil.copy(wheel_file, dest_wheel)

        size_kb = dest_wheel.stat().st_size // 1024
        print(f"   ✅ Built: {dest_wheel.name} ({size_kb}K)")

        return dest_wheel

    finally:
        # Cleanup temp build directory
        if build_dir.exists():
            shutil.rmtree(build_dir)


def main():
    if len(sys.argv) < 2:
        print("Usage: build_extensions.py <extension_name> [platforms...]")
        print("Example: build_extensions.py kindling_otel_azure fabric synapse databricks")
        sys.exit(1)

    extension_name = sys.argv[1]
    platforms = sys.argv[2:] if len(sys.argv) > 2 else ["fabric", "synapse", "databricks"]

    print(f"🔥 Building platform-specific {extension_name} wheels...")

    # Get version
    version = get_version(extension_name)
    print(f"📌 Detected version: {version}")

    # Build for each platform
    wheels = []
    for platform in platforms:
        try:
            wheel = build_platform_wheel(extension_name, platform, version)
            wheels.append(wheel)
        except Exception as e:
            print(f"   ❌ Failed to build {platform}: {e}")
            sys.exit(1)

    print(f"\n📦 Built {len(wheels)} extension wheels:")
    for wheel in wheels:
        size_kb = wheel.stat().st_size // 1024
        print(f"  {wheel.name} ({size_kb}K)")

    print(f"\n🎉 Extension build complete!")


if __name__ == "__main__":
    main()
