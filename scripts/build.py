#!/usr/bin/env python3
"""
Build platform-specific wheels using Poetry with isolated build directories.
Creates runtime wheels plus design-time wheels (kindling-sdk, kindling-cli).
Never modifies source files - builds in isolation.
"""

import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime
from os import environ
from pathlib import Path

PLATFORMS = ["synapse", "databricks", "fabric"]
DIST_DIR = Path("dist")
DESIGN_TIME_PACKAGE_DIRS = [Path("packages/kindling_sdk"), Path("packages/kindling_cli")]

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
    # local: keep all platform files — no stripping
    "local": [],
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
    environ.setdefault("POETRY_CACHE_DIR", "/tmp/poetry-cache")
    environ.setdefault("POETRY_VIRTUALENVS_PATH", "/tmp/poetry-virtualenvs")
    environ.setdefault("VIRTUALENV_OVERRIDE_APP_DATA", "/tmp/virtualenv-app-data")
    environ.setdefault("XDG_DATA_HOME", "/tmp/xdg-data")
    Path(environ["POETRY_CACHE_DIR"]).mkdir(parents=True, exist_ok=True)
    Path(environ["POETRY_VIRTUALENVS_PATH"]).mkdir(parents=True, exist_ok=True)
    Path(environ["VIRTUALENV_OVERRIDE_APP_DATA"]).mkdir(parents=True, exist_ok=True)
    Path(environ["XDG_DATA_HOME"]).mkdir(parents=True, exist_ok=True)

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


def build_local_wheel(version: str) -> tuple[str, int]:
    """Build the kindling-local wheel: all platforms included, full deps.

    Unlike the platform wheels this wheel is NOT stripped — every platform
    module ships so that local/standalone environments can import any of them.
    It also declares pyspark, delta-spark, pandas, and pyarrow as explicit
    dependencies since there is no workspace runtime to supply them.
    """
    print("\n📦 Building kindling-local wheel (all platforms, full deps)...")

    with tempfile.TemporaryDirectory() as temp_dir:
        build_dir = Path(temp_dir)

        # Build wheel in isolation (reuses existing build_wheel helper)
        original_wheel = build_wheel("local", version, build_dir)

        # Copy straight to dist — no stripping needed
        wheel_name = f"kindling_local-{version}-py3-none-any.whl"
        output_path = DIST_DIR / wheel_name
        shutil.copy2(original_wheel, output_path)

        size_kb = output_path.stat().st_size // 1024
        print(f"   ✅ Built: {wheel_name} ({size_kb}K)")
        return wheel_name, size_kb


def build_design_time_wheel(package_dir: Path) -> tuple[str, int]:
    """Build a design-time wheel in-place and copy it to dist/."""
    package_name = package_dir.name
    print(f"\n📦 Building {package_name} wheel...")

    if not (package_dir / "pyproject.toml").exists():
        raise FileNotFoundError(f"Missing pyproject.toml in {package_dir}")

    package_dist = package_dir / "dist"
    if package_dist.exists():
        shutil.rmtree(package_dist)

    result = subprocess.run(
        ["poetry", "build", "--format", "wheel"],
        cwd=package_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Error building {package_name} wheel:\n{result.stderr}")

    wheels = sorted(package_dist.glob("*.whl"))
    if not wheels:
        raise FileNotFoundError(f"No wheel generated for {package_name}")

    source_wheel = wheels[0]
    output_path = DIST_DIR / source_wheel.name
    shutil.copy2(source_wheel, output_path)
    size_kb = output_path.stat().st_size // 1024
    print(f"   ✅ Built: {source_wheel.name} ({size_kb}K)")
    return source_wheel.name, size_kb


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

    # Build each runtime platform wheel
    runtime_results = []
    for platform in PLATFORMS:
        try:
            wheel_name, size_kb = build_platform_wheel(platform, version)
            runtime_results.append((platform, wheel_name, size_kb, True))
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            runtime_results.append((platform, None, 0, False))

    # Build local wheel (all platforms, full deps — for local dev consumers)
    local_result = None
    try:
        wheel_name, size_kb = build_local_wheel(version)
        local_result = ("local", wheel_name, size_kb, True)
    except Exception as e:
        print(f"   ❌ Failed to build kindling-local: {e}")
        local_result = ("local", None, 0, False)

    # Build design-time wheels
    design_results = []
    for package_dir in DESIGN_TIME_PACKAGE_DIRS:
        package_name = package_dir.name
        try:
            wheel_name, size_kb = build_design_time_wheel(package_dir)
            design_results.append((package_name, wheel_name, size_kb, True))
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            design_results.append((package_name, None, 0, False))

    # Summary
    print("\n🎉 All wheels built successfully!")
    print(f"📍 Output directory: {DIST_DIR}")
    print(f"\n📦 Built packages:")

    for file_path in sorted(DIST_DIR.glob("*.whl")):
        size = file_path.stat().st_size // 1024
        print(f"   {file_path.name} ({size}K)")

    print("\n📊 Build summary:")
    for platform, wheel_name, size_kb, success in runtime_results:
        if success:
            print(f"   ✅ {platform}: {size_kb}K")
        else:
            print(f"   ❌ {platform}: FAILED")
    if local_result:
        _, _, size_kb, success = local_result
        if success:
            print(f"   ✅ local: {size_kb}K")
        else:
            print(f"   ❌ local: FAILED")
    for package_name, wheel_name, size_kb, success in design_results:
        if success:
            print(f"   ✅ {package_name}: {size_kb}K")
        else:
            print(f"   ❌ {package_name}: FAILED")

    # Check if any failed
    all_results = runtime_results + ([local_result] if local_result else []) + design_results
    if not all(result[3] for result in all_results):
        print("\n❌ Some builds failed")
        sys.exit(1)

    print("\n🚀 Ready for release/deployment! Runtime wheels contain:")
    print("   📁 Core kindling framework")
    print("   🎯 Platform-specific implementation")
    print("   📦 Platform-specific dependencies")
    print("   🏷️  Pythonic package names (kindling-{platform})")

    print("\n💡 Usage:")
    print(f"   pip install {DIST_DIR}/kindling_local-{version}-py3-none-any.whl    # local dev")
    print(f"   pip install {DIST_DIR}/kindling_synapse-{version}-py3-none-any.whl")
    print(f"   pip install {DIST_DIR}/kindling_databricks-{version}-py3-none-any.whl")
    print(f"   pip install {DIST_DIR}/kindling_fabric-{version}-py3-none-any.whl")
    print(f"   pip install {DIST_DIR}/kindling_sdk-<version>-py3-none-any.whl")
    print(f"   pip install {DIST_DIR}/kindling_cli-<version>-py3-none-any.whl")

    print("\n📤 Next step:")
    print("   poetry run poe deploy       # Deploy to Azure Storage (testing)")
    print("   poetry run poe deploy --release latest  # Deploy from GitHub release (production)")


if __name__ == "__main__":
    main()
