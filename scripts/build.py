#!/usr/bin/env python3
"""
Build Kindling wheels.

Produces one runtime wheel (spark-kindling) that contains every
``platform_*.py`` module and declares platform-specific deps under extras
(see ``[tool.poetry.extras]`` in the root pyproject.toml). Also builds the
design-time wheels: spark-kindling-cli and spark-kindling-sdk.

Replaces the pre-rename three-wheel-per-platform build; users now install
``spark-kindling[synapse]`` / ``[databricks]`` / ``[fabric]`` rather than
picking between three separate distributions.
"""

import shutil
import subprocess
import sys
from datetime import datetime
from os import environ
from pathlib import Path

DIST_DIR = Path("dist")
DESIGN_TIME_PACKAGE_DIRS = [
    Path("packages/kindling_sdk"),
    Path("packages/kindling_cli"),
    Path("packages/kindling_otel_azure"),
]


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


def build_runtime_wheel() -> tuple[str, int]:
    """Build the single combined runtime wheel from the root pyproject.toml."""
    print("\n📦 Building spark-kindling runtime wheel...")

    result = subprocess.run(
        ["poetry", "build", "--format", "wheel"], capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Error building runtime wheel:\n{result.stderr}")

    source_wheels = sorted(Path("dist").glob("spark_kindling-*.whl"))
    if not source_wheels:
        raise FileNotFoundError("No spark_kindling wheel produced in dist/")
    wheel_path = source_wheels[-1]
    size_kb = wheel_path.stat().st_size // 1024
    print(f"   ✅ Built: {wheel_path.name} ({size_kb}K)")
    return wheel_path.name, size_kb


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
    """Build the runtime wheel and design-time wheels."""
    print("🔥 Building Kindling wheels...")
    print(f"📅 Build time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    ensure_poetry_installed()

    print("\n🧹 Cleaning previous builds...")
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)
    DIST_DIR.mkdir()

    results: list[tuple[str, str, int, bool]] = []

    try:
        wheel_name, size_kb = build_runtime_wheel()
        results.append(("spark-kindling", wheel_name, size_kb, True))
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        results.append(("spark-kindling", "", 0, False))

    for package_dir in DESIGN_TIME_PACKAGE_DIRS:
        try:
            wheel_name, size_kb = build_design_time_wheel(package_dir)
            results.append((package_dir.name, wheel_name, size_kb, True))
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            results.append((package_dir.name, "", 0, False))

    print("\n🎉 Build complete.")
    print(f"📍 Output directory: {DIST_DIR}")
    print("\n📦 Built packages:")
    for file_path in sorted(DIST_DIR.glob("*.whl")):
        size = file_path.stat().st_size // 1024
        print(f"   {file_path.name} ({size}K)")

    print("\n📊 Build summary:")
    for name, _wheel_name, size_kb, success in results:
        print(
            f"   {'✅' if success else '❌'} {name}: {size_kb}K"
            if success
            else f"   ❌ {name}: FAILED"
        )

    if not all(r[3] for r in results):
        print("\n❌ Some builds failed")
        sys.exit(1)

    print("\n💡 Install examples:")
    print("   pip install 'spark-kindling[synapse]'")
    print("   pip install 'spark-kindling[databricks]'")
    print("   pip install 'spark-kindling[fabric]'")
    print("   pip install spark-kindling-cli")
    print("   pip install spark-kindling-sdk")

    print("\n📤 Next step:")
    print("   poetry run poe deploy       # Deploy to Azure Storage (testing)")
    print("   poetry run poe deploy --release latest  # Deploy from GitHub release (production)")


if __name__ == "__main__":
    main()
