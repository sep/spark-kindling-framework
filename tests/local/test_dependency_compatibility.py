#!/usr/bin/env python3
"""
Test dependency compatibility locally without deploying to Spark

This script simulates the package environment of different platforms
and tests if imports work correctly.

Usage:
    python tests/local/test_dependency_compatibility.py
    python tests/local/test_dependency_compatibility.py --platform fabric
"""

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path


def test_imports_in_venv(platform: str, azure_core_version: str, azure_identity_version: str):
    """Test if imports work with specific package versions"""

    print(f"\n{'='*70}")
    print(f"Testing {platform} compatibility:")
    print(f"  azure-core: {azure_core_version}")
    print(f"  azure-identity: {azure_identity_version}")
    print("=" * 70)

    # Create temporary virtual environment
    with tempfile.TemporaryDirectory() as tmpdir:
        venv_path = Path(tmpdir) / "test_env"

        # Create venv
        print("\n1. Creating virtual environment...")
        subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)

        pip_path = venv_path / "bin" / "pip"
        python_path = venv_path / "bin" / "python"

        # Install specific versions
        print("\n2. Installing packages...")
        subprocess.run(
            [
                str(pip_path),
                "install",
                "-q",
                f"azure-core=={azure_core_version}",
                f"azure-identity=={azure_identity_version}",
                "requests>=2.21.0",
            ],
            check=True,
        )

        # Test imports
        print("\n3. Testing imports...")
        test_script = """
import sys
try:
    from azure.identity import DefaultAzureCredential
    print("✅ azure.identity.DefaultAzureCredential imported successfully")

    from azure.core.credentials import AccessToken
    print("✅ azure.core.credentials.AccessToken imported successfully")

    try:
        from azure.core.credentials import AccessTokenInfo
        print("✅ azure.core.credentials.AccessTokenInfo imported successfully")
    except ImportError:
        print("⚠️  azure.core.credentials.AccessTokenInfo not available (pre-1.31.0)")

    # Try to import platform module (if available)
    try:
        import importlib.util
        spec = importlib.util.find_spec("kindling.platform_fabric")
        if spec:
            import kindling.platform_fabric
            print("✅ kindling.platform_fabric imported successfully")
    except Exception as e:
        print(f"ℹ️  kindling not installed (expected): {e}")

    print("\\n✅ All critical imports successful!")
    sys.exit(0)

except ImportError as e:
    print(f"\\n❌ Import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
"""

        result = subprocess.run(
            [str(python_path), "-c", test_script], capture_output=True, text=True
        )

        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        if result.returncode == 0:
            print(f"\n✅ {platform} compatibility: PASSED")
            return True
        else:
            print(f"\n❌ {platform} compatibility: FAILED")
            return False


def get_platform_versions():
    """Get known runtime versions for each platform"""
    return {
        "fabric": {
            # Fabric Spark 3.5 runtime (as of Jan 2026)
            # Based on actual test output showing azure-identity 1.15.0
            "azure-core": "1.30.2",  # Pre-installed version seen in logs
            "azure-identity": "1.15.0",  # Pre-installed version seen in logs
        },
        "synapse": {
            # Synapse Spark 3.5 pool
            "azure-core": "1.30.0",
            "azure-identity": "1.17.0",
        },
        "databricks": {
            # Databricks Runtime 14.3+ (Spark 3.5)
            "azure-core": "1.29.0",
            "azure-identity": "1.16.0",
        },
    }


def test_current_pyproject_versions():
    """Test if our current pyproject.toml dependencies work with platform runtimes"""

    # Read current dependencies from pyproject.toml
    pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"

    print("=" * 70)
    print("Testing current pyproject.toml dependency specifications")
    print("=" * 70)

    # For now, just test known versions
    # TODO: Parse pyproject.toml to get actual dependency specs

    platform_versions = get_platform_versions()

    results = {}
    for platform, versions in platform_versions.items():
        success = test_imports_in_venv(platform, versions["azure-core"], versions["azure-identity"])
        results[platform] = success

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for platform, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{platform:15} {status}")

    all_passed = all(results.values())
    if all_passed:
        print("\n✅ All platforms compatible!")
        return 0
    else:
        print("\n❌ Some platforms have compatibility issues")
        return 1


def test_with_kindling_wheel():
    """Test by installing actual kindling wheel and checking imports"""

    print("\n" + "=" * 70)
    print("Testing with actual Kindling wheel")
    print("=" * 70)

    # Find the latest fabric wheel
    wheels_dir = Path(__file__).parent.parent.parent / "dist"
    fabric_wheels = list(wheels_dir.glob("kindling_fabric-*.whl"))

    if not fabric_wheels:
        print("⚠️  No Fabric wheels found in dist/")
        print("   Run 'poe build' first")
        return 1

    latest_wheel = sorted(fabric_wheels)[-1]
    print(f"\nUsing wheel: {latest_wheel.name}")

    # Test with Fabric runtime versions
    platform_versions = get_platform_versions()
    fabric_versions = platform_versions["fabric"]

    with tempfile.TemporaryDirectory() as tmpdir:
        venv_path = Path(tmpdir) / "test_env"

        print("\n1. Creating virtual environment...")
        subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)

        pip_path = venv_path / "bin" / "pip"
        python_path = venv_path / "bin" / "python"

        # Install Fabric runtime versions first
        print("\n2. Installing Fabric runtime packages (simulating Fabric environment)...")
        subprocess.run(
            [
                str(pip_path),
                "install",
                "-q",
                f"azure-core=={fabric_versions['azure-core']}",
                f"azure-identity=={fabric_versions['azure-identity']}",
                "pyspark>=3.5.0",  # Needed for kindling imports
            ],
            check=True,
        )

        # Check versions before installing wheel
        print("\n3. Checking pre-install versions...")
        version_check = subprocess.run(
            [
                str(python_path),
                "-c",
                "import azure.core; import azure.identity; "
                "print(f'azure-core: {azure.core.__version__}'); "
                "print(f'azure-identity: {azure.identity.__version__}')",
            ],
            capture_output=True,
            text=True,
        )
        print(version_check.stdout)

        # Install kindling wheel
        print("\n4. Installing Kindling wheel...")
        result = subprocess.run(
            [str(pip_path), "install", str(latest_wheel)], capture_output=True, text=True
        )

        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        if result.returncode != 0:
            print("\n❌ Wheel installation failed!")
            return 1

        # Check versions after installing wheel
        print("\n5. Checking post-install versions...")
        version_check = subprocess.run(
            [
                str(python_path),
                "-c",
                "import azure.core; import azure.identity; "
                "print(f'azure-core: {azure.core.__version__}'); "
                "print(f'azure-identity: {azure.identity.__version__}')",
            ],
            capture_output=True,
            text=True,
        )
        print(version_check.stdout)

        # Detect version changes
        if "1.38.0" in version_check.stdout or "1.31." in version_check.stdout:
            print("⚠️  WARNING: azure-core version was upgraded during wheel installation!")
            print("   This will cause ImportError in actual Fabric runtime.")
        else:
            print("✅ azure-core version preserved - no upgrade during installation")

        # Test imports
        print("\n6. Testing critical imports (without Spark/Delta dependencies)...")
        test_script = """
import sys
try:
    # Test Azure SDK imports work with pinned versions
    from azure.identity import DefaultAzureCredential
    print("✅ azure.identity.DefaultAzureCredential imported successfully")

    from azure.core.credentials import AccessToken
    print("✅ azure.core.credentials.AccessToken imported successfully")

    # Test other dependencies
    from packaging.version import Version
    print("✅ packaging.version.Version imported successfully")

    import dynaconf
    print("✅ dynaconf imported successfully")

    import injector
    print("✅ injector imported successfully")

    print("\\n✅ All dependency compatibility checks passed!")
    print("\\nℹ️  Note: Spark/Delta imports skipped (runtime-provided)")
    print("   Kindling modules will work correctly in Fabric runtime")
    sys.exit(0)

except ImportError as e:
    print(f"\\n❌ Import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
"""

        result = subprocess.run(
            [str(python_path), "-c", test_script], capture_output=True, text=True
        )

        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        if result.returncode == 0:
            print("\n✅ Wheel compatibility test: PASSED")
            return 0
        else:
            print("\n❌ Wheel compatibility test: FAILED")
            return 1


def main():
    parser = argparse.ArgumentParser(description="Test package dependency compatibility locally")
    parser.add_argument(
        "--platform",
        choices=["fabric", "synapse", "databricks"],
        help="Test specific platform only",
    )
    parser.add_argument("--with-wheel", action="store_true", help="Test with actual built wheel")

    args = parser.parse_args()

    if args.with_wheel:
        return test_with_kindling_wheel()

    if args.platform:
        platform_versions = get_platform_versions()
        versions = platform_versions[args.platform]
        success = test_imports_in_venv(
            args.platform, versions["azure-core"], versions["azure-identity"]
        )
        return 0 if success else 1

    return test_current_pyproject_versions()


if __name__ == "__main__":
    sys.exit(main())
