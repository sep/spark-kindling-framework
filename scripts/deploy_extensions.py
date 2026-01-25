#!/usr/bin/env python3
"""
Deploy Kindling extension wheel to Azure Storage

Deploys a specific extension wheel from packages/ to the same storage location
as the main framework wheels.

Usage:
    python scripts/deploy_extensions.py kindling-otel-azure
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import List, Optional

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from packaging.version import InvalidVersion, Version

# ============================================================================
# CONFIGURATION - Same as main deploy.py
# ============================================================================
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
CONTAINER = os.getenv("AZURE_CONTAINER", "artifacts")
BASE_PATH = os.getenv("AZURE_BASE_PATH", "")
PACKAGES_PATH = f"{BASE_PATH}/packages" if BASE_PATH else "packages"


def find_extension_wheels(extension_name: str) -> List[Path]:
    """
    Find the latest extension wheel in packages/ subdirectories

    Args:
        extension_name: Extension name (e.g., 'kindling-otel-azure')

    Returns:
        List containing only the latest wheel file (or empty if none found)
    """
    packages_dir = Path("packages")
    if not packages_dir.exists():
        print("❌ Error: packages/ directory not found")
        return []

    all_wheels = []

    for ext_dir in packages_dir.iterdir():
        if not ext_dir.is_dir():
            continue

        # Skip the main kindling package (not an extension)
        if ext_dir.name == "kindling":
            continue

        # Convert package_name to directory name: kindling-otel-azure -> kindling_otel_azure
        expected_dir = extension_name.replace("-", "_")
        if ext_dir.name != expected_dir:
            continue

        # Look for dist/ subdirectory
        dist_dir = ext_dir / "dist"
        if not dist_dir.exists():
            continue

        # Find wheel files
        ext_wheels = list(dist_dir.glob("*.whl"))
        if ext_wheels:
            all_wheels.extend(ext_wheels)
            print(f"📦 Found {len(ext_wheels)} wheel(s) in {ext_dir.name}/")

    if not all_wheels:
        return []

    # Parse versions and find the latest
    wheel_versions = []
    for wheel in all_wheels:
        # Extract version from wheel filename: name-version-py3-none-any.whl
        match = re.match(r"^.+-(.+?)-py\d+-none-any\.whl$", wheel.name)
        if match:
            version_str = match.group(1)
            try:
                version = Version(version_str)
                wheel_versions.append((version, wheel))
            except InvalidVersion:
                print(f"⚠️  Could not parse version from: {wheel.name}")
                continue

    if not wheel_versions:
        # If we can't parse versions, just return the most recently modified file
        print("⚠️  Using most recent file by modification time")
        return [max(all_wheels, key=lambda p: p.stat().st_mtime)]

    # Sort by version and return only the latest
    wheel_versions.sort(key=lambda x: x[0], reverse=True)
    latest_wheel = wheel_versions[0][1]
    latest_version = wheel_versions[0][0]

    print(f"📌 Selected latest version: {latest_version}")

    return [latest_wheel]


def deploy_extension_wheels(wheels: List[Path]) -> None:
    """Deploy extension wheels to Azure Storage"""

    print(f"\n☁️  Deploying to: {STORAGE_ACCOUNT}/{CONTAINER}/{PACKAGES_PATH}/\n")

    # Create BlobServiceClient with DefaultAzureCredential
    account_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"

    try:
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)

        # Test connection
        container_client = blob_service_client.get_container_client(CONTAINER)
        container_client.get_container_properties()

    except Exception as e:
        print(f"❌ Error: Failed to authenticate to Azure Storage")
        print(f"Error: {e}")
        print(f"\nTry: az login")
        sys.exit(1)

    # Upload each wheel
    upload_count = 0
    for wheel in wheels:
        dest_path = f"{PACKAGES_PATH}/{wheel.name}"

        print(f"  Uploading: {wheel.name}...")

        blob_client = blob_service_client.get_blob_client(container=CONTAINER, blob=dest_path)

        with open(wheel, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        upload_count += 1

    print(f"\n✅ Successfully deployed {upload_count} extension wheel(s)\n")
    print("🔗 Extensions location:")
    print(f"   abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{PACKAGES_PATH}/")


def main():
    parser = argparse.ArgumentParser(
        description="Deploy Kindling extension wheels to Azure Storage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/deploy_extensions.py kindling-otel-azure

Before deploying, build the extension:
  cd packages/kindling_otel_azure
  poetry build
        """,
    )
    parser.add_argument(
        "extension",
        help="Extension to deploy (e.g., kindling-otel-azure)",
    )

    args = parser.parse_args()

    if not STORAGE_ACCOUNT:
        print("❌ Error: AZURE_STORAGE_ACCOUNT environment variable not set")
        sys.exit(1)

    print(f"📦 Deploying extension: {args.extension}")
    print("=" * 50)
    print()

    # Find wheels
    wheels = find_extension_wheels(args.extension)

    if not wheels:
        print(f"❌ No wheels found for extension: {args.extension}")
        print(f"\nMake sure the extension is built:")
        print(f"  cd packages/{args.extension.replace('-', '_')}")
        print(f"  poetry build")
        sys.exit(1)

    print(f"\n📦 Extension to deploy:")
    for wheel in wheels:
        size = wheel.stat().st_size / 1024  # KB
        print(f"  {wheel.name} ({size:.0f}K)")

    deploy_extension_wheels(wheels)


if __name__ == "__main__":
    main()
