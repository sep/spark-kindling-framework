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


def find_extension_wheels(extension_name: str, platform: Optional[str] = None) -> List[Path]:
    """
    Find extension wheels in dist/ directory

    Args:
        extension_name: Extension name (e.g., 'kindling-otel-azure')
        platform: Optional platform filter (e.g., 'fabric')

    Returns:
        List of matching wheel files
    """
    dist_dir = Path("dist")
    if not dist_dir.exists():
        print("❌ Error: dist/ directory not found")
        return []

    # Convert to underscore format for wheel filename matching
    wheel_prefix = extension_name.replace("-", "_")

    # Build pattern: kindling_otel_azure_fabric-*.whl or kindling_otel_azure-*.whl
    if platform:
        pattern = f"{wheel_prefix}_{platform}-*.whl"
    else:
        pattern = f"{wheel_prefix}*.whl"

    wheels = list(dist_dir.glob(pattern))

    if not wheels:
        print(f"❌ No extension wheels found matching: {pattern}")
        return []

    return wheels


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
