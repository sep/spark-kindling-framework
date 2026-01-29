#!/usr/bin/env python3
"""
Clean up old package versions from Azure Storage, keeping only:
1. The latest non-alpha version
2. Any alpha versions newer than the latest non-alpha

Example:
- 2.0.1 (old non-alpha) -> DELETE
- 2.0.2 (latest non-alpha) -> KEEP
- 2.0.2a1 (alpha based on latest non-alpha) -> KEEP
- 2.0.3a1 (alpha newer than latest non-alpha) -> KEEP
"""

import os
import re
from typing import List, Tuple

from packaging.version import InvalidVersion, Version


def parse_wheel_version(filename: str) -> Tuple[str, Version, bool]:
    """
    Parse wheel filename to extract package name, version, and alpha status.

    Returns:
        Tuple of (package_name, version, is_alpha)
    """
    # Pattern: kindling_{platform}-{version}-py3-none-any.whl
    match = re.match(r"^(kindling_\w+)-([\d.]+(?:a\d+)?)-py3-none-any\.whl$", filename)
    if not match:
        raise ValueError(f"Cannot parse wheel filename: {filename}")

    package_name = match.group(1)
    version_str = match.group(2)

    try:
        version = Version(version_str)
        is_alpha = version.is_prerelease
        return package_name, version, is_alpha
    except InvalidVersion:
        raise ValueError(f"Invalid version in filename: {filename}")


def get_packages_to_keep(wheels: List[str]) -> List[str]:
    """
    Determine which packages to keep based on version logic.

    Args:
        wheels: List of wheel filenames

    Returns:
        List of wheel filenames to keep
    """
    # Group wheels by package name
    packages = {}
    for wheel in wheels:
        try:
            pkg_name, version, is_alpha = parse_wheel_version(wheel)
            if pkg_name not in packages:
                packages[pkg_name] = []
            packages[pkg_name].append((wheel, version, is_alpha))
        except ValueError as e:
            print(f"⚠️  Skipping {wheel}: {e}")
            continue

    # For each package, determine what to keep
    keep = []
    for pkg_name, versions in packages.items():
        # Sort by version (oldest to newest)
        versions.sort(key=lambda x: x[1])

        # Find latest non-alpha version
        non_alphas = [(w, v, a) for w, v, a in versions if not a]
        if not non_alphas:
            # No non-alpha versions, keep all alphas
            keep.extend([w for w, v, a in versions])
            print(f"📦 {pkg_name}: No non-alpha versions, keeping all {len(versions)} alphas")
            continue

        latest_non_alpha_version = non_alphas[-1][1]
        print(f"📦 {pkg_name}: Latest non-alpha version: {latest_non_alpha_version}")

        # Keep latest non-alpha and any newer versions (including alphas)
        for wheel, version, is_alpha in versions:
            if version >= latest_non_alpha_version:
                keep.append(wheel)
                status = "alpha" if is_alpha else "stable"
                print(f"   ✅ Keep: {wheel} ({status}, {version})")
            else:
                print(f"   🗑️  Delete: {wheel} (older than {latest_non_alpha_version})")

    return keep


def main():
    """Clean up old package versions from Azure Storage."""
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    # Get Azure credentials
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "sepstdatalakedev")
    container = os.getenv("AZURE_CONTAINER", "artifacts")

    print("🧹 Cleaning up old package versions")
    print("=" * 80)

    # Connect to Azure Storage
    account_url = f"https://{storage_account}.blob.core.windows.net"
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    container_client = blob_service_client.get_container_client(container)

    # List all wheels in packages/ directory
    print(f"\n📂 Listing packages from {storage_account}/{container}/packages/")
    blobs = container_client.list_blobs(name_starts_with="packages/")
    wheels = [blob.name.replace("packages/", "") for blob in blobs if blob.name.endswith(".whl")]

    if not wheels:
        print("✅ No packages found")
        return

    print(f"📊 Found {len(wheels)} total packages")
    print()

    # Determine what to keep
    keep = get_packages_to_keep(wheels)
    delete = [w for w in wheels if w not in keep]

    print()
    print(f"📊 Summary:")
    print(f"   Keep: {len(keep)} packages")
    print(f"   Delete: {len(delete)} packages")

    if not delete:
        print("\n✅ No old packages to delete")
        return

    # Delete old packages
    print(f"\n🗑️  Deleting {len(delete)} old packages...")
    for wheel in delete:
        blob_path = f"packages/{wheel}"
        blob_client = container_client.get_blob_client(blob_path)
        blob_client.delete_blob()
        print(f"   ✅ Deleted: {wheel}")

    print("\n✅ Package cleanup complete!")


if __name__ == "__main__":
    main()
