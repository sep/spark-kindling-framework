#!/usr/bin/env python3
"""
Deploy wheels to Azure Storage
Usage:
    python scripts/deploy.py              # Deploy from local dist/ (testing)
    python scripts/deploy.py --release    # Deploy from GitHub release (production)
    python scripts/deploy.py --release 0.2.0  # Deploy specific release version
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# ============================================================================
# CONFIGURATION - Update these for your storage account
# ============================================================================
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
CONTAINER = os.getenv("AZURE_CONTAINER", "artifacts")
BASE_PATH = os.getenv("AZURE_BASE_PATH", "")  # Root for all conventional paths
PACKAGES_PATH = f"{BASE_PATH}/packages" if BASE_PATH else "packages"
SCRIPTS_PATH = f"{BASE_PATH}/scripts" if BASE_PATH else "scripts"
RUNTIME_PLATFORMS = ("synapse", "databricks", "fabric")


def get_version_from_pyproject() -> str:
    """Extract version from pyproject.toml"""
    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        raise FileNotFoundError("pyproject.toml not found")

    content = pyproject_path.read_text()
    match = re.search(r'^version = "([^"]+)"', content, re.MULTILINE)
    if not match:
        raise ValueError("Could not find version in pyproject.toml")

    return match.group(1)


def get_latest_git_tag() -> Optional[str]:
    """Get the latest git tag, stripped of 'v' prefix"""
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"], capture_output=True, text=True, check=True
        )
        tag = result.stdout.strip()
        return tag.lstrip("v")
    except subprocess.CalledProcessError:
        return None


def check_git_tag_exists(tag: str) -> bool:
    """Check if a git tag exists"""
    try:
        subprocess.run(["git", "rev-parse", f"v{tag}"], capture_output=True, check=True)
        return True
    except subprocess.CalledProcessError:
        return False


def get_github_repo() -> Optional[str]:
    """Get GitHub repository in format owner/repo"""
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"], capture_output=True, text=True, check=True
        )
        url = result.stdout.strip()

        # Extract owner/repo from various URL formats
        # https://github.com/owner/repo.git
        # git@github.com:owner/repo.git
        match = re.search(r"github\.com[:/](.+?)(?:\.git)?$", url)
        if match:
            return match.group(1).rstrip(".git")

        return None
    except subprocess.CalledProcessError:
        return None


def download_release_wheels(version: str, repo: str, temp_dir: Path) -> None:
    """Download wheels from GitHub release using gh CLI"""
    tag = f"v{version}"

    print(f"📥 Downloading wheels from GitHub release {tag}...")
    print(f"📦 Repository: {repo}")

    try:
        subprocess.run(
            [
                "gh",
                "release",
                "download",
                tag,
                "--pattern",
                "*.whl",
                "--repo",
                repo,
                "--dir",
                str(temp_dir),
            ],
            check=True,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: Failed to download wheels from release {tag}")
        print(f"Make sure the release exists and has wheel attachments")
        print(f"Repository: {repo}")
        sys.exit(1)


def get_wheels(wheels_dir: Path, platform: Optional[str] = None) -> List[Path]:
    """Get runtime wheel files from directory, optionally filtered by platform."""
    if platform:
        pattern = f"kindling_{platform}-*.whl"
        wheels = list(wheels_dir.glob(pattern))
        if not wheels:
            raise FileNotFoundError(
                f"No {platform} wheel found in {wheels_dir}. "
                f"Available platforms: synapse, databricks, fabric"
            )
    else:
        wheels = []
        for runtime_platform in RUNTIME_PLATFORMS:
            wheels.extend(wheels_dir.glob(f"kindling_{runtime_platform}-*.whl"))
        if not wheels:
            raise FileNotFoundError(
                f"No runtime wheels found in {wheels_dir}. "
                "Expected kindling_{synapse|databricks|fabric}-*.whl."
            )
    return sorted(wheels)


def get_bootstrap_script() -> Optional[Path]:
    """Get the bootstrap script from runtime/scripts/"""
    script_path = Path("runtime/scripts/kindling_bootstrap.py")
    if script_path.exists():
        return script_path
    return None


def deploy_bootstrap_script(blob_service_client) -> bool:
    """Deploy bootstrap script to Azure Storage"""
    bootstrap_script = get_bootstrap_script()
    if not bootstrap_script:
        print("⚠️  Bootstrap script not found at runtime/scripts/kindling_bootstrap.py")
        return False

    dest_path = f"{SCRIPTS_PATH}/kindling_bootstrap.py"

    print(f"  Uploading: kindling_bootstrap.py to {SCRIPTS_PATH}/...")

    blob_client = blob_service_client.get_blob_client(container=CONTAINER, blob=dest_path)

    with open(bootstrap_script, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    return True


def deploy_wheels(wheels: List[Path], version: str) -> None:
    """Deploy wheels to Azure Storage using azure-identity"""
    print(f"\n☁️  Deploying to: {STORAGE_ACCOUNT}/{CONTAINER}/{PACKAGES_PATH}/\n")

    # Create BlobServiceClient with DefaultAzureCredential
    # This supports: Azure CLI (az login), environment variables, managed identity
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

    print(f"\n✅ Successfully deployed {upload_count} wheels\n")

    # Deploy bootstrap script
    print("📜 Deploying bootstrap script...")
    if deploy_bootstrap_script(blob_service_client):
        print("✅ Bootstrap script deployed\n")
    else:
        print("⚠️  Bootstrap script deployment skipped\n")

    print("🎉 Deployment complete!\n")
    print("Storage structure:")
    print(f"  {STORAGE_ACCOUNT}/{CONTAINER}/")
    print(f"  ├── {PACKAGES_PATH}/")
    print(f"  │   ├── kindling_databricks-{version}-py3-none-any.whl")
    print(f"  │   ├── kindling_fabric-{version}-py3-none-any.whl")
    print(f"  │   └── kindling_synapse-{version}-py3-none-any.whl")
    print(f"  └── {SCRIPTS_PATH}/")
    print(f"      └── kindling_bootstrap.py")
    print()
    print("🔗 Bootstrap paths:")
    print(
        f"   artifacts_storage_path: abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{PACKAGES_PATH}"
    )
    print(
        f"   bootstrap_script: abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{SCRIPTS_PATH}/kindling_bootstrap.py"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Deploy wheels to Azure Storage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/deploy.py                      # Deploy all platforms from local dist/
  python scripts/deploy.py --platform synapse   # Deploy only synapse wheel
  python scripts/deploy.py --release            # Deploy all from latest GitHub release
  python scripts/deploy.py --release 0.2.0      # Deploy all from specific release
  python scripts/deploy.py --release --platform databricks  # Deploy databricks from release
        """,
    )
    parser.add_argument(
        "--release",
        "-r",
        nargs="?",
        const=True,
        default=False,
        metavar="VERSION",
        help="Deploy from GitHub release (optionally specify version)",
    )
    parser.add_argument(
        "--platform",
        "-p",
        choices=["synapse", "databricks", "fabric"],
        help="Deploy only specific platform (synapse, databricks, or fabric)",
    )

    args = parser.parse_args()

    print("📦 Deploying wheels to Azure Storage")
    print("=" * 50)
    print()

    if args.release:
        # ====================================================================
        # RELEASE MODE: Download from GitHub release
        # ====================================================================

        # Determine version
        if args.release is True:
            version = get_latest_git_tag()
            if not version:
                print("❌ Error: No version specified and no git tags found")
                print("Usage: python scripts/deploy.py --release [version]")
                sys.exit(1)
            print(f"📌 Detected latest version: {version}")
        else:
            version = args.release.lstrip("v")
            print(f"📌 Using version: {version}")

        # Check tag exists
        if not check_git_tag_exists(version):
            print(f"❌ Error: Tag v{version} not found")
            sys.exit(1)

        # Check GitHub CLI
        if not subprocess.run(["which", "gh"], capture_output=True).returncode == 0:
            print("❌ Error: GitHub CLI (gh) not found")
            print("Please install it: https://cli.github.com/")
            sys.exit(1)

        # Get repository
        repo = get_github_repo()
        if not repo:
            print("❌ Error: Could not detect GitHub repository")
            print("Make sure you're in a git repository with a GitHub remote")
            sys.exit(1)

        # Download wheels to temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            download_release_wheels(version, repo, temp_path)
            wheels = get_wheels(temp_path, args.platform)

            platform_msg = f" ({args.platform} only)" if args.platform else ""
            print(f"\n📦 Wheels to deploy{platform_msg}:")
            for wheel in wheels:
                size = wheel.stat().st_size / 1024  # KB
                print(f"  {wheel.name} ({size:.0f}K)")

            deploy_wheels(wheels, version)

    else:
        # ====================================================================
        # LOCAL MODE: Deploy from dist/ (for testing)
        # ====================================================================

        version = get_version_from_pyproject()
        print(f"📌 Using local build version: {version}")

        dist_dir = Path("dist")
        if not dist_dir.exists():
            print("❌ Error: dist/ directory not found")
            print("Run: poetry run poe build")
            sys.exit(1)

        wheels = get_wheels(dist_dir, args.platform)
        platform_msg = f" ({args.platform} only)" if args.platform else ""
        print(f"📦 Using local wheels from: {dist_dir}{platform_msg}")

        print(f"\n📦 Wheels to deploy:")
        for wheel in wheels:
            size = wheel.stat().st_size / 1024  # KB
            print(f"  {wheel.name} ({size:.0f}K)")

        deploy_wheels(wheels, version)


if __name__ == "__main__":
    main()
