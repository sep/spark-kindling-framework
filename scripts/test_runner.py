#!/usr/bin/env python3
"""
Test runner utilities for poe tasks.

Provides flexible test execution with optional platform and test filtering.
"""

import os
import subprocess
import sys
from typing import List, Optional


def _load_dotenv(path: str = ".env") -> None:
    """Best-effort loader for .env files used in local system testing.

    We intentionally keep this minimal (no dependencies). It supports lines like:
      - export KEY=value
      - KEY=value
    and ignores comments/blank lines.

    Values are treated as literals (no shell expansion), which is important for
    entries like EVENTHUB_TEST_CONSUMER_GROUP="$Default".
    """
    if os.environ.get("KINDLING_SKIP_DOTENV", "").strip().lower() in {"1", "true", "yes"}:
        return

    try:
        if not os.path.exists(path):
            return
        with open(path, encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export ") :].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                # Don't overwrite values already set in the environment.
                if key in os.environ:
                    continue
                if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                    value = value[1:-1]
                os.environ[key] = value
    except Exception:
        # Non-fatal: this is just a convenience for local runs.
        return


def build_pytest_args(
    test_path: str,
    platform: Optional[str] = None,
    test: Optional[str] = None,
    extra_args: Optional[List[str]] = None,
) -> List[str]:
    """
    Build pytest command arguments with optional filtering.

    Args:
        test_path: Base path to test directory
        platform: Optional platform filter (fabric, synapse, databricks)
        test: Optional test name/pattern filter
        extra_args: Additional pytest arguments

    Returns:
        List of command arguments for pytest
    """
    args = ["pytest", test_path, "-v", "-s"]

    # Pass platform as a pytest option (injected into tests via conftest)
    if platform:
        args.extend(["--platform", platform])

    # Use -k only for test name filtering
    if test:
        args.extend(["-k", test])

    if extra_args:
        args.extend(extra_args)

    return args


def _run_system_test_preflight(platform: Optional[str]) -> int:
    """Run auth/config preflight checks for a specific platform."""
    if not platform:
        return 0

    cmd = [sys.executable, "tests/system/auth_check.py", "--platform", platform]
    print(f"Running preflight: {' '.join(cmd)}")
    print()
    result = subprocess.run(cmd)
    return result.returncode


def run_unit_tests_ci() -> int:
    """
    Run unit tests with CI-specific reporting outputs.

    Returns:
        Exit code from pytest
    """
    args = build_pytest_args(
        test_path="tests/unit/",
        extra_args=[
            "--cov=packages/kindling",
            "--cov-report=xml",
            "--cov-report=html:htmlcov",
            "--junit-xml=test-results/unit-test-results.xml",
            "--json-report",
            "--json-report-file=test-results/unit-test-report.json",
        ],
    )

    os.makedirs("test-results", exist_ok=True)

    print(f"Running: {' '.join(args)}")
    print()

    result = subprocess.run(args)
    return result.returncode


def run_integration_tests_ci() -> int:
    """
    Run integration tests with CI-specific reporting outputs.

    Returns:
        Exit code from pytest
    """
    args = build_pytest_args(
        test_path="tests/integration/",
        extra_args=[
            "--junit-xml=test-results/integration-test-results.xml",
            "--no-cov",
        ],
    )

    os.makedirs("test-results", exist_ok=True)

    print(f"Running: {' '.join(args)}")
    print()

    result = subprocess.run(args)
    return result.returncode


def run_system_tests(platform: str = "", test: str = "") -> int:
    """
    Run system tests with optional platform and test filtering.

    Args:
        platform: Platform to test (fabric, synapse, databricks). Empty string = all platforms
        test: Specific test name or pattern. Empty string = all tests

    Returns:
        Exit code from pytest

    Examples:
        poe test-system                           # Run all system tests on all platforms
        poe test-system --platform fabric         # Run all Fabric tests
        poe test-system --test deploy_app_as_job # Run specific test on all platforms
        poe test-system --platform fabric --test deploy_app_as_job  # Specific test on Fabric
        poe test-system --test name_mapper       # Run default name mapper test on all platforms
    """
    _load_dotenv()

    # Convert empty strings to None for clarity
    platform_filter = platform if platform else None
    test_filter = test if test else None

    # Avoid coverage file collisions when running multiple system tests concurrently.
    # pytest-cov uses COVERAGE_FILE for the sqlite db path.
    try:
        suffix = platform_filter or "all"
        os.environ.setdefault("COVERAGE_FILE", f".coverage.system.{suffix}")
    except Exception:
        pass

    # Build pytest command
    args = build_pytest_args(
        test_path="tests/system/core/",
        platform=platform_filter,
        test=test_filter,
    )

    # Print command for transparency
    print(f"Running: {' '.join(args)}")
    print()

    # Execute pytest
    result = subprocess.run(args)
    return result.returncode


def run_system_tests_ci(platform: str = "", test: str = "") -> int:
    """
    Run system tests with CI-specific reporting outputs.

    Args:
        platform: Platform to test (fabric, synapse, databricks)
        test: Specific test name or pattern

    Returns:
        Exit code from pytest
    """
    _load_dotenv()

    platform_filter = platform if platform else None
    test_filter = test if test else None

    preflight_rc = _run_system_test_preflight(platform_filter)
    if preflight_rc != 0:
        return preflight_rc

    os.makedirs("test-results", exist_ok=True)

    extra_args = [
        f"--junit-xml=test-results/system-test-results-{platform_filter or 'all'}.xml",
        "--json-report",
        f"--json-report-file=test-results/system-test-report-{platform_filter or 'all'}.json",
    ]

    args = build_pytest_args(
        test_path="tests/system/core/",
        platform=platform_filter,
        test=test_filter,
        extra_args=extra_args,
    )

    print(f"Running: {' '.join(args)}")
    print()

    result = subprocess.run(args)
    return result.returncode


def run_extension_tests(extension: str = "azure-monitor", platform: str = "") -> int:
    """
    Run extension tests with optional platform filtering.

    Args:
        extension: Extension name to test (e.g., "azure-monitor")
        platform: Platform to test (fabric, synapse, databricks). Empty string = all platforms

    Returns:
        Exit code from pytest

    Examples:
        poe test-extension                                    # Run azure-monitor tests on all platforms
        poe test-extension --extension azure-monitor --platform fabric  # Azure Monitor on Fabric
    """
    # Convert empty strings to None
    platform_filter = platform if platform else None

    # Build test path
    test_path = f"tests/system/extensions/{extension}/"

    # Build pytest command
    args = build_pytest_args(
        test_path=test_path,
        platform=platform_filter,
    )

    # Print command for transparency
    print(f"Running: {' '.join(args)}")
    print()

    # Execute pytest
    result = subprocess.run(args)
    return result.returncode


def run_setup_github_secrets(environment: str = "", name: str = "") -> int:
    """
    Publish .env variables to GitHub repository or environment secrets.

    Args:
        environment: Optional GitHub environment name (e.g. dev, test)
        name: Optional single variable name to publish

    Returns:
        Exit code from the shell script
    """
    cmd = ["bash", "scripts/set_github_secrets.sh"]

    if environment:
        cmd.extend(["--environment", environment])
    if name:
        cmd.extend(["--name", name])

    print(f"Running: {' '.join(cmd)}")
    print()

    result = subprocess.run(cmd)
    return result.returncode


def run_cleanup(platform: str = "", skip_packages: bool = False) -> int:
    """
    Run cleanup with optional platform filtering.

    Args:
        platform: Platform to clean (fabric, synapse, databricks). Empty string = all platforms
        skip_packages: Skip cleanup of old packages (default: False)

    Returns:
        Exit code from cleanup script

    Examples:
        poe cleanup                      # Clean all platforms + old packages
        poe cleanup --platform fabric    # Clean Fabric only (skip packages)
        poe cleanup --skip-packages      # Clean all platforms (skip packages)
    """
    # Convert empty strings to None
    platform_filter = platform if platform else None

    # Build cleanup command
    if platform_filter:
        # Platform-specific cleanup (no package cleanup)
        cmd = ["python", "scripts/cleanup_test_resources.py", "--platform", platform_filter]
    else:
        # All platforms + packages (original behavior)
        if skip_packages:
            cmd = ["python", "scripts/cleanup_test_resources.py", "--all"]
        else:
            cmd = [
                "python",
                "scripts/cleanup_test_resources.py",
                "--all",
                "&&",
                "python",
                "scripts/cleanup_old_packages.py",
            ]

    # Print command for transparency
    print(f"Running: {' '.join(cmd)}")
    print()

    # Execute cleanup (use shell for && operator)
    if "&&" in cmd:
        result = subprocess.run(" ".join(cmd), shell=True)
    else:
        result = subprocess.run(cmd)

    return result.returncode


def run_deploy_app(
    app_path: str = "",
    app_name: str = "",
) -> int:
    """
    Deploy a data app to Azure Storage.

    Apps are platform-independent — they're just files in storage.
    Any platform's jobs can reference them by app_name.

    Uses AppPackager to prepare files and uploads to:
        {AZURE_BASE_PATH}/data-apps/{app_name}/

    Args:
        app_path: Path to app directory or .kda file
        app_name: Name for the deployed app. Defaults to the directory name.

    Returns:
        Exit code (0 = success)

    Examples:
        poe deploy-app --app-path tests/data-apps/universal-test-app
        poe deploy-app --app-path my-app.kda --app-name my-app
    """
    import os
    from pathlib import Path

    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    if not app_path:
        print("❌ --app-path is required")
        return 1

    app_path_obj = Path(app_path)
    if not app_path_obj.exists():
        print(f"❌ App path does not exist: {app_path}")
        return 1

    # Default app_name to directory/file name
    if not app_name:
        app_name = app_path_obj.stem if app_path_obj.is_file() else app_path_obj.name

    # Storage config from environment (same vars as deploy.py)
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    container = os.getenv("AZURE_CONTAINER", "artifacts")
    base_path = os.getenv("AZURE_BASE_PATH", "")

    if not storage_account:
        print("❌ AZURE_STORAGE_ACCOUNT environment variable is required")
        return 1

    target_path = f"{base_path}/data-apps/{app_name}" if base_path else f"data-apps/{app_name}"

    print(f"🚀 Deploying app '{app_name}' from {app_path}")
    print(f"   → {storage_account}/{container}/{target_path}/")
    print()

    try:
        # Prepare app files
        from kindling.job_deployment import AppPackager

        packager = AppPackager()
        app_files = packager.prepare_app_files(str(app_path_obj))
        print(f"📦 Prepared {len(app_files)} app files")

        # Connect to Azure Storage
        account_url = f"https://{storage_account}.blob.core.windows.net"
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)

        # Upload each file
        upload_count = 0
        for filename, content in app_files.items():
            blob_path = f"{target_path}/{filename}"
            blob_client = blob_service_client.get_blob_client(container=container, blob=blob_path)
            blob_client.upload_blob(
                content.encode() if isinstance(content, str) else content, overwrite=True
            )
            print(f"  ↑ {filename}")
            upload_count += 1

        print(f"\n✅ Deployed {upload_count} files to data-apps/{app_name}/")
        return 0

    except Exception as e:
        print(f"❌ Deploy failed: {e}")
        return 1


def run_deploy(platform: str = "", release: str = "") -> int:
    """
    Run deployment with optional platform and release filtering.

    Args:
        platform: Platform to deploy (fabric, synapse, databricks). Empty string = all platforms
        release: Release version to deploy. Empty = local dist/, "latest" = latest release, or specific version

    Returns:
        Exit code from deploy script

    Examples:
        poe deploy                           # Deploy all platforms from local dist/
        poe deploy --platform fabric         # Deploy Fabric only from local dist/
        poe deploy --release                 # Deploy all from latest GitHub release
        poe deploy --release 0.4.0           # Deploy all from specific release
        poe deploy --platform fabric --release  # Deploy Fabric from latest release
    """
    # Convert empty strings to None
    platform_filter = platform if platform else None
    release_flag = release if release else None

    # Build deploy command
    cmd = ["python", "scripts/deploy.py"]

    if release_flag:
        cmd.append("--release")
        if release_flag != "latest":
            # Specific version provided
            cmd.append(release_flag)

    if platform_filter:
        cmd.extend(["--platform", platform_filter])

    # Print command for transparency
    print(f"Running: {' '.join(cmd)}")
    print()

    # Execute deploy
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    # Allow direct execution for testing
    import argparse

    parser = argparse.ArgumentParser(description="Test runner for Kindling framework")
    subparsers = parser.add_subparsers(dest="command", help="Test command")

    # System tests subcommand
    system_parser = subparsers.add_parser("system", help="Run system tests")
    system_parser.add_argument(
        "--platform", default="", help="Platform filter (fabric, synapse, databricks)"
    )
    system_parser.add_argument("--test", default="", help="Test name or pattern filter")

    # Extension tests subcommand
    extension_parser = subparsers.add_parser("extension", help="Run extension tests")
    extension_parser.add_argument("--extension", default="azure-monitor", help="Extension name")
    extension_parser.add_argument(
        "--platform", default="", help="Platform filter (fabric, synapse, databricks)"
    )

    # Cleanup subcommand
    cleanup_parser = subparsers.add_parser("cleanup", help="Run cleanup")
    cleanup_parser.add_argument(
        "--platform", default="", help="Platform filter (fabric, synapse, databricks)"
    )
    cleanup_parser.add_argument(
        "--skip-packages", action="store_true", help="Skip cleanup of old packages"
    )

    # Deploy subcommand
    deploy_parser = subparsers.add_parser("deploy", help="Run deployment")
    deploy_parser.add_argument(
        "--platform", default="", help="Platform filter (fabric, synapse, databricks)"
    )
    deploy_parser.add_argument(
        "--release",
        default="",
        nargs="?",
        const="latest",
        help="Deploy from release (latest or specific version)",
    )

    # Deploy-app subcommand
    deploy_app_parser = subparsers.add_parser(
        "deploy-app", help="Deploy a data app to Azure Storage"
    )
    deploy_app_parser.add_argument(
        "--app-path", default="", help="Path to app directory or .kda file"
    )
    deploy_app_parser.add_argument(
        "--app-name", default="", help="Name for the deployed app (defaults to directory name)"
    )

    args = parser.parse_args()

    if args.command == "system":
        sys.exit(run_system_tests(platform=args.platform, test=args.test))
    elif args.command == "extension":
        sys.exit(run_extension_tests(extension=args.extension, platform=args.platform))
    elif args.command == "cleanup":
        sys.exit(run_cleanup(platform=args.platform, skip_packages=args.skip_packages))
    elif args.command == "deploy":
        sys.exit(run_deploy(platform=args.platform, release=args.release))
    elif args.command == "deploy-app":
        sys.exit(run_deploy_app(app_path=args.app_path, app_name=args.app_name))
    else:
        parser.print_help()
        sys.exit(1)
