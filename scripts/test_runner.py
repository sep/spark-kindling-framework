#!/usr/bin/env python3
"""
Test runner utilities for poe tasks.

Provides flexible test execution with optional platform and test filtering.
"""

import subprocess
import sys
from typing import List, Optional


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

    # Add platform and/or test filters using -k option
    k_filters = []
    if platform:
        k_filters.append(platform)
    if test:
        k_filters.append(test)

    if k_filters:
        # Combine filters with 'and' logic
        k_expr = " and ".join(k_filters)
        args.extend(["-k", k_expr])

    if extra_args:
        args.extend(extra_args)

    return args


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
    """
    # Convert empty strings to None for clarity
    platform_filter = platform if platform else None
    test_filter = test if test else None

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

    args = parser.parse_args()

    if args.command == "system":
        sys.exit(run_system_tests(platform=args.platform, test=args.test))
    elif args.command == "extension":
        sys.exit(run_extension_tests(extension=args.extension, platform=args.platform))
    elif args.command == "cleanup":
        sys.exit(run_cleanup(platform=args.platform, skip_packages=args.skip_packages))
    elif args.command == "deploy":
        sys.exit(run_deploy(platform=args.platform, release=args.release))
    else:
        parser.print_help()
        sys.exit(1)
