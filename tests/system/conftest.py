"""
Pytest configuration for system tests.

Configures test markers, fixtures, and settings for system-level integration tests.
"""

import os
import sys
from pathlib import Path
from typing import Optional

import pytest

# All supported platforms - single source of truth
ALL_PLATFORMS = ["fabric", "databricks", "synapse"]
ALL_CLOUDS = ["azure", "aws", "gcp"]


def _detect_databricks_cloud() -> Optional[str]:
    """Detect Databricks cloud provider from environment."""
    explicit = (os.getenv("DATABRICKS_CLOUD") or "").strip().lower()
    if explicit in ALL_CLOUDS:
        return explicit

    host = (os.getenv("DATABRICKS_HOST") or "").strip().lower()
    if not host:
        return None

    if "azuredatabricks.net" in host:
        return "azure"
    if "gcp.databricks.com" in host:
        return "gcp"
    if "cloud.databricks.com" in host or host.endswith(".databricks.com"):
        return "aws"

    return None


def get_cloud_for_platform(platform_name: str) -> Optional[str]:
    """Resolve cloud provider for a platform."""
    if platform_name in {"fabric", "synapse"}:
        return "azure"
    if platform_name == "databricks":
        return _detect_databricks_cloud()
    return None


def get_databricks_system_test_mode() -> str:
    """Return the Databricks system-test profile.

    `uc` validates UC/Volumes behavior.
    `classic` validates DBFS/reference-oriented fallback behavior.
    """
    mode = (os.getenv("KINDLING_DATABRICKS_SYSTEM_TEST_MODE") or "uc").strip().lower()
    return mode if mode in {"uc", "classic"} else "uc"


def get_databricks_uc_volume_root() -> str:
    """Return the Databricks UC /Volumes root for system tests.

    This keeps the system test namespace environment-driven so Terraform and
    runtime config can point at the same managed-volume namespace.
    """
    catalog = (os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG") or "medallion").strip()
    schema = (os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA") or "default").strip()
    volume = (os.getenv("KINDLING_DATABRICKS_RUNTIME_TEMP_VOLUME") or "temp").strip()
    return f"/Volumes/{catalog}/{schema}/{volume}"


def pytest_generate_tests(metafunc):
    """
    Dynamically parametrize tests that need a 'platform' fixture.

    Platform is injected from --platform CLI option or TEST_PLATFORM env var.
    Tests never hardcode platform names - they just declare they need the fixture.
    """
    if "platform" in metafunc.fixturenames:
        platform = metafunc.config.getoption("--platform", default=None) or os.getenv(
            "TEST_PLATFORM"
        )
        if platform:
            platforms = [platform]
        else:
            platforms = ALL_PLATFORMS
        metafunc.parametrize("platform", platforms)


def pytest_configure(config):
    """Configure custom pytest markers"""
    config.addinivalue_line("markers", "fabric: tests that require Microsoft Fabric platform")
    config.addinivalue_line("markers", "databricks: tests that require Databricks platform")
    config.addinivalue_line("markers", "synapse: tests that require Azure Synapse platform")
    config.addinivalue_line("markers", "azure: tests that require Azure cloud")
    config.addinivalue_line("markers", "aws: tests that require AWS cloud")
    config.addinivalue_line("markers", "gcp: tests that require Google Cloud")
    config.addinivalue_line("markers", "system: system-level integration tests")
    config.addinivalue_line("markers", "slow: tests that take significant time to run")


def pytest_collection_modifyitems(config, items):
    """
    Modify test collection to handle platform-specific tests.

    Skip tests that require unavailable credentials.
    """
    for item in items:
        platform_value = None
        # Add platform marker dynamically from parametrize values
        if hasattr(item, "callspec") and "platform" in item.callspec.params:
            platform_value = item.callspec.params["platform"]
            # Add the platform as a marker so -m fabric/databricks/synapse works
            item.add_marker(getattr(pytest.mark, platform_value))

            # Also add cloud marker (azure/aws/gcp) when platform->cloud is known
            cloud_value = get_cloud_for_platform(platform_value)
            if cloud_value:
                item.add_marker(getattr(pytest.mark, cloud_value))

        if not platform_value:
            platform_value = config.getoption("--platform", default=None) or os.getenv(
                "TEST_PLATFORM"
            )

        # Enforce cloud markers for platform-parametrized tests.
        requested_cloud = next((cloud for cloud in ALL_CLOUDS if cloud in item.keywords), None)
        if requested_cloud and platform_value:
            platform_cloud = get_cloud_for_platform(platform_value)
            if platform_cloud and platform_cloud != requested_cloud:
                item.add_marker(
                    pytest.mark.skip(
                        reason=(
                            f"Test requires cloud '{requested_cloud}', "
                            f"but platform '{platform_value}' runs on '{platform_cloud}'."
                        )
                    )
                )
            elif platform_cloud is None:
                item.add_marker(
                    pytest.mark.skip(
                        reason=(
                            f"Test requires cloud '{requested_cloud}', but cloud could not be "
                            f"determined for platform '{platform_value}'."
                        )
                    )
                )

        # Check for platform markers and required credentials
        if "fabric" in item.keywords:
            # Only workspace and lakehouse IDs are required
            # Authentication can come from service principal OR az login
            required_vars = ["FABRIC_WORKSPACE_ID", "FABRIC_LAKEHOUSE_ID"]
            missing = [var for var in required_vars if not os.getenv(var)]
            if missing:
                item.add_marker(
                    pytest.mark.skip(
                        reason=f"Missing Fabric configuration: {', '.join(missing)}. "
                        "Set these env vars or use 'az login' for authentication."
                    )
                )

        if "databricks" in item.keywords:
            # Only workspace host is strictly required
            # Authentication can come from Azure CLI (DefaultAzureCredential pattern)
            required_vars = ["DATABRICKS_HOST"]
            missing = [var for var in required_vars if not os.getenv(var)]
            if missing:
                item.add_marker(
                    pytest.mark.skip(
                        reason=f"Missing Databricks configuration: {', '.join(missing)}. "
                        "Set DATABRICKS_HOST env var and use 'az login' for authentication."
                    )
                )

        if "synapse" in item.keywords:
            # Only workspace name is strictly required
            # Spark pool can be discovered or use default
            # Authentication can come from service principal OR az login
            required_vars = ["SYNAPSE_WORKSPACE_NAME"]
            missing = [var for var in required_vars if not os.getenv(var)]
            if missing:
                item.add_marker(
                    pytest.mark.skip(
                        reason=f"Missing Synapse configuration: {', '.join(missing)}. "
                        "Set SYNAPSE_WORKSPACE_NAME env var."
                    )
                )


@pytest.fixture(scope="session")
def workspace_root():
    """Get workspace root directory"""
    return Path(__file__).parent.parent.parent


@pytest.fixture(scope="session")
def packages_dir(workspace_root):
    """Get packages directory"""
    return workspace_root / "packages"


@pytest.fixture(scope="session", autouse=True)
def add_packages_to_path(packages_dir):
    """Add packages directory to Python path for imports"""
    packages_path = str(packages_dir)
    if packages_path not in sys.path:
        sys.path.insert(0, packages_path)

    kindling_sdk_path = str(packages_dir / "kindling_sdk")
    if kindling_sdk_path not in sys.path:
        sys.path.insert(0, kindling_sdk_path)

    kindling_cli_path = str(packages_dir / "kindling_cli")
    if kindling_cli_path not in sys.path:
        sys.path.insert(0, kindling_cli_path)
    yield
    # Cleanup
    if packages_path in sys.path:
        sys.path.remove(packages_path)
    if kindling_sdk_path in sys.path:
        sys.path.remove(kindling_sdk_path)
    if kindling_cli_path in sys.path:
        sys.path.remove(kindling_cli_path)


@pytest.fixture
def test_timeout():
    """Default timeout for system tests (in seconds)"""
    return int(os.getenv("TEST_TIMEOUT", "600"))  # 10 minutes default


@pytest.fixture
def poll_interval():
    """Default poll interval for status checks (in seconds)"""
    return int(os.getenv("POLL_INTERVAL", "10"))  # 10 seconds default


@pytest.fixture
def test_app_path():
    """Fixture providing path to universal test app from shared data-apps directory"""
    app_path = Path(__file__).parent.parent / "data-apps" / "universal-test-app"
    return app_path


@pytest.fixture
def app_packager():
    """Get app packager utility for preparing app files"""
    from kindling.job_deployment import AppPackager

    return AppPackager()


@pytest.fixture
def job_packager():
    """Backward compatibility alias for app_packager"""
    from kindling.job_deployment import AppPackager

    return AppPackager()


@pytest.fixture
def platform_client(platform):
    """Get platform API client for the parametrized platform

    Returns a tuple of (client, platform_name).
    Platform comes from test parametrization.
    """
    from tests.system.test_helpers import create_platform_client

    return create_platform_client(platform)


@pytest.fixture
def stdout_validator(platform_client):
    """Get stdout stream validator for the active platform

    Returns a StdoutStreamValidator configured for the platform API client.
    Use this to stream and validate stdout logs from Spark jobs.

    Example:
        validator = stdout_validator
        validator.stream_with_callback(job_id, run_id, print_lines=True)
        validator.assert_markers({"bootstrap": ["BOOTSTRAP SCRIPT"]})
    """
    from tests.system.test_helpers import create_stdout_validator

    client, platform_name = platform_client
    return create_stdout_validator(client)


def inject_platform_config(app_files: dict, platform_name: str, test_id: str = None) -> dict:
    """Return app files unchanged.

    System-test platform/runtime settings now travel via job_config
    `config_overrides` so they are available during BOOTSTRAP_CONFIG-driven
    bootstrap initialization. Application settings should remain application-
    specific.
    """
    return app_files
