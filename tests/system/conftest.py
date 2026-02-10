"""
Pytest configuration for system tests.

Configures test markers, fixtures, and settings for system-level integration tests.
"""

import os
import sys
from pathlib import Path

import pytest

# All supported platforms - single source of truth
ALL_PLATFORMS = ["fabric", "databricks", "synapse"]


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
    config.addinivalue_line("markers", "system: system-level integration tests")
    config.addinivalue_line("markers", "slow: tests that take significant time to run")


def pytest_collection_modifyitems(config, items):
    """
    Modify test collection to handle platform-specific tests.

    Skip tests that require unavailable credentials.
    """
    for item in items:
        # Add platform marker dynamically from parametrize values
        if hasattr(item, "callspec") and "platform" in item.callspec.params:
            platform_value = item.callspec.params["platform"]
            # Add the platform as a marker so -m fabric/databricks/synapse works
            item.add_marker(getattr(pytest.mark, platform_value))

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
    yield
    # Cleanup
    if packages_path in sys.path:
        sys.path.remove(packages_path)


@pytest.fixture
def test_timeout():
    """Default timeout for system tests (in seconds)"""
    return int(os.getenv("TEST_TIMEOUT", "600"))  # 10 minutes default


@pytest.fixture
def poll_interval():
    """Default poll interval for status checks (in seconds)"""
    return int(os.getenv("POLL_INTERVAL", "10"))  # 10 seconds default


@pytest.fixture
def databricks_api():
    """Create Databricks API client for system tests (following Fabric/Synapse pattern)"""

    # Get configuration from environment
    workspace_url = os.getenv("DATABRICKS_HOST")
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    container = os.getenv("AZURE_CONTAINER", "artifacts")
    base_path = os.getenv("AZURE_BASE_PATH", "")

    if not workspace_url:
        pytest.skip("DATABRICKS_HOST not configured")

    # Import DatabricksAPI (late import to avoid issues if not available)
    try:
        from kindling.platform_databricks import DatabricksAPI
    except ImportError:
        pytest.skip("DatabricksAPI not available - check platform_databricks.py")

    # Check if service principal credentials are available
    has_sp_creds = all(
        [
            os.getenv("AZURE_CLIENT_ID"),
            os.getenv("AZURE_CLIENT_SECRET"),
            os.getenv("AZURE_TENANT_ID"),
        ]
    )

    if has_sp_creds:
        print("🔐 Using Azure Service Principal authentication for Databricks API")
        # Create API client with service principal authentication
        api = DatabricksAPI(
            workspace_url=workspace_url,
            storage_account=storage_account,
            container=container,
            base_path=base_path,
            azure_tenant_id=os.getenv("AZURE_TENANT_ID"),
            azure_client_id=os.getenv("AZURE_CLIENT_ID"),
            azure_client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        )
    else:
        print("🔐 Using Azure CLI authentication for Databricks API")
        # Create API client with Azure CLI authentication
        api = DatabricksAPI(
            workspace_url=workspace_url,
            storage_account=storage_account,
            container=container,
            base_path=base_path,
        )

    return api


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
