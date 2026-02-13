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
    """Inject platform-specific config into app's settings.yaml before deployment.

    Args:
        app_files: Dict of {filename: content} from AppPackager
        platform_name: Platform name (fabric, databricks, synapse)
        test_id: Optional test ID for unique paths

    Returns:
        Modified app_files dict with platform config merged into settings.yaml
    """
    import yaml

    # Platform-specific config
    platform_config = {
        "fabric": {
            "kindling": {
                "storage": {
                    "table_root": "Tables",
                    "checkpoint_root": "Files/checkpoints",
                }
            }
        },
        "databricks": {
            "kindling": {
                "storage": {
                    "table_root": "/tmp",
                    "checkpoint_root": "/tmp/checkpoints",
                }
            }
        },
        "synapse": {
            "kindling": {
                "storage": {
                    "table_root": "Tables",
                    "checkpoint_root": "Files/checkpoints",
                }
            }
        },
    }

    config_to_merge = platform_config.get(platform_name, {})

    # Load existing settings.yaml if present
    existing_config = {}
    if "settings.yaml" in app_files:
        existing_config = yaml.safe_load(app_files["settings.yaml"]) or {}

    # Deep merge (config_to_merge overrides existing)
    def deep_merge(base, override):
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    merged_config = deep_merge(existing_config, config_to_merge)

    # Update app_files with merged config
    app_files["settings.yaml"] = yaml.dump(merged_config, default_flow_style=False, sort_keys=False)

    return app_files
