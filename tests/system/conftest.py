"""
Pytest configuration for system tests.

Configures test markers, fixtures, and settings for system-level integration tests.
"""

import os
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Optional

import pytest

# All supported platforms - single source of truth
ALL_PLATFORMS = ["fabric", "databricks", "synapse"]
ALL_CLOUDS = ["azure", "aws", "gcp"]
_KINDLING_SYSTEM_CONFIG = None
_KINDLING_SYSTEM_HEARTBEAT_STOP = None
_KINDLING_SYSTEM_HEARTBEAT_THREAD = None
_KINDLING_SYSTEM_HEARTBEAT_PATH = None


def _start_xdist_heartbeat_forwarder(config) -> None:
    """Forward worker heartbeat lines from a shared file to the controller terminal."""
    global _KINDLING_SYSTEM_HEARTBEAT_STOP, _KINDLING_SYSTEM_HEARTBEAT_THREAD

    terminal_reporter = config.pluginmanager.get_plugin("terminalreporter")
    heartbeat_path = _KINDLING_SYSTEM_HEARTBEAT_PATH
    if terminal_reporter is None or not heartbeat_path:
        return

    stop_event = threading.Event()

    def _forward() -> None:
        offset = 0
        while not stop_event.is_set():
            try:
                if os.path.exists(heartbeat_path):
                    with open(heartbeat_path, encoding="utf-8") as heartbeat_file:
                        heartbeat_file.seek(offset)
                        for line in heartbeat_file:
                            line = line.rstrip()
                            if line:
                                terminal_reporter.write_line(line)
                        offset = heartbeat_file.tell()
            except FileNotFoundError:
                pass

            stop_event.wait(1.0)

    _KINDLING_SYSTEM_HEARTBEAT_STOP = stop_event
    _KINDLING_SYSTEM_HEARTBEAT_THREAD = threading.Thread(
        target=_forward,
        name="kindling-system-heartbeat-forwarder",
        daemon=True,
    )
    _KINDLING_SYSTEM_HEARTBEAT_THREAD.start()


def _stop_xdist_heartbeat_forwarder() -> None:
    """Stop the shared heartbeat forwarder thread."""
    global _KINDLING_SYSTEM_HEARTBEAT_STOP, _KINDLING_SYSTEM_HEARTBEAT_THREAD

    if _KINDLING_SYSTEM_HEARTBEAT_STOP is not None:
        _KINDLING_SYSTEM_HEARTBEAT_STOP.set()
    if _KINDLING_SYSTEM_HEARTBEAT_THREAD is not None:
        _KINDLING_SYSTEM_HEARTBEAT_THREAD.join(timeout=2.0)

    _KINDLING_SYSTEM_HEARTBEAT_STOP = None
    _KINDLING_SYSTEM_HEARTBEAT_THREAD = None


def _should_count_report_as_completed(report) -> bool:
    """Return True when a pytest report represents a completed test outcome."""
    if report.when == "call":
        return True

    # Tests skipped or failed during setup never reach call, but they still
    # represent completed outcomes from the runner's perspective.
    if report.when == "setup" and (report.failed or report.skipped):
        return True

    return False


def _classify_completed_report(report) -> str:
    """Classify a completed pytest report for progress display."""
    if report.skipped:
        return "skipped"
    if report.failed:
        return "failed"
    return "passed"


def _resolve_progress_total(config, completed: int) -> int:
    """Resolve the progress total, guarding against missing xdist controller totals."""
    total = getattr(config, "_kindling_system_total_items", 0) or 0
    if total <= 0:
        return completed
    return max(total, completed)


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
    global _KINDLING_SYSTEM_CONFIG, _KINDLING_SYSTEM_HEARTBEAT_PATH
    config.addinivalue_line("markers", "fabric: tests that require Microsoft Fabric platform")
    config.addinivalue_line("markers", "databricks: tests that require Databricks platform")
    config.addinivalue_line("markers", "synapse: tests that require Azure Synapse platform")
    config.addinivalue_line("markers", "azure: tests that require Azure cloud")
    config.addinivalue_line("markers", "aws: tests that require AWS cloud")
    config.addinivalue_line("markers", "gcp: tests that require Google Cloud")
    config.addinivalue_line("markers", "system: system-level integration tests")
    config.addinivalue_line("markers", "slow: tests that take significant time to run")
    config._kindling_system_total_items = 0
    config._kindling_system_completed_items = set()
    config._kindling_system_progress_counts = {"passed": 0, "skipped": 0, "failed": 0}
    _KINDLING_SYSTEM_CONFIG = config

    is_worker = hasattr(config, "workerinput")
    xdist_workers = getattr(getattr(config, "option", None), "numprocesses", 0) or 0
    if not is_worker and xdist_workers:
        heartbeat_file = tempfile.NamedTemporaryFile(
            prefix="kindling-system-heartbeats-",
            suffix=".log",
            delete=False,
        )
        heartbeat_file.close()
        _KINDLING_SYSTEM_HEARTBEAT_PATH = heartbeat_file.name
        os.environ["KINDLING_SYSTEM_HEARTBEAT_FILE"] = heartbeat_file.name
        _start_xdist_heartbeat_forwarder(config)


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

    config._kindling_system_total_items = len(items)


@pytest.hookimpl
def pytest_runtest_logreport(report):
    """Print a simple completed/total progress line for system tests."""
    config = getattr(report, "config", None) or _KINDLING_SYSTEM_CONFIG
    if config is None:
        return

    terminal_reporter = config.pluginmanager.get_plugin("terminalreporter")
    if terminal_reporter is None:
        return

    if not _should_count_report_as_completed(report):
        return

    completed_items = getattr(config, "_kindling_system_completed_items", None)
    if completed_items is None or report.nodeid in completed_items:
        return

    completed_items.add(report.nodeid)
    counts = config._kindling_system_progress_counts
    counts[_classify_completed_report(report)] += 1

    completed = len(completed_items)
    total = _resolve_progress_total(config, completed)
    terminal_reporter.write_line(
        "Progress: "
        f"{completed}/{total} completed "
        f"(passed={counts['passed']} skipped={counts['skipped']} failed={counts['failed']})"
    )


def pytest_unconfigure(config):
    """Clean up xdist heartbeat forwarding state."""
    global _KINDLING_SYSTEM_HEARTBEAT_PATH

    if not hasattr(config, "workerinput"):
        _stop_xdist_heartbeat_forwarder()
        heartbeat_path = _KINDLING_SYSTEM_HEARTBEAT_PATH or os.getenv(
            "KINDLING_SYSTEM_HEARTBEAT_FILE"
        )
        if heartbeat_path and os.path.exists(heartbeat_path):
            try:
                os.unlink(heartbeat_path)
            except OSError:
                pass
        os.environ.pop("KINDLING_SYSTEM_HEARTBEAT_FILE", None)
        _KINDLING_SYSTEM_HEARTBEAT_PATH = None


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
