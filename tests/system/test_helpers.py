"""
Helper utilities for system tests.

Provides reusable patterns for:
- Real-time stdout streaming with validation
- Bootstrap marker detection
- Extension loading verification
- Log content analysis
"""

import os
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

DEFAULT_APP_INSIGHTS_CONNECTION_STRING = (
    "InstrumentationKey=00000000-0000-0000-0000-000000000000;"
    "IngestionEndpoint=https://westus-0.in.applicationinsights.azure.com/"
)


def _coerce_env_config_value(raw_value: str) -> Any:
    """Coerce CONFIG__ env values to primitive Python types when obvious."""
    lowered = raw_value.strip().lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return raw_value


def _deep_merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge dictionaries, with override values winning."""
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def _get_databricks_bootstrap_overrides(test_id: Optional[str]) -> Dict[str, Any]:
    from tests.system.conftest import (
        get_databricks_system_test_mode,
        get_databricks_uc_volume_root,
    )

    databricks_mode = get_databricks_system_test_mode()
    databricks_suffix = f"/{test_id}" if test_id else ""
    databricks_uc_root = get_databricks_uc_volume_root()
    databricks_classic_root = f"dbfs:/tmp/kindling_system_tests{databricks_suffix}".rstrip("/")

    if databricks_mode == "classic":
        return {
            "kindling": {
                "temp_path": databricks_classic_root,
                "delta": {"access_mode": "storage"},
                "storage": {
                    "table_root": f"{databricks_classic_root}/tables",
                    "checkpoint_root": f"{databricks_classic_root}/checkpoints",
                },
                "system_tests": {"databricks": {"mode": databricks_mode}},
            }
        }

    return {
        "kindling": {
            "temp_path": databricks_uc_root,
            "delta": {"access_mode": "catalog"},
            "storage": {
                "table_root": f"{databricks_uc_root}/tables",
                "checkpoint_root": f"{databricks_uc_root}/checkpoints",
            },
            "databricks": {
                "volume_staging_root": databricks_uc_root,
            },
            "system_tests": {"databricks": {"mode": databricks_mode}},
        }
    }


def get_system_platform_config_overrides(
    platform_name: str, test_id: Optional[str] = None
) -> Dict[str, Any]:
    """Return framework/runtime config that should travel with BOOTSTRAP_CONFIG.

    These are platform/runtime defaults used by the system-test harness. They
    should be passed through job config overrides so bootstrap can use them
    before application settings are loaded.
    """
    if platform_name == "databricks":
        return _get_databricks_bootstrap_overrides(test_id)

    if platform_name == "fabric":
        return {
            "kindling": {
                "storage": {
                    "table_root": "Tables",
                    "checkpoint_root": "Files/checkpoints",
                }
            }
        }

    if platform_name == "synapse":
        storage_account = (os.getenv("AZURE_STORAGE_ACCOUNT") or "").strip()
        container = (os.getenv("AZURE_CONTAINER") or "artifacts").strip()
        synapse_root = (
            f"abfss://{container}@{storage_account}.dfs.core.windows.net/kindling_system_tests"
            if storage_account
            else None
        )
        synapse_schema = (os.getenv("KINDLING_SYSTEM_TEST_SYNAPSE_SCHEMA") or "").strip() or None
        effective_schema = synapse_schema or "kindling_system_tests"

        overrides: Dict[str, Any] = {
            "kindling": {
                "delta": {"access_mode": "catalog"},
                "storage": {
                    "table_root": f"{synapse_root}/tables" if synapse_root else "tables",
                    "checkpoint_root": (
                        f"{synapse_root}/checkpoints" if synapse_root else "checkpoints"
                    ),
                    "table_schema": effective_schema,
                },
            }
        }
        if synapse_root:
            overrides["kindling"]["storage"][
                "table_schema_location"
            ] = f"{synapse_root}/schemas/{effective_schema}"
        return overrides

    return {}


def get_env_config_overrides(platform_name: str) -> Dict[str, Any]:
    """
    Parse CONFIG__ environment variables into nested config overrides.

    Supports:
    - CONFIG__kindling__temp_path=/...                      (global)
    - CONFIG__platform_databricks__kindling__temp_path=/... (platform-specific)

    Platform-specific entries are only applied when the platform prefix matches.
    """
    platform_prefix = f"platform_{platform_name}"
    overrides: Dict[str, Any] = {}
    global_entries: List[tuple[List[str], Any]] = []
    platform_entries: List[tuple[List[str], Any]] = []

    for env_key, raw_value in os.environ.items():
        if not env_key.startswith("CONFIG__"):
            continue

        path = env_key[len("CONFIG__") :]
        parts = [part for part in path.split("__") if part]
        if not parts:
            continue

        value = _coerce_env_config_value(raw_value)
        first = parts[0]
        if first.startswith("platform_"):
            if first != platform_prefix:
                continue
            scoped_parts = parts[1:]
            if scoped_parts:
                platform_entries.append((scoped_parts, value))
            continue

        global_entries.append((parts, value))

    # Apply global entries first, then matching platform-specific entries
    # so platform overrides are deterministic regardless os.environ iteration order.
    for parts, value in global_entries + platform_entries:
        current = overrides
        for part in parts[:-1]:
            existing = current.get(part)
            if not isinstance(existing, dict):
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

    return overrides


def _remove_nested_key(config: Dict[str, Any], path: List[str]) -> None:
    current = config
    parents: List[tuple[Dict[str, Any], str]] = []
    for part in path[:-1]:
        next_value = current.get(part)
        if not isinstance(next_value, dict):
            return
        parents.append((current, part))
        current = next_value

    current.pop(path[-1], None)

    for parent, key in reversed(parents):
        child = parent.get(key)
        if isinstance(child, dict) and not child:
            parent.pop(key, None)


def _sanitize_platform_env_overrides(
    platform_name: str,
    overrides: Dict[str, Any],
    platform_overrides: Dict[str, Any],
) -> Dict[str, Any]:
    """Drop env overrides that would break a platform/system-test mode contract."""
    cleaned = dict(overrides)

    if platform_name != "databricks":
        return cleaned

    kindling_overrides = platform_overrides.get("kindling") or {}
    databricks_mode = (
        (((kindling_overrides.get("system_tests") or {}).get("databricks")) or {}).get("mode") or ""
    ).strip()
    if databricks_mode != "classic":
        return cleaned

    for path in [
        ["kindling", "databricks", "volume_staging_root"],
        ["kindling", "storage", "logs_root"],
        ["kindling", "storage", "packages_root"],
    ]:
        _remove_nested_key(cleaned, path)

    return cleaned


def _get_repo_version() -> Optional[str]:
    """Return the version from pyproject.toml for pinning remote installs in system tests."""
    try:
        repo_root = Path(__file__).resolve().parents[2]
        pyproject_path = repo_root / "pyproject.toml"
        content = pyproject_path.read_text(encoding="utf-8")
        match = re.search(r'^version\s*=\s*"([^"]+)"\s*$', content, re.MULTILINE)
        if not match:
            return None
        return match.group(1)
    except Exception:
        return None


def _get_otel_extension_version() -> Optional[str]:
    """Return the version from the kindling-otel-azure package for deterministic installs."""
    try:
        repo_root = Path(__file__).resolve().parents[2]
        pyproject_path = repo_root / "packages" / "kindling_otel_azure" / "pyproject.toml"
        content = pyproject_path.read_text(encoding="utf-8")
        match = re.search(r'^version\s*=\s*"([^"]+)"\s*$', content, re.MULTILINE)
        if not match:
            return None
        return match.group(1)
    except Exception:
        return None


def _extension_package_name(spec: str) -> str:
    """Extract the normalized package name from a requirement spec."""
    return spec.split(">")[0].split("=")[0].split("<")[0].split("!")[0].strip()


def _has_version_spec(spec: str) -> bool:
    """Return True when a requirement string contains an explicit version operator."""
    return any(op in spec for op in [">=", "<=", "==", "!=", ">", "<", "~="])


def _merge_extension_specs(existing: Any, injected: List[str]) -> List[str]:
    """Merge extension specs, preferring explicitly versioned requirements."""
    merged: Dict[str, str] = {}

    normalized_existing: List[str]
    if not existing:
        normalized_existing = []
    elif isinstance(existing, str):
        normalized_existing = [existing]
    else:
        normalized_existing = [str(item) for item in existing]

    for spec in normalized_existing + [str(item) for item in injected if str(item).strip()]:
        pkg_name = _extension_package_name(spec)
        if not pkg_name:
            continue

        if pkg_name in merged:
            if _has_version_spec(spec) and not _has_version_spec(merged[pkg_name]):
                merged[pkg_name] = spec
            continue

        merged[pkg_name] = spec

    return list(merged.values())


def _get_system_test_azure_monitor_defaults(
    platform_name: str, enable_system_test_azure_monitor: bool
) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Return OTEL defaults for system-test runs that require Azure Monitor telemetry."""
    requires_azure_monitor = platform_name == "databricks" or enable_system_test_azure_monitor
    if not requires_azure_monitor:
        return None, None

    connection_string = (
        os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING") or DEFAULT_APP_INSIGHTS_CONNECTION_STRING
    ).strip()
    extension_version = _get_otel_extension_version()
    extension_spec = (
        f"kindling-otel-azure=={extension_version}" if extension_version else "kindling-otel-azure"
    )

    return (
        {
            "kindling": {
                "telemetry": {
                    "azure_monitor": {
                        "enable_logging": True,
                        "enable_tracing": True,
                        "connection_string": connection_string,
                    }
                }
            }
        },
        extension_spec,
    )


def apply_env_config_overrides(job_config: Dict[str, Any], platform_name: str) -> Dict[str, Any]:
    """Merge CONFIG__ env overrides into a job_config payload for create_job()."""
    merged_config = dict(job_config)
    enable_system_test_azure_monitor = bool(
        merged_config.pop("enable_system_test_azure_monitor", False)
    )
    platform_overrides = get_system_platform_config_overrides(
        platform_name, merged_config.get("test_id")
    )
    env_overrides = _sanitize_platform_env_overrides(
        platform_name,
        get_env_config_overrides(platform_name),
        platform_overrides,
    )
    existing_overrides = merged_config.get("config_overrides") or {}
    azure_monitor_defaults, azure_monitor_extension_spec = _get_system_test_azure_monitor_defaults(
        platform_name, enable_system_test_azure_monitor
    )

    combined_overrides: Dict[str, Any] = {}
    # Mode/platform-owned bootstrap defaults must win over ambient CONFIG__ env values
    # so the system-test lane (for example Databricks classic vs uc) stays deterministic.
    # Explicit job_config overrides still win last, except for Azure Monitor defaults
    # that Databricks system tests require for observability.
    for override_source in [env_overrides, platform_overrides, existing_overrides]:
        if override_source:
            combined_overrides = _deep_merge_dict(combined_overrides, override_source)

    if azure_monitor_defaults:
        combined_overrides = _deep_merge_dict(combined_overrides, azure_monitor_defaults)
        kindling_overrides = combined_overrides.setdefault("kindling", {})
        kindling_overrides["extensions"] = _merge_extension_specs(
            kindling_overrides.get("extensions"),
            [azure_monitor_extension_spec] if azure_monitor_extension_spec else [],
        )

    if combined_overrides:
        merged_config["config_overrides"] = combined_overrides

    # Pin the remote runtime install to the local repo version by default.
    # This avoids "latest" selecting a stable release that sorts higher than our alpha bump,
    # and makes system tests deterministic after bump+deploy.
    overrides = merged_config.get("config_overrides") or {}
    if "kindling_version" not in overrides:
        repo_version = _get_repo_version()
        if repo_version:
            overrides = dict(overrides)
            overrides["kindling_version"] = repo_version
            merged_config["config_overrides"] = overrides

    # System tests should be deterministic: Synapse/Databricks pools can retain installed
    # packages across runs. The runtime bootstrap defaults to "install if missing", so
    # without this a job can silently run with an older kindling already present.
    overrides = merged_config.get("config_overrides") or {}
    if "force_reinstall" not in overrides:
        overrides = dict(overrides)
        overrides["force_reinstall"] = True
        merged_config["config_overrides"] = overrides

    # Synapse stdout is retrieved via the sparkhistory driverlog endpoint; diagnostic emitters
    # often require storage access configuration and are not needed by default.
    if platform_name == "synapse" and "configure_diagnostic_emitters" not in merged_config:
        merged_config["configure_diagnostic_emitters"] = False

    return merged_config


class StdoutStreamValidator:
    """Helper for streaming and validating stdout logs from Spark jobs"""

    def __init__(self, api_client: Any):
        """
        Initialize stdout validator.

        Args:
            api_client: Platform API client (FabricAPI, SynapseAPI, or DatabricksAPI)
        """
        self.api_client = api_client
        self.captured_lines: List[str] = []

    def stream_with_callback(
        self,
        job_id: str,
        run_id: str,
        print_lines: bool = True,
        poll_interval: float = 5.0,
        max_wait: float = 300.0,
    ) -> List[str]:
        """
        Stream stdout logs in real-time with optional printing.

        Args:
            job_id: Spark job definition ID
            run_id: Job run/instance ID
            print_lines: Whether to print lines as they arrive (default: True)
            poll_interval: Seconds between polls (default: 5.0)
            max_wait: Maximum seconds to wait (default: 300.0)

        Returns:
            List of captured stdout lines
        """
        self.captured_lines = []

        def callback(line: str):
            """Callback that stores and optionally prints lines"""
            self.captured_lines.append(line)
            if print_lines:
                print(f"  {line}")
                sys.stdout.flush()

        # Use platform API's stdout streaming
        all_lines = self.api_client.stream_stdout_logs(
            job_id=job_id,
            run_id=run_id,
            callback=callback,
            poll_interval=poll_interval,
            max_wait=max_wait,
        )

        return all_lines

    def get_content(self) -> str:
        """Get full captured stdout as string"""
        return "\n".join(self.captured_lines)

    def check_markers(
        self,
        markers: Dict[str, List[str]],
        case_sensitive: bool = False,
    ) -> Dict[str, bool]:
        """
        Check for presence of marker strings in captured stdout.

        Args:
            markers: Dict mapping marker names to list of strings to search for
                     Example: {"bootstrap": ["BOOTSTRAP", "bootstrap.py"],
                              "framework": ["initialize_framework"]}
            case_sensitive: Whether search should be case-sensitive (default: False)

        Returns:
            Dict mapping marker names to boolean (True if any search string found)
        """
        content = self.get_content()
        if not case_sensitive:
            content = content.upper()

        results = {}
        for marker_name, search_strings in markers.items():
            found = False
            for search_str in search_strings:
                check_str = search_str if case_sensitive else search_str.upper()
                if check_str in content:
                    found = True
                    break
            results[marker_name] = found

        return results

    def assert_markers(
        self,
        markers: Dict[str, List[str]],
        case_sensitive: bool = False,
    ) -> None:
        """
        Assert that all markers are present in captured stdout.

        Args:
            markers: Dict mapping marker names to list of strings to search for
            case_sensitive: Whether search should be case-sensitive (default: False)

        Raises:
            AssertionError: If any marker is not found
        """
        results = self.check_markers(markers, case_sensitive)
        missing = [name for name, found in results.items() if not found]

        if missing:
            raise AssertionError(
                f"Missing markers in stdout: {', '.join(missing)}. " f"Expected to find: {markers}"
            )

    def validate_bootstrap_execution(self) -> Dict[str, bool]:
        """
        Validate standard bootstrap execution markers.

        Returns:
            Dict with validation results:
            - bootstrap_start: Bootstrap script started
            - framework_init: Framework initialization executed
            - config_loaded: Configuration loaded
            - no_bootstrap_errors: No bootstrap or framework errors detected
        """
        markers = {
            "bootstrap_start": ["BOOTSTRAP SCRIPT STARTING", "BOOTSTRAP FUNCTION CALLED"],
            "framework_init": ["initialize_framework", "Framework initialization"],
            "config_loaded": ["Configuration loaded", "Initializing DynaconfConfig"],
        }

        results = self.check_markers(markers)

        # Check for critical errors
        content = self.get_content()
        has_errors = any(
            [
                "ERROR: Bootstrap failed" in content,
                "ERROR: (KindlingBootstrap) Framework initialization failed" in content,
                "ImportError: Platform module" in content,
                "cannot import name" in content and "azure" in content.lower(),
            ]
        )
        results["no_bootstrap_errors"] = not has_errors

        return results

    def validate_extension_loading(self, extension_name: str) -> Dict[str, bool]:
        """
        Validate extension loading markers.

        Args:
            extension_name: Name of extension (e.g., "kindling-otel-azure")

        Returns:
            Dict with validation results:
            - extension_install: Extension installation attempted
            - extension_success: Extension loaded successfully
        """
        markers = {
            "extension_install": [
                f"Installing extension",
                extension_name,
                "Loading extension",
            ],
            "extension_success": [
                "Successfully installed extension",
                f"Successfully installed {extension_name}",
            ],
        }

        return self.check_markers(markers)

    def validate_test_app_execution(self, test_id: Optional[str] = None) -> Dict[str, bool]:
        """
        Validate test app execution markers.

        Args:
            test_id: Optional test ID to verify (validates ID appears in logs)

        Returns:
            Dict with validation results:
            - test_markers: Test execution markers found (TEST_ID=)
            - test_id_match: Specific test ID found (if test_id provided)
        """
        results = {}

        # Check for general test markers
        content = self.get_content()
        results["test_markers"] = "TEST_ID=" in content

        # Check for specific test ID if provided
        if test_id:
            results["test_id_match"] = f"TEST_ID={test_id}" in content

        return results

    def validate_tests(
        self,
        test_id: str,
        expected_tests: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Validate that expected test markers appear in captured stdout.

        Looks for markers of the form:
            TEST_ID={test_id} test={test_name} status=PASSED

        Args:
            test_id: The test ID to search for
            expected_tests: List of test names expected to pass

        Returns:
            Dict mapping test names to {passed: bool, status: str, message: str}
        """
        content = self.get_content()
        results = {}

        for test_name in expected_tests:
            passed_marker = f"TEST_ID={test_id} test={test_name} status=PASSED"
            failed_marker = f"TEST_ID={test_id} test={test_name} status=FAILED"
            summary_passed_marker = f"{test_name}: PASSED"
            summary_failed_marker = f"{test_name}: FAILED"

            if passed_marker in content or summary_passed_marker in content:
                results[test_name] = {
                    "passed": True,
                    "status": "PASSED",
                    "message": None,
                }
            elif failed_marker in content or summary_failed_marker in content:
                results[test_name] = {
                    "passed": False,
                    "status": "FAILED",
                    "message": f"Test {test_name} failed",
                }
            else:
                results[test_name] = {
                    "passed": False,
                    "status": "NOT_FOUND",
                    "message": f"No marker found for test {test_name}",
                }

        return results

    def validate_completion(
        self,
        test_id: str,
    ) -> Dict[str, Any]:
        """
        Validate that the test run completed successfully.

        Looks for the completion marker:
            TEST_ID={test_id} status=COMPLETED result=PASSED

        Args:
            test_id: The test ID to search for

        Returns:
            Dict with {passed: bool, status: str, message: str}
        """
        content = self.get_content()

        passed_marker = f"TEST_ID={test_id} status=COMPLETED result=PASSED"
        failed_marker = f"TEST_ID={test_id} status=COMPLETED result=FAILED"

        if passed_marker in content:
            return {"passed": True, "status": "COMPLETED", "message": "All tests passed"}
        elif failed_marker in content:
            return {
                "passed": False,
                "status": "FAILED",
                "message": "Test run completed with failures",
            }
        else:
            return {
                "passed": False,
                "status": "NOT_FOUND",
                "message": "Completion marker not found in stdout",
            }

    def print_validation_summary(
        self,
        validation_results: Dict[str, bool],
        title: str = "Validation Results",
    ) -> None:
        """
        Print a formatted validation summary.

        Args:
            validation_results: Dict mapping check names to boolean results
            title: Title for the summary section
        """
        print(f"\n📊 {title}:")
        for check_name, passed in validation_results.items():
            status = "✅" if passed else "❌"
            friendly_name = check_name.replace("_", " ").title()
            print(f"   {status} {friendly_name}")
        sys.stdout.flush()


def create_stdout_validator(api_client: Any) -> StdoutStreamValidator:
    """
    Factory function to create stdout validator.

    Args:
        api_client: Platform API client (FabricAPI, SynapseAPI, or DatabricksAPI)

    Returns:
        StdoutStreamValidator instance
    """
    return StdoutStreamValidator(api_client)


def get_captured_stdout(validator: StdoutStreamValidator) -> List[str]:
    """Get captured stdout lines from validator"""
    return validator.captured_lines


def create_platform_client(platform: str):
    """
    Create platform API client for testing.

    Uses kindling_sdk platform API factory to create clients from environment variables.
    Wraps ValueError exceptions as pytest.skip for missing configuration.

    Args:
        platform: Platform name ("fabric", "synapse", or "databricks")

    Returns:
        Tuple of (client, platform_name)
    """
    import pytest
    from kindling_sdk.platform_api import create_platform_api_from_env

    try:
        client, platform_name = create_platform_api_from_env(platform)

        original_create_job = client.create_job

        def create_job_with_env_overrides(*args, **kwargs):
            if "job_config" in kwargs:
                kwargs["job_config"] = apply_env_config_overrides(
                    kwargs["job_config"], platform_name
                )
                return original_create_job(*args, **kwargs)

            if len(args) >= 2:
                args_list = list(args)
                args_list[1] = apply_env_config_overrides(args_list[1], platform_name)
                return original_create_job(*args_list, **kwargs)

            return original_create_job(*args, **kwargs)

        client.create_job = create_job_with_env_overrides
        return client, platform_name
    except ValueError as e:
        pytest.skip(str(e))
