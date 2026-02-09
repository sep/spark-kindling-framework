"""
Helper utilities for system tests.

Provides reusable patterns for:
- Real-time stdout streaming with validation
- Bootstrap marker detection
- Extension loading verification
- Log content analysis
"""

import sys
from typing import Any, Callable, Dict, List, Optional, Set


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

            if passed_marker in content:
                results[test_name] = {
                    "passed": True,
                    "status": "PASSED",
                    "message": None,
                }
            elif failed_marker in content:
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

    Uses kindling.platform_provider factory to create clients from environment variables.
    Wraps ValueError exceptions as pytest.skip for missing configuration.

    Args:
        platform: Platform name ("fabric", "synapse", or "databricks")

    Returns:
        Tuple of (client, platform_name)
    """
    import pytest
    from kindling.platform_provider import create_platform_api_from_env

    try:
        return create_platform_api_from_env(platform)
    except ValueError as e:
        pytest.skip(str(e))
