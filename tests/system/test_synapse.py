"""
Synapse System Tests - pytest integration

Runs system tests on Azure Synapse Analytics as part of pytest test suite.
"""

import pytest
import os
import sys
from pathlib import Path
from typing import Dict, Any

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from tests.system.runners.synapse_runner import SynapseTestRunner
except ImportError:
    # Skip if Azure SDK not available
    pytestmark = pytest.mark.skip("Azure SDK not available for Synapse system tests")


class TestSynapseSystemTests:
    """System tests that run on Azure Synapse Analytics"""

    @pytest.fixture(scope="class")
    def synapse_config(self) -> Dict[str, str]:
        """Get Synapse configuration from environment"""
        config = {
            "workspace_name": os.getenv("SYNAPSE_WORKSPACE_NAME"),
            "spark_pool_name": os.getenv("SYNAPSE_SPARK_POOL_NAME"),
            "subscription_id": os.getenv("AZURE_SUBSCRIPTION_ID"),
            "resource_group": os.getenv("AZURE_RESOURCE_GROUP"),
        }

        # Validate required configuration
        missing = [k for k, v in config.items() if not v]
        if missing:
            pytest.skip(f"Missing Synapse configuration: {missing}")

        return config

    @pytest.fixture(scope="class")
    def synapse_runner(self, synapse_config) -> SynapseTestRunner:
        """Create Synapse test runner"""
        return SynapseTestRunner(
            workspace_name=synapse_config["workspace_name"],
            spark_pool_name=synapse_config["spark_pool_name"],
            subscription_id=synapse_config["subscription_id"],
            resource_group=synapse_config["resource_group"],
        )

    @pytest.fixture
    def azure_storage_app_dir(self) -> str:
        """Path to Azure storage test app"""
        return str(Path(__file__).parent / "apps" / "azure-storage-test")

    def test_azure_storage_connectivity(
        self, synapse_runner: SynapseTestRunner, azure_storage_app_dir: str
    ):
        """Test Azure storage connectivity on Synapse"""

        # Run the system test
        result = synapse_runner.run_system_test(
            app_name="azure-storage-test",
            app_directory=azure_storage_app_dir,
            environment_vars={
                "TEST_DATA_ROWS": "5000",  # Smaller dataset for pytest
                "CLEANUP_ON_SUCCESS": "true",
            },
        )

        # Validate results
        assert result["success"], f"System test failed: {result.get('error')}"

        # Validate test results if available
        if "results" in result and result["results"]:
            test_results = result["results"]

            assert test_results["status"] == "passed", f"Test app reported failure: {test_results}"

            # Validate performance metrics
            metrics = test_results.get("metrics", {})
            if "perf_write_throughput_mbps" in metrics:
                # Should meet minimum throughput requirements
                assert (
                    metrics["perf_write_throughput_mbps"] >= 10.0
                ), f"Write throughput too low: {metrics['perf_write_throughput_mbps']} MB/s"

            if "perf_read_throughput_mbps" in metrics:
                assert (
                    metrics["perf_read_throughput_mbps"] >= 30.0
                ), f"Read throughput too low: {metrics['perf_read_throughput_mbps']} MB/s"

            # Validate that all test steps passed
            test_steps = test_results.get("test_steps", [])
            failed_steps = [step for step in test_steps if step["status"] != "passed"]

            assert not failed_steps, f"Test steps failed: {failed_steps}"

    def test_synapse_platform_integration(
        self, synapse_runner: SynapseTestRunner, azure_storage_app_dir: str
    ):
        """Test Synapse platform-specific features"""

        # Run with Synapse-specific configuration
        result = synapse_runner.run_system_test(
            app_name="azure-storage-test",
            app_directory=azure_storage_app_dir,
            environment_vars={
                "TEST_DATA_ROWS": "10000",
                "TEST_PARTITIONS": "8",
                "CLEANUP_ON_SUCCESS": "true",
                # Test managed identity authentication
                "AUTH_TYPE": "managed_identity",
            },
        )

        assert result["success"], f"Synapse integration test failed: {result.get('error')}"

        # Validate Synapse-specific results
        if "results" in result and result["results"]:
            test_results = result["results"]
            assert test_results["platform"] == "synapse"
            assert test_results["status"] == "passed"


# Standalone test functions for individual execution
def test_synapse_azure_storage_standalone():
    """Standalone test that can be run independently"""

    # Check environment
    required_env = [
        "SYNAPSE_WORKSPACE_NAME",
        "SYNAPSE_SPARK_POOL_NAME",
        "AZURE_SUBSCRIPTION_ID",
        "AZURE_RESOURCE_GROUP",
        "AZURE_STORAGE_ACCOUNT",
        "AZURE_CONTAINER",
    ]

    missing = [env for env in required_env if not os.getenv(env)]
    if missing:
        pytest.skip(f"Missing environment variables: {missing}")

    # Create runner
    runner = SynapseTestRunner(
        workspace_name=os.getenv("SYNAPSE_WORKSPACE_NAME"),
        spark_pool_name=os.getenv("SYNAPSE_SPARK_POOL_NAME"),
        subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID"),
        resource_group=os.getenv("AZURE_RESOURCE_GROUP"),
    )

    # Run test
    app_dir = str(Path(__file__).parent / "apps" / "azure-storage-test")
    result = runner.run_system_test("azure-storage-test", app_dir)

    assert result["success"], f"Standalone system test failed: {result.get('error')}"


if __name__ == "__main__":
    """Run system tests directly"""
    pytest.main([__file__, "-v", "-s"])
