#!/usr/bin/env python3
"""
Azure Storage System Test App for Synapse Analytics

This app runs as a Spark job on Synapse to test Azure storage connectivity,
performance, and Kindling framework integration in the actual platform environment.

Entry point for the Kindling App Framework.
"""

import sys
import os
import json
import time
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

# Add the framework path (adjusted for Synapse environment)
sys.path.insert(0, "/opt/spark/work-dir")

try:
    # Import Kindling framework - adjust imports based on how framework is deployed
    from kindling.spark_session import get_spark_session
    from kindling.platform_provider import get_platform_service
    from kindling.spark_log_provider import get_logger
    from kindling.spark_trace import get_trace_provider
    from kindling.data_entities import EntityRegistry, Entity
    from kindling.data_pipes import DataPipesManager

    # Test-specific utilities
    from tests.spark_test_helper import _get_azure_cloud_config

except ImportError as e:
    print(f"❌ Failed to import Kindling framework: {e}")
    print("Framework may not be properly deployed to Synapse")
    sys.exit(1)


class SynapseSystemTestApp:
    """System test app that runs on Synapse Analytics"""

    def __init__(self):
        self.spark = None
        self.logger = None
        self.trace_provider = None
        self.platform_service = None
        self.test_results = {
            "test_name": "azure-storage-test",
            "platform": "synapse",
            "start_time": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "test_steps": [],
            "metrics": {},
            "errors": [],
        }

    def initialize(self) -> bool:
        """Initialize Synapse Spark session and Kindling services"""
        try:
            self.logger = get_logger(__name__)
            self.trace_provider = get_trace_provider()
            self.platform_service = get_platform_service()

            self.logger.info("🚀 Starting Azure Storage System Test on Synapse")

            # Get Spark session with Synapse optimizations
            self.spark = get_spark_session()

            self.logger.info(f"✓ Spark session initialized (version {self.spark.version})")
            self.logger.info(f"✓ Platform: {self.platform_service.get_platform_name()}")

            # Validate Synapse-specific configuration
            self._validate_synapse_environment()

            return True

        except Exception as e:
            error_msg = f"Failed to initialize Synapse environment: {str(e)}"
            self.logger.error(error_msg)
            self._add_error("initialization", error_msg, traceback.format_exc())
            return False

    def _validate_synapse_environment(self):
        """Validate that we're running in Synapse with correct configuration"""

        # Check for Synapse-specific environment variables
        required_env_vars = ["AZURE_STORAGE_ACCOUNT", "AZURE_CONTAINER", "SYNAPSE_WORKSPACE_NAME"]

        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

        # Validate Synapse Spark configuration
        spark_conf = self.spark.conf

        # Check if we have Synapse-specific configurations
        synapse_configs = [
            "spark.synapse.linkedService.useDefaultCredential",
            "spark.sql.adaptive.enabled",
        ]

        for config in synapse_configs:
            try:
                value = spark_conf.get(config)
                self.logger.info(f"Synapse config {config}: {value}")
            except Exception:
                self.logger.warning(f"Synapse config {config} not found")

    def run_azure_storage_tests(self) -> bool:
        """Run comprehensive Azure storage tests"""

        test_steps = [
            ("validate_configuration", self._test_validate_configuration),
            ("test_basic_connectivity", self._test_basic_connectivity),
            ("test_delta_operations", self._test_delta_operations),
            ("test_performance_write", self._test_performance_write),
            ("test_performance_read", self._test_performance_read),
            ("test_kindling_integration", self._test_kindling_integration),
            ("cleanup_test_data", self._cleanup_test_data),
        ]

        overall_success = True

        for step_name, test_func in test_steps:
            self.logger.info(f"🧪 Running test step: {step_name}")

            step_start = time.time()

            try:
                with self.trace_provider.span(
                    component="synapse-system-test",
                    operation=step_name,
                    details={"test_app": "azure-storage-test"},
                ):
                    success = test_func()

                step_duration = time.time() - step_start

                self.test_results["test_steps"].append(
                    {
                        "step": step_name,
                        "status": "passed" if success else "failed",
                        "duration": round(step_duration, 2),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )

                if success:
                    self.logger.info(f"✓ {step_name} completed successfully ({step_duration:.1f}s)")
                else:
                    self.logger.error(f"❌ {step_name} failed ({step_duration:.1f}s)")
                    overall_success = False

            except Exception as e:
                step_duration = time.time() - step_start
                error_msg = f"Exception in {step_name}: {str(e)}"

                self.logger.error(error_msg)
                self._add_error(step_name, error_msg, traceback.format_exc())

                self.test_results["test_steps"].append(
                    {
                        "step": step_name,
                        "status": "error",
                        "duration": round(step_duration, 2),
                        "error": error_msg,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )

                overall_success = False

        return overall_success

    def _test_validate_configuration(self) -> bool:
        """Validate Azure cloud and storage configuration"""
        try:
            # Get configuration from environment
            storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
            container = os.getenv("AZURE_CONTAINER")
            azure_cloud = os.getenv("AZURE_CLOUD", "AZURE_PUBLIC_CLOUD")

            # Get cloud-specific configuration
            cloud_config = _get_azure_cloud_config(azure_cloud)

            self.logger.info(f"Azure Cloud: {cloud_config['name']}")
            self.logger.info(f"Storage Account: {storage_account}")
            self.logger.info(f"Container: {container}")
            self.logger.info(f"DFS Endpoint: {cloud_config['dfs_suffix']}")

            # Store for other tests
            self.storage_account = storage_account
            self.container = container
            self.cloud_config = cloud_config

            return True

        except Exception as e:
            self.logger.error(f"Configuration validation failed: {str(e)}")
            return False

    def _test_basic_connectivity(self) -> bool:
        """Test basic read/write operations to Azure storage"""
        try:
            test_path = self._get_test_path("basic-connectivity")

            # Create test DataFrame
            test_df = self.spark.createDataFrame(
                [
                    ("synapse_test_1", "Hello from Synapse Analytics!", datetime.now()),
                    ("synapse_test_2", "Azure storage connectivity test", datetime.now()),
                    ("synapse_test_3", "Kindling framework system test", datetime.now()),
                ],
                ["id", "message", "timestamp"],
            )

            self.logger.info(f"Writing test data to: {test_path}")

            # Write test data
            write_start = time.time()
            test_df.write.mode("overwrite").parquet(test_path)
            write_duration = time.time() - write_start

            self.logger.info(f"✓ Write completed in {write_duration:.2f}s")

            # Read and validate
            read_start = time.time()
            read_df = self.spark.read.parquet(test_path)
            row_count = read_df.count()
            read_duration = time.time() - read_start

            self.logger.info(f"✓ Read completed in {read_duration:.2f}s, rows: {row_count}")

            # Store performance metrics
            self.test_results["metrics"].update(
                {
                    "basic_write_duration": write_duration,
                    "basic_read_duration": read_duration,
                    "basic_test_rows": row_count,
                }
            )

            return row_count == 3

        except Exception as e:
            self.logger.error(f"Basic connectivity test failed: {str(e)}")
            return False

    def _test_delta_operations(self) -> bool:
        """Test Delta Lake operations"""
        try:
            delta_path = self._get_test_path("delta-operations")

            # Initial Delta write
            initial_df = self.spark.createDataFrame(
                [(1, "initial_record", 100.0), (2, "another_record", 200.0)],
                ["id", "description", "value"],
            )

            self.logger.info(f"Creating Delta table at: {delta_path}")

            initial_df.write.format("delta").mode("overwrite").save(delta_path)

            # Read Delta table
            delta_df = self.spark.read.format("delta").load(delta_path)
            initial_count = delta_df.count()

            # Append more data
            append_df = self.spark.createDataFrame(
                [(3, "appended_record", 300.0)], ["id", "description", "value"]
            )

            append_df.write.format("delta").mode("append").save(delta_path)

            # Verify append
            final_df = self.spark.read.format("delta").load(delta_path)
            final_count = final_df.count()

            self.logger.info(f"✓ Delta operations: initial={initial_count}, final={final_count}")

            self.test_results["metrics"].update(
                {"delta_initial_rows": initial_count, "delta_final_rows": final_count}
            )

            return initial_count == 2 and final_count == 3

        except Exception as e:
            self.logger.error(f"Delta operations test failed: {str(e)}")
            return False

    def _test_performance_write(self) -> bool:
        """Test write performance with larger dataset"""
        try:
            perf_path = self._get_test_path("performance-write")

            # Generate larger test dataset
            row_count = int(os.getenv("TEST_DATA_ROWS", "10000"))
            partitions = int(os.getenv("TEST_PARTITIONS", "4"))

            self.logger.info(f"Generating {row_count} rows in {partitions} partitions")

            # Create performance test data
            perf_df = (
                self.spark.range(row_count)
                .selectExpr(
                    "id",
                    "concat('performance_test_', id) as description",
                    "rand() * 1000 as value",
                    "current_timestamp() as created_at",
                )
                .repartition(partitions)
            )

            # Measure write performance
            write_start = time.time()
            perf_df.write.mode("overwrite").parquet(perf_path)
            write_duration = time.time() - write_start

            # Calculate throughput (rough estimate)
            estimated_size_mb = (row_count * 100) / (1024 * 1024)  # Rough estimate
            throughput_mbps = estimated_size_mb / write_duration if write_duration > 0 else 0

            self.logger.info(f"✓ Performance write: {row_count} rows in {write_duration:.2f}s")
            self.logger.info(f"✓ Estimated throughput: {throughput_mbps:.2f} MB/s")

            self.test_results["metrics"].update(
                {
                    "perf_write_rows": row_count,
                    "perf_write_duration": write_duration,
                    "perf_write_throughput_mbps": throughput_mbps,
                }
            )

            # Check against minimum performance expectation
            min_throughput = float(os.getenv("MIN_WRITE_THROUGHPUT_MBPS", "15.0"))
            return throughput_mbps >= min_throughput

        except Exception as e:
            self.logger.error(f"Performance write test failed: {str(e)}")
            return False

    def _test_performance_read(self) -> bool:
        """Test read performance"""
        try:
            perf_path = self._get_test_path("performance-write")  # Read from write test

            # Measure read performance
            read_start = time.time()
            read_df = self.spark.read.parquet(perf_path)
            row_count = read_df.count()
            read_duration = time.time() - read_start

            # Calculate read throughput
            estimated_size_mb = (row_count * 100) / (1024 * 1024)
            read_throughput_mbps = estimated_size_mb / read_duration if read_duration > 0 else 0

            self.logger.info(f"✓ Performance read: {row_count} rows in {read_duration:.2f}s")
            self.logger.info(f"✓ Read throughput: {read_throughput_mbps:.2f} MB/s")

            self.test_results["metrics"].update(
                {
                    "perf_read_rows": row_count,
                    "perf_read_duration": read_duration,
                    "perf_read_throughput_mbps": read_throughput_mbps,
                }
            )

            # Check against minimum performance expectation
            min_read_throughput = float(os.getenv("MIN_READ_THROUGHPUT_MBPS", "60.0"))
            return read_throughput_mbps >= min_read_throughput

        except Exception as e:
            self.logger.error(f"Performance read test failed: {str(e)}")
            return False

    def _test_kindling_integration(self) -> bool:
        """Test Kindling framework integration with Synapse"""
        try:
            # Test entity registry
            entity_registry = EntityRegistry()

            test_entity = Entity(
                entity_id="system_test.synapse_validation",
                name="Synapse System Test Entity",
                description="Test entity for Synapse system validation",
                entity_type="test",
                table_format="delta",
            )

            entity_registry.register_entity(test_entity)

            # Test data pipes manager
            pipes_manager = DataPipesManager(self.logger)

            self.logger.info("✓ Kindling framework components initialized successfully")

            return True

        except Exception as e:
            self.logger.error(f"Kindling integration test failed: {str(e)}")
            return False

    def _cleanup_test_data(self) -> bool:
        """Clean up test data if configured to do so"""
        try:
            cleanup_enabled = os.getenv("CLEANUP_ON_SUCCESS", "true").lower() == "true"

            if not cleanup_enabled:
                self.logger.info("⏭️ Cleanup disabled, skipping")
                return True

            test_paths = ["basic-connectivity", "delta-operations", "performance-write"]

            for path_suffix in test_paths:
                test_path = self._get_test_path(path_suffix)
                try:
                    # Use Spark to remove the data
                    self.spark.sql(f"DROP TABLE IF EXISTS delta.`{test_path}`")
                    self.logger.info(f"✓ Cleaned up: {test_path}")
                except Exception as e:
                    self.logger.warning(f"Could not clean up {test_path}: {str(e)}")

            return True

        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")
            return False

    def _get_test_path(self, test_suffix: str) -> str:
        """Generate Azure storage path for test data"""
        test_prefix = os.getenv("TEST_PATH_PREFIX", "system-tests/synapse/azure-storage")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        return (
            f"abfss://{self.container}@{self.storage_account}."
            f"{self.cloud_config['dfs_suffix']}/{test_prefix}/{test_suffix}_{timestamp}"
        )

    def _add_error(self, step: str, message: str, traceback_str: str):
        """Add error to test results"""
        self.test_results["errors"].append(
            {
                "step": step,
                "message": message,
                "traceback": traceback_str,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

    def save_results(self) -> bool:
        """Save test results to Azure storage"""
        try:
            self.test_results["end_time"] = datetime.now(timezone.utc).isoformat()
            self.test_results["total_duration"] = (
                datetime.fromisoformat(self.test_results["end_time"].replace("Z", "+00:00"))
                - datetime.fromisoformat(self.test_results["start_time"].replace("Z", "+00:00"))
            ).total_seconds()

            # Save results as JSON
            results_path = self._get_results_path()
            results_json = json.dumps(self.test_results, indent=2)

            # Create DataFrame with results and save
            results_df = self.spark.createDataFrame([(results_json,)], ["results"])
            results_df.coalesce(1).write.mode("overwrite").text(results_path)

            self.logger.info(f"✓ Test results saved to: {results_path}")

            return True

        except Exception as e:
            self.logger.error(f"Failed to save test results: {str(e)}")
            return False

    def _get_results_path(self) -> str:
        """Generate path for saving test results"""
        results_prefix = os.getenv("RESULTS_PATH_PREFIX", "system-test-results/azure-storage-test")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        return (
            f"abfss://{self.container}@{self.storage_account}."
            f"{self.cloud_config['dfs_suffix']}/{results_prefix}/synapse_{timestamp}"
        )

    def run(self) -> int:
        """Main entry point for the system test app"""
        try:
            # Initialize
            if not self.initialize():
                self.test_results["status"] = "initialization_failed"
                return 1

            # Run tests
            self.logger.info("🧪 Starting Azure storage system tests...")
            success = self.run_azure_storage_tests()

            # Set final status
            self.test_results["status"] = "passed" if success else "failed"

            # Save results
            self.save_results()

            # Final status
            if success:
                self.logger.info("🎉 All system tests passed!")
                return 0
            else:
                self.logger.error("❌ Some system tests failed!")
                return 1

        except Exception as e:
            self.logger.error(f"System test app failed: {str(e)}")
            self.test_results["status"] = "error"
            self._add_error("app_execution", str(e), traceback.format_exc())

            # Try to save results even on failure
            try:
                self.save_results()
            except:
                pass

            return 1

        finally:
            if self.spark:
                self.spark.stop()


def main():
    """Entry point called by Kindling App Framework"""
    app = SynapseSystemTestApp()
    return app.run()


if __name__ == "__main__":
    sys.exit(main())
