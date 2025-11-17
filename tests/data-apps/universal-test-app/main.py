#!/usr/bin/env python3
"""
Universal Platform Test App

Uses Kindling framework platform abstraction.
Works on Fabric, Synapse, and Databricks through platform services.
"""

import os
import sys
from datetime import datetime

from kindling.injection import get_kindling_service
from kindling.platform_provider import PlatformServiceProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import *


def get_test_id():
    """Extract test ID from bootstrap config (passed via command line args)"""
    try:
        config_service = get_kindling_service(ConfigService)
        # Bootstrap passes test_id via config:test_id=value command line arg
        test_id = config_service.get("test_id")
        if test_id:
            return str(test_id)
        # If not found, try checking if it's there under a different path
        test_id = config_service.get("kindling.test_id")
        if test_id:
            return str(test_id)
    except Exception as e:
        # Log the exception for debugging
        print(f"ERROR getting test_id from ConfigService: {e}")

    return "unknown"


def get_logger():
    """Get logger from Kindling framework via dependency injection"""
    logger_provider = get_kindling_service(SparkLoggerProvider)
    return logger_provider.get_logger("universal-test-app")


def get_platform_name():
    """Get platform name from platform service via dependency injection"""
    platform_provider = get_kindling_service(PlatformServiceProvider)
    platform_service = platform_provider.get_service()
    return platform_service.get_platform_name()


def test_spark_basic_operations(spark, logger, test_id):
    """Test basic Spark operations - platform agnostic"""
    try:
        # Create simple DataFrame
        data = [("test1", 1, "value1"), ("test2", 2, "value2"), ("test3", 3, "value3")]
        df = spark.createDataFrame(data, ["name", "id", "value"])

        # Perform operations
        count = df.count()
        if count != 3:
            raise ValueError(f"Expected 3 rows, got {count}")

        # Test aggregation
        max_id = df.agg({"id": "max"}).collect()[0][0]
        if max_id != 3:
            raise ValueError(f"Expected max ID 3, got {max_id}")

        return True

    except Exception as e:
        logger.error(f"TEST_ID={test_id} test=spark_operations status=FAILED error={e}")
        return False


def test_framework_available(logger, test_id):
    """Test if Kindling framework is available - platform agnostic"""
    try:
        import kindling

        # Try to import key components
        from kindling.platform_provider import PlatformServiceProvider
        from kindling.spark_log import SparkLogger

        return True

    except ImportError as e:
        logger.error(f"TEST_ID={test_id} test=framework_available status=FAILED error={e}")
        return False


def test_storage_access(logger, test_id):
    """Test storage access via platform abstraction"""
    try:
        # Try mssparkutils first (Fabric/Synapse)
        import __main__

        mssparkutils = getattr(__main__, "mssparkutils", None)

        if not mssparkutils:
            try:
                from notebookutils import mssparkutils
            except ImportError:
                # Try dbutils (Databricks) - access from __main__ globals
                dbutils = getattr(__main__, "dbutils", None)
                if dbutils:
                    files = dbutils.fs.ls("/")
                    return True
                else:
                    return False

        # Use mssparkutils
        files = mssparkutils.fs.ls("/")
        return True

    except Exception as e:
        logger.error(f"TEST_ID={test_id} test=storage_access status=FAILED error={e}")
        return False


# Execute test app directly (no main function - loaded by bootstrap)
# Bootstrap provides logger and framework in exec_globals
app_logger = get_logger()
platform_name = get_platform_name()
test_id = get_test_id()

# Log test start with ID (both logger and stdout for Databricks run_output capture)
msg = f"TEST_ID={test_id} status=STARTED platform={platform_name}"
app_logger.info(msg)
print(msg)

results = {}

# Test 1: Spark session
spark = get_or_create_spark_session()
results["spark_session"] = True
msg = f"TEST_ID={test_id} test=spark_session status=PASSED"
app_logger.info(msg)
print(msg)

# Test 2: Basic Spark operations
results["spark_operations"] = test_spark_basic_operations(spark, app_logger, test_id)
if results["spark_operations"]:
    msg = f"TEST_ID={test_id} test=spark_operations status=PASSED"
    app_logger.info(msg)
    print(msg)

# Test 3: Framework availability
results["framework_available"] = test_framework_available(app_logger, test_id)
if results["framework_available"]:
    msg = f"TEST_ID={test_id} test=framework_available status=PASSED"
    app_logger.info(msg)
    print(msg)

# Test 4: Storage access
results["storage_access"] = test_storage_access(app_logger, test_id)
if results["storage_access"]:
    msg = f"TEST_ID={test_id} test=storage_access status=PASSED"
    app_logger.info(msg)
    print(msg)

# Overall result
overall = all(results.values())
overall_status = "PASSED" if overall else "FAILED"
msg = f"TEST_ID={test_id} status=COMPLETED result={overall_status}"
app_logger.info(msg)
print(msg)

# Exit with appropriate code
sys.exit(0 if overall else 1)
