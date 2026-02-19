"""
Pytest configuration and shared fixtures for Kindling tests.

This file provides common fixtures and configuration for all tests.
"""

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "packages"))
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "kindling_sdk"))
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "kindling_cli"))


# ═══════════════════════════════════════════════════════════════════════════
# Platform-Specific Test Markers Configuration
# ═══════════════════════════════════════════════════════════════════════════


def pytest_configure(config):
    """Register custom markers for platform-specific tests."""
    config.addinivalue_line(
        "markers",
        "synapse: mark test as Synapse-specific (system test that requires Azure Synapse Analytics)",
    )
    config.addinivalue_line(
        "markers",
        "databricks: mark test as Databricks-specific (system test that requires Databricks workspace)",
    )
    config.addinivalue_line(
        "markers",
        "fabric: mark test as Fabric-specific (system test that requires Microsoft Fabric)",
    )
    config.addinivalue_line(
        "markers",
        "standalone: mark test as standalone-specific (requires generic Spark or local environment)",
    )
    config.addinivalue_line(
        "markers", "system: mark test as system test (requires cloud infrastructure)"
    )
    config.addinivalue_line(
        "markers",
        "requires_azure: mark test as requiring Azure credentials (Azure SDK, storage, etc.)",
    )
    config.addinivalue_line("markers", "azure: mark test as requiring Azure cloud")
    config.addinivalue_line("markers", "aws: mark test as requiring AWS cloud")
    config.addinivalue_line("markers", "gcp: mark test as requiring Google Cloud")


def pytest_addoption(parser):
    """Add command line options for platform selection."""
    parser.addoption(
        "--platform",
        action="store",
        default=None,
        choices=["synapse", "databricks", "fabric", "standalone"],
        help="Run only tests for specified platform (synapse, databricks, fabric, or standalone)",
    )
    parser.addoption(
        "--skip-system",
        action="store_true",
        default=False,
        help="Skip all system tests (useful for CI/CD without cloud access)",
    )
    parser.addoption(
        "--require-platform-env",
        action="store_true",
        default=False,
        help="Skip tests if platform environment variables are not set",
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on command line options."""
    platform_filter = config.getoption("--platform")
    skip_system = config.getoption("--skip-system")
    require_platform_env = config.getoption("--require-platform-env")

    # Platform markers mapping
    platform_markers = {
        "synapse": "synapse",
        "databricks": "databricks",
        "fabric": "fabric",
        "standalone": "standalone",
    }

    for item in items:
        # Skip system tests if --skip-system is specified
        if skip_system and "system" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="--skip-system option specified"))

        # Filter by platform if --platform is specified
        if platform_filter:
            # Check if test has the specified platform marker
            has_platform_marker = platform_markers[platform_filter] in item.keywords

            # If test doesn't have the marker, skip it
            if not has_platform_marker:
                item.add_marker(
                    pytest.mark.skip(reason=f"Test not marked for platform: {platform_filter}")
                )

        # Check for required platform environment variables
        if require_platform_env:
            # Check Synapse tests
            if "synapse" in item.keywords:
                synapse_vars = [
                    "SYNAPSE_WORKSPACE_NAME",
                    "SYNAPSE_SPARK_POOL_NAME",
                    "AZURE_SUBSCRIPTION_ID",
                    "AZURE_RESOURCE_GROUP",
                ]
                missing = [v for v in synapse_vars if not os.getenv(v)]
                if missing:
                    item.add_marker(
                        pytest.mark.skip(reason=f"Missing Synapse environment variables: {missing}")
                    )

            # Check Databricks tests
            if "databricks" in item.keywords:
                databricks_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
                missing = [v for v in databricks_vars if not os.getenv(v)]
                if missing:
                    item.add_marker(
                        pytest.mark.skip(
                            reason=f"Missing Databricks environment variables: {missing}"
                        )
                    )

            # Check Fabric tests
            if "fabric" in item.keywords:
                fabric_vars = ["FABRIC_WORKSPACE_ID", "FABRIC_LAKEHOUSE_ID"]
                missing = [v for v in fabric_vars if not os.getenv(v)]
                if missing:
                    item.add_marker(
                        pytest.mark.skip(reason=f"Missing Fabric environment variables: {missing}")
                    )

        # Auto-add 'system' marker to tests that have platform markers
        if any(marker in item.keywords for marker in platform_markers.values()):
            if "system" not in item.keywords:
                item.add_marker(pytest.mark.system)


# ═══════════════════════════════════════════════════════════════════════════
# Shared Test Fixtures
# ═══════════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="session")
def spark_session():
    """
    Provide a Spark session for all tests in the session.
    This uses local mode Spark suitable for testing.
    """
    from tests.spark_test_helper import get_local_spark_session

    spark = get_local_spark_session("KindlingTests")
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def temp_workspace():
    """
    Provide a temporary workspace directory for each test.
    Automatically cleaned up after test completes.
    """
    temp_dir = Path(tempfile.mkdtemp(prefix="kindling_test_"))
    yield temp_dir

    # Cleanup
    if temp_dir.exists():
        shutil.rmtree(temp_dir)


@pytest.fixture
def mock_logger():
    """
    Provide a mock logger for testing.
    Simulates a Log4j logger interface.
    """
    from unittest.mock import Mock

    logger = Mock()
    logger.debug = Mock()
    logger.info = Mock()
    logger.warn = Mock()
    logger.error = Mock()
    logger.getEffectiveLevel = Mock()

    return logger


@pytest.fixture
def mock_spark():
    """
    Provide a mock Spark session for testing without JVM dependencies.
    """
    from unittest.mock import MagicMock, Mock

    spark = Mock()
    spark.sparkContext = Mock()
    spark.sparkContext.getLocalProperty = Mock(return_value=None)

    return spark


@pytest.fixture
def basic_config():
    """
    Provide a basic config object for testing.
    """
    from unittest.mock import Mock

    config = Mock()
    # Make get() return appropriate defaults based on key

    def config_get(key, default=None):
        defaults = {"log_level": "", "print_logging": False}
        return defaults.get(key, default)

    config.get = Mock(side_effect=config_get)

    return config


@pytest.fixture
def sample_dataframe(spark_session):
    """
    Provide a sample DataFrame for testing.
    """
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )

    data = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_notebook_files(temp_workspace):
    """
    Create sample notebook files in the temp workspace.
    Returns dict mapping notebook names to file paths.
    """
    notebooks = {}

    # Create a simple notebook
    notebook1_content = """
# Simple Notebook
print("Hello from notebook 1")
x = 42
"""
    notebook1_path = temp_workspace / "notebook1.py"
    notebook1_path.write_text(notebook1_content)
    notebooks["notebook1"] = notebook1_path

    # Create a notebook with cells
    notebook2_content = """
# COMMAND ----------
# Cell 1: Import libraries
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------
# Cell 2: Define function
def process_data(df):
    return df.select("*")

# COMMAND ----------
# MAGIC %md
# This is a markdown cell
# With multiple lines
"""
    notebook2_path = temp_workspace / "notebook2.py"
    notebook2_path.write_text(notebook2_content)
    notebooks["notebook2"] = notebook2_path

    # Create a notebook in a subfolder
    subfolder = temp_workspace / "subfolder"
    subfolder.mkdir()
    notebook3_content = """
# Notebook in subfolder
result = "success"
"""
    notebook3_path = subfolder / "notebook3.py"
    notebook3_path.write_text(notebook3_content)
    notebooks["notebook3"] = notebook3_path

    return notebooks


@pytest.fixture
def mock_emitter():
    """
    Provide a mock CustomEventEmitter for testing.
    """
    from unittest.mock import Mock

    emitter = Mock()
    emitter.emit_custom_event = Mock()

    return emitter


@pytest.fixture
def mock_logger_provider():
    """
    Provide a mock PythonLoggerProvider for testing.
    """
    from unittest.mock import Mock

    provider = Mock()
    mock_logger = Mock()
    provider.get_logger = Mock(return_value=mock_logger)

    return provider


@pytest.fixture
def mock_config():
    """
    Provide a mock ConfigService for testing.
    """
    from unittest.mock import Mock

    config = Mock()
    config.get = Mock(return_value=False)

    return config


@pytest.fixture
def mock_spark_for_trace():
    """
    Provide a comprehensive mock Spark session for trace testing with JVM components.
    """
    from unittest.mock import MagicMock, Mock

    spark = Mock()

    # Mock Spark context
    spark.sparkContext = Mock()
    spark.sparkContext.appName = "TestApp"
    spark.sparkContext.setLocalProperty = Mock()

    # Mock JVM components
    spark._jvm = Mock()

    # Mock Scala Option
    scala_option = Mock()
    scala_option.apply = Mock(side_effect=lambda x: x)
    scala_option.empty = Mock(return_value=None)
    spark._jvm.scala.Option = scala_option

    # Mock Java UUID
    java_uuid = Mock()
    spark._jvm.java.util.UUID.fromString = Mock(return_value=java_uuid)

    # Mock Log4j MDC
    mdc = Mock()
    mdc.put = Mock()
    mdc.remove = Mock()
    spark._jvm.org.apache.log4j.MDC = mdc

    # Mock SLF4J Level
    spark._jvm.org.slf4j.event.Level.INFO = Mock()

    # Mock ComponentSparkEvent
    spark._jvm.com.microsoft.spark.metricevents.ComponentSparkEvent = Mock(return_value=Mock())

    # Mock listener bus
    listener_bus = Mock()
    listener_bus.post = Mock()
    spark.sparkContext._jsc = Mock()
    spark.sparkContext._jsc.sc = Mock(return_value=Mock())
    spark.sparkContext._jsc.sc().listenerBus = Mock(return_value=listener_bus)

    return spark
