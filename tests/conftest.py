"""
Pytest configuration and shared fixtures for Kindling tests.

This file provides common fixtures and configuration for all tests.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


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
    from unittest.mock import Mock, MagicMock

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
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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
    from unittest.mock import Mock, MagicMock

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
